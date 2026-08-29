#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2014-2016 OpenMarket Ltd
# Copyright (C) 2023 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
# Originally licensed under the Apache License, Version 2.0:
# <http://www.apache.org/licenses/LICENSE-2.0>.
#
# [This file includes modifications made by New Vector Limited]
#
#

import hashlib
import logging
import struct
import time
from typing import (
    TYPE_CHECKING,
    Mapping,
)

from synapse.logging.opentracing import tag_args, trace
from synapse.storage._base import SQLBaseStore
from synapse.storage.database import (
    DatabasePool,
    LoggingDatabaseConnection,
    LoggingTransaction,
)
from synapse.storage.engines import PostgresEngine
from synapse.types import MutableStateMap, StateMap
from synapse.types.state import StateFilter
from synapse.util.caches import intern_string
from synapse.util.iterutils import batch_iter

if TYPE_CHECKING:
    from synapse.server import HomeServer

logger = logging.getLogger(__name__)


MAX_STATE_DELTA_HOPS = 100


def _state_hamt_node_tikv_key(
    namespace: str, room_prefix: bytes, structural_hash: bytes
) -> bytes:
    # Must match `node_tikv_key` in `rust/src/tikv_engine.rs`.
    namespace_hash = hashlib.sha256(namespace.encode("utf-8")).digest()[:16]
    return (
        b"hamt:node:"
        + namespace_hash.hex().encode("ascii")
        + b":"
        + room_prefix.hex().encode("ascii")
        + b":"
        + structural_hash.hex().encode("ascii")
    )


def _state_hamt_root_tikv_key(namespace: str, state_group: int) -> bytes:
    """Return the per-namespace HAMT root key.

    State-group ids are only unique inside one Synapse database, so the
    namespace is part of the key. The room prefix is stored in the
    value, allowing readers to locate a root from only its state-group id.
    """
    namespace_hash = hashlib.sha256(namespace.encode("utf-8")).digest()[:16]
    return (
        b"hamt:root:"
        + namespace_hash.hex().encode("ascii")
        + str(state_group).encode("ascii")
    )


def _encode_state_hamt_root(
    room_prefix: bytes,
    root_hash: bytes,
    lattice: bytes,
    room_id: str,
) -> bytes:
    room_id_bytes = room_id.encode("utf-8")
    return (
        b"\x01"
        + struct.pack(">H", len(room_prefix))
        + room_prefix
        + struct.pack(">H", len(room_id_bytes))
        + room_id_bytes
        + root_hash
        + lattice
    )


def _decode_state_hamt_root(
    value: bytes,
) -> tuple[bytes, bytes, bytes, str]:
    if len(value) < 5 or value[0] != 1:
        raise RuntimeError("invalid or unsupported HAMT root record version")
    prefix_len = struct.unpack(">H", value[1:3])[0]
    room_id_len_offset = 3 + prefix_len
    if len(value) < room_id_len_offset + 2:
        raise RuntimeError("truncated HAMT root record")
    room_id_len = struct.unpack(
        ">H", value[room_id_len_offset : room_id_len_offset + 2]
    )[0]
    room_id_start = room_id_len_offset + 2
    root_start = room_id_start + room_id_len
    if len(value) < root_start + 16:
        raise RuntimeError("truncated HAMT root record")
    room_prefix = value[3:room_id_len_offset]
    room_id = value[room_id_start:root_start].decode("utf-8")
    root_hash = value[root_start : root_start + 16]
    lattice = value[root_start + 16 :]
    return room_prefix, root_hash, lattice, room_id


def put_state_hamt_objects(
    namespace: str,
    room_prefix: bytes,
    nodes: list[tuple[bytes, bytes]],
    roots: list[tuple[bytes, bytes]],
    use_tikv: bool,
) -> tuple[float, float, float, float]:
    worker_started = time.monotonic()
    # Nodes and root pointers live in the same TiKV namespace. State-group ids
    # are only unique within one Synapse database, so namespace both key types
    # to prevent independent deployments from overwriting each other's state.
    # A linear event batch contains many adjacent snapshots and therefore many
    # repeated, unchanged node hashes. Collapse them before sending the batch to
    # TiKV rather than overwriting the same immutable object repeatedly.
    pairs = list(
        {
            _state_hamt_node_tikv_key(
                namespace, room_prefix, structural_hash
            ): node_bytes
            for structural_hash, node_bytes in nodes
        }.items()
    )

    nodes_elapsed_ms = 0.0
    roots_elapsed_ms = 0.0
    if use_tikv:
        from synapse.synapse_rust import tikv_engine

        # State nodes are read through TiKV's Raw KV API, so they must also be
        # written through Raw KV: transactional and raw data use distinct
        # keyspaces. Publish roots only after every immutable node has been
        # written; readers that observe the small remaining visibility window
        # use the bounded retry in `_get_state_groups_from_groups`.
        nodes_bytes = sum(len(key) + len(value) for key, value in pairs)
        nodes_start = time.monotonic()
        tikv_engine.batch_put(pairs)
        nodes_elapsed_ms = (time.monotonic() - nodes_start) * 1000
        logger.debug(
            "[gg-state-timing] state_hamt_nodes_batch_put "
            "count=%d bytes=%d elapsed_ms=%.1f",
            len(pairs),
            nodes_bytes,
            nodes_elapsed_ms,
        )

        roots_bytes = sum(len(key) + len(value) for key, value in roots)
        roots_start = time.monotonic()
        tikv_engine.batch_put(roots)
        roots_elapsed_ms = (time.monotonic() - roots_start) * 1000
        logger.debug(
            "[gg-state-timing] state_hamt_roots_batch_put "
            "count=%d bytes=%d elapsed_ms=%.1f",
            len(roots),
            roots_bytes,
            roots_elapsed_ms,
        )

    worker_finished = time.monotonic()
    return worker_started, worker_finished, nodes_elapsed_ms, roots_elapsed_ms


class StateGroupBackgroundUpdateStore(SQLBaseStore):
    """Defines functions related to state groups needed to run the state background
    updates.
    """

    def __init__(
        self,
        database: DatabasePool,
        db_conn: LoggingDatabaseConnection,
        hs: "HomeServer",
    ):
        super().__init__(database, db_conn, hs)
        self._tikv_namespace_override: str | None = None

    @property
    def tikv_namespace(self) -> str:
        if self._tikv_namespace_override is not None:
            return self._tikv_namespace_override
        db_pool_ns = getattr(self.db_pool, "_tikv_namespace", None)
        if db_pool_ns:
            return str(db_pool_ns)
        return self.hs.config.database.tikv_namespace or self.hs.hostname

    @tikv_namespace.setter
    def tikv_namespace(self, value: str) -> None:
        self._tikv_namespace_override = value
        if hasattr(self, "db_pool") and self.db_pool is not None:
            self.db_pool._tikv_namespace = value
        if hasattr(self.hs.config, "database"):
            self.hs.config.database.tikv_namespace = value

    def _state_hamt_secret(self) -> bytes:
        return hashlib.sha256(self.hs.config.key.macaroon_secret_key).digest()

    @trace
    @tag_args
    def _count_state_group_hops_txn(
        self, txn: LoggingTransaction, state_group: int
    ) -> int:
        """Given a state group, count how many hops there are in the tree.

        This is used to ensure the delta chains don't get too long.
        """
        if isinstance(self.database_engine, PostgresEngine):
            sql = """
                WITH RECURSIVE state(state_group) AS (
                    VALUES(?::bigint)
                    UNION ALL
                    SELECT prev_state_group FROM state_group_edges e, state s
                    WHERE s.state_group = e.state_group
                )
                SELECT count(*) FROM state;
            """

            txn.execute(sql, (state_group,))
            row = txn.fetchone()
            if row and row[0]:
                return row[0]
            else:
                return 0
        else:
            # We don't use WITH RECURSIVE on sqlite3 as there are distributions
            # that ship with an sqlite3 version that doesn't support it (e.g. wheezy)
            next_group: int | None = state_group
            count = 0

            while next_group:
                next_group = self.db_pool.simple_select_one_onecol_txn(
                    txn,
                    table="state_group_edges",
                    keyvalues={"state_group": next_group},
                    retcol="prev_state_group",
                    allow_none=True,
                )
                if next_group:
                    count += 1

            return count

    @trace
    @tag_args
    def _get_state_groups_from_groups_txn(
        self,
        txn: LoggingTransaction,
        groups: list[int],
        state_filter: StateFilter | None = None,
        use_tikv: bool | None = None,
    ) -> Mapping[int, StateMap[str]]:
        """
        Given a number of state groups, fetch the latest state for each group.

        Args:
            txn: The transaction object.
            groups: The given state groups that you want to fetch the latest state for.
            state_filter: The state filter to apply the state we fetch state from the database.
            use_tikv: Whether to query TiKV (defaults to self.tikv_pd_endpoints). Set False for pure SQL fallback.

        Returns:
            Map from state_group to a StateMap at that point.
        """
        if state_filter is None:
            state_filter = StateFilter.all()

        groups = list(groups)
        results: dict[int, MutableStateMap[str]] = {group: {} for group in groups}

        hamt_results, missing_groups = self._get_state_groups_from_hamt_txn(
            txn, groups, state_filter, use_tikv=use_tikv
        )
        results.update(hamt_results)

        if not missing_groups:
            return results

        groups = missing_groups

        logger.debug(
            "Falling back to legacy state-group reads for %s (state_groups_state is "
            "not populated on this branch; expect empty results)",
            groups,
        )

        if isinstance(self.database_engine, PostgresEngine):
            # Temporarily disable sequential scans in this transaction. This is
            # a temporary hack until we can add the right indices in
            txn.execute("SET LOCAL enable_seqscan=off")

            # The below query walks the state_group tree so that the "state"
            # table includes all state_groups in the tree. It then joins
            # against `state_groups_state` to fetch the latest state.
            # It assumes that previous state groups are always numerically
            # lesser.
            # This may return multiple rows per (type, state_key), but last_value
            # should be the same.
            sql = """
                WITH RECURSIVE sgs(state_group) AS (
                    VALUES(?::bigint)
                    UNION ALL
                    SELECT prev_state_group FROM state_group_edges e, sgs s
                    WHERE s.state_group = e.state_group
                )
                %s
            """

            overall_select_query_args: list[int | str] = []

            # This is an optimization to create a select clause per-condition. This
            # makes the query planner a lot smarter on what rows should pull out in the
            # first place and we end up with something that takes 10x less time to get a
            # result.
            use_condition_optimization = (
                not state_filter.include_others and not state_filter.is_full()
            )
            state_filter_condition_combos: list[tuple[str, str | None]] = []
            # We don't need to caclculate this list if we're not using the condition
            # optimization
            if use_condition_optimization:
                for etype, state_keys in state_filter.types.items():
                    if state_keys is None:
                        state_filter_condition_combos.append((etype, None))
                    else:
                        for state_key in state_keys:
                            state_filter_condition_combos.append((etype, state_key))
            # And here is the optimization itself. We don't want to do the optimization
            # if there are too many individual conditions. 10 is an arbitrary number
            # with no testing behind it but we do know that we specifically made this
            # optimization for when we grab the necessary state out for
            # `filter_events_for_client` which just uses 2 conditions
            # (`EventTypes.RoomHistoryVisibility` and `EventTypes.Member`).
            if use_condition_optimization and len(state_filter_condition_combos) < 10:
                select_clause_list: list[str] = []
                for etype, skey in state_filter_condition_combos:
                    if skey is None:
                        where_clause = "(type = ?)"
                        overall_select_query_args.extend([etype])
                    else:
                        where_clause = "(type = ? AND state_key = ?)"
                        overall_select_query_args.extend([etype, skey])

                    select_clause_list.append(
                        f"""
                        (
                            SELECT DISTINCT ON (type, state_key)
                                type, state_key, event_id
                            FROM state_groups_state
                            INNER JOIN sgs USING (state_group)
                            WHERE {where_clause}
                            ORDER BY type, state_key, state_group DESC
                        )
                        """
                    )

                overall_select_clause = " UNION ".join(select_clause_list)
            else:
                where_clause, where_args = state_filter.make_sql_filter_clause()
                # Unless the filter clause is empty, we're going to append it after an
                # existing where clause
                if where_clause:
                    where_clause = " AND (%s)" % (where_clause,)

                overall_select_query_args.extend(where_args)

                overall_select_clause = f"""
                    SELECT DISTINCT ON (type, state_key)
                        type, state_key, event_id
                    FROM state_groups_state
                    WHERE state_group IN (
                        SELECT state_group FROM sgs
                    ) {where_clause}
                    ORDER BY type, state_key, state_group DESC
                """

            for group in groups:
                args: list[int | str] = [group]
                args.extend(overall_select_query_args)

                txn.execute(sql % (overall_select_clause,), args)
                for row in txn:
                    typ, state_key, event_id = row
                    key = (intern_string(typ), intern_string(state_key))
                    results[group][key] = event_id
        else:
            max_entries_returned = state_filter.max_entries_returned()

            where_clause, where_args = state_filter.make_sql_filter_clause()
            # Unless the filter clause is empty, we're going to append it after an
            # existing where clause
            if where_clause:
                where_clause = " AND (%s)" % (where_clause,)

            # XXX: We could `WITH RECURSIVE` here since it's supported on SQLite 3.8.3
            # or higher and our minimum supported version is greater than that.
            #
            # We just haven't put in the time to refactor this.
            for group in groups:
                next_group: int | None = group

                while next_group:
                    # We did this before by getting the list of group ids, and
                    # then passing that list to sqlite to get latest event for
                    # each (type, state_key). However, that was terribly slow
                    # without the right indices (which we can't add until
                    # after we finish deduping state, which requires this func)
                    args = [next_group]
                    args.extend(where_args)

                    txn.execute(
                        "SELECT type, state_key, event_id FROM state_groups_state"
                        " WHERE state_group = ? " + where_clause,
                        args,
                    )
                    results[group].update(
                        ((typ, state_key), event_id)
                        for typ, state_key, event_id in txn
                        if (typ, state_key) not in results[group]
                    )

                    # If the number of entries in the (type,state_key)->event_id dict
                    # matches the number of (type,state_keys) types we were searching
                    # for, then we must have found them all, so no need to go walk
                    # further down the tree... UNLESS our types filter contained
                    # wildcards (i.e. Nones) in which case we have to do an exhaustive
                    # search
                    if (
                        max_entries_returned is not None
                        and len(results[group]) == max_entries_returned
                    ):
                        break

                    next_group = self.db_pool.simple_select_one_onecol_txn(
                        txn,
                        table="state_group_edges",
                        keyvalues={"state_group": next_group},
                        retcol="prev_state_group",
                        allow_none=True,
                    )

        # The results shouldn't be considered mutable.
        if missing_groups:
            logger.debug(
                "Legacy fallback for %s returned %s state entries "
                "(state_groups_state is empty on this branch)",
                missing_groups,
                {group: len(results[group]) for group in missing_groups},
            )
        return results

    def _state_hamt_root_exists_txn(
        self,
        txn: LoggingTransaction,
        state_group: int,
        use_tikv: bool | None = None,
    ) -> bool:
        """Check whether a state group HAMT root exists."""
        if use_tikv is None:
            use_tikv = bool(getattr(self, "tikv_pd_endpoints", None))
        if use_tikv:
            from synapse.synapse_rust import tikv_engine

            return (
                tikv_engine.get(
                    _state_hamt_root_tikv_key(self.tikv_namespace, state_group)
                )
                is not None
            )

        root = self.db_pool.simple_select_one_onecol_txn(
            txn,
            table="state_hamt_roots",
            keyvalues={"state_group": state_group},
            retcol="root_structural_hash",
            allow_none=True,
        )
        return root is not None

    def _get_state_groups_from_hamt_txn(
        self,
        txn: LoggingTransaction,
        groups: list[int],
        state_filter: StateFilter,
        use_tikv: bool | None = None,
    ) -> tuple[dict[int, MutableStateMap[str]], list[int]]:
        _gg_hamt_txn_start = time.monotonic()
        results: dict[int, MutableStateMap[str]] = {}
        missing_groups: list[int] = []
        if use_tikv is None:
            use_tikv = bool(getattr(self, "tikv_pd_endpoints", None))
        exact_keys = (
            state_filter.concrete_types() if not state_filter.has_wildcards() else None
        )

        bulk_results: dict[int, list[tuple[str, str, str]] | None] | None = None
        bulk_selective_results: dict[int, list[tuple[str, str, str]] | None] | None = (
            None
        )
        if len(groups) > 1 and not use_tikv:
            if exact_keys is None:
                bulk_results = self._materialize_state_hamt_from_postgres_many_txn(
                    txn, groups
                )
            else:
                bulk_selective_results = self._lookup_state_hamt_from_postgres_many_txn(
                    txn, groups, exact_keys
                )

        for group in groups:
            if exact_keys is not None:
                entries = (
                    self._lookup_state_hamt_from_tikv(txn, group, exact_keys)
                    if use_tikv
                    else bulk_selective_results[group]
                    if bulk_selective_results is not None
                    else self._lookup_state_hamt_from_postgres_txn(
                        txn, group, exact_keys
                    )
                )
            else:
                entries = (
                    bulk_results[group]
                    if bulk_results is not None
                    else self._materialize_state_hamt_from_tikv(txn, group)
                    if use_tikv
                    else self._materialize_state_hamt_from_postgres_txn(txn, group)
                )
            if entries is None:
                missing_groups.append(group)
                continue

            state_map: MutableStateMap[str] = {}
            for typ, state_key, event_id in entries:
                key = (intern_string(typ), intern_string(state_key))
                state_map[key] = event_id

            results[group] = dict(state_filter.filter_state(state_map))

        logger.debug(
            "[gg-state-timing] _get_state_groups_from_hamt_txn groups=%d "
            "use_tikv=%s elapsed_ms=%.1f missing=%d",
            len(groups),
            use_tikv,
            (time.monotonic() - _gg_hamt_txn_start) * 1000,
            len(missing_groups),
        )
        return results, missing_groups

    def _materialize_state_hamts_from_tikv_direct(
        self, state_groups: list[int]
    ) -> tuple[dict[int, list[tuple[str, str, str]]], list[int]]:
        """Materialize several state groups from TiKV in one node traversal.

        The root records are fetched together, then Rust walks every reachable
        HAMT in lockstep. This deduplicates both root RPCs and node fetches for
        roots that share subtrees, without holding a SQL transaction open.
        """
        from synapse.synapse_rust import tikv_engine

        if not state_groups:
            return {}, []

        root_key_to_group = {
            _state_hamt_root_tikv_key(self.tikv_namespace, state_group): state_group
            for state_group in state_groups
        }
        roots: dict[int, tuple[bytes, bytes]] = {}
        for root_key, root_value in tikv_engine.batch_get(list(root_key_to_group)):
            state_group = root_key_to_group[bytes(root_key)]
            room_prefix, root_hash, _lattice, _stored_room_id = _decode_state_hamt_root(
                bytes(root_value)
            )
            roots[state_group] = (room_prefix, root_hash)

        missing_groups = [
            state_group for state_group in state_groups if state_group not in roots
        ]
        present_groups = [
            state_group for state_group in state_groups if state_group in roots
        ]
        if not present_groups:
            return {}, missing_groups

        entries_by_root = tikv_engine.materialize_state_hamts(
            self.tikv_namespace, [roots[state_group] for state_group in present_groups]
        )
        return dict(zip(present_groups, entries_by_root)), missing_groups

    def _materialize_state_hamt_from_tikv_direct(
        self, state_group: int
    ) -> list[tuple[str, str, str]] | None:
        """Materialize a state_group's HAMT against TiKV directly without a SQL transaction.

        The root pointer (`state_group -> room_prefix + root_hash`) is read
        from TiKV, then the content-addressed node tree is BFS-fetched, decoded,
        and walked entirely in Rust (see `materialize_state_hamt` in `rust/src/tikv_engine.rs`).
        """
        from synapse.synapse_rust import tikv_engine

        root_value = tikv_engine.get(
            _state_hamt_root_tikv_key(self.tikv_namespace, state_group)
        )
        if root_value is None:
            return None
        room_prefix, root_structural_hash, _lattice, _stored_room_id = (
            _decode_state_hamt_root(root_value)
        )
        return tikv_engine.materialize_state_hamt(
            self.tikv_namespace, room_prefix, root_structural_hash
        )

    def _materialize_state_hamt_from_tikv(
        self, txn: LoggingTransaction, state_group: int
    ) -> list[tuple[str, str, str]] | None:
        return self._materialize_state_hamt_from_tikv_direct(state_group)

    def _lookup_state_hamts_from_tikv_direct(
        self, state_groups: list[int], keys: list[tuple[str, str]]
    ) -> tuple[dict[int, list[tuple[str, str, str]]], list[int]]:
        """Selective HAMT key lookup across several state groups in one TiKV traversal.

        Root records are fetched in a single TiKV batch_get, structural keys are derived,
        and Rust traverses the trees across all groups in lockstep with shared BFS node
        batching and L1 node caching.
        """
        _gg_tikv_many_start = time.monotonic()
        from synapse.synapse_rust import state_hamt, tikv_engine

        if not state_groups:
            return {}, []

        root_key_to_group = {
            _state_hamt_root_tikv_key(self.tikv_namespace, state_group): state_group
            for state_group in state_groups
        }
        roots: dict[int, tuple[bytes, bytes, bytes]] = {}
        secret = self._state_hamt_secret()
        for root_key, root_value in tikv_engine.batch_get(list(root_key_to_group)):
            state_group = root_key_to_group[bytes(root_key)]
            room_prefix, root_hash, _lattice, stored_room_id = _decode_state_hamt_root(
                bytes(root_value)
            )
            structural_key = state_hamt.room_structural_key(secret, stored_room_id)
            roots[state_group] = (room_prefix, root_hash, structural_key)

        missing_groups = [
            state_group for state_group in state_groups if state_group not in roots
        ]
        present_groups = [
            state_group for state_group in state_groups if state_group in roots
        ]
        if not present_groups:
            return {}, missing_groups

        queries = [
            (roots[sg][0], roots[sg][1], roots[sg][2], keys) for sg in present_groups
        ]
        entries_by_group = tikv_engine.lookup_state_hamts(self.tikv_namespace, queries)
        logger.debug(
            "[gg-state-timing] _lookup_state_hamts_from_tikv_direct "
            "groups=%d keys=%d elapsed_ms=%.1f",
            len(state_groups),
            len(keys),
            (time.monotonic() - _gg_tikv_many_start) * 1000,
        )
        return dict(zip(present_groups, entries_by_group)), missing_groups

    def _lookup_state_hamt_from_tikv_direct(
        self,
        state_group: int,
        keys: list[tuple[str, str]],
    ) -> list[tuple[str, str, str]] | None:
        """Selective HAMT key lookup directly against TiKV without a SQL transaction."""
        from synapse.synapse_rust import state_hamt, tikv_engine

        root_value = tikv_engine.get(
            _state_hamt_root_tikv_key(self.tikv_namespace, state_group)
        )
        if root_value is None:
            return None
        room_prefix, root_hash, _lattice, room_id = _decode_state_hamt_root(root_value)

        root_key = _state_hamt_node_tikv_key(
            self.tikv_namespace, room_prefix, root_hash
        )
        root_bytes = tikv_engine.get(root_key)
        if root_bytes is None:
            raise RuntimeError(
                f"Missing HAMT root node for state group {state_group}: {root_hash.hex()}"
            )
        nodes: dict[bytes, bytes] = {root_hash: bytes(root_bytes)}
        secret = self._state_hamt_secret()

        while True:
            entries, missing = state_hamt.lookup_state_entries(
                secret,
                room_id,
                root_bytes,
                list(nodes.items()),
                keys,
            )
            missing = [
                bytes(node_hash) for node_hash in missing if node_hash not in nodes
            ]
            if not missing:
                return entries
            key_to_hash = {
                _state_hamt_node_tikv_key(
                    self.tikv_namespace, room_prefix, node_hash
                ): node_hash
                for node_hash in missing
            }
            rows = tikv_engine.batch_get(list(key_to_hash))
            for node_key, node_bytes in rows:
                nodes[key_to_hash[bytes(node_key)]] = bytes(node_bytes)
            unresolved = set(missing) - nodes.keys()
            if unresolved:
                raise RuntimeError(
                    "Missing HAMT child nodes for state group "
                    f"{state_group}: {[node_hash.hex() for node_hash in unresolved]}"
                )

    def _lookup_state_hamt_from_tikv(
        self,
        txn: LoggingTransaction,
        state_group: int,
        keys: list[tuple[str, str]],
    ) -> list[tuple[str, str, str]] | None:
        return self._lookup_state_hamt_from_tikv_direct(state_group, keys)

    def _materialize_state_hamt_from_postgres_txn(
        self, txn: LoggingTransaction, state_group: int
    ) -> list[tuple[str, str, str]] | None:
        _gg_mat_start = time.monotonic()
        from synapse.synapse_rust import state_hamt

        root = self.db_pool.simple_select_one_onecol_txn(
            txn,
            table="state_hamt_roots",
            keyvalues={"state_group": state_group},
            retcol="root_structural_hash",
            allow_none=True,
        )
        if root is None:
            logger.debug(
                "SQL HAMT materialization: no state_hamt_roots row for "
                "state_group %s -> falls through to empty legacy fallback",
                state_group,
            )
            return None
        root_structural_hash = bytes(root)

        root_node = self.db_pool.simple_select_one_onecol_txn(
            txn,
            table="state_hamt_nodes",
            keyvalues={"structural_hash": bytearray(root_structural_hash)},
            retcol="node_bytes",
            allow_none=True,
        )
        if root_node is None:
            logger.warning(
                "SQL HAMT materialization: state_hamt_roots row exists for "
                "state_group %s but root node %s is missing from state_hamt_nodes",
                state_group,
                root_structural_hash.hex(),
            )
            raise RuntimeError(
                "Missing HAMT root node for state group "
                f"{state_group}: {root_structural_hash.hex()}"
            )
        root_node_bytes = bytes(root_node)

        node_bytes_by_hash: dict[bytes, bytes] = {root_structural_hash: root_node_bytes}
        seen_hashes = {root_structural_hash}
        to_fetch = {
            bytes(child_hash)
            for child_hash in state_hamt.node_child_hashes(root_node_bytes)
        }

        while to_fetch:
            current_batch = list(to_fetch)
            to_fetch = set()

            for chunk in batch_iter(current_batch, 100):
                rows = [
                    (bytes(structural_hash), bytes(node_bytes))
                    for structural_hash, node_bytes in self.db_pool.simple_select_many_txn(
                        txn,
                        table="state_hamt_nodes",
                        column="structural_hash",
                        iterable=[
                            bytearray(structural_hash) for structural_hash in chunk
                        ],
                        keyvalues={},
                        retcols=("structural_hash", "node_bytes"),
                    )
                ]
                found_hashes = {node_hash for node_hash, _ in rows}
                missing_hashes = set(chunk) - found_hashes
                if missing_hashes:
                    raise RuntimeError(
                        "Missing HAMT child nodes for state group "
                        f"{state_group}: {[hash.hex() for hash in missing_hashes]}"
                    )

                for structural_hash, node_bytes in rows:
                    node_bytes_by_hash[structural_hash] = node_bytes

                    for child_hash in state_hamt.node_child_hashes(node_bytes):
                        if child_hash not in seen_hashes:
                            seen_hashes.add(child_hash)
                            to_fetch.add(child_hash)

        entries = state_hamt.materialize_state_entries(
            node_bytes_by_hash[root_structural_hash],
            list(node_bytes_by_hash.items()),
        )
        logger.debug(
            "[gg-state-timing] _materialize_state_hamt_from_postgres_txn "
            "group=%d elapsed_ms=%.1f entries=%d",
            state_group,
            (time.monotonic() - _gg_mat_start) * 1000,
            len(entries),
        )
        return entries

    def _materialize_state_hamt_from_postgres_many_txn(
        self, txn: LoggingTransaction, groups: list[int]
    ) -> dict[int, list[tuple[str, str, str]] | None]:
        """Materialize multiple SQL HAMT roots while sharing node fetches."""
        from synapse.synapse_rust import state_hamt

        root_rows = self.db_pool.simple_select_many_txn(
            txn,
            table="state_hamt_roots",
            column="state_group",
            iterable=groups,
            keyvalues={},
            retcols=("state_group", "root_structural_hash"),
        )
        roots = {int(group): bytes(root_hash) for group, root_hash in root_rows}
        results: dict[int, list[tuple[str, str, str]] | None] = dict.fromkeys(
            groups, None
        )
        if not roots:
            return results

        to_fetch = set(roots.values())
        node_bytes_by_hash: dict[bytes, bytes] = {}
        while to_fetch:
            current_batch = list(to_fetch)
            to_fetch = set()
            for chunk in batch_iter(current_batch, 100):
                rows = [
                    (bytes(node_hash), bytes(node_bytes))
                    for node_hash, node_bytes in self.db_pool.simple_select_many_txn(
                        txn,
                        table="state_hamt_nodes",
                        column="structural_hash",
                        iterable=[bytearray(node_hash) for node_hash in chunk],
                        keyvalues={},
                        retcols=("structural_hash", "node_bytes"),
                    )
                ]
                missing = set(chunk) - {node_hash for node_hash, _ in rows}
                if missing:
                    raise RuntimeError(
                        "Missing HAMT nodes for state groups "
                        f"{groups}: {[node_hash.hex() for node_hash in missing]}"
                    )
                for node_hash, node_bytes in rows:
                    if node_hash in node_bytes_by_hash:
                        continue
                    node_bytes_by_hash[node_hash] = node_bytes
                    for child_hash in state_hamt.node_child_hashes(node_bytes):
                        if child_hash not in node_bytes_by_hash:
                            to_fetch.add(child_hash)

        for group, root_hash in roots.items():
            root_bytes = node_bytes_by_hash[root_hash]
            results[group] = state_hamt.materialize_state_entries(
                root_bytes,
                list(node_bytes_by_hash.items()),
            )
        return results

    def _lookup_state_hamt_from_postgres_txn(
        self,
        txn: LoggingTransaction,
        state_group: int,
        keys: list[tuple[str, str]],
    ) -> list[tuple[str, str, str]] | None:
        from synapse.synapse_rust import state_hamt

        _gg_single_start = time.monotonic()
        root = self.db_pool.simple_select_one_onecol_txn(
            txn,
            table="state_hamt_roots",
            keyvalues={"state_group": state_group},
            retcol="root_structural_hash",
            allow_none=True,
        )
        if root is None:
            return None
        root_hash = bytes(root)
        room_id = self.db_pool.simple_select_one_onecol_txn(
            txn,
            table="state_groups",
            keyvalues={"id": state_group},
            retcol="room_id",
            allow_none=False,
        )
        root_node = self.db_pool.simple_select_one_onecol_txn(
            txn,
            table="state_hamt_nodes",
            keyvalues={"structural_hash": bytearray(root_hash)},
            retcol="node_bytes",
            allow_none=True,
        )
        if root_node is None:
            raise RuntimeError(
                f"Missing HAMT root node for state group {state_group}: {root_hash.hex()}"
            )
        root_bytes = bytes(root_node)
        nodes: dict[bytes, bytes] = {root_hash: root_bytes}
        secret = self._state_hamt_secret()

        while True:
            entries, missing = state_hamt.lookup_state_entries(
                secret,
                room_id,
                root_bytes,
                list(nodes.items()),
                keys,
            )
            missing = [
                bytes(node_hash) for node_hash in missing if node_hash not in nodes
            ]
            if not missing:
                logger.debug(
                    "[gg-state-timing] _lookup_state_hamt_from_postgres_txn "
                    "group=%d keys=%d elapsed_ms=%.1f",
                    state_group,
                    len(keys),
                    (time.monotonic() - _gg_single_start) * 1000,
                )
                return entries
            rows = self.db_pool.simple_select_many_txn(
                txn,
                table="state_hamt_nodes",
                column="structural_hash",
                iterable=[bytearray(node_hash) for node_hash in missing],
                keyvalues={},
                retcols=("structural_hash", "node_bytes"),
            )
            nodes.update(
                (bytes(node_hash), bytes(node_bytes)) for node_hash, node_bytes in rows
            )
            unresolved = set(missing) - nodes.keys()
            if unresolved:
                raise RuntimeError(
                    "Missing HAMT child nodes for state group "
                    f"{state_group}: {[node_hash.hex() for node_hash in unresolved]}"
                )

    def _lookup_state_hamt_from_postgres_many_txn(
        self,
        txn: LoggingTransaction,
        groups: list[int],
        keys: list[tuple[str, str]],
    ) -> dict[int, list[tuple[str, str, str]] | None]:
        """Selective HAMT key lookup across several state groups, sharing node
        fetches -- the SQL-mode mirror of the TiKV batched selective lookup
        (`lookup_state_hamts`/`lookup_state_hamts_core` in tikv_engine.rs).

        Each round, every group attempts to resolve `keys` against whatever
        nodes have been fetched so far; any node hashes still missing across
        *all* groups are merged into one shared batch fetch for the next
        round, so groups needing a node in common only ever fetch it once.
        """
        _gg_many_start = time.monotonic()
        from synapse.synapse_rust import state_hamt

        root_rows = self.db_pool.simple_select_many_txn(
            txn,
            table="state_hamt_roots",
            column="state_group",
            iterable=groups,
            keyvalues={},
            retcols=("state_group", "root_structural_hash"),
        )
        roots = {int(group): bytes(root_hash) for group, root_hash in root_rows}
        results: dict[int, list[tuple[str, str, str]] | None] = dict.fromkeys(
            groups, None
        )
        if not roots:
            return results

        room_id_rows = self.db_pool.simple_select_many_txn(
            txn,
            table="state_groups",
            column="id",
            iterable=list(roots.keys()),
            keyvalues={},
            retcols=("id", "room_id"),
        )
        room_ids = {int(group): room_id for group, room_id in room_id_rows}
        secret = self._state_hamt_secret()

        def try_resolve_all() -> set[bytes]:
            """Attempt selective resolution for every group against the
            nodes fetched so far. Returns the union of node hashes still
            missing across all groups (deduped, and excluding anything
            already fetched -- defensive, mirrors the single-group loop)."""
            still_missing: set[bytes] = set()
            for group, root_hash in roots.items():
                entries, missing = state_hamt.lookup_state_entries(
                    secret,
                    room_ids[group],
                    node_bytes_by_hash[root_hash],
                    list(node_bytes_by_hash.items()),
                    keys,
                )
                results[group] = entries
                still_missing.update(
                    bytes(node_hash)
                    for node_hash in missing
                    if bytes(node_hash) not in node_bytes_by_hash
                )
            return still_missing

        node_bytes_by_hash: dict[bytes, bytes] = {}
        to_fetch = set(roots.values())
        rounds = 0
        while to_fetch:
            rounds += 1
            current_batch = list(to_fetch)
            for chunk in batch_iter(current_batch, 100):
                rows = [
                    (bytes(node_hash), bytes(node_bytes))
                    for node_hash, node_bytes in self.db_pool.simple_select_many_txn(
                        txn,
                        table="state_hamt_nodes",
                        column="structural_hash",
                        iterable=[bytearray(node_hash) for node_hash in chunk],
                        keyvalues={},
                        retcols=("structural_hash", "node_bytes"),
                    )
                ]
                missing_from_db = set(chunk) - {node_hash for node_hash, _ in rows}
                if missing_from_db:
                    raise RuntimeError(
                        "Missing HAMT nodes for state groups "
                        f"{groups}: {[node_hash.hex() for node_hash in missing_from_db]}"
                    )
                node_bytes_by_hash.update(rows)

            to_fetch = try_resolve_all()

        logger.debug(
            "[gg-state-timing] _lookup_state_hamt_from_postgres_many_txn "
            "groups=%d keys=%d elapsed_ms=%.1f rounds=%d",
            len(groups),
            len(keys),
            (time.monotonic() - _gg_many_start) * 1000,
            rounds,
        )
        return results


class StateBackgroundUpdateStore(StateGroupBackgroundUpdateStore):
    STATE_GROUP_DEDUPLICATION_UPDATE_NAME = "state_group_state_deduplication"
    STATE_GROUP_INDEX_UPDATE_NAME = "state_group_state_type_index"
    STATE_GROUPS_ROOM_INDEX_UPDATE_NAME = "state_groups_room_id_idx"
    STATE_GROUP_EDGES_UNIQUE_INDEX_UPDATE_NAME = "state_group_edges_unique_idx"

    def __init__(
        self,
        database: DatabasePool,
        db_conn: LoggingDatabaseConnection,
        hs: "HomeServer",
    ):
        super().__init__(database, db_conn, hs)
        self.db_pool.updates.register_background_update_handler(
            self.STATE_GROUP_DEDUPLICATION_UPDATE_NAME,
            self._background_deduplicate_state,
        )
        self.db_pool.updates.register_background_update_handler(
            self.STATE_GROUP_INDEX_UPDATE_NAME, self._background_index_state
        )
        self.db_pool.updates.register_background_index_update(
            self.STATE_GROUPS_ROOM_INDEX_UPDATE_NAME,
            index_name="state_groups_room_id_idx",
            table="state_groups",
            columns=["room_id"],
        )

        # `state_group_edges` can cause severe performance issues if duplicate
        # rows are introduced, which can accidentally be done by well-meaning
        # server admins when trying to restore a database dump, etc.
        # See https://github.com/matrix-org/synapse/issues/11779.
        # Introduce a unique index to guard against that.
        self.db_pool.updates.register_background_index_update(
            self.STATE_GROUP_EDGES_UNIQUE_INDEX_UPDATE_NAME,
            index_name="state_group_edges_unique_idx",
            table="state_group_edges",
            columns=["state_group", "prev_state_group"],
            unique=True,
            # The old index was on (state_group) and was not unique.
            replaces_index="state_group_edges_idx",
        )

    async def _background_deduplicate_state(
        self, progress: dict, batch_size: int
    ) -> int:
        """This background update will slowly deduplicate state by reencoding
        them as deltas.
        """
        last_state_group = progress.get("last_state_group", 0)
        rows_inserted = progress.get("rows_inserted", 0)
        max_group = progress.get("max_group", None)

        BATCH_SIZE_SCALE_FACTOR = 100

        batch_size = max(1, int(batch_size / BATCH_SIZE_SCALE_FACTOR))

        if max_group is None:
            rows = await self.db_pool.execute(
                "_background_deduplicate_state",
                "SELECT coalesce(max(id), 0) FROM state_groups",
            )
            max_group = rows[0][0]

        def reindex_txn(txn: LoggingTransaction) -> tuple[bool, int]:
            new_last_state_group = last_state_group
            for count in range(batch_size):
                txn.execute(
                    "SELECT id, room_id FROM state_groups"
                    " WHERE ? < id AND id <= ?"
                    " ORDER BY id ASC"
                    " LIMIT 1",
                    (new_last_state_group, max_group),
                )
                row = txn.fetchone()
                if row:
                    state_group, room_id = row

                if not row or not state_group:
                    return True, count

                txn.execute(
                    "SELECT state_group FROM state_group_edges WHERE state_group = ?",
                    (state_group,),
                )

                # If we reach a point where we've already started inserting
                # edges we should stop.
                if txn.fetchall():
                    return True, count

                txn.execute(
                    "SELECT coalesce(max(id), 0) FROM state_groups"
                    " WHERE id < ? AND room_id = ?",
                    (state_group, room_id),
                )
                # There will be a result due to the coalesce.
                (prev_group,) = txn.fetchone()  # type: ignore
                new_last_state_group = state_group

                if prev_group:
                    potential_hops = self._count_state_group_hops_txn(txn, prev_group)
                    if potential_hops >= MAX_STATE_DELTA_HOPS:
                        # We want to ensure chains are at most this long,#
                        # otherwise read performance degrades.
                        continue

                    prev_state_by_group = self._get_state_groups_from_groups_txn(
                        txn, [prev_group]
                    )
                    prev_state = prev_state_by_group[prev_group]

                    curr_state_by_group = self._get_state_groups_from_groups_txn(
                        txn, [state_group]
                    )
                    curr_state = curr_state_by_group[state_group]

                    if not set(prev_state.keys()) - set(curr_state.keys()):
                        # We can only do a delta if the current has a strict super set
                        # of keys

                        delta_state = {
                            key: value
                            for key, value in curr_state.items()
                            if prev_state.get(key, None) != value
                        }

                        self.db_pool.simple_delete_txn(
                            txn,
                            table="state_group_edges",
                            keyvalues={"state_group": state_group},
                        )

                        self.db_pool.simple_insert_txn(
                            txn,
                            table="state_group_edges",
                            values={
                                "state_group": state_group,
                                "prev_state_group": prev_group,
                            },
                        )

                        self.db_pool.simple_delete_txn(
                            txn,
                            table="state_groups_state",
                            keyvalues={"state_group": state_group},
                        )

                        self.db_pool.simple_insert_many_txn(
                            txn,
                            table="state_groups_state",
                            keys=(
                                "state_group",
                                "room_id",
                                "type",
                                "state_key",
                                "event_id",
                            ),
                            values=[
                                (state_group, room_id, key[0], key[1], state_id)
                                for key, state_id in delta_state.items()
                            ],
                        )

            progress = {
                "last_state_group": state_group,
                "rows_inserted": rows_inserted + batch_size,
                "max_group": max_group,
            }

            self.db_pool.updates._background_update_progress_txn(
                txn, self.STATE_GROUP_DEDUPLICATION_UPDATE_NAME, progress
            )

            return False, batch_size

        finished, result = await self.db_pool.runInteraction(
            self.STATE_GROUP_DEDUPLICATION_UPDATE_NAME, reindex_txn
        )

        if finished:
            await self.db_pool.updates._end_background_update(
                self.STATE_GROUP_DEDUPLICATION_UPDATE_NAME
            )

        return result * BATCH_SIZE_SCALE_FACTOR

    async def _background_index_state(self, progress: dict, batch_size: int) -> int:
        def reindex_txn(conn: LoggingDatabaseConnection) -> None:
            conn.rollback()
            if isinstance(self.database_engine, PostgresEngine):
                # postgres insists on autocommit for the index
                conn.engine.attempt_to_set_autocommit(conn.conn, True)
                try:
                    txn = conn.cursor()
                    txn.execute(
                        "CREATE INDEX CONCURRENTLY state_groups_state_type_idx"
                        " ON state_groups_state(state_group, type, state_key)"
                    )
                    txn.execute("DROP INDEX IF EXISTS state_groups_state_id")
                finally:
                    conn.engine.attempt_to_set_autocommit(conn.conn, False)
            else:
                txn = conn.cursor()
                txn.execute(
                    "CREATE INDEX state_groups_state_type_idx"
                    " ON state_groups_state(state_group, type, state_key)"
                )
                txn.execute("DROP INDEX IF EXISTS state_groups_state_id")

        await self.db_pool.runWithConnection(reindex_txn)

        await self.db_pool.updates._end_background_update(
            self.STATE_GROUP_INDEX_UPDATE_NAME
        )

        return 1
