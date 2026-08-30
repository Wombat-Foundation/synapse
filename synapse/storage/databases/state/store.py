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

import logging
import time
from typing import (
    TYPE_CHECKING,
    Iterable,
    Mapping,
    Sequence,
    cast,
)

from prometheus_client import Histogram

from synapse.api.constants import EventTypes
from synapse.api.room_versions import RoomVersion
from synapse.events import EventBase
from synapse.events.snapshot import (
    UnpersistedEventContext,
)
from synapse.logging.context import defer_to_thread
from synapse.logging.opentracing import tag_args, trace
from synapse.metrics import SERVER_NAME_LABEL
from synapse.storage._base import SQLBaseStore
from synapse.storage.database import (
    DatabasePool,
    LoggingDatabaseConnection,
    LoggingTransaction,
)
from synapse.storage.databases.state.bg_updates import (
    StateBackgroundUpdateStore,
    _decode_state_hamt_root,
    _encode_state_hamt_root,
    _state_hamt_node_tikv_key,
    _state_hamt_root_tikv_key,
    put_state_hamt_objects,
)
from synapse.storage.engines import PostgresEngine
from synapse.storage.types import Cursor
from synapse.storage.util.sequence import build_sequence_generator
from synapse.types import MutableStateMap, StateKey, StateMap
from synapse.types.state import StateFilter
from synapse.util.caches import intern_string
from synapse.util.caches.dictionary_cache import DictionaryCache
from synapse.util.cancellation import cancellable

if TYPE_CHECKING:
    from synapse.server import HomeServer
    from synapse.storage.databases.state.deletion import StateDeletionDataStore

logger = logging.getLogger(__name__)

state_hamt_precommit_publish_timer = Histogram(
    "synapse_state_hamt_precommit_publish_time_seconds",
    "Time spent publishing HAMT nodes and roots before SQL state-group visibility",
    labelnames=[SERVER_NAME_LABEL],
    buckets=(0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1.0),
)


class StateGroupDataStore(StateBackgroundUpdateStore, SQLBaseStore):
    """A data store for fetching/storing state groups."""

    def __init__(
        self,
        database: DatabasePool,
        db_conn: LoggingDatabaseConnection,
        hs: "HomeServer",
        state_deletion_store: "StateDeletionDataStore",
    ):
        super().__init__(database, db_conn, hs)
        self._state_deletion_store = state_deletion_store
        self.server_name = hs.hostname

        # Originally the state store used a single DictionaryCache to cache the
        # event IDs for the state types in a given state group to avoid hammering
        # on the state_group* tables.
        #
        # The point of using a DictionaryCache is that it can cache a subset
        # of the state events for a given state group (i.e. a subset of the keys for a
        # given dict which is an entry in the cache for a given state group ID).
        #
        # However, this poses problems when performing complicated queries
        # on the store - for instance: "give me all the state for this group, but
        # limit members to this subset of users", as DictionaryCache's API isn't
        # rich enough to say "please cache any of these fields, apart from this subset".
        # This is problematic when lazy loading members, which requires this behaviour,
        # as without it the cache has no choice but to speculatively load all
        # state events for the group, which negates the efficiency being sought.
        #
        # Rather than overcomplicating DictionaryCache's API, we instead split the
        # state_group_cache into two halves - one for tracking non-member events,
        # and the other for tracking member_events.  This means that lazy loading
        # queries can be made in a cache-friendly manner by querying both caches
        # separately and then merging the result.  So for the example above, you
        # would query the members cache for a specific subset of state keys
        # (which DictionaryCache will handle efficiently and fine) and the non-members
        # cache for all state (which DictionaryCache will similarly handle fine)
        # and then just merge the results together.
        #
        # We size the non-members cache to be smaller than the members cache as the
        # vast majority of state in Matrix (today) is member events.

        self._state_group_cache: DictionaryCache[int, StateKey, str] = DictionaryCache(
            name="*stateGroupCache*",
            clock=hs.get_clock(),
            server_name=self.server_name,
            # TODO: this hasn't been tuned yet
            max_entries=50000,
        )
        self._state_group_members_cache: DictionaryCache[int, StateKey, str] = (
            DictionaryCache(
                name="*stateGroupMembersCache*",
                clock=hs.get_clock(),
                server_name=self.server_name,
                max_entries=500000,
            )
        )

        def get_max_state_group_txn(txn: Cursor) -> int:
            txn.execute("SELECT COALESCE(max(id), 0) FROM state_groups")
            return txn.fetchone()[0]  # type: ignore

        self._state_group_seq_gen = build_sequence_generator(
            db_conn,
            self.database_engine,
            get_max_state_group_txn,
            "state_group_id_seq",
            table="state_groups",
            id_column="id",
        )

        self.tikv_pd_endpoints = hs.config.database.tikv_pd_endpoints
        if self.tikv_pd_endpoints:
            try:
                from synapse.synapse_rust import tikv_engine

                tikv_engine.open_client(self.tikv_pd_endpoints)
                logger.info(
                    "Connected to TiKV cluster at %s for state group offload",
                    self.tikv_pd_endpoints,
                )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to connect to TiKV cluster at {self.tikv_pd_endpoints}"
                ) from e

    @trace
    @tag_args
    @cancellable
    async def _get_state_groups_from_groups(
        self, groups: list[int], state_filter: StateFilter
    ) -> dict[int, StateMap[str]]:
        """Returns the state groups for a given set of groups from the
        database, filtering on types of state events.

        Args:
            groups: list of state group IDs to query
            state_filter: The state filter used to fetch state
                from the database.
        Returns:
            Dict of state group to state map.
        """
        if self.tikv_pd_endpoints:
            exact_keys = (
                state_filter.concrete_types()
                if not state_filter.has_wildcards()
                else None
            )

            tikv_results: dict[int, StateMap[str]] = {}

            def fetch_from_tikv_blocking(
                target_groups: list[int],
            ) -> tuple[dict[int, StateMap[str]], list[int]]:
                res: dict[int, StateMap[str]] = {}
                missing: list[int] = []
                if len(target_groups) > 1:
                    entries_by_group: dict[int, list[tuple[str, str, str]]]
                    if exact_keys is not None:
                        entries_by_group, missing = (
                            self._lookup_state_hamts_from_tikv_direct(
                                target_groups, exact_keys
                            )
                        )
                    else:
                        entries_by_group, missing = (
                            self._materialize_state_hamts_from_tikv_direct(
                                target_groups
                            )
                        )
                    for group, batch_entries in entries_by_group.items():
                        batch_state_map: MutableStateMap[str] = {}
                        for typ, state_key, event_id in batch_entries:
                            key = (intern_string(typ), intern_string(state_key))
                            batch_state_map[key] = event_id
                        res[group] = dict(state_filter.filter_state(batch_state_map))
                    return res, missing

                for group in target_groups:
                    entries: list[tuple[str, str, str]] | None
                    if exact_keys is not None:
                        entries = self._lookup_state_hamt_from_tikv_direct(
                            group, exact_keys
                        )
                    else:
                        entries = self._materialize_state_hamt_from_tikv_direct(group)
                    if entries is None:
                        missing.append(group)
                        continue

                    state_map: MutableStateMap[str] = {}
                    for typ, state_key, event_id in entries:
                        key = (intern_string(typ), intern_string(state_key))
                        state_map[key] = event_id
                    res[group] = dict(state_filter.filter_state(state_map))
                return res, missing

            _gg_tikv_start = time.monotonic()
            tikv_results, missing_groups = await defer_to_thread(
                self.hs.get_reactor(),
                fetch_from_tikv_blocking,
                groups,
            )
            logger.debug(
                "[gg-state-timing] _get_state_groups_from_groups tikv_dispatch "
                "groups=%d elapsed_ms=%.1f missing=%d",
                len(groups),
                (time.monotonic() - _gg_tikv_start) * 1000,
                len(missing_groups),
            )

            if missing_groups:
                # TiKV nodes and roots are published before the SQL transaction
                # that creates a state group commits. Therefore an existing
                # state group without a TiKV root is corruption, not a normal
                # cross-worker visibility race. A group that does not exist in
                # SQL is genuinely absent or purged and resolves to an empty
                # state map.
                existing_rows = await self.db_pool.simple_select_many_batch(
                    table="state_groups",
                    column="id",
                    iterable=missing_groups,
                    retcols=("id",),
                    desc="_get_state_groups_from_groups.check_missing_tikv_roots",
                )
                existing_in_sql = {group for (group,) in existing_rows}
                for group in missing_groups:
                    if group not in existing_in_sql:
                        tikv_results[group] = {}
                if existing_in_sql:
                    raise RuntimeError(
                        "State group(s) exist in SQL but have no TiKV HAMT root: "
                        f"{existing_in_sql}"
                    )

            return tikv_results

        # _get_state_groups_from_groups_txn (bg_updates.py) already
        # disambiguates a missing HAMT root from a legitimately empty/purged
        # state group *inside* the transaction, and raises RuntimeError
        # immediately on corruption (an existing `state_groups` row with no
        # HAMT root). HAMT roots are published atomically with the
        # `state_groups` row that references them, so there is no
        # cross-connection visibility race to poll for here: a caller-side
        # retry loop could never observe a corrupt group without the inner
        # txn having already raised on the very first attempt. See the
        # equivalent one-shot check on the TiKV-direct path above.
        chunks = [groups[i : i + 100] for i in range(0, len(groups), 100)]
        _gg_sql_start = time.monotonic()
        results: dict[int, StateMap[str]] = {}
        for chunk in chunks:
            res = await self.db_pool.runInteraction(
                "_get_state_groups_from_groups",
                self._get_state_groups_from_groups_txn,
                chunk,
                state_filter,
            )
            results.update(res)

        logger.debug(
            "[gg-state-timing] _get_state_groups_from_groups sql_dispatch "
            "groups=%d elapsed_ms=%.1f",
            len(groups),
            (time.monotonic() - _gg_sql_start) * 1000,
        )
        return results

    @trace
    @tag_args
    def _get_state_for_group_using_cache(
        self,
        cache: DictionaryCache[int, StateKey, str],
        group: int,
        state_filter: StateFilter,
    ) -> tuple[MutableStateMap[str], bool]:
        """Checks if group is in cache. See `get_state_for_groups`

        Args:
            cache: the state group cache to use
            group: The state group to lookup
            state_filter: The state filter used to fetch state from the database.

        Returns:
             2-tuple (`state_dict`, `got_all`).
                `got_all` is a bool indicating if we successfully retrieved all
                requests state from the cache, if False we need to query the DB for the
                missing state.
        """
        # If we are asked explicitly for a subset of keys, we only ask for those
        # from the cache. This ensures that the `DictionaryCache` can make
        # better decisions about what to cache and what to expire.
        dict_keys = None
        if not state_filter.has_wildcards():
            dict_keys = state_filter.concrete_types()

        cache_entry = cache.get(group, dict_keys=dict_keys)
        state_dict_ids = cache_entry.value

        if cache_entry.full or state_filter.is_full():
            # Either we have everything or want everything, either way
            # `is_all` tells us whether we've gotten everything.
            return state_filter.filter_state(state_dict_ids), cache_entry.full

        # tracks whether any of our requested types are missing from the cache
        missing_types = False

        if state_filter.has_wildcards():
            # We don't know if we fetched all the state keys for the types in
            # the filter that are wildcards, so we have to assume that we may
            # have missed some.
            missing_types = True
        else:
            # There aren't any wild cards, so `concrete_types()` returns the
            # complete list of event types we're wanting.
            for key in state_filter.concrete_types():
                if key not in state_dict_ids and key not in cache_entry.known_absent:
                    missing_types = True
                    break

        return state_filter.filter_state(state_dict_ids), not missing_types

    @trace
    @tag_args
    @cancellable
    async def _get_state_for_groups(
        self, groups: Iterable[int], state_filter: StateFilter | None = None
    ) -> dict[int, MutableStateMap[str]]:
        """Gets the state at each of a list of state groups, optionally
        filtering by type/state_key

        Args:
            groups: list of state groups for which we want
                to get the state.
            state_filter: The state filter used to fetch state
                from the database.
        Returns:
            Dict of state group to state map.
        """
        if state_filter is None:
            state_filter = StateFilter.all()

        member_filter, non_member_filter = state_filter.get_member_split()

        # Now we look them up in the member and non-member caches
        non_member_state, incomplete_groups_nm = self._get_state_for_groups_using_cache(
            groups, self._state_group_cache, state_filter=non_member_filter
        )

        member_state, incomplete_groups_m = self._get_state_for_groups_using_cache(
            groups, self._state_group_members_cache, state_filter=member_filter
        )

        state = dict(non_member_state)
        for group in groups:
            state[group].update(member_state[group])

        # Now fetch any missing groups from the database

        incomplete_groups = incomplete_groups_m | incomplete_groups_nm

        if not incomplete_groups:
            return state

        cache_sequence_nm = self._state_group_cache.sequence
        cache_sequence_m = self._state_group_members_cache.sequence

        # Help the cache hit ratio by expanding the filter a bit
        db_state_filter = state_filter.return_expanded()

        group_to_state_dict = await self._get_state_groups_from_groups(
            list(incomplete_groups), state_filter=db_state_filter
        )

        # Now lets update the caches
        self._insert_into_cache(
            group_to_state_dict,
            db_state_filter,
            cache_seq_num_members=cache_sequence_m,
            cache_seq_num_non_members=cache_sequence_nm,
        )

        # And finally update the result dict, by filtering out any extra
        # stuff we pulled out of the database.
        for group, group_state_dict in group_to_state_dict.items():
            # We just replace any existing entries, as we will have loaded
            # everything we need from the database anyway.
            state[group] = state_filter.filter_state(group_state_dict)

        return state

    @trace
    @tag_args
    def _get_state_for_groups_using_cache(
        self,
        groups: Iterable[int],
        cache: DictionaryCache[int, StateKey, str],
        state_filter: StateFilter,
    ) -> tuple[dict[int, MutableStateMap[str]], set[int]]:
        """Gets the state at each of a list of state groups, optionally
        filtering by type/state_key, querying from a specific cache.

        Args:
            groups: list of state groups for which we want to get the state.
            cache: the cache of group ids to state dicts which
                we will pass through - either the normal state cache or the
                specific members state cache.
            state_filter: The state filter used to fetch state from the
                database.

        Returns:
            Tuple of dict of state_group_id to state map of entries in the
            cache, and the state group ids either missing from the cache or
            incomplete.
        """
        results = {}
        incomplete_groups = set()
        for group in set(groups):
            state_dict_ids, got_all = self._get_state_for_group_using_cache(
                cache, group, state_filter
            )
            results[group] = state_dict_ids

            if not got_all:
                incomplete_groups.add(group)

        return results, incomplete_groups

    def _insert_into_cache(
        self,
        group_to_state_dict: dict[int, StateMap[str]],
        state_filter: StateFilter,
        cache_seq_num_members: int,
        cache_seq_num_non_members: int,
    ) -> None:
        """Inserts results from querying the database into the relevant cache.

        Args:
            group_to_state_dict: The new entries pulled from database.
                Map from state group to state dict
            state_filter: The state filter used to fetch state
                from the database.
            cache_seq_num_members: Sequence number of member cache since
                last lookup in cache
            cache_seq_num_non_members: Sequence number of member cache since
                last lookup in cache
        """

        # We need to work out which types we've fetched from the DB for the
        # member vs non-member caches. This should be as accurate as possible,
        # but can be an underestimate (e.g. when we have wild cards)

        member_filter, non_member_filter = state_filter.get_member_split()
        if member_filter.is_full():
            # We fetched all member events
            member_types = None
        else:
            # `concrete_types()` will only return a subset when there are wild
            # cards in the filter, but that's fine.
            member_types = member_filter.concrete_types()

        if non_member_filter.is_full():
            # We fetched all non member events
            non_member_types = None
        else:
            non_member_types = non_member_filter.concrete_types()

        for group, group_state_dict in group_to_state_dict.items():
            state_dict_members = {}
            state_dict_non_members = {}

            for k, v in group_state_dict.items():
                if k[0] == EventTypes.Member:
                    state_dict_members[k] = v
                else:
                    state_dict_non_members[k] = v

            self._state_group_members_cache.update(
                cache_seq_num_members,
                key=group,
                value=state_dict_members,
                fetched_keys=member_types,
            )

            self._state_group_cache.update(
                cache_seq_num_non_members,
                key=group,
                value=state_dict_non_members,
                fetched_keys=non_member_types,
            )

    def _build_state_hamt_entries(
        self, current_state_ids: StateMap[str]
    ) -> list[tuple[str, str, str]]:
        return [
            (state_key[0], state_key[1], event_id)
            for state_key, event_id in current_state_ids.items()
        ]

    def _prefetch_tikv_hamt_blocking(
        self,
        room_prefix: bytes,
        state_group: int,
        room_id: str,
        updates: list[tuple[str, str, str]],
    ) -> tuple[dict[bytes, bytes], dict[int, tuple[bytes, bytes]]] | None:
        """Fetch a HAMT tree before starting a SQL transaction."""
        from synapse.synapse_rust import state_hamt, tikv_engine

        root_value = tikv_engine.get(
            _state_hamt_root_tikv_key(self.tikv_namespace, state_group)
        )
        if root_value is None:
            return None

        stored_prefix, root_hash, lattice, _stored_room_id = _decode_state_hamt_root(
            root_value
        )
        if stored_prefix != room_prefix:
            raise RuntimeError(
                f"HAMT root for state group {state_group} has the wrong room prefix"
            )

        root_key = _state_hamt_node_tikv_key(
            self.tikv_namespace, room_prefix, root_hash
        )
        root_rows = tikv_engine.batch_get([root_key])
        if not root_rows:
            raise RuntimeError(
                f"Missing HAMT root node while prefetching state group {state_group}"
            )
        nodes = {root_hash: bytes(root_rows[0][1])}

        while True:
            _applied, missing = state_hamt.apply_flat_state_updates(
                self._state_hamt_secret(),
                room_id,
                nodes[root_hash],
                list(nodes.items()),
                lattice,
                updates,
            )
            missing_hashes = {
                bytes(node_hash)
                for node_hash in missing
                if bytes(node_hash) not in nodes
            }
            if not missing_hashes:
                break
            key_to_hash = {
                _state_hamt_node_tikv_key(
                    self.tikv_namespace, room_prefix, node_hash
                ): node_hash
                for node_hash in missing_hashes
            }
            rows = tikv_engine.batch_get(list(key_to_hash))
            found = {
                key_to_hash[bytes(node_key)]: bytes(node_bytes)
                for node_key, node_bytes in rows
            }
            unresolved = missing_hashes - found.keys()
            if unresolved:
                raise RuntimeError(
                    "Missing HAMT nodes while prefetching state group "
                    f"{state_group}: {[node_hash.hex() for node_hash in unresolved]}"
                )
            nodes.update(found)

        return nodes, {state_group: (root_hash, lattice)}

    async def _prefetch_tikv_hamt(
        self,
        room_prefix: bytes,
        state_group: int,
        room_id: str,
        updates: list[tuple[str, str, str]],
    ) -> tuple[dict[bytes, bytes], dict[int, tuple[bytes, bytes]]]:
        _gg_prefetch_start = time.monotonic()
        prefetched = await defer_to_thread(
            self.hs.get_reactor(),
            self._prefetch_tikv_hamt_blocking,
            room_prefix,
            state_group,
            room_id,
            updates,
        )
        if prefetched is not None:
            logger.debug(
                "[gg-state-timing] _prefetch_tikv_hamt group=%d elapsed_ms=%.1f "
                "nodes=%d",
                state_group,
                (time.monotonic() - _gg_prefetch_start) * 1000,
                len(prefetched[0]),
            )
            return prefetched

        # This is an optimization for an incremental update, not the
        # authoritative read path. A predecessor without a published root can
        # be a newly created/legacy group, or be in its publication window;
        # callers can safely rebuild from their full state map in either case.
        # Do not wait here: a write must not block on a fake test clock (or
        # turn a recoverable missed optimization into a failed persistence).
        return {}, {}

    def _persist_state_hamt_txn(
        self,
        txn: LoggingTransaction,
        state_group: int,
        room_id: str,
        room_prefix: bytes,
        current_state_ids: StateMap[str] | None,
        prev_state_group: int | None = None,
        updates: list[tuple[str, str, str]] | None = None,
        local_nodes: dict[bytes, bytes] | None = None,
        local_roots: dict[int, tuple[bytes, bytes]] | None = None,
    ) -> tuple[bytes, bytes, list[tuple[bytes, bytes]]]:
        """Persist a new state_group's HAMT root and nodes.

        If `prev_state_group` has a usable stored root+lattice and
        `updates` names the `(event_type, state_key, event_id)` changes
        that produce `current_state_ids` from `prev_state_group`'s state
        (a delta of any size -- one key for a plain state event, several
        for a state-resolution/merge result whose delta the caller already
        computed), this applies them via O(K log S) path-copying
        (`apply_flat_state_updates`) instead of rebuilding the whole tree
        from `current_state_ids` -- for either backend (SQL or TiKV).
        Otherwise (no prev root/lattice -- e.g. a room's first state
        group -- or no delta given, i.e. the caller only has a full
        current_state_ids map with no known relationship to prev_group)
        this falls back to a full rebuild, exactly as before.

        `local_nodes`: an optional cache of hash->node-bytes the caller
        already holds in memory, consulted before hitting SQL/TiKV. This
        matters for a caller (`store_state_deltas_for_batched`) that
        persists a *chain* of state groups within one transaction: in TiKV
        mode, node writes are deferred until just before the whole transaction
        commits (one batched publish before the transaction commits),
        so state group N+1's incremental update, which needs to read state
        group N's just-written root node back, would otherwise find nothing
        in TiKV yet. SQL mode doesn't need this (nodes are visible to later
        reads in the same transaction), but checking `local_nodes` first is
        harmless there too.
        """
        incremental = None
        if prev_state_group is not None and updates is not None:
            incremental = self._persist_state_hamt_incremental_txn(
                txn,
                state_group,
                room_id,
                room_prefix,
                prev_state_group,
                updates,
                local_nodes=local_nodes,
                local_roots=local_roots,
            )
        if incremental is not None:
            return incremental

        _gg_reb_start = time.monotonic()
        if current_state_ids is None:
            if prev_state_group is None:
                raise RuntimeError("A state map is required for an initial state group")
            current_state_ids = dict(
                self._get_state_groups_from_groups_txn(txn, [prev_state_group])[
                    prev_state_group
                ]
            )
            if updates:
                current_state_ids.update(
                    {
                        (event_type, state_key): event_id
                        for event_type, state_key, event_id in updates
                    }
                )

        from synapse.synapse_rust import state_hamt

        root_structural_hash, _state_group_id, root_lattice, nodes = (
            state_hamt.build_root_handle_with_lattice(
                self._state_hamt_secret(),
                room_id,
                self._build_state_hamt_entries(current_state_ids),
            )
        )

        use_tikv = bool(self.tikv_pd_endpoints)
        if not use_tikv:
            # In SQL mode, persist the full node tree into `state_hamt_nodes`.
            # In TiKV mode the nodes (root included) go to TiKV, keyed by
            # content hash.
            self._store_state_hamt_nodes_txn(txn, nodes)
        # The root pointer (state_group -> room_prefix + root_hash) lives in
        # per-instance SQL. `state_group` is a per-database sequence that
        # restarts at 1 for every Synapse instance, so it is NOT globally
        # unique -- keeping this mapping in shared TiKV would let two instances
        # overwrite each other's `hamt:root:<state_group>` pointer whenever they
        # share one cluster. SQL is per-instance, so it is isolated.
        if not use_tikv:
            self.db_pool.simple_insert_txn(
                txn,
                table="state_hamt_roots",
                values={
                    "state_group": state_group,
                    "room_prefix": bytearray(room_prefix),
                    "root_structural_hash": bytearray(root_structural_hash),
                    "root_lattice": bytearray(root_lattice),
                },
            )

        logger.debug(
            "[gg-state-timing] _persist_state_hamt_txn mode=rebuild "
            "group=%d entries=%d elapsed_ms=%.1f",
            state_group,
            len(current_state_ids),
            (time.monotonic() - _gg_reb_start) * 1000,
        )
        return root_structural_hash, root_lattice, nodes

    def _persist_state_hamt_incremental_txn(
        self,
        txn: LoggingTransaction,
        state_group: int,
        room_id: str,
        room_prefix: bytes,
        prev_state_group: int,
        updates: list[tuple[str, str, str]],
        local_nodes: dict[bytes, bytes] | None = None,
        local_roots: dict[int, tuple[bytes, bytes]] | None = None,
    ) -> tuple[bytes, bytes, list[tuple[bytes, bytes]]] | None:
        """Apply `updates` -- a delta of any size, from a single state event
        to a whole state-resolution/merge result the caller already computed
        as a delta -- against `prev_state_group`'s HAMT root via O(K log S)
        path-copying, instead of materializing and rebuilding the whole
        state map. This is the fix for the O(S)-per-update tax described in
        docs/development-gg/persistent-typed-hamt-architecture.md: cost is
        proportional to len(updates), not to the room's total state size.

        Returns None -- signalling the caller to fall back to a full
        rebuild -- if `prev_state_group` has no stored root+lattice to
        update from: a pre-existing root written before this column
        existed, or a room's very first state group.

        Works for both backends. In TiKV mode, node bytes are fetched from
        and (implicitly, via the returned `new_nodes`) flushed to TiKV --
        mirroring `_lookup_state_hamt_from_tikv_txn`'s fetch pattern and
        `_persist_state_hamt_txn`'s existing "SQL stores nodes in-txn, TiKV
        gets them after commit" split -- rather than `state_hamt_nodes`.
        """
        from synapse.synapse_rust import state_hamt

        _gg_inc_start = time.monotonic()
        use_tikv = bool(self.tikv_pd_endpoints)
        local_roots = local_roots or {}
        if use_tikv and prev_state_group in local_roots:
            prev_root_hash, prev_lattice = local_roots[prev_state_group]
        elif use_tikv:
            from synapse.synapse_rust import tikv_engine

            root_value = tikv_engine.get(
                _state_hamt_root_tikv_key(self.tikv_namespace, prev_state_group)
            )
            if root_value is None:
                return None
            (
                _stored_prefix,
                prev_root_hash,
                prev_lattice,
                _stored_room_id,
            ) = _decode_state_hamt_root(root_value)
        else:
            prev_root = self.db_pool.simple_select_one_txn(
                txn,
                table="state_hamt_roots",
                keyvalues={"state_group": prev_state_group},
                retcols=("root_structural_hash", "root_lattice"),
                allow_none=True,
            )
            if prev_root is None or prev_root[1] is None:
                return None
            prev_root_hash, prev_lattice = bytes(prev_root[0]), bytes(prev_root[1])

        local_nodes = local_nodes or {}
        root_node_bytes = local_nodes.get(prev_root_hash)
        if root_node_bytes is None:
            if use_tikv:
                from synapse.synapse_rust import tikv_engine

                root_node_bytes = tikv_engine.get(
                    _state_hamt_node_tikv_key(
                        self.tikv_namespace, room_prefix, prev_root_hash
                    )
                )
            else:
                root_node_bytes = self.db_pool.simple_select_one_onecol_txn(
                    txn,
                    table="state_hamt_nodes",
                    keyvalues={"structural_hash": bytearray(prev_root_hash)},
                    retcol="node_bytes",
                    allow_none=True,
                )
        if root_node_bytes is None:
            raise RuntimeError(
                "Missing HAMT root node for state group "
                f"{prev_state_group}: {prev_root_hash.hex()}"
            )
        root_bytes = bytes(root_node_bytes)
        nodes: dict[bytes, bytes] = dict(local_nodes)
        nodes[prev_root_hash] = root_bytes

        # Mirrors _lookup_state_hamt_from_postgres_txn /
        # _lookup_state_hamt_from_tikv_txn's retry loop: each round trip
        # surfaces one more tree level's worth of missing hashes, rather
        # than fetching the whole reachable tree up front.
        while True:
            applied, missing = state_hamt.apply_flat_state_updates(
                self._state_hamt_secret(),
                room_id,
                root_bytes,
                list(nodes.items()),
                prev_lattice,
                updates,
            )
            if applied is not None:
                break
            missing = [
                bytes(node_hash)
                for node_hash in missing
                if bytes(node_hash) not in nodes
            ]
            if not missing:
                raise RuntimeError(
                    "apply_flat_state_updates reported no progress for state group "
                    f"{prev_state_group}"
                )
            if use_tikv:
                from synapse.synapse_rust import tikv_engine

                key_to_hash = {
                    _state_hamt_node_tikv_key(
                        self.tikv_namespace, room_prefix, node_hash
                    ): node_hash
                    for node_hash in missing
                }
                rows = [
                    (key_to_hash[bytes(node_key)], bytes(node_bytes))
                    for node_key, node_bytes in tikv_engine.batch_get(list(key_to_hash))
                ]
            else:
                rows = self.db_pool.simple_select_many_txn(
                    txn,
                    table="state_hamt_nodes",
                    column="structural_hash",
                    iterable=[bytearray(node_hash) for node_hash in missing],
                    keyvalues={},
                    retcols=("structural_hash", "node_bytes"),
                )
            found = {
                bytes(node_hash): bytes(node_bytes) for node_hash, node_bytes in rows
            }
            nodes.update(found)
            unresolved = set(missing) - found.keys()
            if unresolved:
                raise RuntimeError(
                    "Missing HAMT child nodes for state group "
                    f"{prev_state_group}: {[node_hash.hex() for node_hash in unresolved]}"
                )

        new_root_hash, _new_state_group_id, new_lattice, new_nodes = applied

        if not use_tikv:
            # In TiKV mode, nodes are published before the surrounding SQL
            # transaction commits, after all groups in the batch are built.
            self._store_state_hamt_nodes_txn(txn, new_nodes)
        if not use_tikv:
            self.db_pool.simple_insert_txn(
                txn,
                table="state_hamt_roots",
                values={
                    "state_group": state_group,
                    "room_prefix": bytearray(room_prefix),
                    "root_structural_hash": bytearray(new_root_hash),
                    "root_lattice": bytearray(new_lattice),
                },
            )
        logger.debug(
            "[gg-state-timing] _persist_state_hamt_incremental_txn "
            "group=%d prev=%d updates=%d nodes=%d elapsed_ms=%.1f",
            state_group,
            prev_state_group,
            len(updates),
            len(new_nodes),
            (time.monotonic() - _gg_inc_start) * 1000,
        )
        return bytes(new_root_hash), new_lattice, new_nodes

    def _store_state_hamt_nodes_txn(
        self,
        txn: LoggingTransaction,
        nodes: list[tuple[bytes, bytes]],
    ) -> None:
        txn.executemany(
            """
            INSERT INTO state_hamt_nodes (structural_hash, node_bytes)
            VALUES (?, ?)
            ON CONFLICT (structural_hash) DO NOTHING
            """,
            [
                (bytearray(structural_hash), bytearray(node_bytes))
                for structural_hash, node_bytes in nodes
            ],
        )

    def _persist_state_group_snapshot_txn(
        self,
        txn: LoggingTransaction,
        state_group: int,
        room_id: str,
        room_prefix: bytes,
        event_id: str,
        current_state_ids: StateMap[str] | None,
        prev_group: int | None = None,
        updates: list[tuple[str, str, str]] | None = None,
        local_nodes: dict[bytes, bytes] | None = None,
        local_roots: dict[int, tuple[bytes, bytes]] | None = None,
    ) -> tuple[bytes, bytes, list[tuple[bytes, bytes]]]:
        self.db_pool.simple_insert_txn(
            txn,
            table="state_groups",
            values={"id": state_group, "room_id": room_id, "event_id": event_id},
        )

        if prev_group is not None:
            # `state_group_edges` is lifecycle ancestry metadata for purge and
            # deletion safety. The live state payload now lives in HAMT/TiKV,
            # so this edge is only ancestry metadata.
            self.db_pool.simple_insert_txn(
                txn,
                table="state_group_edges",
                values={
                    "state_group": state_group,
                    "prev_state_group": prev_group,
                },
            )

        if current_state_ids is not None:
            current_member_state_ids = {
                s: ev
                for (s, ev) in current_state_ids.items()
                if s[0] == EventTypes.Member
            }
            txn.call_after(
                self._state_group_members_cache.update,
                self._state_group_members_cache.sequence,
                key=state_group,
                value=current_member_state_ids,
            )

            current_non_member_state_ids = {
                s: ev
                for (s, ev) in current_state_ids.items()
                if s[0] != EventTypes.Member
            }
            txn.call_after(
                self._state_group_cache.update,
                self._state_group_cache.sequence,
                key=state_group,
                value=current_non_member_state_ids,
            )

        return self._persist_state_hamt_txn(
            txn,
            state_group,
            room_id,
            room_prefix,
            current_state_ids,
            prev_state_group=prev_group,
            updates=updates,
            local_nodes=local_nodes,
            local_roots=local_roots,
        )

    def _publish_state_hamt_objects_before_commit(
        self,
        room_prefix: bytes,
        nodes: list[tuple[bytes, bytes]],
        roots: list[tuple[bytes, bytes]],
    ) -> None:
        """Publish immutable nodes, then roots, before SQL visibility.

        This is called inside ``runInteraction``. If the database retries the
        callback after an OperationalError or deadlock, the same TiKV writes
        may run again. That is safe: nodes are content-addressed and roots are
        overwritten at their immutable state-group key. A failed SQL commit
        can leave unreachable TiKV objects, which are safe to retain.
        """

        worker_started, worker_finished, nodes_elapsed_ms, roots_elapsed_ms = (
            put_state_hamt_objects(
                self.tikv_namespace,
                room_prefix,
                nodes,
                roots,
                True,
            )
        )
        logger.debug(
            "[gg-state-timing] state_hamt_precommit_publish "
            "nodes=%d roots=%d total_ms=%.1f nodes_put_ms=%.1f roots_put_ms=%.1f",
            len(nodes),
            len(roots),
            (worker_finished - worker_started) * 1000,
            nodes_elapsed_ms,
            roots_elapsed_ms,
        )
        state_hamt_precommit_publish_timer.labels(
            **{SERVER_NAME_LABEL: self.hs.hostname}
        ).observe(worker_finished - worker_started)

    @trace
    @tag_args
    async def store_state_deltas_for_batched(
        self,
        events_and_context: list[tuple[EventBase, UnpersistedEventContext]],
        room_id: str,
        prev_group: int,
    ) -> list[tuple[EventBase, UnpersistedEventContext]]:
        """Generate and store state groups for a batch of events.

        Note that all the events must be in a linear chain (ie a <- b <- c).

        Args:
            events_and_context: the events to generate and store a state groups for
            and their associated contexts
            room_id: the id of the room the events were created for
            prev_group: the state group of the last event persisted before the batched events
            were created
        """

        # All events in the batch are in the same room (and hence share the
        # same, immutable room_version) -- see the linear-chain requirement
        # above. Read it off the first event rather than looking it up by
        # room_id, for the same reason as store_state_group: no DB read, no
        # race against a `rooms` row that may not be visible on this
        # connection yet.
        room_version = events_and_context[0][0].room_version

        from synapse.synapse_rust import state_hamt

        room_prefix = state_hamt.room_tikv_prefix(
            self._state_hamt_secret(),
            room_id,
            room_version.msc4291_room_ids_as_hashes,
        )

        initial_nodes: dict[bytes, bytes] = {}
        initial_roots: dict[int, tuple[bytes, bytes]] = {}
        if self.tikv_pd_endpoints:
            batch_updates = [
                (event.type, event.state_key, event.event_id)
                for event, _context in events_and_context
                if event.is_state()
            ]
            initial_nodes, initial_roots = await self._prefetch_tikv_hamt(
                room_prefix, prev_group, room_id, batch_updates
            )

        def insert_deltas_group_txn(
            txn: LoggingTransaction,
            events_and_context: list[tuple[EventBase, UnpersistedEventContext]],
            prev_group: int,
        ) -> tuple[
            list[tuple[EventBase, UnpersistedEventContext]],
            list[tuple[int, bytes, bytes, list[tuple[bytes, bytes]]]],
        ]:
            """Generate and store state groups for the provided events and contexts.

            Requires that we have the state as a delta from the last persisted state group.

            Returns:
                A list of state groups
            """

            # We need to check that the prev group isn't about to be deleted
            is_missing = (
                self._state_deletion_store._check_state_groups_and_bump_deletion_txn(
                    txn,
                    {prev_group},
                )
            )
            if is_missing:
                raise Exception(
                    "Trying to persist state with unpersisted prev_group: %r"
                    % (prev_group,)
                )

            num_state_groups = sum(
                1 for event, _ in events_and_context if event.is_state()
            )

            state_groups = self._state_group_seq_gen.get_next_mult_txn(
                txn, num_state_groups
            )

            sg_before = prev_group
            state_group_iter = iter(state_groups)
            hamt_writes: list[tuple[int, bytes, bytes, list[tuple[bytes, bytes]]]] = []
            # Nodes for state groups persisted earlier *in this same batch*
            # aren't necessarily visible in TiKV yet -- TiKV writes are
            # deferred to a single flush after this whole transaction
            # commits (see below) -- so a later group's incremental update
            # needs this in-memory cache to find its predecessor's root,
            # rather than reading (nothing) back from TiKV mid-transaction.
            local_nodes = dict(initial_nodes)
            local_roots = dict(initial_roots)

            for event, context in events_and_context:
                if not event.is_state():
                    context.state_group_after_event = sg_before
                    context.state_group_before_event = sg_before
                    continue

                sg_after = next(state_group_iter)
                context.state_group_after_event = sg_after
                context.state_group_before_event = sg_before
                context.state_delta_due_to_event = {
                    (event.type, event.state_key): event.event_id
                }
                root_hash, lattice, nodes = self._persist_state_group_snapshot_txn(
                    txn,
                    sg_after,
                    room_id,
                    room_prefix,
                    event.event_id,
                    None,
                    prev_group=sg_before,
                    # A linear batch changes exactly one (type, state_key)
                    # per state event -- this is the delta
                    # _persist_state_hamt_txn needs to try an O(log S)
                    # incremental update against sg_before's HAMT root
                    # instead of rebuilding from all of current_state_ids.
                    updates=[(event.type, event.state_key, event.event_id)],
                    local_nodes=local_nodes,
                    local_roots=local_roots,
                )
                hamt_writes.append((sg_after, root_hash, lattice, nodes))
                local_nodes.update(nodes)
                local_roots[sg_after] = (root_hash, lattice)
                sg_before = sg_after

            # Publish all batched state groups to TiKV BEFORE the SQL
            # transaction commits, so no reader can observe a
            # state_groups row whose TiKV root is absent.
            if self.tikv_pd_endpoints and hamt_writes:
                all_nodes = [node for _, _, _, nodes in hamt_writes for node in nodes]
                roots = [
                    (
                        _state_hamt_root_tikv_key(self.tikv_namespace, group),
                        _encode_state_hamt_root(
                            room_prefix, root_hash, lattice, room_id=room_id
                        ),
                    )
                    for group, root_hash, lattice, _ in hamt_writes
                ]
                self._publish_state_hamt_objects_before_commit(
                    room_prefix,
                    all_nodes,
                    roots,
                )

            return events_and_context, hamt_writes

        events_and_context, hamt_writes = await self.db_pool.runInteraction(
            "store_state_deltas_for_batched.insert_deltas_group",
            insert_deltas_group_txn,
            events_and_context,
            prev_group,
        )

        return events_and_context

    @trace
    @tag_args
    async def store_state_group(
        self,
        event_id: str,
        room_id: str,
        room_version: RoomVersion,
        prev_group: int | None,
        delta_ids: StateMap[str] | None,
        current_state_ids: StateMap[str] | None,
    ) -> int:
        """Store a new state snapshot, returning a newly assigned state group.

        At least one of `current_state_ids` and `prev_group` must be provided.

        Args:
            event_id: The event ID for which the state was calculated
            room_id
            room_version: The version of the room `room_id` is in. Passed
                explicitly rather than looked up, to avoid a lookup that can
                race the `rooms` row for a room not yet visible here -- see
                _put_state_hamt_objects_after_txn.
            prev_group: A previous state group for the room.
            delta_ids: The delta between state at `prev_group` and
                `current_state_ids`, if `prev_group` was given. Same format as
                `current_state_ids`.
            current_state_ids: The state to store. Map of (type, state_key)
                to event_id.

        Returns:
            The state group ID
        """
        _gg_store_start = time.monotonic()

        if prev_group is None and current_state_ids is None:
            raise Exception("current_state_ids and prev_group can't both be None")

        # `updates` is the delta from prev_group's state to current_state_ids,
        # for _persist_state_hamt_txn to apply via O(K log S) path-copying
        # instead of rebuilding the whole tree. It falls back to a full
        # rebuild inside _persist_state_hamt_txn regardless if prev_group has
        # no usable stored root+lattice (e.g. the room's first state group).
        updates: list[tuple[str, str, str]] | None = None

        if current_state_ids is None:
            assert prev_group is not None
            assert delta_ids is not None
            groups = await self._get_state_for_groups([prev_group])
            current_state_ids = dict(groups[prev_group])
            current_state_ids.update(delta_ids)
            # delta_ids already *is* the delta here -- no need to diff.
            updates = [
                (event_type, state_key, event_id)
                for (event_type, state_key), event_id in delta_ids.items()
            ]
        elif prev_group is not None:
            if delta_ids is None:
                raise ValueError(
                    "A state-group delta is required when prev_group is provided"
                )
            updates = [
                (event_type, state_key, event_id)
                for (event_type, state_key), event_id in delta_ids.items()
            ]

        from synapse.synapse_rust import state_hamt

        room_prefix = state_hamt.room_tikv_prefix(
            self._state_hamt_secret(),
            room_id,
            room_version.msc4291_room_ids_as_hashes,
        )

        initial_nodes: dict[bytes, bytes] = {}
        initial_roots: dict[int, tuple[bytes, bytes]] = {}
        if self.tikv_pd_endpoints and prev_group is not None and updates is not None:
            initial_nodes, initial_roots = await self._prefetch_tikv_hamt(
                room_prefix, prev_group, room_id, updates
            )

        def insert_full_state_txn(
            txn: LoggingTransaction, current_state_ids: StateMap[str]
        ) -> tuple[int, bytes, bytes, list[tuple[bytes, bytes]]]:
            if prev_group is not None:
                is_missing = self._state_deletion_store._check_state_groups_and_bump_deletion_txn(
                    txn,
                    {prev_group},
                )
                if is_missing:
                    raise Exception(
                        "Trying to persist state with unpersisted prev_group: %r"
                        % (prev_group,)
                    )

            state_group = self._state_group_seq_gen.get_next_id_txn(txn)
            root_structural_hash, lattice, nodes = (
                self._persist_state_group_snapshot_txn(
                    txn,
                    state_group,
                    room_id,
                    room_prefix,
                    event_id,
                    current_state_ids,
                    prev_group=prev_group,
                    updates=updates,
                    local_nodes=initial_nodes,
                    local_roots=initial_roots,
                )
            )

            # Publish to TiKV BEFORE the SQL transaction commits.  This
            # closes the publication race: no reader can observe the
            # state_groups row until TiKV already has the root + nodes.
            # If TiKV write fails, the exception aborts this callback,
            # the SQL transaction rolls back, and no state_groups row is
            # visible. Orphaned immutable TiKV objects from a succeeded
            # TiKV write / rolled-back SQL commit are safe to retain; a
            # future reachability GC may reclaim them.
            if self.tikv_pd_endpoints:
                self._publish_state_hamt_objects_before_commit(
                    room_prefix,
                    nodes,
                    [
                        (
                            _state_hamt_root_tikv_key(self.tikv_namespace, state_group),
                            _encode_state_hamt_root(
                                room_prefix,
                                root_structural_hash,
                                lattice,
                                room_id=room_id,
                            ),
                        )
                    ],
                )

            return state_group, root_structural_hash, lattice, nodes

        state_group, root_hash, lattice, nodes = await self.db_pool.runInteraction(
            "store_state_group.insert_full_state",
            insert_full_state_txn,
            current_state_ids,
        )

        logger.debug(
            "[gg-state-timing] store_state_group group=%d elapsed_ms=%.1f",
            state_group,
            (time.monotonic() - _gg_store_start) * 1000,
        )
        return state_group

    async def purge_unreferenced_state_groups(
        self,
        room_id: str,
        state_groups_to_sequence_numbers: Mapping[int, int],
    ) -> bool:
        """Deletes no longer referenced state groups and de-deltas any state
        groups that reference them.

        Args:
            room_id: The room the state groups belong to (must all be in the
                same room).
            state_groups_to_delete: Set of all state groups to delete.

        Returns:
            Whether any state groups were actually deleted.
        """

        deleted, state_groups = await self.db_pool.runInteraction(
            "purge_unreferenced_state_groups",
            self._purge_unreferenced_state_groups,
            room_id,
            state_groups_to_sequence_numbers,
        )
        if self.tikv_pd_endpoints and state_groups:
            from synapse.synapse_rust import tikv_engine

            await defer_to_thread(
                self.hs.get_reactor(),
                tikv_engine.batch_delete,
                [
                    _state_hamt_root_tikv_key(self.tikv_namespace, int(state_group))
                    for state_group in state_groups
                ],
            )
        return deleted

    def _purge_unreferenced_state_groups(
        self,
        txn: LoggingTransaction,
        room_id: str,
        state_groups_to_sequence_numbers: Mapping[int, int],
    ) -> tuple[bool, set[int]]:
        state_groups_to_delete = self._state_deletion_store.get_state_groups_ready_for_potential_deletion_txn(
            txn, state_groups_to_sequence_numbers
        )

        if not state_groups_to_delete:
            return False, set()

        logger.info(
            "[purge] found %i state groups to delete", len(state_groups_to_delete)
        )

        rows = cast(
            list[tuple[int]],
            self.db_pool.simple_select_many_txn(
                txn,
                table="state_group_edges",
                column="prev_state_group",
                iterable=state_groups_to_delete,
                keyvalues={},
                retcols=("state_group",),
            ),
        )

        remaining_state_groups = {
            state_group
            for (state_group,) in rows
            if state_group not in state_groups_to_delete
        }

        logger.info(
            "[purge] de-delta-ing %i remaining state groups",
            len(remaining_state_groups),
        )

        # Now we turn the state groups that reference to-be-deleted state
        # groups to non delta versions.
        for sg in remaining_state_groups:
            logger.info("[purge] de-delta-ing remaining state group %s", sg)
            curr_state_by_group = self._get_state_groups_from_groups_txn(txn, [sg])
            curr_state = curr_state_by_group[sg]

            self.db_pool.simple_delete_txn(
                txn, table="state_groups_state", keyvalues={"state_group": sg}
            )

            self.db_pool.simple_delete_txn(
                txn, table="state_group_edges", keyvalues={"state_group": sg}
            )

            self.db_pool.simple_insert_many_txn(
                txn,
                table="state_groups_state",
                keys=("state_group", "room_id", "type", "state_key", "event_id"),
                values=[
                    (sg, room_id, key[0], key[1], state_id)
                    for key, state_id in curr_state.items()
                ],
            )

        logger.info("[purge] removing redundant state groups")
        txn.execute_batch(
            "DELETE FROM state_groups_state WHERE state_group = ?",
            [(sg,) for sg in state_groups_to_delete],
        )
        txn.execute_batch(
            "DELETE FROM state_group_edges WHERE state_group = ?",
            [(sg,) for sg in state_groups_to_delete],
        )
        txn.execute_batch(
            "DELETE FROM state_groups WHERE id = ?",
            [(sg,) for sg in state_groups_to_delete],
        )
        txn.execute_batch(
            "DELETE FROM state_groups_pending_deletion WHERE state_group = ?",
            [(sg,) for sg in state_groups_to_delete],
        )

        # The root pointer lives in per-instance SQL `state_hamt_roots` in
        # both SQL and TiKV mode, so delete it there. The `state_hamt_nodes`
        # objects themselves (and TiKV `hamt:node:*`) are content-addressed
        # and may be shared by other, still-live roots, so they are
        # intentionally retained rather than reference-counted/GC'd here.
        # This trades some unreachable node storage for avoiding an unsafe
        # delete of a node another root still points to.
        txn.execute_batch(
            "DELETE FROM state_hamt_roots WHERE state_group = ?",
            [(sg,) for sg in state_groups_to_delete],
        )

        return True, set(state_groups_to_delete)

    @trace
    @tag_args
    async def get_previous_state_groups(
        self, state_groups: Iterable[int]
    ) -> dict[int, int]:
        """Fetch the previous groups of the given state groups.

        Args:
            state_groups

        Returns:
            A mapping from state group to previous state group.
        """

        rows = cast(
            list[tuple[int, int]],
            await self.db_pool.simple_select_many_batch(
                table="state_group_edges",
                column="state_group",
                iterable=state_groups,
                keyvalues={},
                retcols=("state_group", "prev_state_group"),
                desc="get_previous_state_groups",
            ),
        )

        return dict(rows)

    @trace
    @tag_args
    async def get_next_state_groups(
        self, state_groups: Iterable[int]
    ) -> dict[int, int]:
        """Fetch the groups that have the given state groups as their previous
        state groups.

        Args:
            state_groups

        Returns:
            A mapping from state group to previous state group.
        """

        rows = cast(
            list[tuple[int, int]],
            await self.db_pool.simple_select_many_batch(
                table="state_group_edges",
                column="prev_state_group",
                iterable=state_groups,
                keyvalues={},
                retcols=("state_group", "prev_state_group"),
                desc="get_next_state_groups",
            ),
        )

        return dict(rows)

    async def purge_room_state(self, room_id: str) -> None:
        state_groups = await self.db_pool.simple_select_onecol(
            table="state_groups",
            keyvalues={"room_id": room_id},
            retcol="id",
            desc="get_state_groups_for_room_purge",
        )
        await self.db_pool.runInteraction(
            "purge_room_state",
            self._purge_room_state_txn,
            room_id,
            state_groups,
        )
        if self.tikv_pd_endpoints and state_groups:
            from synapse.synapse_rust import tikv_engine

            await defer_to_thread(
                self.hs.get_reactor(),
                tikv_engine.batch_delete,
                [
                    _state_hamt_root_tikv_key(self.tikv_namespace, int(state_group))
                    for state_group in state_groups
                ],
            )

    def _purge_room_state_txn(
        self,
        txn: LoggingTransaction,
        room_id: str,
        state_groups: Sequence[int] = (),
    ) -> None:
        # Delete all edges that reference a state group linked to room_id
        logger.info("[purge] removing %s from state_group_edges", room_id)

        if isinstance(self.database_engine, PostgresEngine):
            # Disable statement timeouts for this transaction; purging rooms can
            # take a while!
            txn.execute("SET LOCAL statement_timeout = 0")

        txn.execute(
            """
            DELETE FROM state_group_edges AS sge WHERE sge.state_group IN (
                SELECT id FROM state_groups AS sg WHERE sg.room_id = ?
            )""",
            (room_id,),
        )

        # state_groups_state table has a room_id column but no index on it, unlike state_groups,
        # so we delete them by matching the room_id through the state_groups table.
        logger.info("[purge] removing %s from state_groups_state", room_id)
        txn.execute(
            """
            DELETE FROM state_groups_state AS sgs WHERE sgs.state_group IN (
                SELECT id FROM state_groups AS sg WHERE sg.room_id = ?
            )""",
            (room_id,),
        )

        # Delete HAMT root pointers for this room's state groups before the
        # state_groups rows themselves are removed below. This runs against
        # the live room_id predicate inside the same transaction (rather than
        # the `state_groups` list pre-fetched by the caller) so a state group
        # created concurrently, after that pre-fetch but before this
        # transaction started, still has its root cleaned up.
        logger.info("[purge] removing %s from state_hamt_roots", room_id)
        txn.execute(
            """
            DELETE FROM state_hamt_roots AS shr WHERE shr.state_group IN (
                SELECT id FROM state_groups AS sg WHERE sg.room_id = ?
            )""",
            (room_id,),
        )

        logger.info("[purge] removing %s from state_groups", room_id)
        self.db_pool.simple_delete_txn(
            txn,
            table="state_groups",
            keyvalues={"room_id": room_id},
        )
