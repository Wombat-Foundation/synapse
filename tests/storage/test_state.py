#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2018-2021 The Matrix.org Foundation C.I.C.
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

import json
import logging
from typing import cast
from unittest.mock import patch

from immutabledict import immutabledict

from twisted.internet.testing import MemoryReactor

from synapse.api.constants import EventTypes, Membership
from synapse.api.room_versions import RoomVersions
from synapse.events import EventBase
from synapse.events.snapshot import UnpersistedEventContext
from synapse.server import HomeServer
from synapse.types import JsonDict, RoomID, StateMap, UserID, create_requester
from synapse.types.state import StateFilter
from synapse.util.clock import Clock
from synapse.util.stringutils import random_string

from tests.unittest import HomeserverTestCase

logger = logging.getLogger(__name__)


# Selects the TiKV branch in unit tests. It is never passed to open_client or
# used as a network address; integration tests get real PD endpoints from
# SYNAPSE_TEST_TIKV_PD_ENDPOINTS via the homeserver configuration.
_MOCK_TIKV_ENABLED = object()


class StateStoreTestCase(HomeserverTestCase):
    def _enable_mock_tikv(self) -> None:
        self.state_datastore.tikv_pd_endpoints = cast(list[str], _MOCK_TIKV_ENABLED)

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main
        self.storage = hs.get_storage_controllers()
        self.state_datastore = self.storage.state.stores.state
        self.event_builder_factory = hs.get_event_builder_factory()
        self.event_creation_handler = hs.get_event_creation_handler()

        self.u_alice = UserID.from_string("@alice:test")
        self.u_bob = UserID.from_string("@bob:test")

        self.room = RoomID.from_string("!abc123:test")

        self.get_success(
            self.store.store_room(
                self.room.to_string(),
                room_creator_user_id="@creator:text",
                is_public=True,
                room_version=RoomVersions.V1,
            )
        )

    def inject_state_event(
        self, room: RoomID, sender: UserID, typ: str, state_key: str, content: JsonDict
    ) -> EventBase:
        builder = self.event_builder_factory.for_room_version(
            RoomVersions.V1,
            {
                "type": typ,
                "sender": sender.to_string(),
                "state_key": state_key,
                "room_id": room.to_string(),
                "content": content,
            },
        )

        event, unpersisted_context = self.get_success(
            self.event_creation_handler.create_new_client_event(builder)
        )

        context = self.get_success(unpersisted_context.persist(event))

        assert self.storage.persistence is not None
        self.get_success(self.storage.persistence.persist_event(event, context))

        return event

    def assertStateMapEqual(
        self, s1: StateMap[EventBase], s2: StateMap[EventBase]
    ) -> None:
        for t in s1:
            # just compare event IDs for simplicity
            self.assertEqual(s1[t].event_id, s2[t].event_id)
        self.assertEqual(len(s1), len(s2))

    def test_get_state_groups_ids(self) -> None:
        e1 = self.inject_state_event(self.room, self.u_alice, EventTypes.Create, "", {})
        e2 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Name, "", {"name": "test room"}
        )

        state_group_map = self.get_success(
            self.storage.state.get_state_groups_ids(
                self.room.to_string(), [e2.event_id]
            )
        )
        self.assertEqual(len(state_group_map), 1)
        state_map = list(state_group_map.values())[0]
        self.assertDictEqual(
            state_map,
            {(EventTypes.Create, ""): e1.event_id, (EventTypes.Name, ""): e2.event_id},
        )

    def test_state_group_reads_use_hamt_by_default(self) -> None:
        e1 = self.inject_state_event(self.room, self.u_alice, EventTypes.Create, "", {})
        e2 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Name, "", {"name": "test room"}
        )

        state_group = self.get_success(
            self.store._get_state_group_for_event(e2.event_id)
        )
        assert state_group is not None

        self.get_success(
            self.store.db_pool.simple_delete(
                table="state_groups_state",
                keyvalues={"state_group": state_group},
                desc="test_state_group_reads_use_hamt_by_default",
            )
        )

        # The active refactor path materializes state from HAMT first. Removing
        # the legacy SQL snapshot rows should not affect state-group reads.
        state_group_map = self.get_success(
            self.storage.state.stores.state._get_state_groups_from_groups(
                [state_group], StateFilter.all()
            )
        )

        self.assertDictEqual(
            state_group_map[state_group],
            {(EventTypes.Create, ""): e1.event_id, (EventTypes.Name, ""): e2.event_id},
        )

    def test_exact_state_filter_uses_selective_hamt_lookup(self) -> None:
        create = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        name = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Name, "", {"name": "test room"}
        )
        topic = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Topic, "", {"topic": "test topic"}
        )
        state_group = self.get_success(
            self.store._get_state_group_for_event(topic.event_id)
        )
        assert state_group is not None

        materialize_method = (
            "_materialize_state_hamt_from_tikv"
            if self.state_datastore.tikv_pd_endpoints
            else "_materialize_state_hamt_from_postgres_txn"
        )
        with patch.object(
            self.state_datastore,
            materialize_method,
            side_effect=AssertionError("exact filters must not materialize full state"),
        ):
            result = self.get_success(
                self.store.db_pool.runInteraction(
                    "test_exact_state_filter_uses_selective_hamt_lookup",
                    self.state_datastore._get_state_groups_from_groups_txn,
                    [state_group],
                    StateFilter.from_types(
                        [(EventTypes.Name, ""), (EventTypes.Topic, "")]
                    ),
                )
            )

        self.assertDictEqual(
            result[state_group],
            {
                (EventTypes.Name, ""): name.event_id,
                (EventTypes.Topic, ""): topic.event_id,
            },
        )

        # An empty key set excludes that type while ``include_others`` still
        # selects every non-enumerated type, alongside explicitly requested
        # entries. This must use full materialization rather than accidentally
        # treating the empty set as an exact lookup.
        result = self.get_success(
            self.store.db_pool.runInteraction(
                "test_exact_state_filter_uses_selective_hamt_lookup_include_others",
                self.state_datastore._get_state_groups_from_groups_txn,
                [state_group],
                StateFilter.freeze(
                    {EventTypes.Name: set(), EventTypes.Create: {""}},
                    include_others=True,
                ),
            )
        )
        self.assertDictEqual(
            result[state_group],
            {
                (EventTypes.Create, ""): create.event_id,
                (EventTypes.Topic, ""): topic.event_id,
            },
        )

    def test_empty_state_group_does_not_retry(self) -> None:
        from twisted.internet import defer

        state_group = self.get_success(
            self.state_datastore.store_state_group(
                event_id="$empty-state-group",
                room_id=self.room.to_string(),
                room_version=RoomVersions.V1,
                prev_group=None,
                delta_ids=None,
                current_state_ids={},
            )
        )

        self._enable_mock_tikv()
        try:
            with (
                patch.object(
                    self.state_datastore,
                    "_materialize_state_hamt_from_tikv_direct",
                    return_value=[],
                ),
                patch.object(
                    self.state_datastore.hs.get_clock(),
                    "sleep",
                    return_value=defer.succeed(None),
                ) as sleep,
            ):
                state_group_map = self.get_success(
                    self.state_datastore._get_state_groups_from_groups(
                        [state_group], StateFilter.all()
                    )
                )

                self.assertDictEqual(state_group_map[state_group], {})
                sleep.assert_not_called()
        finally:
            self.state_datastore.tikv_pd_endpoints = []

    def test_state_group_hamt_corruption_does_not_fallback_to_sql(self) -> None:
        if self.state_datastore.tikv_pd_endpoints:
            # This test manipulates the SQL-backed state_hamt_roots/
            # state_hamt_nodes tables directly to simulate corruption. Those
            # tables are only written when TiKV is *not* configured (see
            # _persist_state_hamt_txn) -- when TiKV is in use they stay
            # empty, so this test's own setup doesn't apply. See
            # test_state_group_hamt_corruption_does_not_fallback_to_sql_tikv
            # for the TiKV equivalent.
            self.skipTest("Not applicable when TiKV is configured")

        event = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        state_group = self.get_success(
            self.store._get_state_group_for_event(event.event_id)
        )
        assert state_group is not None

        # Insert a corrupt replacement node, then repoint the root at it. This
        # simulates corrupt node content rather than a missing node row.
        garbage_structural_hash = random_string(16).encode("ascii")
        self.get_success(
            self.store.db_pool.simple_insert(
                table="state_hamt_nodes",
                values={
                    # Postgres rejects raw `bytes` (c.f. matrix-org/synapse#6186);
                    # wrap in `bytearray`, matching the rest of this codebase.
                    "structural_hash": bytearray(garbage_structural_hash),
                    "node_bytes": bytearray(b"not a valid persisted HAMT node"),
                },
                desc="test_state_group_hamt_corruption.insert_garbage_node",
            )
        )
        self.get_success(
            self.store.db_pool.simple_update_one(
                table="state_hamt_roots",
                keyvalues={"state_group": state_group},
                updatevalues={
                    "root_structural_hash": bytearray(garbage_structural_hash)
                },
                desc="test_state_group_hamt_corruption.repoint_root",
            )
        )

        # If a HAMT root exists, missing/corrupt nodes are data corruption.
        # Do not hide that by falling back to the legacy SQL snapshot.
        failure = self.get_failure(
            self.storage.state.stores.state._get_state_groups_from_groups(
                [state_group], StateFilter.all()
            ),
            RuntimeError,
        )
        self.assertIn(
            "Failed to decode persisted HAMT node",
            str(failure.value),
        )

    def test_state_group_hamt_corruption_does_not_fallback_to_sql_tikv(self) -> None:
        """TiKV equivalent of test_state_group_hamt_corruption_does_not_fallback_to_sql.

        When TiKV is configured, the full HAMT trie lives in TiKV (keyed by
        content hash); only the root pointer (state_group -> room_prefix +
        root_hash) lives in per-instance SQL `state_hamt_roots`. So the
        corruption is simulated by inserting an undecodable node into TiKV
        and repointing the SQL root pointer at it.
        """
        if not self.state_datastore.tikv_pd_endpoints:
            self.skipTest("Requires TiKV -- set SYNAPSE_TEST_TIKV_PD_ENDPOINTS to run")

        from synapse.storage.databases.state.bg_updates import _state_hamt_node_tikv_key
        from synapse.synapse_rust import tikv_engine

        event = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        state_group = self.get_success(
            self.store._get_state_group_for_event(event.event_id)
        )
        assert state_group is not None

        # Read the per-instance SQL root pointer.
        row = self.get_success(
            self.store.db_pool.simple_select_one(
                table="state_hamt_roots",
                keyvalues={"state_group": state_group},
                retcols=("room_prefix", "root_structural_hash"),
                allow_none=True,
            )
        )
        assert row is not None, "Expected a HAMT root pointer to exist in SQL"
        room_prefix = bytes(row[0])

        garbage_structural_hash = random_string(16).encode("ascii")
        garbage_node_key = _state_hamt_node_tikv_key(
            self.state_datastore.tikv_namespace, room_prefix, garbage_structural_hash
        )
        tikv_engine.put(garbage_node_key, b"not a valid persisted HAMT node")

        self.get_success(
            self.store.db_pool.simple_update_one(
                table="state_hamt_roots",
                keyvalues={"state_group": state_group},
                updatevalues={
                    "root_structural_hash": bytearray(garbage_structural_hash)
                },
                desc="test_state_group_hamt_corruption.repoint_root",
            )
        )

        # If a HAMT root exists, missing/corrupt nodes are data corruption.
        # Do not hide that by falling back to the legacy SQL snapshot.
        failure = self.get_failure(
            self.storage.state.stores.state._get_state_groups_from_groups(
                [state_group], StateFilter.all()
            ),
            RuntimeError,
        )
        self.assertIn(
            "Failed to decode persisted HAMT node",
            str(failure.value),
        )

    def test_prefetch_tikv_hamt_blocking_missing_child_raises(self) -> None:
        """Verify that _prefetch_tikv_hamt_blocking raises RuntimeError on missing child nodes."""
        from unittest.mock import patch

        from synapse.storage.databases.state.bg_updates import (
            _encode_state_hamt_root,
            _state_hamt_node_tikv_key,
            _state_hamt_root_tikv_key,
        )
        from synapse.synapse_rust import state_hamt

        room_prefix = b"01234567"
        room_id = "!test:test"
        server_secret = self.state_datastore._state_hamt_secret()
        # Build a multi-entry state HAMT with enough events to force child nodes
        entries = [
            (f"org.matrix.test.{i}", f"key_{i}", f"$event_{i}:test") for i in range(100)
        ]
        root_hash, _sg, lattice, nodes = state_hamt.build_root_handle_with_lattice(
            server_secret, room_id, entries
        )
        nodes_dict = dict(nodes)
        self.assertGreater(len(nodes_dict), 1, "Expected multi-node HAMT tree")
        root_bytes = nodes_dict[root_hash]
        encoded_root = _encode_state_hamt_root(
            room_prefix, root_hash, lattice, room_id=room_id
        )

        def mock_get(key: bytes) -> bytes | None:
            if key == _state_hamt_root_tikv_key(
                self.state_datastore.tikv_namespace, 1234
            ):
                return encoded_root
            return None

        # Return ONLY the root node, simulating missing child nodes in TiKV
        def mock_batch_get(keys: list[bytes]) -> list[tuple[bytes, bytes]]:
            res = []
            for k in keys:
                if k == _state_hamt_node_tikv_key(
                    self.state_datastore.tikv_namespace, room_prefix, root_hash
                ):
                    res.append((k, root_bytes))
            return res

        with patch("synapse.synapse_rust.tikv_engine.get", side_effect=mock_get):
            with patch(
                "synapse.synapse_rust.tikv_engine.batch_get", side_effect=mock_batch_get
            ):
                with self.assertRaises(RuntimeError) as ctx:
                    self.state_datastore._prefetch_tikv_hamt_blocking(
                        room_prefix,
                        1234,
                        room_id,
                        [("org.matrix.test.0", "key_0", "$new_event_0:test")],
                    )
                self.assertIn(
                    "Missing HAMT nodes while prefetching", str(ctx.exception)
                )

    def test_direct_tikv_reads_selective_and_full_mocked(self) -> None:
        """Verify direct TiKV read helpers for selective key lookups and full materialization."""
        from unittest.mock import patch

        from synapse.storage.databases.state.bg_updates import (
            _encode_state_hamt_root,
            _state_hamt_node_tikv_key,
            _state_hamt_root_tikv_key,
        )
        from synapse.synapse_rust import state_hamt

        room_prefix = b"01234567"
        room_id = "!test:test"
        server_secret = self.state_datastore._state_hamt_secret()
        entries = [
            (EventTypes.Create, "", "$create:test"),
            (EventTypes.Name, "", "$name:test"),
        ]
        root_hash, _sg, lattice, nodes = state_hamt.build_root_handle_with_lattice(
            server_secret, room_id, entries
        )
        nodes_dict = dict(nodes)
        root_bytes = nodes_dict[root_hash]
        encoded_root = _encode_state_hamt_root(
            room_prefix, root_hash, lattice, room_id=room_id
        )

        def mock_get(key: bytes) -> bytes | None:
            if key == _state_hamt_root_tikv_key(
                self.state_datastore.tikv_namespace, 1234
            ):
                return encoded_root
            if key == _state_hamt_node_tikv_key(
                self.state_datastore.tikv_namespace, room_prefix, root_hash
            ):
                return root_bytes
            return None

        def mock_batch_get(keys: list[bytes]) -> list[tuple[bytes, bytes]]:
            res = []
            for k in keys:
                for h, nb in nodes:
                    if k == _state_hamt_node_tikv_key(
                        self.state_datastore.tikv_namespace, room_prefix, h
                    ):
                        res.append((k, nb))
            return res

        with patch("synapse.synapse_rust.tikv_engine.get", side_effect=mock_get):
            with patch(
                "synapse.synapse_rust.tikv_engine.batch_get", side_effect=mock_batch_get
            ):
                # 1. Selective key lookup (pure TiKV with embedded room_id)
                entries_selective = (
                    self.state_datastore._lookup_state_hamt_from_tikv_direct(
                        1234, [(EventTypes.Name, "")]
                    )
                )
                self.assertIsNotNone(entries_selective)
                assert entries_selective is not None
                self.assertEqual(
                    entries_selective, [(EventTypes.Name, "", "$name:test")]
                )

                # 2. Absent root returns None
                entries_absent = (
                    self.state_datastore._lookup_state_hamt_from_tikv_direct(
                        9999, [(EventTypes.Name, "")]
                    )
                )
                self.assertIsNone(entries_absent)

                # 3. Full materialization
                with patch(
                    "synapse.synapse_rust.tikv_engine.materialize_state_hamt",
                    return_value=entries,
                ):
                    entries_full = (
                        self.state_datastore._materialize_state_hamt_from_tikv_direct(
                            1234
                        )
                    )
                    self.assertIsNotNone(entries_full)
                    assert entries_full is not None
                    self.assertIn((EventTypes.Create, "", "$create:test"), entries_full)
                    self.assertIn((EventTypes.Name, "", "$name:test"), entries_full)

    def test_fallback_sql_does_not_call_tikv_in_transaction(self) -> None:
        """Verify that pure SQL fallback mode (use_tikv=False) makes zero TiKV calls inside runInteraction."""
        from unittest.mock import patch

        event = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        state_group = self.get_success(
            self.store._get_state_group_for_event(event.event_id)
        )
        assert state_group is not None

        with (
            patch("synapse.synapse_rust.tikv_engine.get") as mock_get,
            patch("synapse.synapse_rust.tikv_engine.batch_get") as mock_batch_get,
            patch(
                "synapse.synapse_rust.tikv_engine.materialize_state_hamt"
            ) as mock_mat,
        ):
            self._enable_mock_tikv()
            try:
                sql_res = self.get_success(
                    self.store.db_pool.runInteraction(
                        "test_fallback_sql",
                        self.state_datastore._get_state_groups_from_groups_txn,
                        [state_group],
                        StateFilter.all(),
                        use_tikv=False,
                    )
                )
                self.assertIn(state_group, sql_res)
                mock_get.assert_not_called()
                mock_batch_get.assert_not_called()
                mock_mat.assert_not_called()
            finally:
                self.state_datastore.tikv_pd_endpoints = []

    def test_pure_tikv_avoids_sql_transactions(self) -> None:
        """Verify that pure TiKV reads avoid all SQL transactions during state retrieval."""
        from unittest.mock import patch

        event = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        state_group = self.get_success(
            self.store._get_state_group_for_event(event.event_id)
        )
        assert state_group is not None

        self._enable_mock_tikv()
        try:
            with (
                patch.object(
                    self.state_datastore,
                    "_materialize_state_hamt_from_tikv_direct",
                    return_value=[(EventTypes.Create, "", event.event_id)],
                ),
                patch.object(
                    self.store.db_pool,
                    "runInteraction",
                    wraps=self.store.db_pool.runInteraction,
                ) as spy_run_interaction,
            ):
                res = self.get_success(
                    self.state_datastore._get_state_groups_from_groups(
                        [state_group], StateFilter.all()
                    )
                )
                self.assertEqual(
                    res[state_group], {(EventTypes.Create, ""): event.event_id}
                )
                called_descs = [
                    call.args[0] for call in spy_run_interaction.call_args_list
                ]
                self.assertNotIn("_get_state_groups_from_groups", called_descs)
                self.assertNotIn(
                    "_get_state_groups_from_groups.fallback_sql", called_descs
                )
        finally:
            self.state_datastore.tikv_pd_endpoints = []

    def test_exact_filter_read_avoids_run_interaction(self) -> None:
        """Verify that exact-filter reads under TiKV execute real Rust lookup and avoid state fetch SQL transactions."""
        from unittest.mock import patch

        from synapse.storage.databases.state.bg_updates import (
            _encode_state_hamt_root,
            _state_hamt_node_tikv_key,
            _state_hamt_root_tikv_key,
        )
        from synapse.synapse_rust import state_hamt

        room_prefix = b"01234567"
        room_id = self.room.to_string()
        server_secret = self.state_datastore._state_hamt_secret()
        entries = [
            (EventTypes.Create, "", "$create:test"),
            (EventTypes.Name, "", "$name:test"),
        ]
        root_hash, _sg, lattice, nodes = state_hamt.build_root_handle_with_lattice(
            server_secret, room_id, entries
        )
        nodes_dict = dict(nodes)
        root_bytes = nodes_dict[root_hash]
        encoded_root = _encode_state_hamt_root(
            room_prefix, root_hash, lattice, room_id=room_id
        )

        self.get_success(
            self.store.db_pool.simple_insert(
                table="state_groups",
                values={"id": 88888, "room_id": room_id, "event_id": "$create:test"},
                desc="test_exact_filter.insert_sg",
            )
        )

        def mock_get(key: bytes) -> bytes | None:
            if key == _state_hamt_root_tikv_key(
                self.state_datastore.tikv_namespace, 88888
            ):
                return encoded_root
            if key == _state_hamt_node_tikv_key(
                self.state_datastore.tikv_namespace, room_prefix, root_hash
            ):
                return root_bytes
            return None

        def mock_batch_get(keys: list[bytes]) -> list[tuple[bytes, bytes]]:
            res = []
            for k in keys:
                for h, nb in nodes:
                    if k == _state_hamt_node_tikv_key(
                        self.state_datastore.tikv_namespace, room_prefix, h
                    ):
                        res.append((k, nb))
            return res

        self._enable_mock_tikv()
        try:
            with (
                patch("synapse.synapse_rust.tikv_engine.get", side_effect=mock_get),
                patch(
                    "synapse.synapse_rust.tikv_engine.batch_get",
                    side_effect=mock_batch_get,
                ),
                patch.object(
                    self.store.db_pool,
                    "runInteraction",
                    wraps=self.store.db_pool.runInteraction,
                ) as spy_run_interaction,
            ):
                res = self.get_success(
                    self.state_datastore._get_state_groups_from_groups(
                        [88888],
                        StateFilter.from_types([(EventTypes.Name, "")]),
                    )
                )
                self.assertEqual(res[88888], {(EventTypes.Name, ""): "$name:test"})
                called_descs = [
                    call.args[0] for call in spy_run_interaction.call_args_list
                ]
                self.assertNotIn("_get_state_groups_from_groups", called_descs)
                self.assertNotIn(
                    "_get_state_groups_from_groups.fallback_sql", called_descs
                )
        finally:
            self.state_datastore.tikv_pd_endpoints = []

    def test_transient_missing_tikv_root_retries_and_succeeds(self) -> None:
        """Verify that when a TiKV root is temporarily absent (cross-worker race), it retries and succeeds on 10ms retry."""
        from unittest.mock import patch

        from twisted.internet import defer

        event = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        state_group = self.get_success(
            self.store._get_state_group_for_event(event.event_id)
        )
        assert state_group is not None

        # Return None on attempt 0, then return valid entries on attempt 1
        responses = [None, [(EventTypes.Create, "", event.event_id)]]

        def mock_mat(sg: int) -> list[tuple[str, str, str]] | None:
            if responses:
                return responses.pop(0)
            return [(EventTypes.Create, "", event.event_id)]

        self._enable_mock_tikv()
        try:
            with (
                patch.object(
                    self.state_datastore,
                    "_materialize_state_hamt_from_tikv_direct",
                    side_effect=mock_mat,
                ),
                patch.object(
                    self.state_datastore.hs.get_clock(),
                    "sleep",
                    return_value=defer.succeed(None),
                ) as mock_sleep,
            ):
                res = self.get_success(
                    self.state_datastore._get_state_groups_from_groups(
                        [state_group], StateFilter.all()
                    )
                )
                self.assertEqual(
                    res[state_group], {(EventTypes.Create, ""): event.event_id}
                )
                self.assertEqual(mock_sleep.call_count, 1)
        finally:
            self.state_datastore.tikv_pd_endpoints = []

    def test_existing_unresolved_group_raises(self) -> None:
        """Verify that an existing state group in SQL raises RuntimeError when TiKV root is unresolved."""
        from unittest.mock import patch

        from twisted.internet import defer

        state_group = 999998
        self.get_success(
            self.store.db_pool.simple_insert(
                table="state_groups",
                values={
                    "id": state_group,
                    "room_id": self.room.to_string(),
                    "event_id": "$fake:test",
                },
                desc="test_unresolved.insert_sg",
            )
        )

        self._enable_mock_tikv()
        try:
            with (
                patch("synapse.synapse_rust.tikv_engine.get", return_value=None),
                patch.object(
                    self.state_datastore.hs.get_clock(),
                    "sleep",
                    return_value=defer.succeed(None),
                ),
            ):
                self.get_failure(
                    self.state_datastore._get_state_groups_from_groups(
                        [state_group], StateFilter.all()
                    ),
                    RuntimeError,
                )
        finally:
            self.state_datastore.tikv_pd_endpoints = []

    def test_nonexistent_group_returns_empty_dict(self) -> None:
        """Verify that a nonexistent state group (not in SQL) returns {} without raising."""
        from unittest.mock import patch

        from twisted.internet import defer

        nonexistent_group = 9999991

        self._enable_mock_tikv()
        try:
            with (
                patch("synapse.synapse_rust.tikv_engine.get", return_value=None),
                patch.object(
                    self.state_datastore.hs.get_clock(),
                    "sleep",
                    return_value=defer.succeed(None),
                ),
            ):
                res = self.get_success(
                    self.state_datastore._get_state_groups_from_groups(
                        [nonexistent_group], StateFilter.all()
                    )
                )
                self.assertEqual(res[nonexistent_group], {})
        finally:
            self.state_datastore.tikv_pd_endpoints = []

    def test_mixed_existing_and_nonexistent_groups_under_tikv(self) -> None:
        """Verify that requests with both existing TiKV-retried groups and nonexistent groups return all keys."""
        from unittest.mock import patch

        from twisted.internet import defer

        event = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        state_group = self.get_success(
            self.store._get_state_group_for_event(event.event_id)
        )
        assert state_group is not None
        nonexistent_group = 9999999

        # Simulate state_group missing on attempt 0 then resolved on attempt 1,
        # while nonexistent_group never exists in TiKV or SQL
        responses = [None, [(EventTypes.Create, "", event.event_id)]]

        def mock_mat(sg: int) -> list[tuple[str, str, str]] | None:
            if sg == state_group:
                return (
                    responses.pop(0)
                    if responses
                    else [(EventTypes.Create, "", event.event_id)]
                )
            return None

        self._enable_mock_tikv()
        try:
            with (
                patch.object(
                    self.state_datastore,
                    "_materialize_state_hamt_from_tikv_direct",
                    side_effect=mock_mat,
                ),
                patch.object(
                    self.state_datastore.hs.get_clock(),
                    "sleep",
                    return_value=defer.succeed(None),
                ),
            ):
                res = self.get_success(
                    self.state_datastore._get_state_groups_from_groups(
                        [state_group, nonexistent_group], StateFilter.all()
                    )
                )
                self.assertIn(state_group, res)
                self.assertIn(nonexistent_group, res)
                self.assertEqual(
                    res[state_group], {(EventTypes.Create, ""): event.event_id}
                )
                self.assertEqual(res[nonexistent_group], {})
        finally:
            self.state_datastore.tikv_pd_endpoints = []

    def test_mixed_batch_with_unresolved_existing_group_raises(self) -> None:
        """An unresolved existing group must not be masked by other results."""
        from unittest.mock import patch

        from twisted.internet import defer

        event = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        valid_group = self.get_success(
            self.store._get_state_group_for_event(event.event_id)
        )
        assert valid_group is not None

        unresolved_group = 8888881
        self.get_success(
            self.store.db_pool.simple_insert(
                table="state_groups",
                values={
                    "id": unresolved_group,
                    "room_id": self.room.to_string(),
                    "event_id": "$unresolved:test",
                },
                desc="test_unresolved.insert_sg",
            )
        )
        nonexistent_group = 9999992

        def mock_mat(sg: int) -> list[tuple[str, str, str]] | None:
            if sg == valid_group:
                return [(EventTypes.Create, "", event.event_id)]
            return None

        self._enable_mock_tikv()
        try:
            with (
                patch.object(
                    self.state_datastore,
                    "_materialize_state_hamt_from_tikv_direct",
                    side_effect=mock_mat,
                ),
                patch.object(
                    self.state_datastore.hs.get_clock(),
                    "sleep",
                    return_value=defer.succeed(None),
                ),
            ):
                self.get_failure(
                    self.state_datastore._get_state_groups_from_groups(
                        [valid_group, unresolved_group, nonexistent_group],
                        StateFilter.all(),
                    ),
                    RuntimeError,
                )
        finally:
            self.state_datastore.tikv_pd_endpoints = []

    def test_exact_filter_partial_match_under_pure_tikv(self) -> None:
        """Verify that exact key lookups under pure TiKV return partial matches correctly without SQL transactions."""
        from unittest.mock import patch

        from synapse.storage.databases.state.bg_updates import (
            _encode_state_hamt_root,
            _state_hamt_node_tikv_key,
            _state_hamt_root_tikv_key,
        )
        from synapse.synapse_rust import state_hamt

        room_prefix = b"01234567"
        room_id = self.room.to_string()
        server_secret = self.state_datastore._state_hamt_secret()
        entries = [
            (EventTypes.Create, "", "$create:test"),
            (EventTypes.Name, "", "$name:test"),
            (EventTypes.Topic, "", "$topic:test"),
        ]
        root_hash, _sg, lattice, nodes = state_hamt.build_root_handle_with_lattice(
            server_secret, room_id, entries
        )
        nodes_dict = dict(nodes)
        root_bytes = nodes_dict[root_hash]
        encoded_root = _encode_state_hamt_root(
            room_prefix, root_hash, lattice, room_id=room_id
        )

        state_group = 88889
        self.get_success(
            self.store.db_pool.simple_insert(
                table="state_groups",
                values={
                    "id": state_group,
                    "room_id": room_id,
                    "event_id": "$create:test",
                },
                desc="test_exact_partial.insert_sg",
            )
        )

        def mock_get(key: bytes) -> bytes | None:
            if key == _state_hamt_root_tikv_key(
                self.state_datastore.tikv_namespace, state_group
            ):
                return encoded_root
            if key == _state_hamt_node_tikv_key(
                self.state_datastore.tikv_namespace, room_prefix, root_hash
            ):
                return root_bytes
            return None

        def mock_batch_get(keys: list[bytes]) -> list[tuple[bytes, bytes]]:
            res = []
            for k in keys:
                for h, nb in nodes:
                    if k == _state_hamt_node_tikv_key(
                        self.state_datastore.tikv_namespace, room_prefix, h
                    ):
                        res.append((k, nb))
            return res

        self._enable_mock_tikv()
        try:
            with (
                patch("synapse.synapse_rust.tikv_engine.get", side_effect=mock_get),
                patch(
                    "synapse.synapse_rust.tikv_engine.batch_get",
                    side_effect=mock_batch_get,
                ),
            ):
                # Filter requesting m.room.name (present) and m.room.join_rules (absent)
                res = self.get_success(
                    self.state_datastore._get_state_groups_from_groups(
                        [state_group],
                        StateFilter.from_types(
                            [
                                (EventTypes.Name, ""),
                                (EventTypes.JoinRules, ""),
                            ]
                        ),
                    )
                )
                self.assertEqual(
                    res[state_group], {(EventTypes.Name, ""): "$name:test"}
                )
        finally:
            self.state_datastore.tikv_pd_endpoints = []

    def test_non_v1_root_record_raises_corruption(self) -> None:
        """Verify that any non-v1 root record is treated as corruption and raises RuntimeError."""
        from unittest.mock import patch

        from synapse.storage.databases.state.bg_updates import (
            _state_hamt_root_tikv_key,
        )

        corrupt_root = b"\x02\x00\x0801234567\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        state_group = 88890

        def mock_get(key: bytes) -> bytes | None:
            if key == _state_hamt_root_tikv_key(
                self.state_datastore.tikv_namespace, state_group
            ):
                return corrupt_root
            return None

        self._enable_mock_tikv()
        try:
            with patch("synapse.synapse_rust.tikv_engine.get", side_effect=mock_get):
                self.get_failure(
                    self.state_datastore._get_state_groups_from_groups(
                        [state_group],
                        StateFilter.from_types([(EventTypes.Name, "")]),
                    ),
                    RuntimeError,
                )
        finally:
            self.state_datastore.tikv_pd_endpoints = []

    def test_tikv_hamt_keys_are_namespaced(self) -> None:
        """Independent SQL databases must not share TiKV state-group keys."""
        from synapse.storage.databases.state.bg_updates import (
            _state_hamt_node_tikv_key,
            _state_hamt_root_tikv_key,
        )

        room_prefix = b"01234567"
        structural_hash = b"0123456789abcdef"
        self.assertNotEqual(
            _state_hamt_root_tikv_key("worker-one", 1),
            _state_hamt_root_tikv_key("worker-two", 1),
        )
        self.assertNotEqual(
            _state_hamt_node_tikv_key("worker-one", room_prefix, structural_hash),
            _state_hamt_node_tikv_key("worker-two", room_prefix, structural_hash),
        )

    def test_get_state_groups(self) -> None:
        e1 = self.inject_state_event(self.room, self.u_alice, EventTypes.Create, "", {})
        e2 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Name, "", {"name": "test room"}
        )

        state_group_map = self.get_success(
            self.storage.state.get_state_groups(self.room.to_string(), [e2.event_id])
        )
        self.assertEqual(len(state_group_map), 1)
        state_list = list(state_group_map.values())[0]

        self.assertEqual({ev.event_id for ev in state_list}, {e1.event_id, e2.event_id})

    def test_get_state_for_event(self) -> None:
        # this defaults to a linear DAG as each new injection defaults to whatever
        # forward extremities are currently in the DB for this room.
        e1 = self.inject_state_event(self.room, self.u_alice, EventTypes.Create, "", {})
        e2 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Name, "", {"name": "test room"}
        )
        e3 = self.inject_state_event(
            self.room,
            self.u_alice,
            EventTypes.Member,
            self.u_alice.to_string(),
            {"membership": Membership.JOIN},
        )
        e4 = self.inject_state_event(
            self.room,
            self.u_bob,
            EventTypes.Member,
            self.u_bob.to_string(),
            {"membership": Membership.JOIN},
        )
        e5 = self.inject_state_event(
            self.room,
            self.u_bob,
            EventTypes.Member,
            self.u_bob.to_string(),
            {"membership": Membership.LEAVE},
        )

        # check we get the full state as of the final event
        state = self.get_success(self.storage.state.get_state_for_event(e5.event_id))

        self.assertIsNotNone(e4)

        self.assertStateMapEqual(
            {
                (e1.type, e1.state_key): e1,
                (e2.type, e2.state_key): e2,
                (e3.type, e3.state_key): e3,
                # e4 is overwritten by e5
                (e5.type, e5.state_key): e5,
            },
            state,
        )

        # check we can filter to the m.room.name event (with a '' state key)
        state = self.get_success(
            self.storage.state.get_state_for_event(
                e5.event_id, StateFilter.from_types([(EventTypes.Name, "")])
            )
        )

        self.assertStateMapEqual({(e2.type, e2.state_key): e2}, state)

        # check we can filter to the m.room.name event (with a wildcard None state key)
        state = self.get_success(
            self.storage.state.get_state_for_event(
                e5.event_id, StateFilter.from_types([(EventTypes.Name, None)])
            )
        )

        self.assertStateMapEqual({(e2.type, e2.state_key): e2}, state)

        # check we can grab the m.room.member events (with a wildcard None state key)
        state = self.get_success(
            self.storage.state.get_state_for_event(
                e5.event_id, StateFilter.from_types([(EventTypes.Member, None)])
            )
        )

        self.assertStateMapEqual(
            {(e3.type, e3.state_key): e3, (e5.type, e5.state_key): e5}, state
        )

        # check we can grab a specific room member without filtering out the
        # other event types
        state = self.get_success(
            self.storage.state.get_state_for_event(
                e5.event_id,
                state_filter=StateFilter(
                    types=immutabledict(
                        {EventTypes.Member: frozenset({self.u_alice.to_string()})}
                    ),
                    include_others=True,
                ),
            )
        )

        self.assertStateMapEqual(
            {
                (e1.type, e1.state_key): e1,
                (e2.type, e2.state_key): e2,
                (e3.type, e3.state_key): e3,
            },
            state,
        )

        # check that we can grab everything except members
        state = self.get_success(
            self.storage.state.get_state_for_event(
                e5.event_id,
                state_filter=StateFilter(
                    types=immutabledict({EventTypes.Member: frozenset()}),
                    include_others=True,
                ),
            )
        )

        self.assertStateMapEqual(
            {(e1.type, e1.state_key): e1, (e2.type, e2.state_key): e2}, state
        )

        #######################################################
        # _get_state_for_group_using_cache tests against a full cache
        #######################################################

        room_id = self.room.to_string()
        group_ids = self.get_success(
            self.storage.state.get_state_groups_ids(room_id, [e5.event_id])
        )
        group = list(group_ids.keys())[0]

        # test _get_state_for_group_using_cache correctly filters out members
        # with types=[]
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset()}),
                include_others=True,
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual(
            {
                (e1.type, e1.state_key): e1.event_id,
                (e2.type, e2.state_key): e2.event_id,
            },
            state_dict,
        )

        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_members_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset()}),
                include_others=True,
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual({}, state_dict)

        # test _get_state_for_group_using_cache correctly filters in members
        # with wildcard types
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: None}), include_others=True
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual(
            {
                (e1.type, e1.state_key): e1.event_id,
                (e2.type, e2.state_key): e2.event_id,
            },
            state_dict,
        )

        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_members_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: None}), include_others=True
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual(
            {
                (e3.type, e3.state_key): e3.event_id,
                # e4 is overwritten by e5
                (e5.type, e5.state_key): e5.event_id,
            },
            state_dict,
        )

        # test _get_state_for_group_using_cache correctly filters in members
        # with specific types
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset({e5.state_key})}),
                include_others=True,
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual(
            {
                (e1.type, e1.state_key): e1.event_id,
                (e2.type, e2.state_key): e2.event_id,
            },
            state_dict,
        )

        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_members_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset({e5.state_key})}),
                include_others=True,
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual({(e5.type, e5.state_key): e5.event_id}, state_dict)

        # test _get_state_for_group_using_cache correctly filters in members
        # with specific types
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_members_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset({e5.state_key})}),
                include_others=False,
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual({(e5.type, e5.state_key): e5.event_id}, state_dict)

        #######################################################
        # deliberately remove e2 (room name) from the _state_group_cache

        cache_entry = self.state_datastore._state_group_cache.get(group)
        state_dict_ids = cache_entry.value

        self.assertEqual(cache_entry.full, True)
        self.assertEqual(cache_entry.known_absent, set())
        self.assertDictEqual(
            state_dict_ids,
            {
                (e1.type, e1.state_key): e1.event_id,
                (e2.type, e2.state_key): e2.event_id,
            },
        )

        state_dict_ids.pop((e2.type, e2.state_key))
        self.state_datastore._state_group_cache.invalidate(group)
        self.state_datastore._state_group_cache.update(
            sequence=self.state_datastore._state_group_cache.sequence,
            key=group,
            value=state_dict_ids,
            # list fetched keys so it knows it's partial
            fetched_keys=((e1.type, e1.state_key),),
        )

        cache_entry = self.state_datastore._state_group_cache.get(group)
        state_dict_ids = cache_entry.value

        self.assertEqual(cache_entry.full, False)
        self.assertEqual(cache_entry.known_absent, set())
        self.assertDictEqual(state_dict_ids, {})

        ############################################
        # test that things work with a partial cache

        # test _get_state_for_group_using_cache correctly filters out members
        # with types=[]
        room_id = self.room.to_string()
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset()}),
                include_others=True,
            ),
        )

        self.assertEqual(is_all, False)
        self.assertDictEqual({}, state_dict)

        room_id = self.room.to_string()
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_members_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset()}),
                include_others=True,
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual({}, state_dict)

        # test _get_state_for_group_using_cache correctly filters in members
        # wildcard types
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: None}), include_others=True
            ),
        )

        self.assertEqual(is_all, False)
        self.assertDictEqual({}, state_dict)

        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_members_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: None}), include_others=True
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual(
            {
                (e3.type, e3.state_key): e3.event_id,
                (e5.type, e5.state_key): e5.event_id,
            },
            state_dict,
        )

        # test _get_state_for_group_using_cache correctly filters in members
        # with specific types
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset({e5.state_key})}),
                include_others=True,
            ),
        )

        self.assertEqual(is_all, False)
        self.assertDictEqual({}, state_dict)

        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_members_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset({e5.state_key})}),
                include_others=True,
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual({(e5.type, e5.state_key): e5.event_id}, state_dict)

        # test _get_state_for_group_using_cache correctly filters in members
        # with specific types
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset({e5.state_key})}),
                include_others=False,
            ),
        )

        self.assertEqual(is_all, False)
        self.assertDictEqual({}, state_dict)

        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_members_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset({e5.state_key})}),
                include_others=False,
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual({(e5.type, e5.state_key): e5.event_id}, state_dict)

    def test_batched_state_group_storing(self) -> None:
        creation_event = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        state_to_event = self.get_success(
            self.storage.state.get_state_groups(
                self.room.to_string(), [creation_event.event_id]
            )
        )
        current_state_group = list(state_to_event.keys())[0]
        state_map = dict(
            self.get_success(
                self.storage.state.get_state_ids_for_group(current_state_group)
            )
        )
        prev_event_id = creation_event.event_id

        # create some unpersisted events and event contexts to store against room
        events_and_context: list[tuple[EventBase, UnpersistedEventContext]] = []
        builder = self.event_builder_factory.for_room_version(
            RoomVersions.V1,
            {
                "type": EventTypes.Name,
                "sender": self.u_alice.to_string(),
                "state_key": "",
                "room_id": self.room.to_string(),
                "content": {"name": "first rename of room"},
            },
        )

        event1, unpersisted_context1 = self.get_success(
            self.event_creation_handler.create_new_client_event_for_batch(
                builder,
                requester=create_requester(self.u_alice),
                prev_event_ids=[prev_event_id],
                state_map=dict(state_map),
                current_state_group=current_state_group,
            )
        )
        events_and_context.append((event1, unpersisted_context1))
        prev_event_id = event1.event_id
        if event1.is_state():
            state_map[(event1.type, event1.state_key)] = event1.event_id

        builder2 = self.event_builder_factory.for_room_version(
            RoomVersions.V1,
            {
                "type": EventTypes.JoinRules,
                "sender": self.u_alice.to_string(),
                "state_key": "",
                "room_id": self.room.to_string(),
                "content": {"join_rule": "private"},
            },
        )

        event2, unpersisted_context2 = self.get_success(
            self.event_creation_handler.create_new_client_event_for_batch(
                builder2,
                requester=create_requester(self.u_alice),
                prev_event_ids=[prev_event_id],
                state_map=dict(state_map),
                current_state_group=current_state_group,
            )
        )
        events_and_context.append((event2, unpersisted_context2))
        prev_event_id = event2.event_id
        if event2.is_state():
            state_map[(event2.type, event2.state_key)] = event2.event_id

        builder3 = self.event_builder_factory.for_room_version(
            RoomVersions.V1,
            {
                "type": EventTypes.Message,
                "sender": self.u_alice.to_string(),
                "room_id": self.room.to_string(),
                "content": {"body": "hello from event 3", "msgtype": "m.text"},
            },
        )

        event3, unpersisted_context3 = self.get_success(
            self.event_creation_handler.create_new_client_event_for_batch(
                builder3,
                requester=create_requester(self.u_alice),
                prev_event_ids=[prev_event_id],
                state_map=dict(state_map),
                current_state_group=current_state_group,
            )
        )
        events_and_context.append((event3, unpersisted_context3))
        prev_event_id = event3.event_id

        builder4 = self.event_builder_factory.for_room_version(
            RoomVersions.V1,
            {
                "type": EventTypes.JoinRules,
                "sender": self.u_alice.to_string(),
                "state_key": "",
                "room_id": self.room.to_string(),
                "content": {"join_rule": "public"},
            },
        )

        event4, unpersisted_context4 = self.get_success(
            self.event_creation_handler.create_new_client_event_for_batch(
                builder4,
                requester=create_requester(self.u_alice),
                prev_event_ids=[prev_event_id],
                state_map=dict(state_map),
                current_state_group=current_state_group,
            )
        )
        events_and_context.append((event4, unpersisted_context4))

        processed_events_and_context = self.get_success(
            self.hs.get_datastores().state.store_state_deltas_for_batched(
                events_and_context, self.room.to_string(), current_state_group
            )
        )

        # check that only state events are in state_groups, and all state events are in state_groups
        res = cast(
            list[tuple[str]],
            self.get_success(
                self.store.db_pool.simple_select_list(
                    table="state_groups",
                    keyvalues=None,
                    retcols=("event_id",),
                )
            ),
        )

        events = []
        for result in res:
            self.assertNotIn(event3.event_id, result)  # XXX
            events.append(result[0])

        for event, _ in processed_events_and_context:
            if event.is_state():
                self.assertIn(event.event_id, events)

        # The HAMT path is now the source of truth for live state snapshots.
        # `state_groups_state` should not receive rows for freshly written state
        # groups anymore.
        for event, context in processed_events_and_context:
            if event.is_state():
                state = cast(
                    list[tuple[str, str, str]],
                    self.get_success(
                        self.store.db_pool.simple_select_list(
                            table="state_groups_state",
                            keyvalues={"state_group": context.state_group_after_event},
                            retcols=("type", "state_key", "event_id"),
                        )
                    ),
                )
                self.assertEqual(state, [])

                groups = cast(
                    list[tuple[str]],
                    self.get_success(
                        self.store.db_pool.simple_select_list(
                            table="state_group_edges",
                            keyvalues={"state_group": context.state_group_after_event},
                            retcols=("prev_state_group",),
                        )
                    ),
                )
                self.assertEqual(len(groups), 1)
                self.assertEqual(context.state_group_before_event, groups[0][0])

        final_sg = processed_events_and_context[-1][1].state_group_after_event
        assert final_sg is not None
        final_state = self.get_success(
            self.state_datastore._get_state_groups_from_groups(
                [final_sg], StateFilter.all()
            )
        )
        self.assertEqual(
            final_state[final_sg][(EventTypes.Create, "")], creation_event.event_id
        )
        self.assertEqual(final_state[final_sg][(EventTypes.Name, "")], event1.event_id)
        self.assertEqual(
            final_state[final_sg][(EventTypes.JoinRules, "")], event4.event_id
        )


class CurrentStateDeltaStreamTestCase(HomeserverTestCase):
    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        super().prepare(reactor, clock, hs)
        self.store = hs.get_datastores().main
        self.storage = hs.get_storage_controllers()
        self.state_datastore = self.storage.state.stores.state
        self.event_creation_handler = hs.get_event_creation_handler()
        self.event_builder_factory = hs.get_event_builder_factory()

        # Create a made-up room and a user.
        self.alice_user_id = UserID.from_string("@alice:test")
        self.room = RoomID.from_string("!abc1234:test")

        self.get_success(
            self.store.store_room(
                self.room.to_string(),
                room_creator_user_id="@creator:text",
                is_public=True,
                room_version=RoomVersions.V1,
            )
        )

    def inject_state_event(
        self, room: RoomID, sender: UserID, typ: str, state_key: str, content: JsonDict
    ) -> EventBase:
        builder = self.event_builder_factory.for_room_version(
            RoomVersions.V1,
            {
                "type": typ,
                "sender": sender.to_string(),
                "state_key": state_key,
                "room_id": room.to_string(),
                "content": content,
            },
        )

        event, unpersisted_context = self.get_success(
            self.event_creation_handler.create_new_client_event(builder)
        )

        context = self.get_success(unpersisted_context.persist(event))

        assert self.storage.persistence is not None
        self.get_success(self.storage.persistence.persist_event(event, context))

        return event

    def test_get_partial_current_state_deltas_limit(self) -> None:
        """
        Tests that `get_partial_current_state_deltas` actually returns `limit` rows.

        Regression test for https://github.com/element-hq/synapse/pull/18960.
        """
        # Inject a create event which other events can auth with.
        self.inject_state_event(
            self.room, self.alice_user_id, EventTypes.Create, "", {}
        )

        limit = 2

        # Make N*2 state changes in the room, resulting in 2N+1 total state
        # events (including the create event) in the room.
        for i in range(limit * 2):
            self.inject_state_event(
                self.room,
                self.alice_user_id,
                EventTypes.Name,
                "",
                {"name": f"rename #{i}"},
            )

        # Call the function under test. This must return <= `limit` rows.
        max_stream_id = self.store.get_room_max_stream_ordering()
        clipped_stream_id, deltas = self.get_success(
            self.store.get_partial_current_state_deltas(
                prev_stream_id=0,
                max_stream_id=max_stream_id,
                limit=limit,
            )
        )

        self.assertLessEqual(
            len(deltas), limit, f"Returned {len(deltas)} rows, expected at most {limit}"
        )

        # Advancing from the clipped point should eventually drain the remainder.
        # Make sure we make progress and don’t get stuck.
        if deltas:
            next_prev = clipped_stream_id
            next_clipped, next_deltas = self.get_success(
                self.store.get_partial_current_state_deltas(
                    prev_stream_id=next_prev, max_stream_id=max_stream_id, limit=limit
                )
            )
            self.assertNotEqual(
                next_clipped, clipped_stream_id, "Did not advance clipped_stream_id"
            )
            # Still should respect the limit.
            self.assertLessEqual(len(next_deltas), limit)

    def test_non_unique_stream_ids_in_current_state_delta_stream(self) -> None:
        """
        Tests that `get_partial_current_state_deltas` always returns entire
        groups of state deltas (grouped by `stream_id`), and never part of one.

        We check by passing a `limit` that to the function that, if followed
        blindly, would split a group of state deltas that share a `stream_id`.
        The test passes if that group is not returned at all (because doing so
        would overshoot the limit of returned state deltas).

        Regression test for https://github.com/element-hq/synapse/pull/18960.
        """
        # Inject a create event to start with.
        self.inject_state_event(
            self.room, self.alice_user_id, EventTypes.Create, "", {}
        )

        # Then inject one "real" m.room.name event. This will give us a stream_id that
        # we can create some more (fake) events with.
        self.inject_state_event(
            self.room,
            self.alice_user_id,
            EventTypes.Name,
            "",
            {"name": "rename #1"},
        )

        # Get the stream_id of the last-inserted event.
        max_stream_id = self.store.get_room_max_stream_ordering()

        # Make 3 more state changes in the room, resulting in 5 total state
        # events (including the create event, and the first name update) in
        # the room.
        #
        # All of these state deltas have the same `stream_id` as the original name event.
        # Do so by editing the table directly as that's the simplest way to have
        # all share the same `stream_id`.
        self.get_success(
            self.store.db_pool.simple_insert_many(
                "current_state_delta_stream",
                keys=(
                    "stream_id",
                    "room_id",
                    "type",
                    "state_key",
                    "event_id",
                    "prev_event_id",
                    "instance_name",
                ),
                values=[
                    (
                        max_stream_id,
                        self.room.to_string(),
                        EventTypes.Name,
                        "",
                        f"${random_string(5)}:test",
                        json.dumps({"name": f"rename #{i}"}),
                        "master",
                    )
                    for i in range(3)
                ],
                desc="inject_room_name_state_events",
            )
        )

        # Call the function under test with a limit of 4. Without the limit, we
        # would return 5 state deltas:
        #
        # C N N N N
        # 1 2 3 4 5
        #
        # C = m.room.create
        # N = m.room.name
        #
        # With the limit, we should return only the create event, as returning 4
        # state deltas would result in splitting a group:
        #
        # 2 3 3 3 3 - state IDs/groups
        # C N N N N
        # 1 2 3 4 X

        clipped_stream_id, deltas = self.get_success(
            self.store.get_partial_current_state_deltas(
                prev_stream_id=0,
                max_stream_id=max_stream_id,
                limit=4,
            )
        )

        # 2 is the stream ID of the m.room.create event.
        self.assertEqual(clipped_stream_id, 2)
        self.assertEqual(
            len(deltas),
            1,
            f"Returned {len(deltas)} rows, expected only one (the create event): {deltas}",
        )

        # Advance once more with our limit of 4. We should now get all 4
        # `m.room.name` state deltas as they can fit under the limit.
        clipped_stream_id, next_deltas = self.get_success(
            self.store.get_partial_current_state_deltas(
                prev_stream_id=clipped_stream_id, max_stream_id=max_stream_id, limit=4
            )
        )
        self.assertEqual(
            clipped_stream_id, 3
        )  # The stream ID of the 4 m.room.name events.

        self.assertEqual(
            len(next_deltas),
            4,
            f"Returned {len(next_deltas)} rows, expected all 4 m.room.name events: {next_deltas}",
        )

    def test_get_partial_current_state_deltas_does_not_enter_infinite_loop(
        self,
    ) -> None:
        """
        Tests that `get_partial_current_state_deltas` does not repeatedly return
        zero entries due to the passed `limit` parameter being less than the
        size of the next group of state deltas from the given `prev_stream_id`.
        """
        # Inject a create event to start with.
        self.inject_state_event(
            self.room, self.alice_user_id, EventTypes.Create, "", {}
        )

        # Then inject one "real" m.room.name event. This will give us a stream_id that
        # we can create some more (fake) events with.
        self.inject_state_event(
            self.room,
            self.alice_user_id,
            EventTypes.Name,
            "",
            {"name": "rename #1"},
        )

        # Get the stream_id of the last-inserted event.
        max_stream_id = self.store.get_room_max_stream_ordering()

        # Make 3 more state changes in the room, resulting in 5 total state
        # events (including the create event, and the first name update) in
        # the room.
        #
        # All of these state deltas have the same `stream_id` as the original name event.
        # Do so by editing the table directly as that's the simplest way to have
        # all share the same `stream_id`.
        self.get_success(
            self.store.db_pool.simple_insert_many(
                "current_state_delta_stream",
                keys=(
                    "stream_id",
                    "room_id",
                    "type",
                    "state_key",
                    "event_id",
                    "prev_event_id",
                    "instance_name",
                ),
                values=[
                    (
                        max_stream_id,
                        self.room.to_string(),
                        EventTypes.Name,
                        "",
                        f"${random_string(5)}:test",
                        json.dumps({"name": f"rename #{i}"}),
                        "master",
                    )
                    for i in range(3)
                ],
                desc="inject_room_name_state_events",
            )
        )

        # Call the function under test with a limit of 4. Without the limit, we would return
        # 5 state deltas:
        #
        # C N N N N
        # 1 2 3 4 5
        #
        # C = m.room.create
        # N = m.room.name
        #
        # With the limit, we should return only the create event, as returning 4
        # state deltas would result in splitting a group:
        #
        # 2 3 3 3 3 - state IDs/groups
        # C N N N N
        # 1 2 3 4 X

        clipped_stream_id, deltas = self.get_success(
            self.store.get_partial_current_state_deltas(
                prev_stream_id=2,  # Start after the create event (which has stream_id 2).
                max_stream_id=max_stream_id,
                limit=2,  # Less than the size of the next group (which is 4).
            )
        )

        self.assertEqual(
            clipped_stream_id, 3
        )  # The stream ID of the 4 m.room.name events.

        # We should get all 4 `m.room.name` state deltas, instead of 0, which
        # would result in the caller entering an infinite loop.
        self.assertEqual(
            len(deltas),
            4,
            f"Returned {len(deltas)} rows, expected 4 even though it broke our limit: {deltas}",
        )
