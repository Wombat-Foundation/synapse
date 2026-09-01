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
                room_creator_user_id=self.u_alice.to_string(),
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

        When TiKV is configured, both the full HAMT trie and its root pointer
        live in TiKV. The corruption is simulated by inserting an undecodable
        node and repointing the TiKV root record at it.
        """
        if not self.state_datastore.tikv_pd_endpoints:
            self.skipTest("Requires TiKV -- set SYNAPSE_TEST_TIKV_PD_ENDPOINTS to run")

        from synapse.storage.databases.state.bg_updates import (
            _decode_state_hamt_root,
            _encode_state_hamt_root,
            _state_hamt_node_tikv_key,
            _state_hamt_root_tikv_key,
        )
        from synapse.synapse_rust import tikv_engine

        event = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        state_group = self.get_success(
            self.store._get_state_group_for_event(event.event_id)
        )
        assert state_group is not None

        root_key = _state_hamt_root_tikv_key(
            self.state_datastore.tikv_namespace, state_group
        )
        root_value = tikv_engine.get(root_key)
        assert root_value is not None, "Expected a HAMT root record to exist in TiKV"
        room_prefix, _root_hash, lattice, room_id = _decode_state_hamt_root(root_value)

        garbage_structural_hash = random_string(16).encode("ascii")
        garbage_node_key = _state_hamt_node_tikv_key(
            self.state_datastore.tikv_namespace, room_prefix, garbage_structural_hash
        )
        tikv_engine.put(garbage_node_key, b"not a valid persisted HAMT node")

        tikv_engine.put(
            root_key,
            _encode_state_hamt_root(
                room_prefix, garbage_structural_hash, lattice, room_id=room_id
            ),
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

    def test_multi_group_selective_lookup_real_tikv(self) -> None:
        """Verify multi-group selective lookup against real TiKV with multiple divergent rooms."""
        if not self.state_datastore.tikv_pd_endpoints:
            self.skipTest("Requires TiKV -- set SYNAPSE_TEST_TIKV_PD_ENDPOINTS to run")

        # Create room 1 with Create and Name events
        event1 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        sg1 = self.get_success(self.store._get_state_group_for_event(event1.event_id))
        event2 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Name, "", {"name": "Room 1 Name"}
        )
        sg2 = self.get_success(self.store._get_state_group_for_event(event2.event_id))

        # Create room 2 with Create and Topic events
        room2 = RoomID.from_string("!room2:test")
        self.get_success(
            self.store.store_room(
                room2.to_string(),
                room_creator_user_id=self.u_alice.to_string(),
                is_public=True,
                room_version=RoomVersions.V1,
            )
        )
        event3 = self.inject_state_event(room2, self.u_alice, EventTypes.Create, "", {})
        event4 = self.inject_state_event(
            room2, self.u_alice, EventTypes.Topic, "", {"topic": "Room 2 Topic"}
        )
        sg3 = self.get_success(self.store._get_state_group_for_event(event3.event_id))
        sg4 = self.get_success(self.store._get_state_group_for_event(event4.event_id))
        assert (
            sg1 is not None and sg2 is not None and sg3 is not None and sg4 is not None
        )

        # Look up m.room.name across state groups
        state_filter = StateFilter.from_types([(EventTypes.Name, "")])
        res = self.get_success(
            self.storage.state.stores.state._get_state_groups_from_groups(
                [sg1, sg2, sg4], state_filter
            )
        )
        self.assertEqual(
            res,
            {
                sg1: {},
                sg2: {(EventTypes.Name, ""): event2.event_id},
                sg4: {},
            },
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
                if k == _state_hamt_root_tikv_key(
                    self.state_datastore.tikv_namespace, 1234
                ):
                    res.append((k, encoded_root))
                    continue
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

                # 3. Multi-group selective key lookup
                with patch(
                    "synapse.synapse_rust.tikv_engine.lookup_state_hamts",
                    return_value=[[(EventTypes.Name, "", "$name:test")]],
                ) as mock_lookup:
                    multi_res, missing = (
                        self.state_datastore._lookup_state_hamts_from_tikv_direct(
                            [1234, 9999], [(EventTypes.Name, "")]
                        )
                    )
                    self.assertEqual(missing, [9999])
                    self.assertEqual(
                        multi_res, {1234: [(EventTypes.Name, "", "$name:test")]}
                    )
                    mock_lookup.assert_called_once()

                # 4. Full materialization
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

    def test_multi_group_exact_filter_under_tikv_uses_batch_lookup(self) -> None:
        """Verify that fetching exact state filter across multiple groups uses _lookup_state_hamts_from_tikv_direct."""
        from unittest.mock import patch

        event1 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        sg1 = self.get_success(self.store._get_state_group_for_event(event1.event_id))
        event2 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Name, "", {"name": "room_name"}
        )
        sg2 = self.get_success(self.store._get_state_group_for_event(event2.event_id))
        assert sg1 is not None and sg2 is not None

        # Enable pure TiKV mode
        self._enable_mock_tikv()
        try:
            state_filter = StateFilter.from_types([(EventTypes.Name, "")])

            with patch.object(
                self.state_datastore,
                "_lookup_state_hamts_from_tikv_direct",
                return_value=(
                    {
                        sg1: [],
                        sg2: [(EventTypes.Name, "", event2.event_id)],
                    },
                    [],
                ),
            ) as mock_batch_lookup:
                res = self.get_success(
                    self.storage.state.stores.state._get_state_groups_from_groups(
                        [sg1, sg2], state_filter
                    )
                )
                mock_batch_lookup.assert_called_once()
                self.assertEqual(
                    res,
                    {
                        sg1: {},
                        sg2: {(EventTypes.Name, ""): event2.event_id},
                    },
                )
        finally:
            self.state_datastore.tikv_pd_endpoints = []

    def test_multi_group_exact_filter_under_pure_sql_shares_node_fetches(self) -> None:
        """SQL-mode mirror of test_multi_group_exact_filter_under_tikv_uses_batch_lookup:
        with no TiKV configured, a selective (exact_keys) lookup across several
        state groups must go through _lookup_state_hamt_from_postgres_many_txn,
        not the singular per-group SQL loop, and must return correct per-group
        results without mocking anything -- this exercises the real SQL HAMT
        node-sharing path end to end."""
        if self.state_datastore.tikv_pd_endpoints:
            # This test specifically exercises the pure-SQL HAMT path, which
            # only applies when TiKV is not configured -- see the identical
            # guard on test_state_group_hamt_corruption_does_not_fallback_to_sql.
            self.skipTest("Not applicable when TiKV is configured")

        event1 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        sg1 = self.get_success(self.store._get_state_group_for_event(event1.event_id))
        event2 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Name, "", {"name": "room_name"}
        )
        sg2 = self.get_success(self.store._get_state_group_for_event(event2.event_id))
        assert sg1 is not None and sg2 is not None
        assert not self.state_datastore.tikv_pd_endpoints

        state_filter = StateFilter.from_types([(EventTypes.Name, "")])

        with (
            patch.object(
                self.state_datastore,
                "_lookup_state_hamt_from_postgres_many_txn",
                wraps=self.state_datastore._lookup_state_hamt_from_postgres_many_txn,
            ) as mock_many,
            patch.object(
                self.state_datastore,
                "_lookup_state_hamt_from_postgres_txn",
                wraps=self.state_datastore._lookup_state_hamt_from_postgres_txn,
            ) as mock_singular,
        ):
            res = self.get_success(
                self.storage.state.stores.state._get_state_groups_from_groups(
                    [sg1, sg2], state_filter
                )
            )

        # The batched path was used exactly once for both groups together;
        # the per-group singular path was never reached.
        mock_many.assert_called_once()
        mock_singular.assert_not_called()

        self.assertEqual(
            res,
            {
                sg1: {},
                sg2: {(EventTypes.Name, ""): event2.event_id},
            },
        )

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

    def test_existing_unresolved_group_raises(self) -> None:
        """Verify that an existing state group in SQL raises RuntimeError when TiKV root is unresolved."""
        from unittest.mock import patch

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
        self.get_success(
            self.store.db_pool.simple_insert(
                table="state_group_edges",
                values={"state_group": state_group, "prev_state_group": 1},
                desc="test_unresolved.insert_edge",
            )
        )

        self._enable_mock_tikv()
        try:
            with patch("synapse.synapse_rust.tikv_engine.get", return_value=None):
                self.get_failure(
                    self.state_datastore._get_state_groups_from_groups(
                        [state_group], StateFilter.all()
                    ),
                    RuntimeError,
                )
        finally:
            self.state_datastore.tikv_pd_endpoints = []

    def test_existing_unresolved_group_raises_in_sql_mode(self) -> None:
        """Verify that an existing state group with no HAMT root raises
        RuntimeError via the SQL/legacy-HAMT path (TiKV disabled), instead of
        silently falling through to the legacy `state_groups_state` walk and
        returning an empty state map."""
        if self.state_datastore.tikv_pd_endpoints:
            self.skipTest("TiKV configured; corruption check only applies in SQL mode")

        state_group = 999997
        self.get_success(
            self.store.db_pool.simple_insert(
                table="state_groups",
                values={
                    "id": state_group,
                    "room_id": self.room.to_string(),
                    "event_id": "$fake-sql:test",
                },
                desc="test_unresolved_sql.insert_sg",
            )
        )
        self.get_success(
            self.store.db_pool.simple_insert(
                table="state_group_edges",
                values={"state_group": state_group, "prev_state_group": 1},
                desc="test_unresolved_sql.insert_edge",
            )
        )

        self.get_failure(
            self.state_datastore._get_state_groups_from_groups(
                [state_group], StateFilter.all()
            ),
            RuntimeError,
        )

    def test_nonexistent_group_returns_empty_dict(self) -> None:
        """Verify that a nonexistent state group (not in SQL) returns {} without raising."""
        from unittest.mock import patch

        nonexistent_group = 9999991

        self._enable_mock_tikv()
        try:
            with patch("synapse.synapse_rust.tikv_engine.get", return_value=None):
                res = self.get_success(
                    self.state_datastore._get_state_groups_from_groups(
                        [nonexistent_group], StateFilter.all()
                    )
                )
                self.assertEqual(res[nonexistent_group], {})
        finally:
            self.state_datastore.tikv_pd_endpoints = []

    def test_tikv_write_failure_keeps_sql_fallback(self) -> None:
        """A node-write failure leaves the committed SQL HAMT mirror usable."""
        from unittest.mock import patch

        self._enable_mock_tikv()
        try:
            with patch(
                "synapse.synapse_rust.tikv_engine.batch_put",
                side_effect=RuntimeError("TiKV connection refused"),
            ):
                self.get_failure(
                    self.state_datastore.store_state_group(
                        event_id="$fake:event",
                        room_id=self.room.to_string(),
                        room_version=RoomVersions.V1,
                        prev_group=None,
                        delta_ids=None,
                        current_state_ids={
                            (EventTypes.Create, ""): "$fake:event",
                        },
                    ),
                    RuntimeError,
                )

            # TiKV publication runs after the SQL transaction, so the state
            # group remains available through the SQL fallback until TiKV can
            # be reached again.
            row = self.get_success(
                self.store.db_pool.simple_select_one_onecol(
                    table="state_groups",
                    keyvalues={"event_id": "$fake:event"},
                    retcol="id",
                    allow_none=True,
                )
            )
            self.assertIsNotNone(row)
            state_group = cast(int, row)
            with patch("synapse.synapse_rust.tikv_engine.get", return_value=None):
                state = self.get_success(
                    self.state_datastore._get_state_groups_from_groups(
                        [state_group], StateFilter.all()
                    )
                )
            self.assertEqual(
                state[state_group], {(EventTypes.Create, ""): "$fake:event"}
            )
        finally:
            self.state_datastore.tikv_pd_endpoints = []

    def test_tikv_root_write_failure_keeps_sql_fallback(self) -> None:
        """A root-write failure leaves the committed SQL HAMT mirror usable."""
        from unittest.mock import patch

        self._enable_mock_tikv()
        try:
            calls: list[list[tuple[bytes, bytes]]] = []

            def fail_roots(pairs: list[tuple[bytes, bytes]]) -> None:
                calls.append(pairs)
                if all(key.startswith(b"hamt:root:") for key, _value in pairs):
                    raise RuntimeError("TiKV root write failed")

            with patch(
                "synapse.synapse_rust.tikv_engine.batch_put",
                side_effect=fail_roots,
            ):
                self.get_failure(
                    self.state_datastore.store_state_group(
                        event_id="$root-failure:event",
                        room_id=self.room.to_string(),
                        room_version=RoomVersions.V1,
                        prev_group=None,
                        delta_ids=None,
                        current_state_ids={
                            (EventTypes.Create, ""): "$root-failure:event"
                        },
                    ),
                    RuntimeError,
                )

            self.assertEqual(len(calls), 2)
            self.assertTrue(all(key.startswith(b"hamt:node:") for key, _ in calls[0]))
            self.assertTrue(all(key.startswith(b"hamt:root:") for key, _ in calls[1]))
            row = self.get_success(
                self.store.db_pool.simple_select_one_onecol(
                    table="state_groups",
                    keyvalues={"event_id": "$root-failure:event"},
                    retcol="id",
                    allow_none=True,
                )
            )
            self.assertIsNotNone(row)
        finally:
            self.state_datastore.tikv_pd_endpoints = []

    def test_tikv_publishes_nodes_before_roots(self) -> None:
        """A successful TiKV-mode write publishes nodes before the root."""
        from unittest.mock import patch

        self._enable_mock_tikv()
        try:
            calls: list[list[tuple[bytes, bytes]]] = []

            def record_put(pairs: list[tuple[bytes, bytes]]) -> None:
                calls.append(pairs)

            with patch(
                "synapse.synapse_rust.tikv_engine.batch_put",
                side_effect=record_put,
            ):
                state_group = self.get_success(
                    self.state_datastore.store_state_group(
                        event_id="$ordered-write:event",
                        room_id=self.room.to_string(),
                        room_version=RoomVersions.V1,
                        prev_group=None,
                        delta_ids=None,
                        current_state_ids={
                            (EventTypes.Create, ""): "$ordered-write:event"
                        },
                    )
                )

            self.assertEqual(len(calls), 2)
            self.assertTrue(all(key.startswith(b"hamt:node:") for key, _ in calls[0]))
            self.assertTrue(all(key.startswith(b"hamt:root:") for key, _ in calls[1]))
            row = self.get_success(
                self.store.db_pool.simple_select_one_onecol(
                    table="state_groups",
                    keyvalues={"id": state_group},
                    retcol="event_id",
                    allow_none=False,
                )
            )
            self.assertEqual(row, "$ordered-write:event")
        finally:
            self.state_datastore.tikv_pd_endpoints = []

    def test_mixed_existing_and_nonexistent_groups_under_tikv(self) -> None:
        """Verify that present and genuinely absent groups resolve together."""
        from unittest.mock import patch

        event = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        state_group = self.get_success(
            self.store._get_state_group_for_event(event.event_id)
        )
        assert state_group is not None
        nonexistent_group = 9999999

        def mock_mat(
            groups: list[int],
        ) -> tuple[dict[int, list[tuple[str, str, str]]], list[int]]:
            return (
                (
                    {state_group: [(EventTypes.Create, "", event.event_id)]}
                    if state_group in groups
                    else {}
                ),
                [group for group in groups if group != state_group],
            )

        self._enable_mock_tikv()
        try:
            with patch.object(
                self.state_datastore,
                "_materialize_state_hamts_from_tikv_direct",
                side_effect=mock_mat,
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
        self.get_success(
            self.store.db_pool.simple_insert(
                table="state_group_edges",
                values={"state_group": unresolved_group, "prev_state_group": 1},
                desc="test_unresolved.insert_edge",
            )
        )
        nonexistent_group = 9999992

        def mock_mat(
            groups: list[int],
        ) -> tuple[dict[int, list[tuple[str, str, str]]], list[int]]:
            return (
                (
                    {valid_group: [(EventTypes.Create, "", event.event_id)]}
                    if valid_group in groups
                    else {}
                ),
                [group for group in groups if group != valid_group],
            )

        def mock_mat_single(sg: int) -> list[tuple[str, str, str]] | None:
            if sg == valid_group:
                entries_by_group, _ = mock_mat([sg])
                return entries_by_group.get(sg)
            return None

        self._enable_mock_tikv()
        try:
            with (
                patch.object(
                    self.state_datastore,
                    "_materialize_state_hamts_from_tikv_direct",
                    side_effect=mock_mat,
                ),
                patch.object(
                    self.state_datastore,
                    "_materialize_state_hamt_from_tikv_direct",
                    side_effect=mock_mat_single,
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

    def test_purge_room_state_tikv_uses_returned_delete_ids(self) -> None:
        """TiKV cleanup must use IDs returned by the purge transaction.

        A PostgreSQL READ COMMITTED purge can delete a state group which was
        committed after the old pre-fetch but before the DELETE statement. In
        that case the transaction returns both IDs, while the old pre-fetch
        contains only the first. The caller must pass the former to TiKV.
        """
        from unittest.mock import patch

        from twisted.internet import defer as _defer

        from synapse.storage.databases.state.bg_updates import (
            _state_hamt_root_tikv_key,
        )

        # The stale result the removed prefetch would have returned.
        stale_ids = [1]
        deleted_ids = [1, 2]
        tikv_namespace = self.state_datastore.tikv_namespace
        deleted_keys: list[list[bytes]] = []

        def capture_batch_delete(keys: list[bytes]) -> None:
            deleted_keys.append(keys)

        self._enable_mock_tikv()
        try:
            with patch.object(
                self.state_datastore.db_pool,
                "simple_select_onecol",
                return_value=_defer.succeed(stale_ids),
            ) as prefetch:
                with patch.object(
                    self.state_datastore,
                    "_purge_room_state_txn",
                    return_value=deleted_ids,
                ) as purge_transaction:
                    with patch(
                        "synapse.storage.databases.state.store.defer_to_thread",
                        side_effect=lambda _reactor, fn, *args, **kwargs: (
                            fn(*args, **kwargs),
                            _defer.succeed(None),
                        )[1],
                    ):
                        with patch(
                            "synapse.synapse_rust.tikv_engine.batch_delete",
                            side_effect=capture_batch_delete,
                        ):
                            self.get_success(
                                self.state_datastore.purge_room_state(
                                    self.room.to_string()
                                )
                            )

                purge_transaction.assert_called_once()
                prefetch.assert_not_called()
        finally:
            self.state_datastore.tikv_pd_endpoints = []

        self.assertEqual(
            len(deleted_keys), 1, "batch_delete must be called exactly once"
        )
        self.assertEqual(
            set(deleted_keys[0]),
            {_state_hamt_root_tikv_key(tikv_namespace, sg) for sg in deleted_ids},
        )

    def test_purge_room_state_concurrent_insertion_no_orphans(self) -> None:
        """Verify PostgreSQL READ COMMITTED concurrent insertion race during purge."""
        import threading
        from typing import Any
        from unittest.mock import patch

        from synapse.storage.database import LoggingTransaction

        from tests.utils import USE_POSTGRES_FOR_TESTS

        if not USE_POSTGRES_FOR_TESTS:
            self.skipTest("Requires PostgreSQL")

        room_id_str = "!purge_race:test"
        room_id = RoomID.from_string(room_id_str)
        self.get_success(
            self.store.store_room(
                room_id_str,
                room_creator_user_id=self.u_alice.to_string(),
                is_public=True,
                room_version=RoomVersions.V1,
            )
        )

        event1 = self.inject_state_event(
            room_id, self.u_alice, EventTypes.Create, "", {}
        )
        sg1 = self.get_success(self.store._get_state_group_for_event(event1.event_id))
        assert sg1 is not None

        self.get_success(
            self.store.db_pool.simple_insert(
                table="state_groups_pending_deletion",
                values={"state_group": sg1, "insertion_ts": 123456789},
            )
        )

        from synapse.storage.engines._base import IsolationLevel

        original_runInteraction = self.store.db_pool.runInteraction

        async def mock_runInteraction(
            desc: str, func: Any, *args: Any, **kwargs: Any
        ) -> Any:
            if desc == "purge_room_state":
                kwargs["isolation_level"] = IsolationLevel.READ_COMMITTED
            return await original_runInteraction(desc, func, *args, **kwargs)

        with patch.object(
            self.store.db_pool, "runInteraction", side_effect=mock_runInteraction
        ):
            resume_before_event = threading.Event()
            resume_after_event = threading.Event()
            bg_ready_to_insert_sg2 = threading.Event()
            bg_ready_to_insert_sg3 = threading.Event()

            original_execute = LoggingTransaction.execute

            db_config = self.hs.config.database.get_single_database()
            conn_args = dict(db_config.config.get("args", {}))
            conn_args.pop("cp_min", None)
            conn_args.pop("cp_max", None)

            # Use lists to pass out values from the background thread
            generated_ids = []
            bg_thread_error = []

            def background_worker() -> None:
                import psycopg2

                try:
                    conn = psycopg2.connect(**conn_args)
                    conn.autocommit = True
                    cursor = conn.cursor()

                    # --- BRANCH 1: Commit BEFORE parent DELETE ---
                    if bg_ready_to_insert_sg2.wait(timeout=10.0):
                        try:
                            # Allocate sg2 via PostgreSQL sequence
                            cursor.execute("SELECT nextval('state_group_id_seq')")
                            row = cursor.fetchone()
                            assert row is not None
                            sg2 = row[0]
                            generated_ids.append(sg2)

                            cursor.execute(
                                "INSERT INTO state_groups (id, room_id, event_id) VALUES (%s, %s, %s)",
                                (sg2, room_id_str, "$fake_event2:test"),
                            )
                            # Actual outgoing edge for sg2
                            cursor.execute(
                                "INSERT INTO state_group_edges (state_group, prev_state_group) VALUES (%s, %s)",
                                (sg2, sg1),
                            )
                            cursor.execute(
                                "INSERT INTO state_groups_state (state_group, room_id, type, state_key, event_id) VALUES (%s, %s, %s, %s, %s)",
                                (sg2, room_id_str, "m.room.name", "", "$name2:test"),
                            )
                            cursor.execute(
                                "INSERT INTO state_groups_pending_deletion (state_group, insertion_ts) VALUES (%s, %s)",
                                (sg2, 123456789),
                            )
                            # Dummy HAMT root fixture
                            cursor.execute(
                                "INSERT INTO state_hamt_roots (state_group, room_prefix, root_structural_hash) VALUES (%s, %s, %s)",
                                (
                                    sg2,
                                    psycopg2.Binary(b"prefix12"),
                                    psycopg2.Binary(b"0123456789abcdef"),
                                ),
                            )
                        finally:
                            resume_before_event.set()

                    # --- BRANCH 2: Commit AFTER parent DELETE ---
                    if bg_ready_to_insert_sg3.wait(timeout=10.0):
                        try:
                            cursor.execute("SELECT nextval('state_group_id_seq')")
                            row = cursor.fetchone()
                            assert row is not None
                            sg3 = row[0]
                            generated_ids.append(sg3)

                            cursor.execute(
                                "INSERT INTO state_groups (id, room_id, event_id) VALUES (%s, %s, %s)",
                                (sg3, room_id_str, "$fake_event3:test"),
                            )
                            cursor.execute(
                                "INSERT INTO state_groups_state (state_group, room_id, type, state_key, event_id) VALUES (%s, %s, %s, %s, %s)",
                                (sg3, room_id_str, "m.room.name", "", "$name3:test"),
                            )
                        finally:
                            resume_after_event.set()

                    cursor.close()
                    conn.close()
                except Exception as e:
                    bg_thread_error.append(e)
                    resume_before_event.set()
                    resume_after_event.set()

            bg_thread = threading.Thread(target=background_worker)
            bg_thread.start()

            try:

                def mock_execute(
                    txn: LoggingTransaction, sql: str, parameters: Any = None
                ) -> object:
                    if "DELETE FROM state_groups WHERE room_id =" in sql:
                        bg_ready_to_insert_sg2.set()
                        if not resume_before_event.wait(timeout=10.0):
                            raise Exception("Timeout waiting for resume_before_event")

                        if bg_thread_error:
                            raise Exception(
                                f"Background thread error: {bg_thread_error[0]}"
                            )

                        res = original_execute(txn, sql, parameters)

                        bg_ready_to_insert_sg3.set()
                        if not resume_after_event.wait(timeout=10.0):
                            raise Exception("Timeout waiting for resume_after_event")
                        return res
                    return original_execute(txn, sql, parameters)

                with patch(
                    "synapse.storage.database.LoggingTransaction.execute",
                    side_effect=mock_execute,
                    autospec=True,
                ):
                    self.get_success(self.state_datastore.purge_room_state(room_id_str))

            finally:
                # Ensure the background thread never leaks if an assertion fails
                bg_ready_to_insert_sg2.set()
                bg_ready_to_insert_sg3.set()
                bg_thread.join(timeout=5.0)

            self.assertFalse(bg_thread.is_alive(), "Background thread failed to exit")
            if bg_thread_error:
                raise bg_thread_error[0]

            self.assertEqual(
                len(generated_ids), 2, "Background worker did not generate sg2 and sg3"
            )
            sg2, sg3 = generated_ids

            # 1. Assert sg1 and sg2 parents are deleted. sg3 is not.
            def get_parents(txn: LoggingTransaction) -> list[int]:
                txn.execute(
                    f"SELECT id FROM state_groups WHERE id IN ({sg1}, {sg2}, {sg3})"
                )
                return [row[0] for row in txn.fetchall()]

            parents = self.get_success(
                self.store.db_pool.runInteraction("get_parents", get_parents)
            )
            self.assertNotIn(sg1, parents, "sg1 parent survived")
            self.assertNotIn(sg2, parents, "sg2 parent survived")
            self.assertIn(sg3, parents, "sg3 parent did not survive")

            # 2. Assert NO orphans exist for sg1 or sg2 in any child table
            def check_orphans(txn: LoggingTransaction) -> dict[str, int]:
                res = {}
                child_tables = [
                    "state_groups_state",
                    "state_group_edges",
                    "state_hamt_roots",
                    "state_groups_pending_deletion",
                ]
                for table in child_tables:
                    txn.execute(
                        f"""
                        SELECT count(*) FROM {table}
                        WHERE state_group IN ({sg1}, {sg2})
                          AND NOT EXISTS (
                              SELECT 1 FROM state_groups
                              WHERE state_groups.id = {table}.state_group
                          )
                        """
                    )
                    fetch_res = txn.fetchone()
                    assert fetch_res is not None
                    res[table] = fetch_res[0]
                return res

            orphans = self.get_success(
                self.store.db_pool.runInteraction("check_orphans", check_orphans)
            )
            for table, count in orphans.items():
                self.assertEqual(
                    count, 0, f"Found {count} orphans for sg1/sg2 in {table}"
                )

            # 3. Explicitly verify sg3's single expected child row
            def check_sg3_children(txn: LoggingTransaction) -> dict[str, list[int]]:
                res = {}
                for table in [
                    "state_groups_state",
                    "state_group_edges",
                    "state_hamt_roots",
                    "state_groups_pending_deletion",
                ]:
                    txn.execute(
                        f"SELECT state_group FROM {table} WHERE state_group = {sg3}"
                    )
                    res[table] = [row[0] for row in txn.fetchall()]
                return res

            sg3_children = self.get_success(
                self.store.db_pool.runInteraction(
                    "check_sg3_children", check_sg3_children
                )
            )
            self.assertIn(
                sg3,
                sg3_children["state_groups_state"],
                "sg3 child missing from state_groups_state",
            )
            self.assertNotIn(
                sg3, sg3_children["state_group_edges"], "sg3 has unexpected edge"
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
