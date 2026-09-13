#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2026 Element Creations Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

"""Unit tests for `embedded_event_auth_chains.py`'s closure-walk logic,
against a fake in-memory mtxdb engine and a fake SQL transaction -- these
exercise the module's own logic (walk correctness, is_complete gating,
cold-import, cache-generation invalidation) independent of the real Rust
engine or a running homeserver, per the plan's Verification section.
"""

from typing import Any
from unittest import TestCase

from synapse.storage.databases.main.embedded_event_auth_chains import (
    ClosureCache,
    IncompleteAuthGraph,
    bump_room_generation,
    embed_auth_edges_batch,
    get_embedded_auth_edges_batch,
    get_or_create_short_ids,
)

NAMESPACE = "test-ns"
ROOM_ID = "!room:example.org"


class FakeEngine:
    """A tiny in-memory stand-in for `synapse_rust.mtxdb_engine`, scoped to
    just the auth-chain-closure primitives this module calls.
    """

    def __init__(self) -> None:
        self._counters: dict[tuple[str, str], int] = {}
        self._forward: dict[tuple[str, str, str], int] = {}
        self._reverse: dict[tuple[str, str, int], str] = {}
        self._edges: dict[tuple[str, str, int], list[int]] = {}
        self._purged: set[tuple[str, str]] = set()

    def get_or_create_short_ids(
        self, namespace: str, room_id: str, event_ids: list[str]
    ) -> list[int]:
        out = []
        for event_id in event_ids:
            key = (namespace, room_id, event_id)
            if key in self._forward:
                out.append(self._forward[key])
                continue
            counter_key = (namespace, room_id)
            next_id = self._counters.get(counter_key, 0) + 1
            self._counters[counter_key] = next_id
            self._forward[key] = next_id
            self._reverse[(namespace, room_id, next_id)] = event_id
            out.append(next_id)
        return out

    def resolve_short_ids_to_event_ids(
        self, namespace: str, room_id: str, short_ids: list[int]
    ) -> list[str | None]:
        return [self._reverse.get((namespace, room_id, s)) for s in short_ids]

    def auth_chain_edges_get(
        self, namespace: str, room_id: str, short_ids: list[int]
    ) -> list[list[int] | None]:
        return [self._edges.get((namespace, room_id, s)) for s in short_ids]

    def auth_chain_edges_put(
        self, namespace: str, room_id: str, rows: list[tuple[int, list[int]]]
    ) -> None:
        for short_id, auth_short_ids in rows:
            self._edges[(namespace, room_id, short_id)] = list(auth_short_ids)

    def auth_chain_purge_room(self, namespace: str, room_id: str) -> None:
        key = (namespace, room_id)
        self._forward = {k: v for k, v in self._forward.items() if k[:2] != key}
        self._reverse = {k: v for k, v in self._reverse.items() if k[:2] != key}
        self._edges = {k: v for k, v in self._edges.items() if k[:2] != key}
        self._counters.pop(key, None)


class FakeTxn:
    """Fakes just the `execute(...).fetchall()`/`.fetchone()` surface
    `_fetch_auth_event_ids_from_sql` needs, backed by a hand-built
    `event_auth` graph.
    """

    def __init__(
        self, auth_edges: dict[str, list[str]], known_events: set[str] | None = None
    ) -> None:
        self._auth_edges = auth_edges
        self._known_events = (
            known_events if known_events is not None else set(auth_edges)
        )

    def execute(self, sql: str, params: tuple[Any, ...]) -> "FakeTxn":
        if sql.startswith("SELECT auth_id"):
            (event_id,) = params
            self._last_rows = [(a,) for a in self._auth_edges.get(event_id, [])]
        elif sql.startswith("SELECT 1 FROM events"):
            (event_id,) = params
            self._last_rows = [(1,)] if event_id in self._known_events else []
        else:
            raise AssertionError(f"unexpected SQL: {sql}")
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._last_rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._last_rows[0] if self._last_rows else None


def _install_fake_engine(
    monkeypatch_target: dict[str, Any], engine: FakeEngine
) -> None:
    import synapse.storage.databases.embedded_engine as embedded_engine_module

    embedded_engine_module.get_embedded_engine = lambda name: engine  # type: ignore[assignment]


class ClosureWalkTests(TestCase):
    def setUp(self) -> None:
        self.engine = FakeEngine()
        import synapse.storage.databases.embedded_engine as embedded_engine_module

        self._real_get_embedded_engine = embedded_engine_module.get_embedded_engine
        embedded_engine_module.get_embedded_engine = lambda name: self.engine  # type: ignore[assignment]
        self.cache = ClosureCache()

    def tearDown(self) -> None:
        import synapse.storage.databases.embedded_engine as embedded_engine_module

        embedded_engine_module.get_embedded_engine = self._real_get_embedded_engine

    def _short_id(self, event_id: str) -> int:
        return get_or_create_short_ids(None, NAMESPACE, ROOM_ID, [event_id])[0]

    def test_diamond_shaped_graph_and_leaf_with_zero_auth_events(self) -> None:
        # create -> (nothing)
        # a -> create
        # b -> create
        # c -> a, b   (diamond: c's closure should include create only once)
        graph = {
            "$create": [],
            "$a": ["$create"],
            "$b": ["$create"],
            "$c": ["$a", "$b"],
        }
        txn = FakeTxn(graph)

        create_id = self._short_id("$create")
        a_id = self._short_id("$a")
        b_id = self._short_id("$b")
        c_id = self._short_id("$c")

        closure = self.cache.get_closure(txn, None, NAMESPACE, ROOM_ID, c_id)
        self.assertEqual(set(closure), {a_id, b_id, create_id})
        # Ancestors only -- c itself is not included.
        self.assertNotIn(c_id, closure)

        # The leaf's own closure is empty (zero auth events), not an error.
        leaf_closure = self.cache.get_closure(txn, None, NAMESPACE, ROOM_ID, create_id)
        self.assertEqual(len(leaf_closure), 0)

    def test_cold_import_embeds_edges_from_sql_on_first_touch(self) -> None:
        graph = {"$create": [], "$a": ["$create"]}
        txn = FakeTxn(graph)
        create_id = self._short_id("$create")
        a_id = self._short_id("$a")

        # Nothing embedded yet.
        self.assertEqual(
            get_embedded_auth_edges_batch(None, NAMESPACE, ROOM_ID, [a_id])[a_id],
            None,
        )

        closure = self.cache.get_closure(txn, None, NAMESPACE, ROOM_ID, a_id)
        self.assertEqual(set(closure), {create_id})

        # Now embedded, idempotently re-embeddable.
        embedded = get_embedded_auth_edges_batch(None, NAMESPACE, ROOM_ID, [a_id])[a_id]
        self.assertEqual(embedded, [create_id])
        embed_auth_edges_batch(None, NAMESPACE, ROOM_ID, [(a_id, [create_id])])
        self.assertEqual(
            get_embedded_auth_edges_batch(None, NAMESPACE, ROOM_ID, [a_id])[a_id],
            [create_id],
        )

    def test_incomplete_graph_raises_and_is_not_cached(self) -> None:
        # "$a" claims "$missing" as an auth event, but SQL has no
        # event_auth rows *and* no events row for "$missing" -- a genuine
        # gap, not an ordinary miss.
        graph = {"$a": ["$missing"]}
        txn = FakeTxn(graph, known_events={"$a"})
        a_id = self._short_id("$a")

        with self.assertRaises(IncompleteAuthGraph):
            self.cache.get_closure(txn, None, NAMESPACE, ROOM_ID, a_id)

        # An incomplete walk must not be cached: retrying (even with the
        # same broken graph) must raise again, not silently return a
        # wrong/partial cached closure.
        with self.assertRaises(IncompleteAuthGraph):
            self.cache.get_closure(txn, None, NAMESPACE, ROOM_ID, a_id)

    def test_cache_generation_invalidation_on_purge(self) -> None:
        graph = {"$create": [], "$a": ["$create"]}
        txn = FakeTxn(graph)
        create_id = self._short_id("$create")
        a_id = self._short_id("$a")

        first = self.cache.get_closure(txn, None, NAMESPACE, ROOM_ID, a_id)
        self.assertEqual(set(first), {create_id})

        # Purge the room: bump generation first (as purge_events.py must),
        # then delete the underlying skeleton.
        bump_room_generation(NAMESPACE, ROOM_ID)
        self.engine.auth_chain_purge_room(NAMESPACE, ROOM_ID)

        # Re-allocate short ids in the "new" room generation. An unused
        # placeholder alloc first shifts every subsequent id so
        # `create2_id` is guaranteed *not* to numerically equal
        # `create_id` -- otherwise a stale-cache bug could coincidentally
        # produce the right-looking set after the counter resets.
        self._short_id("$placeholder")
        new_graph = {"$create2": [], "$a": ["$create2"]}
        txn2 = FakeTxn(new_graph)
        create2_id = self._short_id("$create2")
        a2_id = self._short_id("$a")
        self.assertNotEqual(create2_id, create_id)

        second = self.cache.get_closure(txn2, None, NAMESPACE, ROOM_ID, a2_id)
        # The post-purge lookup must recompute against the new graph, not
        # serve the pre-purge cached bitmap for (old_generation, a_id).
        self.assertEqual(set(second), {create2_id})
        self.assertNotEqual(set(second), set(first))

    def test_get_closures_batch_shares_memo_across_roots(self) -> None:
        graph = {
            "$create": [],
            "$a": ["$create"],
            "$b": ["$a"],
            "$c": ["$a"],
        }
        txn = FakeTxn(graph)
        create_id = self._short_id("$create")
        a_id = self._short_id("$a")
        b_id = self._short_id("$b")
        c_id = self._short_id("$c")

        results = self.cache.get_closures_batch(
            txn, None, NAMESPACE, ROOM_ID, [b_id, c_id]
        )
        self.assertEqual(set(results[b_id]), {a_id, create_id})
        self.assertEqual(set(results[c_id]), {a_id, create_id})
