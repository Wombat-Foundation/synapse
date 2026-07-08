#!/usr/bin/env python
#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2026 Element Creations Ltd.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.

import argparse
import asyncio
import time
from typing import Any, Collection, Iterable, cast

import synapse.event_auth  # Import event_auth first to resolve circular import dependency
import synapse.state  # noqa: F401
import synapse.state.v2 as v2
from synapse.api.room_versions import RoomVersion, RoomVersions, StateResolutionVersions
from synapse.events import EventBase
from synapse.state import StateDifference
from synapse.util.duration import Duration


# Mock Clock
class MockClock:
    def time_msec(self) -> int:
        return int(time.time() * 1000)

    async def sleep(self, duration: Duration) -> None:
        await asyncio.sleep(0)


# Mock Room Version
class MockRoomVersion:
    def __init__(self, real_version: RoomVersion, state_res: int | None) -> None:
        self.state_res = state_res
        for attr in dir(real_version):
            if not attr.startswith("_"):
                try:
                    setattr(self, attr, getattr(real_version, attr))
                except (AttributeError, TypeError):
                    pass
        self.state_res = state_res


# Mock Event compatible with PyO3 translation layer
class MockEvent:
    room_version: MockRoomVersion | None = None

    def __init__(
        self,
        event_id: str,
        sender: str,
        event_type: str,
        state_key: str | None,
        content: dict,
        origin_server_ts: int = 0,
        depth: int = 1,
        auth_event_ids: list[str] | None = None,
        prev_event_ids: list[str] | None = None,
        room_id: str = "!room:example.com",
    ):
        self.event_id = event_id
        self.sender = sender
        self.type = event_type
        self.state_key = state_key
        self.content = content
        self.room_id = room_id
        self._auth_event_ids: list[str] = auth_event_ids or []
        self._prev_event_ids: list[str] = prev_event_ids or []
        self.depth = depth
        self.origin_server_ts = origin_server_ts
        self.rejected_reason = None

    @property
    def membership(self) -> str | None:
        return self.content.get("membership")

    def auth_event_ids(self) -> list[str]:
        return self._auth_event_ids

    def prev_event_ids(self) -> list[str]:
        return self._prev_event_ids

    def get_state_key(self) -> str | None:
        return self.state_key


# Mock Store matching Synapse's StateResolutionStore API
class MockStateResolutionStore:
    def __init__(self, event_map: dict[str, Any]):
        self.event_map = event_map
        self.auth_chains: dict[str, set[str]] = {}

    async def get_events(
        self, event_ids: Collection[str], allow_rejected: bool = False
    ) -> dict[str, EventBase]:
        return cast(
            dict[str, EventBase],
            {eid: self.event_map[eid] for eid in event_ids if eid in self.event_map},
        )

    def _get_auth_chain(self, event_ids: Iterable[str]) -> list[str]:
        if self.auth_chains:
            result = set()
            for eid in event_ids:
                if eid in self.auth_chains:
                    result.update(self.auth_chains[eid])
            return list(result)

        result = set()
        stack = list(event_ids)
        while stack:
            event_id = stack.pop()
            if event_id in result:
                continue
            result.add(event_id)
            event = self.event_map[event_id]
            for aid in event.auth_event_ids():
                stack.append(aid)
        return list(result)

    async def get_auth_chain_difference(
        self,
        room_id: str,
        auth_sets: list[set[str]],
        conflicted_state: set[str] | None,
        additional_backwards_reachable_conflicted_events: set[str] | None,
    ) -> StateDifference:
        chains = [frozenset(self._get_auth_chain(a)) for a in auth_sets]
        common = set(chains[0]).intersection(*chains[1:])
        return StateDifference(
            auth_difference=set().union(*chains) - common,
            conflicted_subgraph=None,
        )


async def main() -> None:
    import logging

    logging.basicConfig(level=logging.INFO, force=True)
    parser = argparse.ArgumentParser(
        description="Benchmark state resolution V2 (Rust vs Python)"
    )
    parser.add_argument(
        "-p", "--partitions", type=int, default=50, help="Number of partitions (P)"
    )
    parser.add_argument(
        "-n",
        "--events",
        type=int,
        default=100,
        help="Conflicting events per partition (N)",
    )
    parser.add_argument(
        "--jsonl",
        type=str,
        default=None,
        help="Path to JSONL DAG file to resolve",
    )
    args = parser.parse_args()
    P = args.partitions
    N = args.events

    room_id = "!room:example.com"
    real_version = RoomVersions.V6 if args.jsonl else RoomVersions.V2
    room_version_rust = MockRoomVersion(real_version, StateResolutionVersions.V2)
    room_version_py = MockRoomVersion(real_version, None)

    # All MockEvent instances will share room_version_rust initially
    MockEvent.room_version = room_version_rust

    if args.jsonl:
        import json

        print(f"Loading DAG from {args.jsonl}...")
        event_map: dict[str, Any] = {}
        events_list = []
        with open(args.jsonl, "r") as f:
            for line in f:
                if not line.strip():
                    continue
                d = json.loads(line)
                ev = MockEvent(
                    event_id=d["event_id"],
                    sender=d["sender"],
                    event_type=d["type"],
                    state_key=d.get("state_key"),
                    content=d["content"],
                    origin_server_ts=d["origin_server_ts"],
                    depth=d["depth"],
                    auth_event_ids=d["auth_events"],
                    prev_event_ids=d["prev_events"],
                    room_id=d["room_id"],
                )
                event_map[ev.event_id] = ev
                events_list.append(ev)

        # Sort events topologically by depth to simulate chronological ordering
        events_list.sort(key=lambda e: (e.depth, e.origin_server_ts))

        # Precompute auth chains for all events
        auth_chains: dict[str, set[str]] = {}
        for ev in events_list:
            chain = {ev.event_id}
            for aid in ev.auth_event_ids():
                if aid in auth_chains:
                    chain.update(auth_chains[aid])
            auth_chains[ev.event_id] = chain

        async def run_simulation(
            room_version_to_use: MockRoomVersion,
        ) -> tuple[float, dict]:
            # Set the room version for events
            MockEvent.room_version = room_version_to_use

            # Initialize store
            store = MockStateResolutionStore(event_map)
            store.auth_chains = auth_chains  # attach precomputed chains

            clock = MockClock()
            room_id = events_list[0].room_id

            # Map from event_id to the state *after* that event
            event_states: dict[str, dict[tuple[str, str], str]] = {}

            start_time = time.perf_counter()

            # Iterate through events and construct/resolve state
            for ev in events_list:
                prev_ids = ev.prev_event_ids()

                # Compute state before this event
                if not prev_ids:
                    state_before: dict[tuple[str, str], str] = {}
                elif len(prev_ids) == 1:
                    prev_id = prev_ids[0]
                    state_before = dict(event_states.get(prev_id, {}))
                else:
                    # Merge point! We must resolve the states after the prev events
                    state_sets = []
                    for pid in prev_ids:
                        if pid in event_states:
                            state_sets.append(event_states[pid])

                    if not state_sets:
                        state_before = {}
                    elif len(state_sets) == 1:
                        state_before = dict(state_sets[0])
                    else:
                        state_before = dict(
                            await v2.resolve_events_with_store(
                                cast(Any, clock),
                                room_id,
                                cast(RoomVersion, room_version_to_use),
                                state_sets,
                                None,
                                cast(Any, store),
                            )
                        )

                # Compute state after this event
                state_after = dict(state_before)
                if ev.state_key is not None:
                    state_after[(ev.type, ev.state_key)] = ev.event_id

                event_states[ev.event_id] = state_after

            duration = time.perf_counter() - start_time
            # Get state of the last event
            final_state = event_states[events_list[-1].event_id]
            return duration, final_state

        print("Simulating resolution using Rust (rezzy)...")
        dur_rust, res_rust = await run_simulation(room_version_rust)

        print("Simulating resolution using Python fallback...")
        dur_py, res_py = await run_simulation(room_version_py)

        # Restore
        MockEvent.room_version = room_version_rust

        assert res_rust == res_py, (
            "Error: Resolved states differ between Rust and Python!"
        )

        print("\nSimulation Results:")
        print("+" + "-" * 20 + "+" + "-" * 15 + "+" + "-" * 15 + "+")
        print(f"| {'Implementation':<18} | {'Duration (s)':<13} | {'Speedup':<13} |")
        print("+" + "-" * 20 + "+" + "-" * 15 + "+" + "-" * 15 + "+")
        print(f"| {'Python V2':<18} | {dur_py:<13.5f} | {'1.0x (Baseline)':<13} |")
        print(
            f"| {'Rust (rezzy)':<18} | {dur_rust:<13.5f} | {dur_py / dur_rust:<12.1f}x |"
        )
        print("+" + "-" * 20 + "+" + "-" * 15 + "+" + "-" * 15 + "+")
        return

    # Baseline Events
    event_map = {}

    # 1. CREATE
    create = MockEvent(
        "$CREATE",
        "@alice:example.com",
        "m.room.create",
        "",
        {"creator": "@alice:example.com"},
        1000,
    )
    event_map[create.event_id] = create

    # 2. MEMBERS
    alice_join = MockEvent(
        "$IMA",
        "@alice:example.com",
        "m.room.member",
        "@alice:example.com",
        {"membership": "join"},
        1001,
    )
    alice_join._auth_event_ids = [create.event_id]
    event_map[alice_join.event_id] = alice_join

    pl = MockEvent(
        "$IPOWER",
        "@alice:example.com",
        "m.room.power_levels",
        "",
        {"users": {"@alice:example.com": 100}, "users_default": 0},
        1002,
    )
    pl._auth_event_ids = [create.event_id, alice_join.event_id]
    event_map[pl.event_id] = pl

    # Join rules
    jr = MockEvent(
        "$IJR",
        "@alice:example.com",
        "m.room.join_rules",
        "",
        {"join_rule": "public"},
        1003,
    )
    jr._auth_event_ids = [create.event_id, alice_join.event_id, pl.event_id]
    event_map[jr.event_id] = jr

    baseline_state = {
        ("m.room.create", ""): create.event_id,
        ("m.room.member", "@alice:example.com"): alice_join.event_id,
        ("m.room.power_levels", ""): pl.event_id,
        ("m.room.join_rules", ""): jr.event_id,
    }

    # Generate P parallel partitions, each with N conflicting events
    state_sets = []

    for p in range(P):
        sender = f"@user_{p}:example.com"
        # Join user to the room first
        join_ev = MockEvent(
            f"$JOIN_{p}",
            sender,
            "m.room.member",
            sender,
            {"membership": "join"},
            2000 + p,
        )
        join_ev._auth_event_ids = [create.event_id, jr.event_id, pl.event_id]
        event_map[join_ev.event_id] = join_ev

        part_state = dict(baseline_state)
        part_state[("m.room.member", sender)] = join_ev.event_id

        prev_id = join_ev.event_id
        for i in range(N):
            # Each event changes a topic or custom type to create conflicts
            ev_id = f"$EV_{p}_{i}"
            ev = MockEvent(
                ev_id,
                sender,
                f"org.example.test_{i}",
                f"state_key_{i}",
                {"value": f"val_{p}_{i}"},
                3000 + p * N + i,
            )
            ev._auth_event_ids = [create.event_id, join_ev.event_id, pl.event_id]
            ev._prev_event_ids = [prev_id]
            event_map[ev_id] = ev

            assert ev.state_key is not None
            part_state[(ev.type, ev.state_key)] = ev_id
            prev_id = ev_id

        state_sets.append(part_state)

    clock = MockClock()
    store = MockStateResolutionStore(event_map)

    print("Benchmark Configuration:")
    print(f"  - Partitions: {P}")
    print(f"  - Conflicting events per partition: {N}")
    print(f"  - Total events in map: {len(event_map)}")
    print("  - Warm-up resolution...")

    # Warmup
    await v2.resolve_events_with_store(
        cast(Any, clock),
        room_id,
        cast(RoomVersion, room_version_rust),
        state_sets,
        cast(dict[str, EventBase], event_map),
        cast(Any, store),
    )

    # 1. Benchmark Rust (rezzy)
    start_rust = time.perf_counter()
    res_rust = dict(
        await v2.resolve_events_with_store(
            cast(Any, clock),
            room_id,
            cast(RoomVersion, room_version_rust),
            state_sets,
            cast(dict[str, EventBase], event_map),
            cast(Any, store),
        )
    )
    dur_rust = time.perf_counter() - start_rust

    # 2. Benchmark Python (fallback)
    # Configure MockEvent.room_version to use the Python-only mock room version
    MockEvent.room_version = room_version_py

    start_py = time.perf_counter()
    res_py = dict(
        await v2.resolve_events_with_store(
            cast(Any, clock),
            room_id,
            cast(RoomVersion, room_version_py),
            state_sets,
            cast(dict[str, EventBase], event_map),
            cast(Any, store),
        )
    )
    dur_py = time.perf_counter() - start_py

    # Restore
    MockEvent.room_version = room_version_rust

    assert res_rust == res_py, "Error: Resolved states differ between Rust and Python!"

    print("\nBenchmark Results:")
    print("+" + "-" * 20 + "+" + "-" * 15 + "+" + "-" * 15 + "+")
    print(f"| {'Implementation':<18} | {'Duration (s)':<13} | {'Speedup':<13} |")
    print("+" + "-" * 20 + "+" + "-" * 15 + "+" + "-" * 15 + "+")
    print(f"| {'Python V2':<18} | {dur_py:<13.5f} | {'1.0x (Baseline)':<13} |")
    print(f"| {'Rust (rezzy)':<18} | {dur_rust:<13.5f} | {dur_py / dur_rust:<12.1f}x |")
    print("+" + "-" * 20 + "+" + "-" * 15 + "+" + "-" * 15 + "+")


if __name__ == "__main__":
    asyncio.run(main())
