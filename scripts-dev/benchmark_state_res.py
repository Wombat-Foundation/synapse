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
import cProfile
import io
import json
import pstats
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Collection, Iterable, cast

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import synapse.event_auth  # noqa: E402  # Import first to resolve circular import dependency
import synapse.state  # noqa: F401,E402
import synapse.state.v2 as v2  # noqa: E402
from synapse.api.room_versions import (  # noqa: E402
    RoomVersion,
    RoomVersions,
    StateResolutionVersions,
)
from synapse.events import EventBase  # noqa: E402
from synapse.state import StateDifference  # noqa: E402
from synapse.util.duration import Duration  # noqa: E402


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

    class _InternalMetadata:
        def is_soft_failed(self) -> bool:
            return False

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
        self.internal_metadata = MockEvent._InternalMetadata()

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


@dataclass(slots=True)
class RunStats:
    total_s: float
    bookkeeping_s: float
    resolve_s: float
    merge_points: int


def _print_profile(profile: cProfile.Profile, title: str, limit: int) -> None:
    stream = io.StringIO()
    stats = pstats.Stats(profile, stream=stream)
    stats.sort_stats("cumtime")
    stats.print_stats(limit)
    print(f"\n{title}")
    print(stream.getvalue().rstrip())


def _load_jsonl_events(path: str) -> tuple[dict[str, Any], list[MockEvent]]:
    print(f"Loading DAG from {path}...")
    event_map: dict[str, Any] = {}
    events_list: list[MockEvent] = []
    room_id_hint = None
    filename = Path(path).name
    match = re.match(
        r"^(?:local|remote)-dag-(.+)-v\d+-.+\.jsonl$",
        filename,
    )
    if match:
        room_id_hint = f"!{match.group(1)}"

    with open(path, "r") as f:
        for line_no, line in enumerate(f, start=1):
            if not line.strip():
                continue

            d = json.loads(line)
            if "event_id" not in d:
                keys = ", ".join(sorted(d.keys()))
                raise ValueError(
                    "The --jsonl input must include a top-level 'event_id' field for each "
                    f"event. The first parsed row at line {line_no} had keys: {keys}. "
                    "This file looks like a DAG export that omits event IDs, so the "
                    "benchmark cannot build its event_map or resolve prev/auth chains from it."
                )

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
                room_id=d.get("room_id", room_id_hint) or "",
            )
            if not ev.room_id:
                raise ValueError(
                    "The --jsonl input must include a top-level 'room_id' field, or use "
                    "a file name that encodes the room id (for example local-dag-<room>-v*.jsonl). "
                    f"The row at line {line_no} had no room_id and the filename {filename!r} "
                    "did not match the expected pattern."
                )
            event_map[ev.event_id] = ev
            events_list.append(ev)

    return event_map, events_list


def _load_legacy_jsonl_events(path: str) -> tuple[dict[str, Any], list[MockEvent]]:
    """Backward-compatible loader for older JSONL dumps with explicit event IDs."""
    return _load_jsonl_events(path)


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
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Print a cProfile summary for each benchmarked run",
    )
    parser.add_argument(
        "--profile-limit",
        type=int,
        default=20,
        help="Number of cProfile rows to print per run",
    )
    args = parser.parse_args()
    P = args.partitions
    N = args.events

    room_id = "!room:example.com"
    real_version = RoomVersions.V2
    if args.jsonl:
        if "v11" in args.jsonl:
            real_version = getattr(
                RoomVersions, "V11", RoomVersions.V10
            )  # Fallback to V10 if V11 not found?
        elif "v6" in args.jsonl:
            real_version = RoomVersions.V6
        else:
            real_version = RoomVersions.V6
    room_version_rust = MockRoomVersion(real_version, StateResolutionVersions.V2)

    # All MockEvent instances will share room_version_rust initially
    MockEvent.room_version = room_version_rust

    if args.jsonl:
        event_map, events_list = _load_jsonl_events(args.jsonl)

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
            disable_rust: bool = False,
        ) -> tuple[RunStats, dict]:
            # Set the room version for events
            MockEvent.room_version = room_version_to_use

            rust_res_module: Any = None
            rust_resolve_fn: Any = None
            if disable_rust:
                try:
                    import synapse.synapse_rust.state_res as rust_res

                    rust_res_module = rust_res
                    rust_resolve_fn = cast(Any, rust_res.resolve_v2_via_lattice_fold)

                    def _disabled_rust(*args: Any, **kwargs: Any) -> None:
                        raise RuntimeError("Rust resolver disabled for Python baseline")

                    cast(Any, rust_res).resolve_v2_via_lattice_fold = _disabled_rust
                except Exception:
                    rust_res_module = None
                    rust_resolve_fn = None

            # Initialize store
            store = MockStateResolutionStore(event_map)
            store.auth_chains = auth_chains  # attach precomputed chains

            clock = MockClock()
            room_id = events_list[0].room_id

            # Map from event_id to the state *after* that event
            event_states: dict[str, dict[tuple[str, str], str]] = {}
            start_time = time.perf_counter()
            bookkeeping_s = 0.0
            resolve_s = 0.0
            merge_points = 0

            try:
                # Iterate through events and construct/resolve state
                for ev in events_list:
                    loop_start = time.perf_counter()
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
                            merge_points += 1
                            print(
                                f"Resolving {len(state_sets)} states at {ev.event_id}"
                            )
                            resolve_start = time.perf_counter()
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
                            resolve_s += time.perf_counter() - resolve_start

                    # Compute state after this event
                    state_after = dict(state_before)
                    if ev.state_key is not None:
                        state_after[(ev.type, ev.state_key)] = ev.event_id

                    event_states[ev.event_id] = state_after
                    bookkeeping_s += time.perf_counter() - loop_start

                duration = time.perf_counter() - start_time
                # Get state of the last event
                final_state = event_states[events_list[-1].event_id]
                return (
                    RunStats(
                        total_s=duration,
                        bookkeeping_s=bookkeeping_s - resolve_s,
                        resolve_s=resolve_s,
                        merge_points=merge_points,
                    ),
                    final_state,
                )
            finally:
                if rust_res_module is not None and rust_resolve_fn is not None:
                    rust_res_module.resolve_v2_via_lattice_fold = rust_resolve_fn

        async def run_profiled_simulation(
            room_version_to_use: MockRoomVersion,
            title: str,
            disable_rust: bool = False,
        ) -> tuple[RunStats, dict]:
            if not args.profile:
                return await run_simulation(room_version_to_use, disable_rust)

            profiler = cProfile.Profile()
            profiler.enable()
            try:
                result = await run_simulation(room_version_to_use, disable_rust)
            finally:
                profiler.disable()
            _print_profile(profiler, title, args.profile_limit)
            return result

        print("Simulating resolution using Rust (rezzy)...")
        stats_rust, res_rust = await run_profiled_simulation(
            room_version_rust, "cProfile: Rust run"
        )

        print("Simulating resolution using Python fallback...")
        try:
            stats_py, res_py = await run_profiled_simulation(
                room_version_rust, "cProfile: Python run", disable_rust=True
            )
        except Exception as e:
            print(
                "Python fallback benchmark failed for this DAG. "
                f"The Rust run completed, but the Python path raised: {type(e).__name__}: {e}"
            )
            return

        # Restore
        MockEvent.room_version = room_version_rust

        assert res_rust == res_py, (
            "Error: Resolved states differ between Rust and Python!"
        )

        print("\nSimulation Results:")
        print("+" + "-" * 20 + "+" + "-" * 15 + "+" + "-" * 15 + "+" + "-" * 15 + "+")
        print(
            f"| {'Implementation':<18} | {'Duration (s)':<13} | {'Resolve (s)':<13} | {'Speedup':<13} |"
        )
        print("+" + "-" * 20 + "+" + "-" * 15 + "+" + "-" * 15 + "+" + "-" * 15 + "+")
        print(
            f"| {'Python V2':<18} | {stats_py.total_s:<13.5f} | {stats_py.resolve_s:<13.5f} | {'1.0x (Baseline)':<13} |"
        )
        print(
            f"| {'Rust (rezzy)':<18} | {stats_rust.total_s:<13.5f} | {stats_rust.resolve_s:<13.5f} | {stats_py.total_s / stats_rust.total_s:<12.1f}x |"
        )
        print("+" + "-" * 20 + "+" + "-" * 15 + "+" + "-" * 15 + "+" + "-" * 15 + "+")
        print("\nStage breakdown:")
        print(
            f"  - Python total: {stats_py.total_s:.5f}s, bookkeeping: {stats_py.bookkeeping_s:.5f}s, resolver: {stats_py.resolve_s:.5f}s, merge points: {stats_py.merge_points}"
        )
        print(
            f"  - Rust total:   {stats_rust.total_s:.5f}s, bookkeeping: {stats_rust.bookkeeping_s:.5f}s, resolver: {stats_rust.resolve_s:.5f}s, merge points: {stats_rust.merge_points}"
        )
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
    state_sets: list[dict[tuple[str, str], str]] = []

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

    async def run_resolution(
        room_version_to_use: MockRoomVersion,
        title: str,
        disable_rust: bool = False,
    ) -> tuple[RunStats, dict]:
        MockEvent.room_version = room_version_to_use

        rust_res_module: Any = None
        rust_resolve_fn: Any = None
        if disable_rust:
            try:
                import synapse.synapse_rust.state_res as rust_res

                rust_res_module = rust_res
                rust_resolve_fn = cast(Any, rust_res.resolve_v2_via_lattice_fold)

                def _disabled_rust(*args: Any, **kwargs: Any) -> None:
                    raise RuntimeError("Rust resolver disabled for Python baseline")

                cast(Any, rust_res).resolve_v2_via_lattice_fold = _disabled_rust
            except Exception:
                rust_res_module = None
                rust_resolve_fn = None

        profiler = cProfile.Profile() if args.profile else None
        if profiler is not None:
            profiler.enable()

        try:
            start = time.perf_counter()
            resolved_state = dict(
                await v2.resolve_events_with_store(
                    cast(Any, clock),
                    room_id,
                    cast(RoomVersion, room_version_to_use),
                    state_sets,
                    cast(dict[str, EventBase], event_map),
                    cast(Any, store),
                )
            )
            duration = time.perf_counter() - start
        finally:
            if profiler is not None:
                profiler.disable()
                _print_profile(profiler, title, args.profile_limit)
            if rust_res_module is not None and rust_resolve_fn is not None:
                rust_res_module.resolve_v2_via_lattice_fold = rust_resolve_fn

        return (
            RunStats(
                total_s=duration,
                bookkeeping_s=0.0,
                resolve_s=duration,
                merge_points=1,
            ),
            resolved_state,
        )

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
    stats_rust, res_rust = await run_resolution(room_version_rust, "cProfile: Rust run")

    # 2. Benchmark Python (fallback)
    # Configure MockEvent.room_version to use the Python-only mock room version
    stats_py, res_py = await run_resolution(
        room_version_rust, "cProfile: Python run", disable_rust=True
    )

    # Restore
    MockEvent.room_version = room_version_rust

    assert res_rust == res_py, "Error: Resolved states differ between Rust and Python!"

    print("\nBenchmark Results:")
    print("+" + "-" * 20 + "+" + "-" * 15 + "+" + "-" * 15 + "+" + "-" * 15 + "+")
    print(
        f"| {'Implementation':<18} | {'Duration (s)':<13} | {'Resolver (s)':<13} | {'Speedup':<13} |"
    )
    print("+" + "-" * 20 + "+" + "-" * 15 + "+" + "-" * 15 + "+" + "-" * 15 + "+")
    print(
        f"| {'Python V2':<18} | {stats_py.total_s:<13.5f} | {stats_py.resolve_s:<13.5f} | {'1.0x (Baseline)':<13} |"
    )
    print(
        f"| {'Rust (rezzy)':<18} | {stats_rust.total_s:<13.5f} | {stats_rust.resolve_s:<13.5f} | {stats_py.total_s / stats_rust.total_s:<12.1f}x |"
    )
    print("+" + "-" * 20 + "+" + "-" * 15 + "+" + "-" * 15 + "+" + "-" * 15 + "+")
    print("\nStage breakdown:")
    print(
        f"  - Python total: {stats_py.total_s:.5f}s, resolver: {stats_py.resolve_s:.5f}s, merge points: {stats_py.merge_points}"
    )
    print(
        f"  - Rust total:   {stats_rust.total_s:.5f}s, resolver: {stats_rust.resolve_s:.5f}s, merge points: {stats_rust.merge_points}"
    )


if __name__ == "__main__":
    asyncio.run(main())
