from __future__ import annotations

import atexit
import hashlib
import os
import threading
import time
from collections import defaultdict
from contextlib import contextmanager
from enum import Enum, auto
from typing import IO, Iterable, Iterator

# Module-level flag: when True, all DURABLE-tier sync() calls are suppressed.
# Set once during HomeServer init via configure_sync(); never mutated after.
_sync_disabled: bool = False

# ── FFI boundary timing (opt-in via SYNAPSE_PG_TIMINGS=1) ────────────────
_FFI_TIMINGS: dict[str, float] = defaultdict(float)
_FFI_TIMING_COUNTS: dict[str, int] = defaultdict(int)
_FFI_TIMING_LOCK: "threading.Lock | None" = (
    threading.Lock() if os.environ.get("SYNAPSE_PG_TIMINGS") else None
)

_ffi_timings_file: IO[str] | None = None
if os.environ.get("SYNAPSE_PG_TIMINGS"):
    _ffi_timings_path = os.environ.get("SYNAPSE_PG_TIMINGS_FILE")
    if _ffi_timings_path:
        try:
            _ffi_timings_file = open(_ffi_timings_path, "a")
        except OSError:
            pass


def _ffi_timings_print(*args: object) -> None:
    import sys

    print(*args, file=sys.stderr)
    if _ffi_timings_file is not None:
        print(*args, file=_ffi_timings_file)


def ffi_timing(tag: str, elapsed: float) -> None:
    if not os.environ.get("SYNAPSE_PG_TIMINGS"):
        return
    lock = _FFI_TIMING_LOCK
    if lock is not None:
        with lock:
            _FFI_TIMINGS[tag] += elapsed
            _FFI_TIMING_COUNTS[tag] += 1
    else:
        _FFI_TIMINGS[tag] += elapsed
        _FFI_TIMING_COUNTS[tag] += 1


def _print_ffi_timings() -> None:
    if not os.environ.get("SYNAPSE_PG_TIMINGS"):
        return
    lock = _FFI_TIMING_LOCK
    if lock is None:
        return
    with lock:
        if not _FFI_TIMINGS:
            return
        timings = dict(_FFI_TIMINGS)
        counts = dict(_FFI_TIMING_COUNTS)
    _ffi_timings_print("\n=== FFI boundary timings ===")
    _ffi_timings_print(
        f"  {'':50s}  {'total':>9s}  {'calls':>6s}  {'avg':>11s}",
    )
    for tag in sorted(timings):
        total_s = timings[tag]
        count = counts[tag]
        total_ms = total_s * 1000
        avg_ms = (total_s / count) * 1000 if count else 0.0
        _ffi_timings_print(
            f"  {tag:50s}  {total_ms:8.1f}ms  {count:6d}  {avg_ms:10.3f}ms",
        )
    total_s = sum(timings.values())
    total_count = sum(counts.values())
    total_ms = total_s * 1000
    _ffi_timings_print("")
    _ffi_timings_print(
        f"  {'TOTAL':50s}  {total_ms:8.1f}ms  {total_count:6d}",
    )
    _ffi_timings_print("==============================")
    _ffi_timings_print("")


if os.environ.get("SYNAPSE_PG_TIMINGS"):
    atexit.register(_print_ffi_timings)


@contextmanager
def mirror_timing(tag: str) -> Iterator[None]:
    """Bracket an entire mirror-helper call (Python row/metadata construction
    *and* the native call it eventually makes) under one `mirror_<tag>`
    entry in the same aggregate report `ffi_timing` feeds -- distinct from
    the narrower `ffi_<tag>` entries individual call sites record around
    just the native call itself. Diffing `mirror_<tag>` against the matching
    `ffi_<tag>` isolates the Python-side share (encoding, list/dict
    construction, `get_embedded_engine` lookup) of a given helper's cost.

    Deliberately brackets the whole containing helper rather than timing
    every sub-step (e.g. every `get_embedded_engine()` lookup) individually
    -- more, finer-grained timers add their own overhead and risk
    perturbing the very measurement they're trying to take.

    No-op (near-zero overhead: one dict lookup, no timer read) when
    `SYNAPSE_PG_TIMINGS` isn't set, same as `ffi_timing`.
    """
    if not os.environ.get("SYNAPSE_PG_TIMINGS"):
        yield
        return
    start = time.monotonic()
    try:
        yield
    finally:
        ffi_timing(f"mirror_{tag}", time.monotonic() - start)


def configure_sync(*, no_sync: bool) -> None:
    """Set the module-level sync-disable flag.  Call once during init."""
    global _sync_disabled
    _sync_disabled = no_sync


def namespace_hash(namespace: str) -> bytes:
    """16-byte digest of a namespace, used to key every embedded mirror.

    Namespaced keys keep multiple homeservers sharing one mtxdb file from
    colliding on event ids / state-group ids. Kept here so every
    embedded_* module derives keys identically (see `_state_hamt_node_key`
    in `rust/src/database/core.rs` for the matching Rust-side derivation).
    """
    return hashlib.sha256(namespace.encode("utf-8")).digest()[:16]


class SyncTier(Enum):
    """Classification for embedded-sidecar write durability.

    DURABLE: No SQL fallback exists, or the write uses accumulating/delta
    semantics (counters, auth-chain links, HAMT roots).  A lost unflushed
    write here means silent data loss or incorrect state, so sync() is
    called after every batch.

    CACHE: A SQL fallback exists on the read path.  A lost unflushed write
    just means a slower read via that fallback, not data loss.  sync() is
    skipped to avoid per-event fsync cost on the hottest write path.
    """

    DURABLE = auto()
    CACHE = auto()


class Pool(Enum):
    """Which of mtxdb's three storage pools a DURABLE write touched.

    `sync()` on the Rust side used to always fsync `state`, `event_dag`,
    and `auth_chain` together, regardless of which one a given caller
    actually wrote to -- e.g. an `event_json_put` (event_dag, sharded
    across 256 locator-bucket collections) forced a flush of every
    dirty event_dag shard on the next unrelated `state.py` DURABLE sync,
    and vice versa. Passing the pool(s) a call site actually dirtied to
    `maybe_sync` lets it call the matching Rust-side `sync_state` /
    `sync_event_dag` / `sync_auth_chain` instead of the blanket `sync`,
    so unrelated modules stop forcing each other's flushes.
    """

    STATE = auto()
    EVENT_DAG = auto()
    AUTH_CHAIN = auto()


def maybe_sync(tier: SyncTier, pools: Iterable[Pool] | None = None) -> None:
    """Sync mtxdb for DURABLE writes; no-op for CACHE writes.

    A DURABLE write has no SQL fallback, or uses accumulating/delta
    semantics (counters, auth-chain links, HAMT roots). A lost unflushed
    write here means silent data loss or incorrect state, so sync() is
    called after every batch.

    A CACHE write has a SQL fallback on the read path. A lost unflushed
    write just means a slower read via that fallback, not data loss.

    `pools`: which pool(s) this call site's batch actually wrote to (see
    `Pool`'s doc comment). Omit only for a generic backstop that isn't
    tied to a specific write (e.g. a periodic timer flush) -- that syncs
    all three pools, same as before this parameter existed. A call site
    that knows what it wrote should always pass `pools` explicitly, so a
    write to one pool doesn't force a flush of another pool's unrelated
    dirty shards.
    """
    if tier is not SyncTier.DURABLE:
        return

    if _sync_disabled:
        return

    from synapse.storage.databases.embedded_engine import get_embedded_engine

    engine = get_embedded_engine("mtxdb")
    if pools is None:
        _st = time.monotonic()
        engine.sync()
        ffi_timing("ffi_sync_all", time.monotonic() - _st)
        return
    pool_set = set(pools)
    if Pool.STATE in pool_set:
        _st = time.monotonic()
        engine.sync_state()
        ffi_timing("ffi_sync_state", time.monotonic() - _st)
    if Pool.EVENT_DAG in pool_set:
        _st = time.monotonic()
        engine.sync_event_dag()
        ffi_timing("ffi_sync_event_dag", time.monotonic() - _st)
    if Pool.AUTH_CHAIN in pool_set:
        _st = time.monotonic()
        engine.sync_auth_chain()
        ffi_timing("ffi_sync_auth_chain", time.monotonic() - _st)
