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

# ── Event-driven coalescing flush (replaces per-write and periodic sync) ──
# Write helpers call mark_dirty(pool) after a successful embedded write.
# On the clean→dirty transition a single delayed flush is scheduled via the
# Twisted reactor; only dirty pools are synced and the flags are cleared
# after a successful sync.  maybe_sync(DURABLE) remains for explicit
# barriers (purge, destructive redaction, shutdown) that need immediate
# durability.
_DIRTY_POOLS: set[Pool] = set()
_FLUSH_SCHEDULED: bool = False
_FLUSH_DELAY: float = 0.5  # seconds — batches bursts without stale data


def _flush_dirty_pools() -> None:
    """Sync only dirty pools and clear the dirty flags.

    Called by the reactor after a short delay following the clean→dirty
    transition.  Removes the flushed pools from ``_DIRTY_POOLS`` so a
    new write can schedule another flush if needed.
    """
    global _FLUSH_SCHEDULED
    _FLUSH_SCHEDULED = False
    if not _DIRTY_POOLS or _sync_disabled:
        return
    pools_to_sync = set(_DIRTY_POOLS)
    _DIRTY_POOLS.clear()
    _do_sync(pools_to_sync)


def mark_dirty(pool: Pool) -> None:
    """Mark a pool as having unflushed writes.

    On the first clean→dirty transition for *any* pool, schedules a
    delayed flush (``_FLUSH_DELAY`` seconds) so a burst of rapid writes
    coalesces into a single fsync.  Subsequent dirty marks while a flush
    is already scheduled are free (one ``set.add``).
    """
    global _FLUSH_SCHEDULED
    if _sync_disabled:
        return
    _DIRTY_POOLS.add(pool)
    if not _FLUSH_SCHEDULED:
        _FLUSH_SCHEDULED = True
        from twisted.internet import reactor

        reactor.callLater(_FLUSH_DELAY, _flush_dirty_pools)  # type: ignore[attr-defined]


def sync_flush(pools: Iterable[Pool] | None = None) -> None:
    """Immediately sync the specified pools (or all if ``None``).

    Used for explicit durability barriers: purge, destructive redaction,
    and shutdown.  Also clears dirty flags for the synced pools so the
    next ``mark_dirty`` correctly re-schedules.
    """
    if _sync_disabled:
        return
    if pools is not None:
        _DIRTY_POOLS.difference_update(pools)
    else:
        _DIRTY_POOLS.clear()
    pool_set = (
        set(pools)
        if pools is not None
        else {Pool.STATE, Pool.EVENT_DAG, Pool.AUTH_CHAIN}
    )
    _do_sync(pool_set)


def _do_sync(pool_set: set[Pool]) -> None:
    """Perform the actual sync for the given set of pools."""
    from synapse.storage.databases.embedded_engine import get_embedded_engine

    engine = get_embedded_engine("mtxdb")
    if pool_set == {Pool.STATE, Pool.EVENT_DAG, Pool.AUTH_CHAIN}:
        _st = time.monotonic()
        engine.sync()
        ffi_timing("ffi_sync_all", time.monotonic() - _st)
        return
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


# ── FFI boundary timing (opt-in via SYNAPSE_PG_TIMINGS=1) ────────────────
_FFI_TIMINGS: dict[str, float] = defaultdict(float)
_FFI_TIMING_COUNTS: dict[str, int] = defaultdict(int)
_FFI_LATENCIES: dict[str, list[float]] = defaultdict(list)
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
            _FFI_LATENCIES[tag].append(elapsed)
    else:
        _FFI_TIMINGS[tag] += elapsed
        _FFI_TIMING_COUNTS[tag] += 1
        _FFI_LATENCIES[tag].append(elapsed)


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
        latencies = {k: sorted(v) for k, v in _FFI_LATENCIES.items() if v}
    _ffi_timings_print("\n=== FFI boundary timings ===")
    has_hist = bool(latencies)
    if has_hist:
        _ffi_timings_print(
            f"  {'':50s}  {'total':>9s}  {'calls':>6s}  {'avg':>11s}  {'p50':>10s}  {'p95':>10s}  {'p99':>10s}",
        )
    else:
        _ffi_timings_print(
            f"  {'':50s}  {'total':>9s}  {'calls':>6s}  {'avg':>11s}",
        )

    def _percentile(sorted_vals: list[float], p: float) -> float:
        if not sorted_vals:
            return 0.0
        idx = int(len(sorted_vals) * p)
        idx = min(idx, len(sorted_vals) - 1)
        return sorted_vals[idx]

    for tag in sorted(timings):
        total_s = timings[tag]
        count = counts[tag]
        total_ms = total_s * 1000
        avg_ms = (total_s / count) * 1000 if count else 0.0
        if tag in latencies:
            s = latencies[tag]
            p50_ms = _percentile(s, 0.50) * 1000
            p95_ms = _percentile(s, 0.95) * 1000
            p99_ms = _percentile(s, 0.99) * 1000
            _ffi_timings_print(
                f"  {tag:50s}  {total_ms:8.1f}ms  {count:6d}  {avg_ms:10.3f}ms  {p50_ms:9.3f}ms  {p95_ms:9.3f}ms  {p99_ms:9.3f}ms",
            )
        else:
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


# ── mtxdb runtime stats (opt-in via SYNAPSE_MTXDB_STATS=1) ──────────────


def _print_mtxdb_stats() -> None:
    """Print an end-of-run mtxdb runtime stats report for all three pools."""
    if not os.environ.get("SYNAPSE_MTXDB_STATS"):
        return
    try:
        from synapse.storage.databases.embedded_engine import get_embedded_engine

        engine = get_embedded_engine("mtxdb")
        s = engine.stats()
    except Exception:
        return

    import sys

    out = sys.stderr

    def _fmt_us(us: int) -> str:
        if us < 1000:
            return f"{us}us"
        if us < 1_000_000:
            return f"{us / 1000:.1f}ms"
        return f"{us / 1_000_000:.2f}s"

    print("\n=== mtxdb runtime stats ===", file=out)
    for pool_name in ("state", "event_dag", "auth_chain"):
        ps = s.get(pool_name, {})
        if not ps:
            continue
        print(f"\n  [{pool_name}]", file=out)
        print(
            f"    collections: {ps.get('collection_count', 0)}  shards: {ps.get('shard_count', 0)}  index: {ps.get('index_bytes', 0):,}B",
            file=out,
        )

        # Read counters (only meaningful when stats_enabled was true).
        gc = ps.get("get_calls", 0)
        gm = ps.get("get_misses", 0)
        gmc = ps.get("get_many_calls", 0)
        gmr = ps.get("get_many_records", 0)
        gmm = ps.get("get_many_misses", 0)
        if gc or gmc:
            hit_rate = ps.get("cache_hit_rate", 0.0)
            print(
                f"    get: {gc} calls, {gm} misses | get_many: {gmc} calls, {gmr} records, {gmm} misses",
                file=out,
            )
            print(
                f"    cache: hits={ps.get('cache_hits', 0)}  misses={ps.get('cache_misses', 0)}  rate={hit_rate:.3f}",
                file=out,
            )

        # Write / batch counters.
        pc = ps.get("put_calls", 0)
        pmc = ps.get("put_many_calls", 0)
        pmr = ps.get("put_many_records", 0)
        pmb = ps.get("put_many_bytes", 0)
        if pc or pmc:
            avg_r = pmr / pmc if pmc else 0
            avg_b = pmb / pmc if pmc else 0
            fast = ps.get("put_many_fast_path_calls", 0)
            clone = ps.get("put_many_clone_path_calls", 0)
            print(
                f"    put: {pc} calls, {ps.get('put_bytes', 0):,}B | put_many: {pmc} calls, {pmr:,} records, {pmb:,}B (avg {avg_r:.1f}r/{avg_b:.0f}B)",
                file=out,
            )
            if fast or clone:
                print(
                    f"    put_many path: fast={fast}  clone={clone}  index_clone={_fmt_us(ps.get('index_clone_time_us', 0))}",
                    file=out,
                )

        # Sync / persistence.
        sc = ps.get("sync_calls", 0)
        if sc:
            print(
                f"    sync: {sc} calls  checkpoint_writes={ps.get('checkpoint_writes', 0)}  delta_appends={ps.get('delta_appends', 0)}  invalidations={ps.get('delta_invalidations', 0)}",
                file=out,
            )

        # Shard write stats (persisted across opens).
        sw = ps.get("shard_write_count", 0)
        sb = ps.get("shard_bytes_written", 0)
        ss = ps.get("shard_sync_count", 0)
        if sw:
            print(f"    shard writes: {sw:,} records, {sb:,}B, {ss} syncs", file=out)

        # Index ops.
        ig = ps.get("index_grow_count", 0)
        ir = ps.get("index_rebuild_count", 0)
        if ig or ir:
            print(f"    index: grows={ig}  rebuilds={ir}", file=out)

        # Repack.
        rc = ps.get("repack_count", 0)
        if rc:
            print(
                f"    repack: {rc} calls, kept={ps.get('repack_kept', 0):,}, dropped={ps.get('repack_dropped', 0):,}",
                file=out,
            )

        # Open timings.
        ot = ps.get("last_open_timings")
        if ot:
            print(
                f"    last open: {_fmt_us(ot.get('total_us', 0))} (shard={_fmt_us(ot.get('shard_open_us', 0))} metadata={_fmt_us(ot.get('metadata_load_us', 0))} checkpoint={_fmt_us(ot.get('checkpoint_decode_us', 0))} index={_fmt_us(ot.get('index_materialization_us', 0))} delta={_fmt_us(ot.get('delta_replay_us', 0))} scan={_fmt_us(ot.get('full_scan_us', 0))})",
                file=out,
            )

        # Sync timings.
        st = ps.get("last_sync_timings")
        if st:
            print(
                f"    last sync: {_fmt_us(st.get('total_us', 0))} (flush={_fmt_us(st.get('pack_flush_us', 0))} fsync={_fmt_us(st.get('pack_fsync_us', 0))} sidecar={_fmt_us(st.get('sidecar_us', 0))} delta={_fmt_us(st.get('delta_log_us', 0))} checkpoint={_fmt_us(st.get('checkpoint_us', 0))})",
                file=out,
            )

    print("=============================\n", file=out)


if os.environ.get("SYNAPSE_MTXDB_STATS"):
    atexit.register(_print_mtxdb_stats)


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
    """Immediate durability barrier — sync the specified pools now.

    Use for destructive operations (purge, redaction) and shutdown.
    Normal write paths should call ``mark_dirty(pool)`` instead; the
    coalescing flush handles durability without per-write fsyncs.
    """
    if tier is not SyncTier.DURABLE:
        return
    sync_flush(pools)
