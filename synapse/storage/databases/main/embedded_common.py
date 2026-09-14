from __future__ import annotations

import hashlib
from enum import Enum, auto
from typing import Iterable


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

    from synapse.storage.databases.embedded_engine import get_embedded_engine

    engine = get_embedded_engine("mtxdb")
    if pools is None:
        engine.sync()
        return
    pool_set = set(pools)
    if Pool.STATE in pool_set:
        engine.sync_state()
    if Pool.EVENT_DAG in pool_set:
        engine.sync_event_dag()
    if Pool.AUTH_CHAIN in pool_set:
        engine.sync_auth_chain()
