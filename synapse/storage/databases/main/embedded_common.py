from __future__ import annotations

import hashlib
from enum import Enum, auto


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


def maybe_sync(tier: SyncTier) -> None:
    """Sync mtxdb for DURABLE writes; no-op for CACHE writes.

    A DURABLE write has no SQL fallback, or uses accumulating/delta
    semantics (counters, auth-chain links, HAMT roots). A lost unflushed
    write here means silent data loss or incorrect state, so sync() is
    called after every batch.

    A CACHE write has a SQL fallback on the read path. A lost unflushed
    write just means a slower read via that fallback, not data loss.
    """
    if tier is SyncTier.DURABLE:
        from synapse.storage.databases.embedded_engine import get_embedded_engine

        engine = get_embedded_engine("mtxdb")
        engine.sync()
