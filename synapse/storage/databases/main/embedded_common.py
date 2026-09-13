from __future__ import annotations

from enum import Enum, auto


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
