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
    """Call sync() if the tier requires durability."""
    if tier == SyncTier.DURABLE:
        from synapse.synapse_rust.mtxdb_engine import sync

        sync()
