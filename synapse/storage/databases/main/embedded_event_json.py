#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2026 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

"""Mirrors `event_json` (the raw event blob: internal_metadata, json,
format_version, keyed by event_id) into the same embedded mtxdb keyspace the
state HAMT uses -- same rationale: write-once, immutable-ish, content-
addressed by event_id, pure point lookups (`get_event`), no
aggregation/joins needed against the blob itself (see
scripts-dev/benchmark_event_json_storage.py for the measurements: mtxdb
beat Postgres 23x at batch=1, 3.8x at batch=100).

Layout is room-aware (see `rust/src/database/mtxdb.rs`'s `event_json_*`
functions for the exact derivations):

- A sharded **locator** maps event_id -> its room's EventDag collection:
  256 deterministic bucket collections derived from the event's 128-bit
  identity hash, so no single index accumulates the server's whole event
  history (the old single global collection did -- see below).
- The room's `EventDag` collection holds two domain-separated records per
  event: the framed **body** (`event-body:` node) and, at initial persist, a
  compact **prev-edge** record (`prev-edges:` node) of its predecessor ids.
  Both coexist safely because their node ids are derived from differently
  tagged keys.

`event_json` (Postgres) stays authoritative and is always written; the
embedded engine is consulted first on reads, then the **legacy** global
`event_json:<ns>:<id>` keys that this module used before the layout became
room-aware (the Rust read path falls through to those automatically so
pre-migration mirror entries keep serving until a backfill retires them),
and finally a normal SQL `event_json` fetch.

Unlike the HAMT nodes/roots this mirrors, `event_json` rows are NOT
write-once/immutable in practice: censoring, expiry, and re-signing all
replace a row's `json` in place. Both of those paths explicitly re-mirror
the new value into mtxdb as part of the same transaction that updates SQL
(passing an empty prev list, so the write-once edge record is preserved).
The read-path SQL fallback in `events_worker.py`, however, deliberately
does NOT write back into mtxdb on a miss -- doing so racing a concurrent
censor/expiry could land a stale pre-censor value in mtxdb after the pruned
one, quietly undoing it, and there's no version/CAS scheme here to prevent
that. So a mirror gap (e.g. an id that predates this feature) stays a
permanent SQL fallback rather than self-healing; closing that gap needs an
explicit, serialized backfill job, not a read-path write.

Reuses the same `embedded_hamt_engine`/`embedded_hamt_path` config and mtxdb
keyspace the state store already opens (one flat keyspace, prefixed keys --
`hamt:node:...`, `hamt:root:...`, `event_json:...` -- rather than a second
mtxdb directory/config knob), and is on whenever that is -- see
`open_embedded_event_json_engine`. Namespacing via `embedded_hamt_namespace`
keeps multiple homeservers sharing one mtxdb file from colliding on event_id.
"""

from __future__ import annotations

import logging
import struct
from typing import TYPE_CHECKING

from synapse.storage.databases.main.embedded_common import (
    SyncTier,
    maybe_sync,
)

if TYPE_CHECKING:
    from synapse.server import HomeServer

logger = logging.getLogger(__name__)


def open_embedded_event_json_engine(hs: "HomeServer") -> bool:
    """Return whether the optional embedded event-JSON backend is enabled --
    on whenever the embedded engine itself is (`embedded_hamt_engine` +
    `embedded_hamt_path` configured), same as every other embedded mirror.

    Keys are namespaced by `embedded_hamt_namespace` (see `_event_json_key`),
    same scheme `embedded_event_to_state_group.py`/
    `embedded_event_auth_chain_links.py` already use, so multiple
    homeservers sharing one mtxdb file don't collide on event_id.
    """
    return bool(
        hs.config.database.embedded_hamt_engine
        and hs.config.database.embedded_hamt_path
    )


def _encode_event_json_record(
    internal_metadata: str, json: str, format_version: int | None
) -> bytes:
    """Encode event_json record. The entry type tag (0x04) is prepended by
    batch_put, so this function only encodes the payload."""
    internal_metadata_bytes = internal_metadata.encode("utf-8")
    json_bytes = json.encode("utf-8")
    # format_version is nullable in the schema (older rows); encode as a
    # signed int with -1 standing in for NULL rather than adding a presence
    # flag byte.
    return (
        struct.pack(">i", -1 if format_version is None else format_version)
        + struct.pack(">I", len(internal_metadata_bytes))
        + internal_metadata_bytes
        + json_bytes
    )


def _decode_event_json_record(value: bytes) -> tuple[str, str, int | None]:
    """Decode an event_json payload returned by `event_json_get`."""
    if len(value) < 8:
        raise RuntimeError("truncated event_json record")
    (format_version_raw,) = struct.unpack(">i", value[0:4])
    (metadata_len,) = struct.unpack(">I", value[4:8])
    metadata_start = 8
    json_start = metadata_start + metadata_len
    if len(value) < json_start:
        raise RuntimeError("truncated event_json record")
    internal_metadata = value[metadata_start:json_start].decode("utf-8")
    json_str = value[json_start:].decode("utf-8")
    format_version = None if format_version_raw == -1 else format_version_raw
    return internal_metadata, json_str, format_version


def put_event_json_batch(
    engine_name: str | None,
    namespace: str,
    rows: list[tuple[str, str, str, str, int | None]],
    *,
    sync: bool = False,
) -> None:
    """`rows`: `(event_id, room_id, internal_metadata, json, format_version)`.
    Called from the event persister only (the sole writer of `event_json`),
    synchronously in the persisting transaction -- same reasoning as
    `_store_state_hamt_root_embedded_txn`: an mtxdb call is local, no
    network round-trip to justify deferring past commit.

    By default, does not call sync() after the write: unlike every other
    embedded sidecar, get_event_json_batch's caller falls back to SQL on a
    miss (see its docstring), so an unflushed write lost to a crash before
    the next fsync just means a slower read via that fallback, not silent
    data loss -- not worth paying a synchronous fsync on this hot a path
    for every persisted event.

    For censorship/expiry operations, pass `sync=True` to ensure the
    replacement is durable before returning, preventing a crash from
    leaving stale pre-censor content in the mirror.
    """
    from synapse.synapse_rust.mtxdb_engine import event_json_put

    tuples = [
        (
            room_id,
            event_id,
            _encode_event_json_record(internal_metadata, json, format_version),
        )
        for event_id, room_id, internal_metadata, json, format_version in rows
    ]
    event_json_put(namespace, tuples)

    if sync:
        maybe_sync(SyncTier.DURABLE)


def get_event_json_batch(
    engine_name: str | None, namespace: str, event_ids: list[str]
) -> dict[str, tuple[str, str, int | None]]:
    """Returns `event_id -> (internal_metadata, json, format_version)` for
    every id found in the embedded engine; a missing id is simply absent
    from the result (the caller falls back to SQL for it).
    """
    from synapse.synapse_rust.mtxdb_engine import event_json_get

    found = event_json_get(namespace, event_ids)
    return {
        event_id: _decode_event_json_record(record)
        for event_id, record in found
        if record is not None
    }


def delete_event_json_batch(
    engine_name: str | None, namespace: str, event_ids: list[str]
) -> None:
    """Removes `event_id`s from the embedded mirror. Must be called wherever
    `event_json` rows are deleted from SQL (purge_events.py) so the mirror
    doesn't retain data the user asked to be purged -- see also
    `put_event_json_batch`, which is called wherever `event_json` is
    replaced in place (censor_events.py) rather than deleted.
    """
    if not event_ids:
        return
    from synapse.synapse_rust.mtxdb_engine import event_json_delete

    event_json_delete(namespace, event_ids)
    maybe_sync(SyncTier.DURABLE)
