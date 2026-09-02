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

"""Mirrors `event_to_state_groups` (event_id -> state_group, a pure point
lookup with no aggregation/joins needed against it -- see
`_get_state_group_for_event(s)` in `state.py`) into the same embedded mdbx
keyspace `event_json` and the state HAMT use. `event_to_state_groups`
(Postgres) stays authoritative and is always written; the embedded engine is
consulted first on reads and any event_id it's missing falls back to a
normal SQL fetch.

NOT write-once/immutable, same caveat as `event_json`: partial-state events
(fast-join) get their `state_group` rewritten in place once resolution
completes -- see `state.py`'s `_update_state_for_partial_state_event_txn`,
a real `simple_update_txn` on this table, not an insert. That path
re-mirrors the new value into mdbx as part of the same transaction that
updates SQL (see `put_event_to_state_group_batch`'s callers). The read-path
SQL fallback deliberately does NOT write back into mdbx on a miss, for the
same race-safety reason as `embedded_event_json.py`: a reader could still
land a stale pre-update value in mdbx after a concurrent
`update_state_for_partial_state_event` already updated it, with no
version/CAS scheme here to prevent that. A mirror gap stays a permanent SQL
fallback rather than self-healing; closing it needs an explicit, serialized
backfill job, not a read-path write.
"""

from __future__ import annotations

import logging
import struct

logger = logging.getLogger(__name__)


def _event_to_state_group_key(event_id: str) -> bytes:
    return b"event_to_state_group:" + event_id.encode("utf-8")


def put_event_to_state_group_batch(rows: list[tuple[str, int]]) -> None:
    """`rows`: `(event_id, state_group)`. Called both from the event
    persister (initial insert, `_store_event_state_mappings_txn`) and from
    `update_state_for_partial_state_event` (in-place rewrite once partial
    state resolves) -- synchronously, in the same transaction that writes
    SQL, same reasoning as `embedded_event_json.put_event_json_batch`.
    """
    from synapse.synapse_rust import mdbx_engine

    pairs = [
        (_event_to_state_group_key(event_id), struct.pack(">q", state_group))
        for event_id, state_group in rows
    ]
    mdbx_engine.batch_put(pairs)


def get_state_group_for_events_batch(event_ids: list[str]) -> dict[str, int]:
    """Returns `event_id -> state_group` for every id found in the embedded
    engine; a missing id is simply absent from the result (the caller falls
    back to SQL for it).
    """
    from synapse.synapse_rust import mdbx_engine

    keys = [_event_to_state_group_key(event_id) for event_id in event_ids]
    key_to_event_id = dict(zip(keys, event_ids))
    found = mdbx_engine.batch_get(keys)
    out = {}
    for key, value in found:
        value = bytes(value)
        if len(value) != 8:
            raise RuntimeError("invalid event_to_state_group record")
        (state_group,) = struct.unpack(">q", value)
        out[key_to_event_id[bytes(key)]] = state_group
    return out


def delete_event_to_state_group_batch(event_ids: list[str]) -> None:
    """Removes `event_id`s from the embedded mirror. Must be called wherever
    `event_to_state_groups` rows are deleted from SQL (purge_events.py) so
    the mirror doesn't retain data the user asked to be purged.
    """
    if not event_ids:
        return
    from synapse.synapse_rust import mdbx_engine

    keys = [_event_to_state_group_key(event_id) for event_id in event_ids]
    mdbx_engine.batch_delete(keys)
