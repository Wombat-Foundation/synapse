--
-- This file is licensed under the Affero General Public License (AGPL) version 3.
--
-- Copyright (C) 2026 Element Creations Ltd.
--
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.
--
-- See the GNU Affero General Public License for more details:
-- <https://www.gnu.org/licenses/agpl-3.0.html>.

CREATE TABLE IF NOT EXISTS state_hamt_roots (
    state_group BIGINT PRIMARY KEY,
    room_id TEXT NOT NULL,
    structural_hash TEXT NOT NULL,
    state_group_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS state_hamt_roots_room_id_idx ON state_hamt_roots(room_id);

CREATE TABLE IF NOT EXISTS state_hamt_nodes (
    structural_hash TEXT PRIMARY KEY,
    node_bytes TEXT NOT NULL
);
