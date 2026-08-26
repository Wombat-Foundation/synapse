from collections.abc import Sequence

def room_structural_key(server_secret: bytes, room_id: str) -> bytes: ...
def room_tikv_prefix(
    server_secret: bytes,
    room_id: str,
    msc4291_room_ids_as_hashes: bool,
) -> bytes: ...
def build_root_handle(
    server_secret: bytes,
    room_id: str,
    entries: Sequence[tuple[str, str, str]],
) -> tuple[tuple[bytes, bytes], list[tuple[bytes, bytes]]]: ...
def build_root_handle_with_lattice(
    server_secret: bytes,
    room_id: str,
    entries: Sequence[tuple[str, str, str]],
) -> tuple[bytes, bytes, bytes, list[tuple[bytes, bytes]]]:
    """Returns (structural_hash, state_group_id, lattice_bytes, nodes).

    Same as `build_root_handle`, but also returns the full, retained
    2048-byte `LtHash` lattice (not just its collapsed `state_group_id`
    digest). A caller must keep `lattice_bytes` alongside the root if it
    wants to apply further incremental updates via
    `apply_flat_state_updates` later — the digest alone cannot be
    "un-collapsed" back into an updatable lattice.
    """

def apply_flat_state_updates(
    server_secret: bytes,
    room_id: str,
    root_node_bytes: bytes,
    nodes: Sequence[tuple[bytes, bytes]],
    lattice_bytes: bytes,
    updates: Sequence[tuple[str, str, str | None]],
) -> tuple[bytes, bytes, bytes, list[tuple[bytes, bytes]]]:
    """Applies single-key changes to an existing flat HAMT root via
    O(log S) path-copying, without materializing or rebuilding the whole
    state map.

    Returns (structural_hash, state_group_id, lattice_bytes, new_nodes) for
    the resulting root. `new_nodes` contains *only* the newly created nodes
    (i.e. excluding anything already present in `nodes`) — this is the
    O(changed-path) node set, not the whole reachable tree.

    `nodes` must include the nodes along the path(s) to every key being
    changed, plus the root itself; the caller is expected to fetch only
    what's needed for `updates`, not the whole tree. `lattice_bytes` is the
    *retained* lattice for the current root (from `build_root_handle_with_lattice`
    or a prior call to this function), not the collapsed `state_group_id`.

    `updates` is `(event_type, state_key, new_event_id)`, where
    `new_event_id=None` means "remove this key".

    Raises if a resolver lookup misses a hash not present in `nodes` (the
    error names the missing hash so the caller can fetch it and retry) or on
    a HAMT hash collision. Nothing is partially applied on error.
    """

def build_typed_root(
    server_secret: bytes,
    room_id: str,
    entries: Sequence[tuple[str, str, str]],
) -> tuple[bytes, bytes, bytes, list[tuple[bytes, bytes]]]:
    """Returns (structural_hash, state_group_id, root_bytes, nodes).

    `state_group_id` is the unkeyed, cross-server-comparable LtHash-derived
    identity (matches `build_root_handle`'s second tuple element for the same
    logical state); `structural_hash` is the typed directory's local, keyed
    structural identity and must not be used as a state-group identifier.
    """

def decode_typed_root(
    root_bytes: bytes,
) -> tuple[bytes, bytes, list[tuple[str, bytes]]]:
    """Returns (structural_hash, state_group_id, directory)."""

def materialize_state_entries(
    root_node_bytes: bytes,
    nodes: Sequence[tuple[bytes, bytes]],
) -> list[tuple[str, str, str]]: ...
def lookup_state_entries(
    server_secret: bytes,
    room_id: str,
    root_node_bytes: bytes,
    nodes: Sequence[tuple[bytes, bytes]],
    keys: Sequence[tuple[str, str]],
) -> tuple[list[tuple[str, str, str]], list[bytes]]: ...
def node_child_hashes(node_bytes: bytes) -> list[bytes]: ...
def reachability_audit(
    root_node_bytes: bytes,
    roots: Sequence[bytes],
    universe: Sequence[bytes],
    nodes: Sequence[tuple[bytes, bytes]],
) -> tuple[list[bytes], list[bytes]]: ...
def unreachable_node_hashes(
    root_node_bytes: bytes,
    roots: Sequence[bytes],
    universe: Sequence[bytes],
    nodes: Sequence[tuple[bytes, bytes]],
) -> list[bytes]: ...
