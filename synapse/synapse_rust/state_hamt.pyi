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
def build_typed_root(
    server_secret: bytes,
    room_id: str,
    entries: Sequence[tuple[str, str, str]],
) -> tuple[bytes, bytes, list[tuple[bytes, bytes]]]: ...
def decode_typed_root(
    root_bytes: bytes,
) -> tuple[bytes, list[tuple[str, bytes]]]: ...
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
