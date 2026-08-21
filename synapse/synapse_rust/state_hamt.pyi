from collections.abc import Sequence

def room_structural_key(server_secret: bytes, room_id: str) -> bytes: ...
def build_root_handle(
    server_secret: bytes,
    room_id: str,
    entries: Sequence[tuple[str, str, str]],
) -> tuple[tuple[bytes, bytes], list[tuple[bytes, bytes]]]: ...
def materialize_state_entries(
    root_node_bytes: bytes,
    nodes: Sequence[tuple[bytes, bytes]],
) -> list[tuple[str, str, str]]: ...
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
