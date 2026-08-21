from collections.abc import Iterable

def room_structural_key(server_secret: bytes, room_id: str) -> bytes: ...
def build_root_handle(
    server_secret: bytes,
    room_id: str,
    entries: Iterable[tuple[int, int]],
) -> tuple[tuple[bytes, bytes], list[tuple[bytes, bytes]]]: ...
