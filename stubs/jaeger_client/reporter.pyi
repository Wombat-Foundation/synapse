from __future__ import annotations

from typing import Any

from .span import Span

class Reporter:
    def set_process(self, service_name: str, tags: Any, max_length: int) -> None: ...
    def report_span(self, span: Span) -> None: ...
    def close(self) -> None: ...

# Compatibility name used by Synapse's reporter subclass; it is not exported
# from the jaeger_client package root in current releases.
BaseReporter = Reporter

class NullReporter(Reporter): ...

class InMemoryReporter(Reporter):
    def __init__(self) -> None: ...
    def get_spans(self) -> list[Span]: ...
