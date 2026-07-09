from __future__ import annotations

from typing import Any

class SpanContext: ...

class Span:
    context: SpanContext
    start_time: float | None
    end_time: float | None

class Tracer:
    active_span: Span | None

class ConstSampler:
    def __init__(self, decision: bool) -> None: ...

class Config:
    sampler: Any

    def __init__(
        self,
        config: Any,
        service_name: str,
        scope_manager: Any,
        metrics_factory: Any = ...,
    ) -> None: ...
    def create_tracer(self, sampler: Any, reporter: Any = ...) -> Any: ...
    def initialize_tracer(self) -> None: ...
