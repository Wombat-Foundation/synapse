from __future__ import annotations

from typing import Any

from opentracing import Tracer as OpenTracingTracer

from .reporter import BaseReporter
from .span import Span

class Tracer(OpenTracingTracer):
    active_span: Span | None

class Sampler: ...

class ConstSampler(Sampler):
    def __init__(self, decision: bool) -> None: ...

class Config:
    sampler: Any

    def __init__(
        self,
        config: Any,
        metrics: Any = ...,
        service_name: str = ...,
        metrics_factory: Any = ...,
        validate: bool = ...,
        scope_manager: Any = ...,
    ) -> None: ...
    def create_tracer(
        self, reporter: BaseReporter, sampler: Any, throttler: Any = ...
    ) -> Tracer: ...
    def initialize_tracer(self, io_loop: Any = ...) -> Tracer | None: ...
