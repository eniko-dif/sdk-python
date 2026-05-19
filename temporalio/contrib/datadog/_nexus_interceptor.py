"""Datadog tracing interceptor for Temporal Nexus operation inbound calls."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import nexusrpc.handler

import temporalio.worker

from temporalio.contrib.datadog._helpers import _TEMPORAL_TAG_PREFIX, _baggage_service_name

if TYPE_CHECKING:
    from temporalio.contrib.datadog._interceptor import DatadogTracingInterceptor


class _NexusOperationInboundInterceptor(
    temporalio.worker.NexusOperationInboundInterceptor
):
    def __init__(
        self,
        next: temporalio.worker.NexusOperationInboundInterceptor,
        root: DatadogTracingInterceptor,
    ) -> None:
        super().__init__(next)
        self.root = root

    def _ctx_from_nexus(self, headers: Mapping[str, str] | None) -> Any:
        if not headers:
            return None
        return self.root._extract(headers)

    async def execute_nexus_operation_start(
        self, input: temporalio.worker.ExecuteNexusOperationStartInput
    ) -> (
        nexusrpc.handler.StartOperationResultSync[Any]
        | nexusrpc.handler.StartOperationResultAsync
    ):
        parent_ctx = self._ctx_from_nexus(input.ctx.headers)
        parent_service = _baggage_service_name(parent_ctx)
        operation = "RunStartNexusOperationHandler"
        span = self.root.tracer.start_span(
            f"{_TEMPORAL_TAG_PREFIX}{operation}",
            child_of=parent_ctx,
            resource=f"{input.ctx.service}/{input.ctx.operation}",
            activate=True,
        )
        self.root._apply_tags(
            span,
            operation,
            {"NexusService": input.ctx.service, "NexusOperation": input.ctx.operation},
            parent_service,
        )
        _exc: BaseException | None = None
        try:
            result = await self.next.execute_nexus_operation_start(input)
        except BaseException as exc:
            _exc = exc
            raise
        finally:
            self.root._record_finish(span, operation, _exc)
            span.finish()
        return result

    async def execute_nexus_operation_cancel(
        self, input: temporalio.worker.ExecuteNexusOperationCancelInput
    ) -> None:
        parent_ctx = self._ctx_from_nexus(input.ctx.headers)
        parent_service = _baggage_service_name(parent_ctx)
        operation = "RunCancelNexusOperationHandler"
        span = self.root.tracer.start_span(
            f"{_TEMPORAL_TAG_PREFIX}{operation}",
            child_of=parent_ctx,
            resource=f"{input.ctx.service}/{input.ctx.operation}",
            activate=True,
        )
        self.root._apply_tags(
            span,
            operation,
            {"NexusService": input.ctx.service, "NexusOperation": input.ctx.operation},
            parent_service,
        )
        _exc: BaseException | None = None
        try:
            result = await self.next.execute_nexus_operation_cancel(input)
        except BaseException as exc:
            _exc = exc
            raise
        finally:
            self.root._record_finish(span, operation, _exc)
            span.finish()
        return result
