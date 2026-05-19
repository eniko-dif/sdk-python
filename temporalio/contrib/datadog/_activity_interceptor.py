"""Datadog tracing interceptor for Temporal activity inbound calls."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import temporalio.activity
import temporalio.worker

from temporalio.contrib.datadog._helpers import (
    _BAGGAGE_ITEM_SERVICE,
    _baggage_service_name,
    _set_baggage,
)
from temporalio.contrib.datadog._id_generator import gen_span_id

if TYPE_CHECKING:
    from temporalio.contrib.datadog._interceptor import DatadogTracingInterceptor


class _ActivityInboundInterceptor(temporalio.worker.ActivityInboundInterceptor):
    def __init__(
        self,
        next: temporalio.worker.ActivityInboundInterceptor,
        root: DatadogTracingInterceptor,
    ) -> None:
        super().__init__(next)
        self.root = root

    async def execute_activity(
        self, input: temporalio.worker.ExecuteActivityInput
    ) -> Any:
        info = temporalio.activity.info()
        parent_ctx = self.root._extract_context_from_headers(input.headers)
        parent_service = _baggage_service_name(parent_ctx)
        attributes: dict[str, Any] = {
            "ActivityID": info.activity_id,
            "ActivityType": info.activity_type,
            "Attempt": info.attempt,
        }
        if info.workflow_id:
            attributes["WorkflowID"] = info.workflow_id
        if info.workflow_run_id:
            attributes["RunID"] = info.workflow_run_id
        if info.workflow_namespace:
            attributes["Namespace"] = info.workflow_namespace

        span = self.root.tracer.start_span(
            "temporal.RunActivity",
            child_of=parent_ctx,
            resource=info.activity_type,
            activate=True,
        )
        # Deterministic ID so retries of the same activity attempt have the
        # same span ID across worker restarts.
        idempotency_key = f"{info.workflow_run_id}:{info.activity_id}:{info.attempt}"
        span.span_id = gen_span_id(idempotency_key)
        self.root._apply_tags(span, "RunActivity", attributes, parent_service)
        if self.root.service_name:
            _set_baggage(span.context, _BAGGAGE_ITEM_SERVICE, self.root.service_name)
        _exc: BaseException | None = None
        try:
            result = await super().execute_activity(input)
        except BaseException as exc:
            _exc = exc
            raise
        finally:
            self.root._record_finish(span, "RunActivity", _exc)
            span.finish()
        return result
