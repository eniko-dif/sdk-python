"""Datadog tracing interceptor for Temporal."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import temporalio.api.common.v1
import temporalio.client
import temporalio.converter
import temporalio.worker
import temporalio.workflow

from temporalio.contrib.datadog._activity_interceptor import _ActivityInboundInterceptor
from temporalio.contrib.datadog._client_interceptor import _ClientOutboundInterceptor
from temporalio.contrib.datadog._helpers import (
    Carrier,
    _BAGGAGE_ITEM_SERVICE,
    _CompletedSpanParams,
    _InputWithHeaders,
    _WorkflowConfig,
    _baggage_service_name,
    _normalize_temporal_tag_key,
    _set_baggage,
    _should_skip_error,
)
from temporalio.contrib.datadog._id_generator import gen_span_id
from temporalio.contrib.datadog._nexus_interceptor import _NexusOperationInboundInterceptor
from temporalio.contrib.datadog._workflow_interceptor import (
    DatadogTracingWorkflowInboundInterceptor,
)

# NOTE: ``ddtrace`` is intentionally NOT imported at module level. The workflow
# sandbox snapshots ``sys.modules`` at startup and reloads modules through its
# own restricted importer; importing ``ddtrace`` from within that import path
# trips both an asyncio-loop conflict during ddtrace init and a ``builtins.open``
# restriction triggered by pytest's assertion rewriter. The interceptor instead
# resolves the tracer and propagator lazily inside ``__init__`` (which runs on
# the host process) and caches them as instance attributes so the sandbox
# extern function never re-imports anything.

_DEFAULT_HEADER_KEY = "_dd-trace-data"
_PEER_SERVICE_TAG = "peer.service"
_SPAN_KIND_TAG = "span.kind"
_PRODUCER = "producer"
_CONSUMER = "consumer"

_OPERATION_SPAN_KIND: dict[str, str] = {
    "StartActivity": _PRODUCER,
    "RunActivity": _CONSUMER,
    "StartChildWorkflow": _PRODUCER,
    "StartWorkflow": _PRODUCER,
    "SignalWithStartWorkflow": _PRODUCER,
    "WorkflowStarted": _CONSUMER,
    "WorkflowEnded": _CONSUMER,
    "RunWorkflow": _CONSUMER,
    "SignalWorkflow": _PRODUCER,
    "SignalChildWorkflow": _PRODUCER,
    "SignalExternalWorkflow": _PRODUCER,
    "HandleSignal": _CONSUMER,
    "QueryWorkflow": _PRODUCER,
    "HandleQuery": _CONSUMER,
    "StartWorkflowUpdate": _PRODUCER,
    "StartUpdateWithStartWorkflow": _PRODUCER,
    "ValidateUpdate": _CONSUMER,
    "HandleUpdate": _CONSUMER,
    "StartNexusOperation": _PRODUCER,
    "RunStartNexusOperationHandler": _CONSUMER,
    "RunCancelNexusOperationHandler": _CONSUMER,
}


@dataclass(frozen=True)
class FinishContext:
    """Context passed to a user-supplied ``on_finish`` callback.

    Attributes:
        operation: The Temporal operation name (e.g. ``"RunWorkflow"``).
        exception: The exception that caused the span to fail, or ``None``.
    """

    operation: str
    exception: BaseException | None


class DatadogTracingInterceptor(
    temporalio.client.Interceptor, temporalio.worker.Interceptor
):
    """Temporal interceptor that emits Datadog spans for client and worker operations.

    Apply by passing an instance to both :py:meth:`temporalio.client.Client.connect`
    (so client-side calls are traced) and :py:class:`temporalio.worker.Worker`
    (so activity/workflow execution is traced). When passed only on the client,
    the worker inherits the same interceptor.

    Args:
        tracer: A ``ddtrace`` tracer; defaults to ``ddtrace.tracer`` (the
            global tracer initialised by ``ddtrace-run`` or
            ``import ddtrace.auto``).
        service_name: Service name used for ``peer.service`` cross-service
            correlation and for the ``servicename`` baggage item written on
            outbound calls. If unset, baggage-based correlation is disabled.
            This does NOT override ``DD_SERVICE`` for the ``service`` field
            on spans.
        disable_signal_tracing: If ``True``, no spans are created for signal
            operations.
        disable_query_tracing: If ``True``, no spans are created for query
            operations.
        disable_update_tracing: If ``True``, no spans are created for update
            operations.
        extra_tags: Additional tags applied to every emitted span. Useful for
            request-agnostic dimensions such as ``atlas-user-agent``.
        on_finish: Optional callback invoked when a span finishes. The
            returned mapping is applied as additional tags before the span
            is closed.
        header_key: Temporal header key under which the Datadog propagation
            carrier is stored. Mirrors the underscore-prefix convention used
            by other contrib interceptors.
        always_create_workflow_spans: If ``True``, workflow-side spans are
            emitted even when the client did not start a span. Off by default
            because such spans become trace roots and may appear orphaned.
        use_real_workflow_spans: If ``True``, an additional ``RunWorkflow``
            span is kept open on the host process (keyed by run ID) and
            finished with actual wall-clock duration when the workflow
            completes. Errors are recorded on the span. Off by default because
            the workflow sandbox forbids importing ``ddtrace``, so the span
            cannot be tracked from inside the workflow — it has to be held on
            the host, which means if the worker process restarts mid-workflow
            the open span is lost and that run produces no real-duration span
            (the zero-duration ``WorkflowStarted`` / ``WorkflowEnded`` markers
            still fire on the new worker). ``ContinueAsNewError`` closes the
            span cleanly without recording an error.

    Note:
        ``dd-trace-py``'s ``patch(logging=True)`` is responsible for injecting
        ``dd.trace_id`` / ``dd.span_id`` into log records. The interceptor
        deliberately does not call ``patch`` itself; opt in from your
        application entrypoint.
    """

    def __init__(  # type: ignore[reportMissingSuperCall]
        self,
        tracer: Any | None = None,
        *,
        service_name: str | None = None,
        disable_signal_tracing: bool = False,
        disable_query_tracing: bool = False,
        disable_update_tracing: bool = False,
        extra_tags: Mapping[str, str] | None = None,
        on_finish: Callable[[FinishContext], Mapping[str, Any] | None] | None = None,
        header_key: str = _DEFAULT_HEADER_KEY,
        always_create_workflow_spans: bool = False,
        use_real_workflow_spans: bool = False,
    ) -> None:
        """Initialize the Datadog tracing interceptor."""
        if tracer is None:
            import ddtrace

            tracer = ddtrace.tracer  # type: ignore[reportPrivateImportUsage]
        # Resolve the HTTP propagator now and cache it on the instance so the
        # extern function (which fires from inside the sandbox) never has to
        # do its own ``ddtrace`` import.
        from ddtrace.propagation.http import HTTPPropagator

        self.tracer = tracer
        self._http_propagator = HTTPPropagator
        self.service_name = service_name
        self.disable_signal_tracing = disable_signal_tracing
        self.disable_query_tracing = disable_query_tracing
        self.disable_update_tracing = disable_update_tracing
        self.extra_tags: dict[str, str] = dict(extra_tags) if extra_tags else {}
        self.on_finish = on_finish
        self.header_key = header_key
        self.always_create_workflow_spans = always_create_workflow_spans
        self.use_real_workflow_spans = use_real_workflow_spans
        self._open_workflow_spans: dict[str, Any] = {}
        self.payload_converter = temporalio.converter.PayloadConverter.default

    def intercept_client(
        self, next: temporalio.client.OutboundInterceptor
    ) -> temporalio.client.OutboundInterceptor:
        """Wrap the client outbound chain."""
        return _ClientOutboundInterceptor(next, self)

    def intercept_activity(
        self, next: temporalio.worker.ActivityInboundInterceptor
    ) -> temporalio.worker.ActivityInboundInterceptor:
        """Wrap the activity inbound chain."""
        return _ActivityInboundInterceptor(next, self)

    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> type[DatadogTracingWorkflowInboundInterceptor]:
        """Register sandbox externs and return the workflow inbound interceptor."""
        input.unsafe_extern_functions["__temporal_datadog_completed_span"] = (
            self._emit_completed_span
        )
        input.unsafe_extern_functions["__temporal_datadog_finish_workflow"] = (
            self._finish_open_workflow_span
        )
        input.unsafe_extern_functions["__temporal_datadog_config"] = self._workflow_config
        return DatadogTracingWorkflowInboundInterceptor

    def intercept_nexus_operation(
        self, next: temporalio.worker.NexusOperationInboundInterceptor
    ) -> temporalio.worker.NexusOperationInboundInterceptor:
        """Wrap the Nexus operation inbound chain."""
        return _NexusOperationInboundInterceptor(next, self)

    def _inject(self, context: Any) -> Carrier:
        carrier: Carrier = {}
        if context is None:
            return carrier
        self._http_propagator.inject(context, carrier)
        return carrier

    def _extract(self, carrier: Mapping[str, str] | None) -> Any:
        if not carrier:
            return None
        ctx = self._http_propagator.extract(carrier)
        if ctx is None or getattr(ctx, "trace_id", None) is None:
            return None
        return ctx

    def _workflow_config(self) -> _WorkflowConfig:
        return _WorkflowConfig(
            header_key=self.header_key,
            disable_signal_tracing=self.disable_signal_tracing,
            disable_query_tracing=self.disable_query_tracing,
            disable_update_tracing=self.disable_update_tracing,
            always_create_workflow_spans=self.always_create_workflow_spans,
            use_real_workflow_spans=self.use_real_workflow_spans,
        )

    def _carrier_to_payload(self, carrier: Carrier) -> temporalio.api.common.v1.Payload:
        return self.payload_converter.to_payloads([carrier])[0]

    def _payload_to_carrier(
        self, payload: temporalio.api.common.v1.Payload
    ) -> Carrier | None:
        decoded = self.payload_converter.from_payloads([payload])[0]
        if not isinstance(decoded, dict):
            return None
        return {str(k): str(v) for k, v in decoded.items()}

    def _inject_headers(
        self,
        headers: Mapping[str, temporalio.api.common.v1.Payload],
        context: Any,
    ) -> Mapping[str, temporalio.api.common.v1.Payload]:
        if self.service_name:
            _set_baggage(context, _BAGGAGE_ITEM_SERVICE, self.service_name)
        carrier = self._inject(context)
        if not carrier:
            return headers
        return {**headers, self.header_key: self._carrier_to_payload(carrier)}

    def _extract_carrier_from_headers(
        self, headers: Mapping[str, temporalio.api.common.v1.Payload]
    ) -> Carrier | None:
        payload = headers.get(self.header_key)
        if payload is None:
            return None
        return self._payload_to_carrier(payload)

    def _extract_context_from_headers(
        self, headers: Mapping[str, temporalio.api.common.v1.Payload]
    ) -> Any:
        carrier = self._extract_carrier_from_headers(headers)
        return self._extract(carrier)

    def _apply_tags(
        self,
        span: Any,
        operation: str,
        attributes: Mapping[str, Any] | None,
        parent_service_name: str | None,
    ) -> None:
        for key, value in self.extra_tags.items():
            span.set_tag(key, value)
        if attributes:
            for key, value in attributes.items():
                span.set_tag(_normalize_temporal_tag_key(key), value)
        kind = _OPERATION_SPAN_KIND.get(operation)
        if kind:
            span.set_tag(_SPAN_KIND_TAG, kind)
            if (
                kind == _CONSUMER
                and parent_service_name
                and parent_service_name != self.service_name
            ):
                span.set_tag(_PEER_SERVICE_TAG, parent_service_name)

    def _record_finish(
        self,
        span: Any,
        operation: str,
        exc: BaseException | None,
    ) -> None:
        if exc is not None and not _should_skip_error(exc):
            span.set_exc_info(type(exc), exc, exc.__traceback__)
        if self.on_finish is not None:
            extras = self.on_finish(FinishContext(operation=operation, exception=exc))
            if extras:
                for key, value in extras.items():
                    span.set_tag(key, value)

    @contextmanager
    def _client_span(
        self,
        operation: str,
        resource: str,
        *,
        attributes: Mapping[str, Any] | None = None,
        input_with_headers: _InputWithHeaders | None = None,
    ) -> Iterator[Any]:
        span = self.tracer.start_span(
            f"temporal.{operation}",
            resource=resource,
            activate=True,
        )
        self._apply_tags(span, operation, attributes, parent_service_name=None)
        try:
            if input_with_headers is not None:
                input_with_headers.headers = self._inject_headers(
                    input_with_headers.headers, span.context
                )
            yield span
        except BaseException as exc:
            self._record_finish(span, operation, exc)
            span.finish()
            raise
        else:
            self._record_finish(span, operation, None)
            span.finish()

    def _finish_open_workflow_span(
        self,
        run_id: str,
        exception: BaseException | None,
        evicted: bool = False,
    ) -> None:
        """Sandbox extern: close the host-side ``RunWorkflow`` span for a run.

        No-op when ``use_real_workflow_spans`` is off or the span is missing
        (e.g. the worker started mid-run and never opened one). When the run
        ended via ``ContinueAsNewError``, the span is tagged
        ``temporal.continued_as_new=True`` and the exception is not recorded
        as an error. When the run ended via a mid-flight eviction
        (terminate / run-timeout / cache eviction), the span is tagged
        ``temporal.evicted=True`` and the injected ``CancelledError`` is
        likewise not recorded as a real failure.
        """
        span = self._open_workflow_spans.pop(run_id, None)
        if span is None:
            return
        if evicted:
            span.set_tag("temporal.evicted", True)
        elif isinstance(exception, temporalio.workflow.ContinueAsNewError):
            span.set_tag("temporal.continued_as_new", True)
        elif exception is not None and not _should_skip_error(exception):
            span.set_exc_info(type(exception), exception, exception.__traceback__)
        self._record_finish(span, "RunWorkflow", None if evicted else exception)
        span.finish()

    def _emit_completed_span(self, params: _CompletedSpanParams) -> Carrier | None:
        """Sandbox extern: emit a workflow-side span.

        Default behavior: a zero-duration span whose start and finish both
        happen at ``params.time_ns``. Returns the propagation carrier so
        outbound workflow calls can parent themselves to it.

        Optional behavior (``use_real_workflow_spans=True`` and
        ``params.operation == "RunWorkflow"``): the span is opened, stored
        keyed by run ID, and **not** finished here — ``_finish_open_workflow_span``
        closes it later with real wall-clock duration.
        """
        parent_ctx = (
            self._extract(params.parent_carrier) if params.parent_carrier else None
        )
        if parent_ctx is None and not self.always_create_workflow_spans:
            return None
        parent_service = _baggage_service_name(parent_ctx)

        span = self.tracer.start_span(
            f"temporal.{params.operation}",
            child_of=parent_ctx,
            resource=params.resource,
            activate=False,
        )
        if params.idempotency_key:
            span.span_id = gen_span_id(params.idempotency_key)
        self._apply_tags(span, params.operation, params.attributes, parent_service)
        if self.service_name:
            _set_baggage(span.context, _BAGGAGE_ITEM_SERVICE, self.service_name)

        if self.use_real_workflow_spans and params.operation == "RunWorkflow":
            # Store the open span; it will be finished by the CompleteWorkflow call.
            assert params.idempotency_key, "RunWorkflow must supply idempotency_key (run_id)"
            start_seconds = params.time_ns / 1_000_000_000
            span.start_ns = params.time_ns
            span.start = start_seconds
            self._open_workflow_spans[params.idempotency_key] = span
            return self._inject(span.context)

        if params.exception is not None and not _should_skip_error(params.exception):
            span.set_exc_info(
                type(params.exception),
                params.exception,
                params.exception.__traceback__,
            )
        if self.on_finish is not None:
            extras = self.on_finish(
                FinishContext(operation=params.operation, exception=params.exception)
            )
            if extras:
                for key, value in extras.items():
                    span.set_tag(key, value)
        # Zero-duration: end at the same wall-clock as the start.
        start_seconds = params.time_ns / 1_000_000_000
        span.start_ns = params.time_ns
        span.start = start_seconds
        span.finish(finish_time=start_seconds)

        return self._inject(span.context)

