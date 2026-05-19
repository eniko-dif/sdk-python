"""Datadog tracing interceptors for Temporal workflow inbound and outbound calls."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, NoReturn, cast

import temporalio.api.common.v1
import temporalio.converter
import temporalio.worker
import temporalio.workflow

from temporalio.contrib.datadog._helpers import (
    Carrier,
    _CompletedSpanParams,
    _WorkflowConfig,
    _merge_str_headers,
)


class DatadogTracingWorkflowInboundInterceptor(
    temporalio.worker.WorkflowInboundInterceptor
):
    """Workflow-side interceptor that emits zero-duration Datadog spans.

    Inside the workflow sandbox, ``ddtrace`` cannot be imported. All span
    emission therefore goes through a sandbox extern function registered by
    the parent :class:`DatadogTracingInterceptor`.
    """

    def __init__(self, next: temporalio.worker.WorkflowInboundInterceptor) -> None:
        """Initialize the workflow-side interceptor."""
        super().__init__(next)
        externs = temporalio.workflow.extern_functions()
        self._completed_span_extern = cast(
            Callable[[_CompletedSpanParams], Carrier | None],
            externs["__temporal_datadog_completed_span"],
        )
        self._finish_workflow_span_extern = cast(
            Callable[[str, BaseException | None, bool], None],
            externs["__temporal_datadog_finish_workflow"],
        )
        config_extern = cast(
            Callable[[], _WorkflowConfig], externs["__temporal_datadog_config"]
        )
        self._config = config_extern()
        self._payload_converter = temporalio.converter.PayloadConverter.default
        self._workflow_carrier: Carrier | None = None
        self._workflow_carrier_loaded = False
        self._went_live = False

    def init(self, outbound: temporalio.worker.WorkflowOutboundInterceptor) -> None:
        """Wrap the workflow outbound chain."""
        super().init(_WorkflowOutboundInterceptor(outbound, self))

    def _observe_live_state(self) -> bool:
        """Track whether this run ever executed live on this worker.

        Returns the current ``is_replaying()`` state and, as a side effect,
        flips ``_went_live`` to True the first time we observe a non-replay
        step. Used in the ``execute_workflow`` finally block to distinguish a
        replay-to-completion (new worker after a crash; original worker
        emitted ``WorkflowEnded`` already) from a mid-flight eviction
        (terminate / run-timeout / cache eviction — ``is_replaying`` is forced
        True during teardown but ``_went_live`` is True because we ran code
        earlier on this worker).
        """
        replaying = temporalio.workflow.unsafe.is_replaying()
        if not replaying:
            self._went_live = True
        return replaying

    def _parent_workflow_carrier(self) -> Carrier | None:
        if not self._workflow_carrier_loaded:
            self._workflow_carrier_loaded = True
            payload = temporalio.workflow.info().headers.get(self._config.header_key)
            if payload is not None:
                decoded = self._payload_converter.from_payloads([payload])[0]
                if isinstance(decoded, dict):
                    self._workflow_carrier = {
                        str(k): str(v) for k, v in decoded.items()
                    }
        return self._workflow_carrier

    def _emit_inbound_span(
        self,
        operation: str,
        *,
        resource: str | None = None,
        attributes: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
        new_span_even_on_replay: bool = False,
        parent_carrier: Carrier | None = None,
        exception: BaseException | None = None,
    ) -> None:
        if self._observe_live_state() and not new_span_even_on_replay:
            return
        info = temporalio.workflow.info()
        attrs: dict[str, Any] = {
            "WorkflowID": info.workflow_id,
            "RunID": info.run_id,
            "WorkflowType": info.workflow_type,
        }
        if attributes:
            attrs.update(attributes)
        self._completed_span_extern(
            _CompletedSpanParams(
                parent_carrier=parent_carrier or self._parent_workflow_carrier(),
                operation=operation,
                resource=resource or info.workflow_type,
                time_ns=temporalio.workflow.time_ns(),
                attributes=attrs,
                idempotency_key=idempotency_key,
                exception=exception,
            )
        )

    def _emit_outbound_span(
        self,
        operation: str,
        *,
        resource: str,
        attributes: Mapping[str, Any] | None = None,
        outbound_input: Any | None = None,
        outbound_input_with_str_headers: Any | None = None,
    ) -> None:
        if self._observe_live_state():
            return
        info = temporalio.workflow.info()
        attrs: dict[str, Any] = {
            "WorkflowID": info.workflow_id,
            "RunID": info.run_id,
            "WorkflowType": info.workflow_type,
        }
        if attributes:
            attrs.update(attributes)
        updated = self._completed_span_extern(
            _CompletedSpanParams(
                parent_carrier=self._parent_workflow_carrier(),
                operation=operation,
                resource=resource,
                time_ns=temporalio.workflow.time_ns(),
                attributes=attrs,
                idempotency_key=None,
            )
        )
        if updated is None:
            return
        if outbound_input is not None:
            payload = self._payload_converter.to_payloads([updated])[0]
            outbound_input.headers = {
                **outbound_input.headers,
                self._config.header_key: payload,
            }
        if outbound_input_with_str_headers is not None:
            outbound_input_with_str_headers.headers = _merge_str_headers(
                outbound_input_with_str_headers.headers, updated
            )

    async def execute_workflow(
        self, input: temporalio.worker.ExecuteWorkflowInput
    ) -> Any:
        """Emit ``WorkflowStarted``/``WorkflowEnded`` markers, optionally bracketed by a real-duration ``RunWorkflow`` span.

        The zero-duration markers are always emitted as a pair (replay-safe;
        the Python sandbox forbids ddtrace imports so duration cannot be
        measured from inside). When ``use_real_workflow_spans=True`` an
        additional ``RunWorkflow`` span is opened on the host process and
        finished with wall-clock duration on completion; if the worker
        restarts mid-run, that span is lost.

        ``ContinueAsNewError`` is control flow, not a real completion: the
        ``WorkflowEnded`` marker (and the ``RunWorkflow`` span, when enabled)
        are tagged ``temporal.continued_as_new=true`` and the exception is
        not recorded as an error, so a CAN chain shows up as a sequence of
        paired start/end markers rather than orphaned starts.

        Mid-flight evictions (terminate, run/execution timeout, LRU cache
        eviction) are detected via ``_went_live``: the SDK forces
        ``is_replaying=True`` during teardown, but if we observed live
        execution earlier on this worker we still emit ``WorkflowEnded``
        (and finish the ``RunWorkflow`` span) tagged
        ``temporal.evicted=true``. The injected ``CancelledError`` is not
        recorded as a real failure in that case.
        """
        info = temporalio.workflow.info()
        self._emit_inbound_span(
            "WorkflowStarted",
            idempotency_key=f"{info.run_id}:WorkflowStarted",
        )
        if self._config.use_real_workflow_spans:
            self._emit_inbound_span(
                "RunWorkflow",
                idempotency_key=info.run_id,
            )
        _exc: BaseException | None = None
        try:
            return await super().execute_workflow(input)
        except BaseException as exc:
            _exc = exc
            raise
        finally:
            is_eviction = (
                self._went_live and temporalio.workflow.unsafe.is_replaying()
            )
            end_attributes: dict[str, Any] = {}
            if isinstance(_exc, temporalio.workflow.ContinueAsNewError):
                end_attributes["temporal.continued_as_new"] = True
            if is_eviction:
                end_attributes["temporal.evicted"] = True
            self._emit_inbound_span(
                "WorkflowEnded",
                idempotency_key=f"{info.run_id}:WorkflowEnded",
                attributes=end_attributes or None,
                exception=None if is_eviction else _exc,
                new_span_even_on_replay=self._went_live,
            )
            if self._config.use_real_workflow_spans:
                self._finish_workflow_span_extern(info.run_id, _exc, is_eviction)

    async def handle_signal(self, input: temporalio.worker.HandleSignalInput) -> None:
        """Emit the ``HandleSignal`` span (unless signal tracing is disabled)."""
        if self._config.disable_signal_tracing:
            return await super().handle_signal(input)
        self._emit_inbound_span(
            "HandleSignal",
            resource=input.signal,
            attributes={"SignalName": input.signal},
            parent_carrier=self._carrier_from_headers(input.headers),
        )
        await super().handle_signal(input)

    async def handle_query(self, input: temporalio.worker.HandleQueryInput) -> Any:
        """Emit the ``HandleQuery`` span (created even on replay)."""
        if self._config.disable_query_tracing:
            return await super().handle_query(input)
        self._emit_inbound_span(
            "HandleQuery",
            resource=input.query,
            attributes={"QueryType": input.query},
            new_span_even_on_replay=True,
            parent_carrier=self._carrier_from_headers(input.headers),
        )
        return await super().handle_query(input)

    def handle_update_validator(
        self, input: temporalio.worker.HandleUpdateInput
    ) -> None:
        """Emit the ``ValidateUpdate`` span."""
        if self._config.disable_update_tracing:
            return super().handle_update_validator(input)
        self._emit_inbound_span(
            "ValidateUpdate",
            resource=input.update,
            attributes={"UpdateName": input.update},
            new_span_even_on_replay=True,
            parent_carrier=self._carrier_from_headers(input.headers),
        )
        super().handle_update_validator(input)

    async def handle_update_handler(
        self, input: temporalio.worker.HandleUpdateInput
    ) -> Any:
        """Emit the ``HandleUpdate`` span."""
        if self._config.disable_update_tracing:
            return await super().handle_update_handler(input)
        self._emit_inbound_span(
            "HandleUpdate",
            resource=input.update,
            attributes={"UpdateName": input.update},
            parent_carrier=self._carrier_from_headers(input.headers),
        )
        return await super().handle_update_handler(input)

    def _carrier_from_headers(
        self, headers: Mapping[str, temporalio.api.common.v1.Payload]
    ) -> Carrier | None:
        payload = headers.get(self._config.header_key)
        if payload is None:
            return None
        decoded = self._payload_converter.from_payloads([payload])[0]
        if not isinstance(decoded, dict):
            return None
        return {str(k): str(v) for k, v in decoded.items()}


class _WorkflowOutboundInterceptor(temporalio.worker.WorkflowOutboundInterceptor):
    def __init__(
        self,
        next: temporalio.worker.WorkflowOutboundInterceptor,
        root: DatadogTracingWorkflowInboundInterceptor,
    ) -> None:
        super().__init__(next)
        self.root = root

    def continue_as_new(self, input: temporalio.worker.ContinueAsNewInput) -> NoReturn:
        carrier = self.root._parent_workflow_carrier()
        if carrier is not None:
            payload = self.root._payload_converter.to_payloads([carrier])[0]
            input.headers = {
                **input.headers,
                self.root._config.header_key: payload,
            }
        super().continue_as_new(input)

    async def signal_child_workflow(
        self, input: temporalio.worker.SignalChildWorkflowInput
    ) -> None:
        if not self.root._config.disable_signal_tracing:
            self.root._emit_outbound_span(
                "SignalChildWorkflow",
                resource=input.signal,
                attributes={"SignalName": input.signal, "ChildWorkflowID": input.child_workflow_id},
                outbound_input=input,
            )
        await super().signal_child_workflow(input)

    async def signal_external_workflow(
        self, input: temporalio.worker.SignalExternalWorkflowInput
    ) -> None:
        if not self.root._config.disable_signal_tracing:
            self.root._emit_outbound_span(
                "SignalExternalWorkflow",
                resource=input.signal,
                attributes={"SignalName": input.signal, "ExternalWorkflowID": input.workflow_id},
                outbound_input=input,
            )
        await super().signal_external_workflow(input)

    def start_activity(
        self, input: temporalio.worker.StartActivityInput
    ) -> temporalio.workflow.ActivityHandle:
        self.root._emit_outbound_span(
            "StartActivity",
            resource=input.activity,
            attributes={"ActivityType": input.activity},
            outbound_input=input,
        )
        return super().start_activity(input)

    async def start_child_workflow(
        self, input: temporalio.worker.StartChildWorkflowInput
    ) -> temporalio.workflow.ChildWorkflowHandle:
        self.root._emit_outbound_span(
            "StartChildWorkflow",
            resource=input.workflow,
            attributes={"ChildWorkflowID": input.id, "ChildWorkflowType": input.workflow},
            outbound_input=input,
        )
        return await super().start_child_workflow(input)

    def start_local_activity(
        self, input: temporalio.worker.StartLocalActivityInput
    ) -> temporalio.workflow.ActivityHandle:
        self.root._emit_outbound_span(
            "StartActivity",
            resource=input.activity,
            attributes={"ActivityType": input.activity, "Local": True},
            outbound_input=input,
        )
        return super().start_local_activity(input)

    async def start_nexus_operation(
        self, input: temporalio.worker.StartNexusOperationInput[Any, Any]
    ) -> temporalio.workflow.NexusOperationHandle[Any]:
        self.root._emit_outbound_span(
            "StartNexusOperation",
            resource=f"{input.service}/{input.operation_name}",
            attributes={"NexusService": input.service, "NexusOperation": input.operation_name},
            outbound_input_with_str_headers=input,
        )
        return await super().start_nexus_operation(input)
