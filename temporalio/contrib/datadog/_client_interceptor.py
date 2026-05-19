"""Datadog tracing interceptor for Temporal client outbound calls."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import temporalio.client

if TYPE_CHECKING:
    from temporalio.contrib.datadog._interceptor import DatadogTracingInterceptor


class _ClientOutboundInterceptor(temporalio.client.OutboundInterceptor):
    def __init__(
        self,
        next: temporalio.client.OutboundInterceptor,
        root: DatadogTracingInterceptor,
    ) -> None:
        super().__init__(next)
        self.root = root

    async def start_workflow(
        self, input: temporalio.client.StartWorkflowInput
    ) -> temporalio.client.WorkflowHandle[Any, Any]:
        operation = (
            "SignalWithStartWorkflow" if input.start_signal else "StartWorkflow"
        )
        with self.root._client_span(
            operation,
            resource=input.workflow,
            attributes={"WorkflowID": input.id, "WorkflowType": input.workflow},
            input_with_headers=input,
        ):
            return await super().start_workflow(input)

    async def signal_workflow(
        self, input: temporalio.client.SignalWorkflowInput
    ) -> None:
        if self.root.disable_signal_tracing:
            return await super().signal_workflow(input)
        with self.root._client_span(
            "SignalWorkflow",
            resource=input.signal,
            attributes={"WorkflowID": input.id, "SignalName": input.signal},
            input_with_headers=input,
        ):
            return await super().signal_workflow(input)

    async def query_workflow(self, input: temporalio.client.QueryWorkflowInput) -> Any:
        if self.root.disable_query_tracing:
            return await super().query_workflow(input)
        with self.root._client_span(
            "QueryWorkflow",
            resource=input.query,
            attributes={"WorkflowID": input.id, "QueryType": input.query},
            input_with_headers=input,
        ):
            return await super().query_workflow(input)

    async def start_workflow_update(
        self, input: temporalio.client.StartWorkflowUpdateInput
    ) -> temporalio.client.WorkflowUpdateHandle[Any]:
        if self.root.disable_update_tracing:
            return await super().start_workflow_update(input)
        with self.root._client_span(
            "StartWorkflowUpdate",
            resource=input.update,
            attributes={"WorkflowID": input.id, "UpdateName": input.update},
            input_with_headers=input,
        ):
            return await super().start_workflow_update(input)

    async def start_update_with_start_workflow(
        self, input: temporalio.client.StartWorkflowUpdateWithStartInput
    ) -> temporalio.client.WorkflowUpdateHandle[Any]:
        if self.root.disable_update_tracing:
            return await super().start_update_with_start_workflow(input)
        attrs: dict[str, Any] = {
            "WorkflowID": input.start_workflow_input.id,
            "WorkflowType": input.start_workflow_input.workflow,
        }
        if input.update_workflow_input.update_id is not None:
            attrs["UpdateID"] = input.update_workflow_input.update_id
        with self.root._client_span(
            "StartUpdateWithStartWorkflow",
            resource=input.start_workflow_input.workflow,
            attributes=attrs,
            input_with_headers=input.start_workflow_input,
        ):
            dd_payload = input.start_workflow_input.headers.get(self.root.header_key)
            if dd_payload is not None:
                input.update_workflow_input.headers = {
                    **input.update_workflow_input.headers,
                    self.root.header_key: dd_payload,
                }
            return await super().start_update_with_start_workflow(input)

    async def start_activity(
        self, input: temporalio.client.StartActivityInput
    ) -> temporalio.client.ActivityHandle[Any]:
        with self.root._client_span(
            "StartActivity",
            resource=input.activity_type,
            attributes={
                "ActivityID": input.id,
                "ActivityType": input.activity_type,
            },
            input_with_headers=input,
        ):
            return await super().start_activity(input)
