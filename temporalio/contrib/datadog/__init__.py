"""Datadog tracing integration for the Temporal Python SDK.

This package provides a Datadog (``ddtrace``) tracing interceptor with
feature parity with the Go SDK's ``contrib/datadog/tracing`` package.

Usage::

    from ddtrace import patch
    from temporalio.client import Client
    from temporalio.contrib.datadog import DatadogTracingInterceptor

    patch(logging=True)  # opt in to dd.trace_id log injection

    interceptor = DatadogTracingInterceptor(
        service_name="my-service",
        extra_tags={"deployment.environment": "prod"},
    )
    client = await Client.connect("localhost:7233", interceptors=[interceptor])
"""

from temporalio.contrib.datadog._interceptor import (
    DatadogTracingInterceptor,
    DatadogTracingWorkflowInboundInterceptor,
    FinishContext,
)

__all__ = [
    "DatadogTracingInterceptor",
    "DatadogTracingWorkflowInboundInterceptor",
    "FinishContext",
]
