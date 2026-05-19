"""Tests for the Datadog tracing interceptor."""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from typing import Any

import pytest

pytest.importorskip("ddtrace")

from temporalio.client import Client
from temporalio.contrib.datadog import DatadogTracingInterceptor
from temporalio.contrib.datadog._id_generator import gen_span_id
from temporalio.worker import Worker

from tests.contrib.datadog._workflows import (
    TraceParam,
    TraceWorkflow,
    echo_activity,
)


class _CollectingWriter:
    """Capture ddtrace spans in-memory for assertions.

    The agent writer's interface is small — we only need ``write`` and a few
    no-op lifecycle methods so the tracer can wire it in without erroring.
    """

    def __init__(self) -> None:
        self.spans: list[Any] = []

    def write(self, spans: Iterable[Any] | None = None) -> None:
        if spans:
            self.spans.extend(spans)

    def flush_queue(self) -> None:
        pass

    def stop(self, *_args: Any, **_kwargs: Any) -> None:
        pass

    def recreate(self) -> "_CollectingWriter":
        return self


@pytest.fixture
def tracer_and_writer() -> Any:
    from ddtrace.trace import tracer as global_tracer

    prior = global_tracer._span_aggregator.writer  # type: ignore[attr-defined]
    writer = _CollectingWriter()
    global_tracer._span_aggregator.writer = writer  # type: ignore[attr-defined]
    yield global_tracer, writer
    global_tracer._span_aggregator.writer = prior  # type: ignore[attr-defined]


async def test_workflow_and_activity_propagation(client: Client, tracer_and_writer: Any) -> None:
    tracer, writer = tracer_and_writer
    interceptor = DatadogTracingInterceptor(
        tracer=tracer,
        service_name="test-service",
        extra_tags={"atlas-user-agent": "test-agent/1.0"},
    )

    cfg = client.config()
    cfg["interceptors"] = [interceptor]
    client = Client(**cfg)

    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[TraceWorkflow],
        activities=[echo_activity],
    ):
        workflow_id = f"wf-{uuid.uuid4()}"
        result = await client.execute_workflow(
            TraceWorkflow.run,
            TraceParam(use_activity=True),
            id=workflow_id,
            task_queue=task_queue,
        )
        assert result == "done"

    names = sorted({span.name for span in writer.spans})
    assert "temporal.StartWorkflow" in names
    assert "temporal.WorkflowStarted" in names
    assert "temporal.WorkflowEnded" in names
    assert "temporal.RunWorkflow" not in names
    assert "temporal.StartActivity" in names
    assert "temporal.RunActivity" in names

    # extra_tags applied to every span
    for span in writer.spans:
        assert span.get_tag("atlas-user-agent") == "test-agent/1.0", (
            f"span {span.name} missing extra tag"
        )


async def test_workflow_span_id_is_deterministic(client: Client, tracer_and_writer: Any) -> None:
    tracer, writer = tracer_and_writer
    interceptor = DatadogTracingInterceptor(
        tracer=tracer,
        service_name="test-service",
        use_real_workflow_spans=True,
    )

    cfg = client.config()
    cfg["interceptors"] = [interceptor]
    client = Client(**cfg)

    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[TraceWorkflow],
        activities=[echo_activity],
    ):
        workflow_id = f"wf-{uuid.uuid4()}"
        handle = await client.start_workflow(
            TraceWorkflow.run,
            TraceParam(),
            id=workflow_id,
            task_queue=task_queue,
        )
        await handle.result()
        run_id = handle.first_execution_run_id
        assert run_id is not None

    run_workflow_spans = [s for s in writer.spans if s.name == "temporal.RunWorkflow"]
    assert run_workflow_spans, "expected at least one RunWorkflow span"
    expected = gen_span_id(run_id)
    assert all(s.span_id == expected for s in run_workflow_spans), (
        f"RunWorkflow span IDs {[s.span_id for s in run_workflow_spans]} "
        f"did not match gen_span_id({run_id}) = {expected}"
    )


async def test_span_kind_and_peer_service(client: Client, tracer_and_writer: Any) -> None:
    tracer, writer = tracer_and_writer
    interceptor = DatadogTracingInterceptor(
        tracer=tracer,
        service_name="callee-service",
        use_real_workflow_spans=True,
    )

    cfg = client.config()
    cfg["interceptors"] = [interceptor]
    client = Client(**cfg)

    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[TraceWorkflow],
        activities=[echo_activity],
    ):
        await client.execute_workflow(
            TraceWorkflow.run,
            TraceParam(use_activity=True),
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )

    # Client-side spans are producers.
    start_workflow = next(s for s in writer.spans if s.name == "temporal.StartWorkflow")
    assert start_workflow.get_tag("span.kind") == "producer"

    # Server-side spans are consumers.
    run_workflow = next(s for s in writer.spans if s.name == "temporal.RunWorkflow")
    assert run_workflow.get_tag("span.kind") == "consumer"
    run_activity = next(s for s in writer.spans if s.name == "temporal.RunActivity")
    assert run_activity.get_tag("span.kind") == "consumer"

    # Same-service case: peer.service must not be set.
    assert run_workflow.get_tag("peer.service") is None
    assert run_activity.get_tag("peer.service") is None


async def test_disable_signal_tracing(client: Client, tracer_and_writer: Any) -> None:
    tracer, writer = tracer_and_writer
    interceptor = DatadogTracingInterceptor(
        tracer=tracer,
        disable_signal_tracing=True,
    )

    cfg = client.config()
    cfg["interceptors"] = [interceptor]
    client = Client(**cfg)

    task_queue = f"tq-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[TraceWorkflow],
        activities=[echo_activity],
    ):
        handle = await client.start_workflow(
            TraceWorkflow.run,
            TraceParam(),
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await handle.signal(TraceWorkflow.kick)
        await handle.result()

    assert not any("Signal" in span.name for span in writer.spans), [
        s.name for s in writer.spans
    ]


def test_gen_span_id_algorithm() -> None:
    # FNV offset basis (matches Go's hash/fnv.New64()).
    assert gen_span_id("") == 0xCBF29CE484222325
    # Deterministic and 64-bit-bounded.
    assert gen_span_id("hello") == gen_span_id("hello")
    assert 0 < gen_span_id("hello") <= 0xFFFFFFFFFFFFFFFF
    # Different inputs produce different outputs.
    assert gen_span_id("hello") != gen_span_id("world")
