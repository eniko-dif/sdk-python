# Datadog Tracing Interceptor

`temporalio.contrib.datadog` provides a [Datadog APM](https://docs.datadoghq.com/tracing/) interceptor for Temporal workflows, activities, signals, queries, updates, and Nexus operations.

The interceptor mirrors the Go SDK's [`contrib/datadog/tracing`](https://github.com/temporalio/sdk-go/tree/main/contrib/datadog/tracing) package and additionally supports:

- Span kinds (`producer` / `consumer`) for distributed-trace UX
- `peer.service` tagging for cross-service inbound correlation
- A `servicename` baggage item propagated on outbound calls
- Custom per-span tags via `extra_tags`
- Deterministic span IDs derived from workflow `run_id` + activity attempt, so retries and worker restarts dedupe in APM

## Install

```
pip install 'temporalio[datadog]'
```

## Usage

```python
from ddtrace import patch
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.contrib.datadog import DatadogTracingInterceptor

patch(logging=True)  # opt-in: injects dd.trace_id / dd.span_id into log records

interceptor = DatadogTracingInterceptor(
    service_name="my-service",
    extra_tags={"deployment.environment": "prod"},
)

client = await Client.connect("localhost:7233", interceptors=[interceptor])
worker = Worker(client, task_queue="q", workflows=[MyWF], activities=[my_activity])
await worker.run()
```

Pass the interceptor on `Client.connect(...)` and the worker inherits it automatically. Pass it on `Worker(...)` instead if you want worker-only tracing.

## Options

| Option | Default | Purpose |
| --- | --- | --- |
| `tracer` | `ddtrace.tracer` | Override the tracer (useful in tests). |
| `service_name` | `None` | Used for `peer.service` correlation and the `servicename` baggage item. Does NOT override `DD_SERVICE`. |
| `disable_signal_tracing` | `False` | Suppress signal-side spans. |
| `disable_query_tracing` | `False` | Suppress query-side spans. |
| `disable_update_tracing` | `False` | Suppress update-side spans. |
| `extra_tags` | `{}` | Tags applied to every emitted span. |
| `on_finish` | `None` | Callback invoked at span finish; returned mapping is applied as tags. |
| `header_key` | `"_dd-trace-data"` | Temporal header key for the Datadog propagation carrier. |
| `always_create_workflow_spans` | `False` | If `True`, emit workflow-side spans even when the client did not start one. |
| `use_real_workflow_spans` | `False` | If `True`, additionally emit a `RunWorkflow` span held open on the host for the lifetime of the workflow and finished with actual wall-clock duration. See caveats below. |

## Notes on the workflow sandbox

Python workflow code runs in a sandbox that forbids importing `ddtrace`. Workflow-side spans are emitted through a sandbox extern function registered by the interceptor — your workflow code never touches `ddtrace` directly.

Every workflow run always emits a paired set of zero-duration markers, `temporal.WorkflowStarted` and `temporal.WorkflowEnded`, so dashboards and alerts have a replay-safe signal that survives worker restarts. `WorkflowEnded` carries the workflow's exception (if any) as the span error. `ContinueAsNew` is control flow rather than failure: the `WorkflowEnded` marker is still emitted (so the pair is intact) but it is tagged `temporal.continued_as_new=true` and the exception is not recorded as an error, so a CAN chain of N runs shows up as N paired start/end markers.

Mid-flight evictions (server-initiated termination, run/execution timeout, LRU cache eviction) are also detected. The SDK forces `is_replaying=True` while tearing down the workflow, which would otherwise suppress the `WorkflowEnded` marker. The interceptor tracks whether the workflow ever observed a non-replay step on this worker; if it did and the teardown happens during replay, `WorkflowEnded` is emitted anyway with `temporal.evicted=true`, and the injected `CancelledError` is not recorded as a real failure. The only case that still produces an orphaned `WorkflowStarted` is a workflow that is evicted before it ever executes live on the current worker.

When `use_real_workflow_spans=True`, an additional `temporal.RunWorkflow` span is opened on the host process at workflow entry and finished with true wall-clock duration on completion. This matches the Go SDK's `RunWorkflow` semantics and is useful for end-to-end latency views. Caveats:

- **Wall-clock includes wait time.** For workflows that sleep or wait on signals, the span duration is dominated by idle time, not execution time.
- **Worker restart loses the span.** The open span lives in process memory. If the worker restarts mid-workflow, the `RunWorkflow` span is dropped — the zero-duration markers still fire on the new worker.
- **`ContinueAsNew` is handled cleanly.** The open span is closed without recording an error when the workflow continues as new.
- **Evictions are handled cleanly.** Termination, run/execution timeout, and LRU cache eviction close the span with `temporal.evicted=true` instead of recording the injected `CancelledError` as a failure.

## Logger injection

`dd-trace-py`'s `patch(logging=True)` injects `dd.trace_id` and `dd.span_id` into log records. This interceptor does NOT call `patch()` itself — opt in from your application entrypoint.
