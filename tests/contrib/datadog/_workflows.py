"""Workflow and activity definitions for Datadog contrib tests.

Kept in a separate module from ``test_datadog.py`` so the workflow sandbox
can re-import it without pulling in ``ddtrace`` (which schedules asyncio work
during init and conflicts with the running sandbox loop).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy


@dataclass
class TraceParam:
    use_activity: bool = False


@activity.defn
async def echo_activity(value: str) -> str:
    return value


@workflow.defn
class TraceWorkflow:
    @workflow.run
    async def run(self, param: TraceParam) -> str:
        if param.use_activity:
            await workflow.execute_activity(
                echo_activity,
                "hello",
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
        return "done"

    @workflow.signal
    def kick(self) -> None:
        pass
