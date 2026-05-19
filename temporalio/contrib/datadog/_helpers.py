"""Shared helper functions and constants for the Datadog tracing interceptor."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, TypeAlias, cast

import temporalio.api.common.v1

import temporalio.activity
import temporalio.workflow

Carrier: TypeAlias = dict[str, str]

_BAGGAGE_ITEM_SERVICE = "servicename"
_TEMPORAL_TAG_PREFIX = "temporal."


@dataclass(frozen=True)
class _CompletedSpanParams:
    """Parameters passed from the workflow sandbox to the extern function."""

    parent_carrier: Carrier | None
    operation: str
    resource: str
    time_ns: int
    attributes: Mapping[str, Any]
    idempotency_key: str | None
    exception: BaseException | None = None


@dataclass
class _WorkflowConfig:
    """Configuration handed to the workflow-side interceptor through the sandbox extern bridge."""

    header_key: str
    disable_signal_tracing: bool
    disable_query_tracing: bool
    disable_update_tracing: bool
    always_create_workflow_spans: bool
    use_real_workflow_spans: bool


def _normalize_temporal_tag_key(key: str) -> str:
    """Mirror Go upstream contrib's ``temporal.<x>`` tag naming."""
    if key.startswith(_TEMPORAL_TAG_PREFIX):
        return key
    if key.lower().startswith("temporal"):
        return _TEMPORAL_TAG_PREFIX + key[len("temporal") :].lstrip(".")
    return _TEMPORAL_TAG_PREFIX + key


def _baggage_service_name(ctx: Any) -> str | None:
    if ctx is None:
        return None
    getter = getattr(ctx, "get_baggage_item", None)
    if callable(getter):
        return cast(str | None, getter(_BAGGAGE_ITEM_SERVICE))
    return None


def _set_baggage(ctx: Any, key: str, value: str) -> None:
    setter = getattr(ctx, "set_baggage_item", None)
    if callable(setter):
        setter(key, value)


def _merge_str_headers(
    existing: Mapping[str, str] | None, carrier: Carrier
) -> Mapping[str, str]:
    out: dict[str, str] = {**existing} if existing else {}
    for key, value in carrier.items():
        out[key] = value
    return out


def _should_skip_error(exc: BaseException | None) -> bool:
    """Return ``True`` if the span should not be tagged with this exception.
    Skip control-flow exceptions, not actual failures.
    """
    if exc is None:
        return True
    if isinstance(exc, temporalio.workflow.ContinueAsNewError):
        return True
    if isinstance(exc, temporalio.activity._CompleteAsyncError):
        return True
    return False


class _InputWithHeaders(Protocol):
    """Protocol for inputs carrying mutable ``headers``."""

    headers: Mapping[str, temporalio.api.common.v1.Payload]
