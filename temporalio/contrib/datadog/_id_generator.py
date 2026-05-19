"""Deterministic span ID generation for Temporal Datadog tracing."""

from __future__ import annotations

_FNV_OFFSET_64 = 0xCBF29CE484222325
_FNV_PRIME_64 = 0x100000001B3
_FNV_MASK_64 = 0xFFFFFFFFFFFFFFFF


def gen_span_id(key: str) -> int:
    """Compute a 64-bit FNV-1 hash of a UTF-8 encoded string.

    Used to derive deterministic Datadog span IDs from a Temporal idempotency
    key (typically the workflow run ID), so that long-running workflows that
    cross process boundaries continue to correlate in APM.

    Matches the byte-for-byte output of Go's ``hash/fnv.New64()`` (which is
    FNV-1, not FNV-1a), so a workflow span emitted by a Go worker and one
    emitted by a Python worker for the same run ID produce the same span ID.

    Args:
        key: The string to hash.

    Returns:
        A 64-bit unsigned integer suitable for use as a Datadog span ID.
    """
    h = _FNV_OFFSET_64
    for byte in key.encode("utf-8"):
        h = (h * _FNV_PRIME_64) & _FNV_MASK_64
        h ^= byte
    return h
