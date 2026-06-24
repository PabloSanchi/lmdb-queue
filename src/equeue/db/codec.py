"""Msgpack serialization for job payloads."""

from __future__ import annotations

from typing import Any

import msgpack


def encode(payload: Any) -> bytes:
    try:
        return msgpack.packb(payload, use_bin_type=True)
    except Exception as exc:
        raise TypeError(f"payload is not msgpack-serialisable: {type(payload).__name__}") from exc


def decode(data: bytes) -> Any:
    return msgpack.unpackb(data, raw=False)
