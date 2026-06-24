"""Claim lease encoding stored under lease/<job_id>."""

from __future__ import annotations

import secrets
import time

from .keys import F64

CLAIM_TOKEN_LEN = 16
_LEASE_HEADER_LEN = F64.size


def new_claim_token() -> bytes:
    return secrets.token_bytes(CLAIM_TOKEN_LEN)


def pack_lease(expiry: float, claim_token: bytes) -> bytes:
    if len(claim_token) != CLAIM_TOKEN_LEN:
        raise ValueError(f"claim token must be {CLAIM_TOKEN_LEN} bytes")
    return F64.pack(expiry) + claim_token


def lease_expiry(raw: bytes) -> float:
    return F64.unpack(raw[:_LEASE_HEADER_LEN])[0]


def lease_claim_token(raw: bytes) -> bytes:
    return raw[_LEASE_HEADER_LEN : _LEASE_HEADER_LEN + CLAIM_TOKEN_LEN]


def new_lease(lease_time: float, *, now: float | None = None) -> tuple[bytes, bytes]:
    """Return ``(packed_lease_bytes, claim_token)`` for a new claim."""
    now = time.time() if now is None else now
    token = new_claim_token()
    return pack_lease(now + lease_time, token), token
