"""LMDB key prefixes, struct formats, and key builders."""

from __future__ import annotations

import struct

U64 = struct.Struct(">Q")
F64 = struct.Struct("<d")
U32 = struct.Struct("<I")

PFX_JOB: bytes = b"job/"
PFX_STATE: bytes = b"state/"
PFX_LEASE: bytes = b"lease/"
PFX_RETRY: bytes = b"retry/"
PFX_QUEUED: bytes = b"queued/"

META_TAIL: bytes = b"meta/tail"
META_STATS: bytes = b"meta/stats"

PFX_STATE_OFFSET = len(PFX_STATE)
PFX_LEASE_OFFSET = len(PFX_LEASE)
PFX_QUEUED_OFFSET = len(PFX_QUEUED)
JOB_TIMESTAMP_SIZE = F64.size  # enqueued_at prefix at the start of job/ values

EMPTY = b""


def key_job(job_id: int) -> bytes:
    return PFX_JOB + U64.pack(job_id)


def key_state(job_id: int) -> bytes:
    return PFX_STATE + U64.pack(job_id)


def key_lease(job_id: int) -> bytes:
    return PFX_LEASE + U64.pack(job_id)


def key_retry(job_id: int) -> bytes:
    return PFX_RETRY + U64.pack(job_id)


def key_queued(job_id: int) -> bytes:
    return PFX_QUEUED + U64.pack(job_id)
