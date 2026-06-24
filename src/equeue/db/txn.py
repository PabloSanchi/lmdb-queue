"""LMDB read/write helpers and job state transitions."""

from __future__ import annotations

import lmdb

from ..exceptions import QueueCorrupted
from .keys import EMPTY, META_STATS, U32, U64, key_lease, key_queued, key_retry, key_state
from .lease import lease_expiry
from .schema import JobState, Stats


def meta_get(txn: lmdb.Transaction, key: bytes) -> int:
    raw = txn.get(key)
    return U64.unpack(raw)[0] if raw else 0


def meta_set(txn: lmdb.Transaction, key: bytes, val: int) -> None:
    txn.put(key, U64.pack(val))


def stats_get(txn: lmdb.Transaction) -> Stats:
    raw = txn.get(META_STATS)
    if not raw:
        raise QueueCorrupted("Queue stats are not defined")
    return Stats.unpack(raw)


def stats_set(txn: lmdb.Transaction, stats: Stats) -> None:
    txn.put(META_STATS, stats.pack())


def set_pending(txn: lmdb.Transaction, job_id: int) -> None:
    """Transition job to PENDING and insert it into the queued/ index."""
    txn.put(key_state(job_id), JobState.PENDING)
    txn.put(key_queued(job_id), EMPTY)


def set_running(txn: lmdb.Transaction, job_id: int, packed_lease: bytes) -> None:
    """Transition job to RUNNING, write the lease, and remove it from the queued/ index."""
    txn.put(key_state(job_id), JobState.RUNNING)
    txn.put(key_lease(job_id), packed_lease)
    txn.delete(key_queued(job_id))


def set_done(txn: lmdb.Transaction, job_id: int) -> None:
    """Transition job to DONE and clear its retry counter."""
    txn.put(key_state(job_id), JobState.DONE)
    txn.delete(key_retry(job_id))


def set_failed(txn: lmdb.Transaction, job_id: int) -> None:
    """Transition job to FAILED (payload and retry count are kept for inspection)."""
    txn.put(key_state(job_id), JobState.FAILED)


def retry_count(txn: lmdb.Transaction, job_id: int) -> int:
    raw = txn.get(key_retry(job_id))
    return U32.unpack(raw)[0] if raw else 0


def lease_is_expired_or_missing(txn: lmdb.Transaction, job_id: int, now: float) -> bool:
    raw_lease = txn.get(key_lease(job_id))
    return raw_lease is None or now > lease_expiry(raw_lease)


def requeue_running(txn: lmdb.Transaction, job_id: int) -> None:
    """Move a RUNNING job back to PENDING and clear its lease."""
    set_pending(txn, job_id)
    txn.delete(key_lease(job_id))
