from __future__ import annotations

import asyncio
import contextlib as cl
import os
import threading
import time
from collections.abc import AsyncIterator, Callable, Iterator
from functools import partial
from typing import Any, Generic, TypeVar

import lmdb

from .db import (
    F64,
    JOB_TIMESTAMP_SIZE,
    META_TAIL,
    PFX_QUEUED,
    PFX_QUEUED_OFFSET,
    PFX_STATE,
    U32,
    U64,
    JobState,
    Stats,
    decode,
    encode,
    iter_prefix,
    key_job,
    key_lease,
    key_retry,
    key_state,
    lease_claim_token,
    lease_is_expired_or_missing,
    meta_get,
    meta_set,
    new_lease,
    parse_state_cursor,
    requeue_running,
    retry_count,
    set_done,
    set_failed,
    set_pending,
    set_running,
    stats_get,
    stats_set,
)
from .exceptions import QueueClosed, QueueCorrupted, QueueEmpty
from .record import Record
from .util import retry_until_timeout

_POLL_INTERVAL = 0.01
_THREAD_JOIN_TIMEOUT = 2.0
_QUEUE_EMPTY_MSG = "No job became available before the timeout expired"

T = TypeVar("T")


class Queue(Generic[T]):
    """
    Thread-safe, persistent, crash-resilient FIFO queue backed by LMDB.

    Provides at-least-once delivery with automatic lease expiry recovery
    and retry handling. Designed as a drop-in embedded alternative to
    broker-based queues.

    Basic usage::

        with Queue("./myqueue") as q:
            q.put({"task": "send_email", "to": "user@example.com"})

            record = q.get()
            try:
                process(record.payload)
                record.ack()
            except Exception:
                record.nack()

            # Or automatic ack/nack via context manager:
            with q.processing() as record:
                process(record.payload)

    Args:
        path: Filesystem directory where LMDB files are stored.
            Created automatically if it does not exist.
        lease_time: Seconds a job may remain RUNNING before its lease
            expires and it becomes eligible for recovery. Defaults to 30.0.
        max_retries: Maximum ``nack()`` attempts before a job is permanently
            marked FAILED. A value of 0 permits exactly one attempt.
            Defaults to 3.
        map_size: Maximum virtual address space for the LMDB environment in
            bytes. Safe to set large; physical pages are only allocated as
            data is written. Defaults to 1 GiB.
        sync: If True, every write transaction calls ``fsync``. Safer but
            slower. Defaults to False.
        do_recover: If True, a background thread periodically reclaims
            jobs with expired leases. Defaults to True.
        recover_interval: Seconds between lease-expiry scans. Defaults to 15.0.
        do_vacuum: If True, a background thread purges DONE records from the
            database. Defaults to True.
        vacuum_interval: Seconds between vacuum cycles. Defaults to 300.0.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        lease_time: float = 30.0,
        max_retries: int = 3,
        map_size: int = 2**30,
        sync: bool = False,
        do_recover: bool = True,
        recover_interval: float = 15.0,
        do_vacuum: bool = True,
        vacuum_interval: float = 300.0,
    ):
        self._validate_config(
            lease_time=lease_time,
            max_retries=max_retries,
            map_size=map_size,
            recover_interval=recover_interval,
            vacuum_interval=vacuum_interval,
        )

        self.path = os.fspath(path)
        self._lease_time = lease_time
        self._max_retries = max_retries
        self._recover_interval = recover_interval
        self._vacuum_interval = vacuum_interval

        self._env = lmdb.open(
            self.path,
            map_size=map_size,
            max_dbs=0,
            writemap=True,
            map_async=True,
            sync=sync,
            metasync=False,
            readahead=False,
            meminit=False,
        )

        self._stop_event = threading.Event()
        self._env_closed = False

        self._recover()

        self._vacuum_thread = (
            self._start_background_thread(target=self._vacuum_loop, name="queue-vacuum")
            if do_vacuum
            else None
        )
        self._requeue_thread = (
            self._start_background_thread(target=self._requeue_loop, name="queue-recover")
            if do_recover
            else None
        )

    @property
    def _closed(self) -> bool:
        return self._stop_event.is_set()

    def put(self, item: T) -> int:
        """
        Add an item to the queue.

        Args:
            item: Value to enqueue.

        Returns:
            The assigned job ID.

        Raises:
            QueueClosed: The queue has been closed.
        """
        self._assert_open()
        return self._put(encode(item))

    def get(self, timeout: float | None = None) -> Record[T]:
        """
        Claim the next available job.

        Returns a :class:`Record` that holds the payload and is the **only**
        handle allowed to ``ack()`` or ``nack()`` this delivery. Each record
        carries an internal claim token; completion succeeds only while this
        worker still holds the active lease.

        Args:
            timeout: Maximum time to wait for a job. If None, blocks until
                a job becomes available.

        Returns:
            A claimed :class:`Record`.

        Raises:
            QueueEmpty: No job became available before the timeout expired.
            QueueClosed: The queue is closed.
        """
        deadline = time.monotonic() + timeout if timeout is not None else None

        while True:
            self._assert_open()

            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise QueueEmpty(_QUEUE_EMPTY_MSG)
            else:
                remaining = None

            job_id = self._poll_job(remaining)
            record = self._claim(job_id)
            if record is not None:
                return record

            # Lost the claim race to another worker. Yield briefly instead of
            # hot-spinning the read/claim loop while jobs remain in queued/.
            self._stop_event.wait(_POLL_INTERVAL)

    @cl.contextmanager
    def processing(self, timeout: float | None = None) -> Iterator[Record[T]]:
        """
        Retrieve and process a job using a context manager.

        The record is acknowledged if the context exits normally. If an
        exception is raised, ``record.nack()`` is called automatically.

        Args:
            timeout: Passed through to ``get()``.

        Yields:
            The claimed :class:`Record`.

        Raises:
            QueueEmpty: No job became available before the timeout expired.
            QueueClosed: The queue is closed.
            Exception: Re-raises any exception raised inside the context.
        """
        record = self.get(timeout=timeout)
        try:
            yield record
        except Exception:
            record.nack()
            raise
        else:
            record.ack()

    def stats(self) -> dict[str, int]:
        """
        Return queue statistics.

        All counters are maintained incrementally; this method performs no
        LMDB scan and is O(1).

        Returns:
            A dictionary containing:

            - pending: Jobs waiting to be processed.
            - running: Jobs currently leased by a worker.
            - done: Jobs completed successfully (not yet vacuumed).
            - failed: Jobs that exceeded the retry limit.
            - total: Total jobs ever added; never decreases, survives restarts.
        """
        self._assert_open()
        with self._env.begin(write=False) as txn:
            return stats_get(txn).to_dict()

    def close(self) -> None:
        """
        Stop background threads and close the queue.

        This method is idempotent and may be called multiple times. If a
        background thread fails to stop in time, the LMDB environment is left
        open (rather than closed underneath a live thread, which could crash
        the process) and a ``RuntimeError`` is raised; a later ``close()`` call
        retries the shutdown.
        """
        self._stop_event.set()

        if self._env_closed:
            return

        for thread in (self._vacuum_thread, self._requeue_thread):
            if thread is not None and thread.is_alive():
                thread.join(timeout=_THREAD_JOIN_TIMEOUT)
                if thread.is_alive():
                    raise RuntimeError(f"{thread.name} did not stop within {_THREAD_JOIN_TIMEOUT}s")

        self._env.sync(True)
        self._env.close()
        self._env_closed = True

    def _put(self, payload: bytes) -> int:
        """
        Write a serialized payload to LMDB as a new job.

        Args:
            payload: Encoded job data.

        Returns:
            The assigned job ID.
        """
        with self._env.begin(write=True) as txn:
            job_id = meta_get(txn, META_TAIL)
            txn.put(key_job(job_id), F64.pack(time.time()) + payload)
            set_pending(txn, job_id)
            meta_set(txn, META_TAIL, job_id + 1)

            stats = stats_get(txn)
            stats.pending += 1
            stats.total += 1
            stats_set(txn, stats)

        return job_id

    def _claim(self, job_id: int) -> Record[T] | None:
        """
        Attempt to claim a pending job.

        Returns:
            A :class:`Record` if the job was successfully claimed, else ``None``.
        """
        with self._env.begin(write=True) as txn:
            if txn.get(key_state(job_id)) != JobState.PENDING:
                return None

            payload_raw = txn.get(key_job(job_id))
            if payload_raw is None or len(payload_raw) < JOB_TIMESTAMP_SIZE:
                raise QueueCorrupted(f"job {job_id} is PENDING but payload is missing")

            enqueued_at = F64.unpack(payload_raw[:JOB_TIMESTAMP_SIZE])[0]
            try:
                payload = decode(payload_raw[JOB_TIMESTAMP_SIZE:])
            except Exception as exc:
                raise QueueCorrupted(f"job {job_id} payload could not be decoded") from exc

            retries = retry_count(txn, job_id)

            packed_lease, claim_token = new_lease(self._lease_time)
            set_running(txn, job_id, packed_lease)

            stats = stats_get(txn)
            stats.running += 1
            stats.pending -= 1
            stats_set(txn, stats)

        return Record(
            job_id=job_id,
            payload=payload,
            retries=retries,
            enqueued_at=enqueued_at,
            _finish=partial(self._finish, job_id, claim_token),
        )

    def _finish(self, job_id: int, claim_token: bytes, *, requeue: bool) -> None:
        self._assert_open()

        with self._env.begin(write=True) as txn:
            state = txn.get(key_state(job_id))
            if state != JobState.RUNNING:
                raise QueueCorrupted(f"job {job_id} is not RUNNING (state={state!r})")

            raw_lease = txn.get(key_lease(job_id))
            if raw_lease is None:
                raise QueueCorrupted(f"job {job_id} has no active lease")

            if lease_claim_token(raw_lease) != claim_token:
                raise QueueCorrupted(f"job {job_id} claim token does not match active lease")

            txn.delete(key_lease(job_id))

            stats = stats_get(txn)
            stats.running -= 1

            if not requeue:
                set_done(txn, job_id)
                stats.done += 1
            else:
                retries_next = retry_count(txn, job_id) + 1
                txn.put(key_retry(job_id), U32.pack(retries_next))

                if retries_next > self._max_retries:
                    set_failed(txn, job_id)
                    stats.failed += 1
                else:
                    set_pending(txn, job_id)
                    stats.pending += 1

            stats_set(txn, stats)

    def _poll_job(self, max_wait: float | None) -> int:
        return retry_until_timeout(
            self._peek_job,
            max_wait=max_wait,
            exception=QueueEmpty,
            error_message=_QUEUE_EMPTY_MSG,
            poll_interval=_POLL_INTERVAL,
        )

    def _peek_job(self) -> int | None:
        """
        Return the job ID of the next claimable job, or ``None`` if the
        ``queued/`` index is empty.

        Raises:
            QueueClosed: The queue has been closed.
        """
        if self._closed:
            raise QueueClosed("Queue is closed or was closed during polling")

        with self._env.begin(write=False) as txn:
            cursor = txn.cursor()
            if cursor.set_range(PFX_QUEUED) and cursor.key().startswith(PFX_QUEUED):
                return U64.unpack_from(cursor.key(), offset=PFX_QUEUED_OFFSET)[0]
            return None

    def _recover(self) -> int:
        """
        Rebuild queue state from LMDB on startup.

        Scans all job records, counts each state, and moves RUNNING jobs with
        expired or missing leases back to PENDING. Writes the final counters to
        the ``meta/stats`` key so they are available immediately after startup.

        Returns:
            The number of jobs moved from RUNNING back to PENDING.
        """
        now = time.time()
        stats = Stats()
        requeued = 0

        with self._env.begin(write=True) as txn:
            stats.total = meta_get(txn, META_TAIL)

            for cursor in iter_prefix(txn.cursor(), PFX_STATE):
                job_id, state = parse_state_cursor(cursor)

                match state:
                    case JobState.DONE:
                        stats.done += 1
                    case JobState.FAILED:
                        stats.failed += 1
                    case JobState.PENDING:
                        stats.pending += 1
                    case JobState.RUNNING:
                        if lease_is_expired_or_missing(txn, job_id, now):
                            requeue_running(txn, job_id)
                            stats.pending += 1
                            requeued += 1
                        else:
                            stats.running += 1
                    case _:
                        raise QueueCorrupted(f"job {job_id} has unknown state {state!r}")

            stats_set(txn, stats)

        return requeued

    def _requeue_loop(self) -> None:
        self._run_periodic(interval=self._recover_interval, action=self._requeue_expired)

    def _requeue_expired(self) -> int:
        """
        Move expired or orphaned RUNNING jobs back to PENDING.

        Scans ``state/`` for RUNNING jobs whose lease is expired or missing.

        Returns:
            The number of jobs moved back to PENDING.
        """
        if self._closed:
            return 0

        now = time.time()
        requeued = 0

        with self._env.begin(write=True) as txn:
            for cursor in iter_prefix(txn.cursor(), PFX_STATE):
                job_id, state = parse_state_cursor(cursor)
                if state == JobState.RUNNING and lease_is_expired_or_missing(txn, job_id, now):
                    requeue_running(txn, job_id)
                    requeued += 1

            if requeued:
                stats = stats_get(txn)
                stats.running -= requeued
                stats.pending += requeued
                stats_set(txn, stats)

        return requeued

    def _vacuum_loop(self) -> None:
        self._run_periodic(interval=self._vacuum_interval, action=self._vacuum)

    def _vacuum(self) -> int:
        """
        Delete DONE jobs from the database to free disk space.

        Only DONE records are removed. FAILED records are kept so that the
        failure state can be inspected later. All related keys (``job/``,
        ``state/``, ``retry/``, ``lease/``) are deleted in one atomic transaction.

        Returns:
            The number of jobs deleted.
        """
        if self._closed:
            return 0

        removed = 0

        with self._env.begin(write=True) as txn:
            done_ids: list[int] = []
            for cursor in iter_prefix(txn.cursor(), PFX_STATE):
                job_id, state = parse_state_cursor(cursor)
                if state == JobState.DONE:
                    done_ids.append(job_id)

            for job_id in done_ids:
                txn.delete(key_job(job_id))
                txn.delete(key_state(job_id))
                txn.delete(key_retry(job_id))
                txn.delete(key_lease(job_id))
                removed += 1

            if removed:
                stats = stats_get(txn)
                stats.done -= removed
                stats_set(txn, stats)

        return removed

    def _assert_open(self) -> None:
        if self._closed:
            raise QueueClosed()

    @staticmethod
    def _validate_config(
        *,
        lease_time: float,
        max_retries: int,
        map_size: int,
        recover_interval: float,
        vacuum_interval: float,
    ) -> None:
        if lease_time <= 0:
            raise ValueError("lease_time must be > 0")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if map_size <= 0:
            raise ValueError("map_size must be > 0")
        if recover_interval <= 0:
            raise ValueError("recover_interval must be > 0")
        if vacuum_interval <= 0:
            raise ValueError("vacuum_interval must be > 0")

    def _start_background_thread(
        self,
        *,
        target: Callable[[], None],
        name: str,
    ) -> threading.Thread:
        thread = threading.Thread(target=target, name=name, daemon=True)
        thread.start()
        return thread

    def _run_periodic(self, *, interval: float, action: Callable[[], object]) -> None:
        while not self._stop_event.wait(timeout=interval):
            action()

    def __enter__(self) -> Queue[T]:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


class AsyncQueue(Generic[T]):
    """
    Async wrapper around :class:`Queue`.

    Blocking queue operations are executed in worker threads so they
    do not block the event loop.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        lease_time: float = 30.0,
        max_retries: int = 3,
        map_size: int = 2**30,
        sync: bool = False,
        do_recover: bool = True,
        recover_interval: float = 15.0,
        do_vacuum: bool = True,
        vacuum_interval: float = 300.0,
    ) -> None:
        self._q: Queue[T] = Queue(
            path,
            lease_time=lease_time,
            max_retries=max_retries,
            map_size=map_size,
            sync=sync,
            do_recover=do_recover,
            recover_interval=recover_interval,
            do_vacuum=do_vacuum,
            vacuum_interval=vacuum_interval,
        )

    async def put(self, item: T) -> int:
        """
        Add an item to the queue.

        Args:
            item: Value to enqueue.

        Returns:
            The assigned job ID.

        Raises:
            QueueClosed: The queue has been closed.
        """
        return await asyncio.to_thread(self._q.put, item)

    async def get(self, timeout: float | None = None) -> Record[T]:
        """
        Claim the next available job.

        Returns:
            A claimed :class:`Record`.

        Raises:
            QueueEmpty: No job became available before the timeout expired.
            QueueClosed: The queue is closed.
        """
        return await asyncio.to_thread(self._q.get, timeout=timeout)

    @cl.asynccontextmanager
    async def processing(self, timeout: float | None = None) -> AsyncIterator[Record[T]]:
        """
        Async version of :meth:`Queue.processing`.

        Yields:
            The claimed :class:`Record`.
        """
        record = await self.get(timeout=timeout)
        try:
            yield record
        except Exception:
            await asyncio.to_thread(record.nack)
            raise
        else:
            await asyncio.to_thread(record.ack)

    async def stats(self) -> dict[str, int]:
        """
        Return queue statistics.

        Returns:
            A dictionary containing:

            - pending: Jobs waiting to be processed.
            - running: Jobs currently leased by a worker.
            - done: Jobs completed successfully (not yet vacuumed).
            - failed: Jobs that exceeded the retry limit.
            - total: Total jobs ever added; never decreases.
        """
        return await asyncio.to_thread(self._q.stats)

    async def close(self) -> None:
        """
        Stop background threads and close the queue.

        This method is idempotent and may be called multiple times.
        """
        await asyncio.to_thread(self._q.close)

    async def __aenter__(self) -> AsyncQueue[T]:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()
