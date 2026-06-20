__all__ = [
    "EQueueError",
    "QueueEmpty",
    "QueueClosed",
    "QueueCorrupted",
]


class EQueueError(Exception):
    """Base exception for all EQueue errors."""

    __slots__ = ()


class QueueEmpty(EQueueError):
    """Raised by :meth:`Queue.get` when no job becomes available before the timeout."""

    __slots__ = ()


class QueueClosed(EQueueError):
    """Raised when an operation is attempted on a closed queue."""

    __slots__ = ()


class QueueCorrupted(EQueueError):
    """
    Raised when LMDB state is inconsistent or a completion attempt is invalid.

    Examples: calling ``ack()`` on a job that is no longer RUNNING,
    presenting a stale claim token, or finding missing data on disk.
    """

    __slots__ = ()
