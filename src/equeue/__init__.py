from importlib.metadata import version

from equeue.exceptions import EQueueError, QueueClosed, QueueCorrupted, QueueEmpty
from equeue.queue import AsyncQueue, Queue
from equeue.record import Record

__version__ = version("equeue")

__all__ = [
    "__version__",
    "AsyncQueue",
    "EQueueError",
    "Queue",
    "QueueClosed",
    "QueueCorrupted",
    "QueueEmpty",
    "Record",
]
