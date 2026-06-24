"""Queue counters and job state values stored in LMDB."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import Enum

_5U64 = struct.Struct("<5Q")


@dataclass(slots=True)
class Stats:
    """
    All queue counters stored as a single LMDB entry.

    Reading or writing stats is one O(1) LMDB operation.
    All fields default to zero and are updated atomically inside write transactions.
    """

    pending: int = 0
    running: int = 0
    done: int = 0
    failed: int = 0
    total: int = 0

    def to_dict(self) -> dict[str, int]:
        """Return a plain dict suitable for external use."""
        return {
            "pending": self.pending,
            "running": self.running,
            "done": self.done,
            "failed": self.failed,
            "total": self.total,
        }

    def pack(self) -> bytes:
        """Serialize all counters to bytes for storage in LMDB."""
        return _5U64.pack(
            self.pending,
            self.running,
            self.done,
            self.failed,
            self.total,
        )

    @classmethod
    def unpack(cls, data: bytes) -> Stats:
        """Deserialize bytes read from LMDB into a Stats instance."""
        return cls(*_5U64.unpack(data))


class JobState(bytes, Enum):
    """
    Single-byte lifecycle state stored under ``state/<job_id>``.
    """

    PENDING = b"P"
    RUNNING = b"R"
    DONE = b"D"
    FAILED = b"F"
