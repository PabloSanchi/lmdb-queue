"""LMDB cursor iteration helpers."""

from __future__ import annotations

from collections.abc import Iterator

import lmdb

from .keys import PFX_STATE_OFFSET, U64


def iter_prefix(cursor: lmdb.Cursor, prefix: bytes) -> Iterator[lmdb.Cursor]:
    """Yield ``cursor`` at each key that starts with ``prefix``."""
    if not cursor.set_range(prefix):
        return
    while cursor.key().startswith(prefix):
        yield cursor
        if not cursor.next():
            break


def parse_state_cursor(cursor: lmdb.Cursor) -> tuple[int, bytes]:
    """Extract (job_id, state) from a cursor positioned at a ``state/`` key."""
    job_id = U64.unpack_from(cursor.key(), offset=PFX_STATE_OFFSET)[0]
    return job_id, cursor.value()
