from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any


def retry_until_timeout(
    callback: Callable[[], Any],
    max_wait: float | None,
    exception: type[BaseException],
    error_message: str = "",
    poll_interval: float = 0.01,
) -> Any:
    """
    Repeatedly execute a callback until it returns a truthy value or the
    deadline is reached.

    Args:
        callback: Called on each iteration; return value is tested for
            truthiness.
        max_wait: Maximum seconds to wait. ``None`` means wait indefinitely.
        exception: Raised when ``max_wait`` expires without a truthy result.
        error_message: Message attached to the raised exception.
        poll_interval: Sleep duration between unsuccessful attempts.

    Returns:
        The first truthy return value from ``callback``.

    Raises:
        exception: When the deadline expires without a truthy result.
    """
    start_time = time.monotonic()

    while True:
        result = callback()
        if result is not None:
            return result

        if max_wait is not None:
            elapsed = time.monotonic() - start_time
            remaining = max_wait - elapsed
            if remaining <= 0:
                raise exception(error_message)
            time.sleep(min(poll_interval, remaining))
        else:
            time.sleep(poll_interval)
