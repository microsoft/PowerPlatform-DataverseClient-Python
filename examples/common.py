# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Common utilities shared across example scripts.
"""

import time


def backoff(op, *, delays=(0, 2, 5, 10, 20, 20)):
    """
    Retry helper with exponential backoff.

    Useful for operations that may fail temporarily due to metadata propagation
    delays or transient service issues.

    :param op: Callable to execute (no arguments).
    :param delays: Tuple of delay times in seconds. First attempt uses delays[0],
        subsequent retries use delays[1], delays[2], etc.
    :return: Result of successful op() call.
    :raises: Last exception if all attempts fail.
    """
    last = None
    total_delay = 0
    attempts = 0
    for d in delays:
        if d:
            time.sleep(d)
            total_delay += d
        attempts += 1
        try:
            result = op()
            if attempts > 1:
                retry_count = attempts - 1
                print(
                    f"   * Backoff succeeded after {retry_count} retry(s); waited {total_delay}s total."
                )
            return result
        except Exception as ex:  # noqa: BLE001
            last = ex
            continue
    if last:
        if attempts:
            retry_count = max(attempts - 1, 0)
            print(
                f"   [WARN] Backoff exhausted after {retry_count} retry(s); waited {total_delay}s total."
            )
        raise last


__all__ = ["backoff"]
