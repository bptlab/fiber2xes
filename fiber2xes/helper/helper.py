"""
Provides basic utility functions.
"""

import functools
import time


def timer(func):
    """
    Decorator to benchmark functions

    Keyword arguments:
    func - function which should be benchmarked
    """

    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        print("Finished "+str(func.__name__)+" in "+str(run_time)+"s")
        return value
    return wrapper_timer
