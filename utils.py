from models import FileName
from functools import wraps
import time
import logging

logger = logging.getLogger(__name__)


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        print(args, kwargs)
        result = func(*args, **kwargs)
        total_time = time.perf_counter() - start_time
        logger.info(f"Function {func.__name__} total time {total_time:.4f} seconds.")
        return result
    return timeit_wrapper()


def parse_file_name(name: str) -> FileName:
    return FileName(*name.split("_", 6))

