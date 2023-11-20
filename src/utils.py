import logging
from itertools import islice

import numpy as np

from src.const import NO_DATA
from src.models import FileName

logger = logging.getLogger(__name__)


def parse_file_name(name: str) -> FileName:
    """
    parse file name to dataclass
    """
    return FileName(*name.split("_", 6))


def batched(iterable, batch_size):
    """
    split any iterable to batches
    """
    if batch_size < 1:
        raise ValueError("Too small batch_size")
    it = iter(iterable)
    while batch := tuple(islice(it, batch_size)):
        yield batch


def process_nan(value):
    """
    check NaN and return special value if needed
    """
    if np.isnan(value):
        return NO_DATA
    return value


def convert_float(value, multiplier):
    """
    round float to 6 digits and convert to int by multiply
    """
    return int(float(format(value, '.6f')) * multiplier)
