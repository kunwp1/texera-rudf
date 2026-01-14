"""
Texera R Plugin - R language support for Apache Texera

This plugin provides R operator executors using rpy2.

License: GPLv2 (due to rpy2 dependency)
"""

from texera_r.RTupleExecutor import RTupleExecutor, RTupleSourceExecutor
from texera_r.RTableExecutor import RTableExecutor, RTableSourceExecutor
from texera_r.r_utils import convert_r_to_py, extract_tuple_from_r

__version__ = "0.1.0"
__author__ = "Texera Team"
__license__ = "GPLv2"

__all__ = [
    "RTupleExecutor",
    "RTupleSourceExecutor",
    "RTableExecutor",
    "RTableSourceExecutor",
    "convert_r_to_py",
    "extract_tuple_from_r",
]
