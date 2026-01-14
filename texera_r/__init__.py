# This file is part of texera-r-plugin
#
# Copyright (C) 2024 The Texera Team
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
# This plugin is licensed under GPLv2 due to the rpy2 dependency.
#

"""
Texera R Plugin - R language support for Apache Texera

This plugin provides R operator executors using rpy2.
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
