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

import rpy2
import rpy2.rinterface as rinterface
import rpy2.robjects as robjects
import warnings

# Import from Texera core (must be available at runtime)
from core.models import Tuple
from core.models.type.large_binary import largebinary

warnings.filterwarnings(action="ignore", category=UserWarning, module=r"rpy2*")


def convert_r_to_py(value: rpy2.robjects):
    """
    :param value: A value that is from one of rpy2's many types (from rpy2.robjects)
    :return: A Python representation of the value, if convertable.
        If not, it returns the value itself
    """
    # Check if it's a largebinary object (Python largebinary wrapped in R)
    if isinstance(value, largebinary):
        return value
    if isinstance(value, robjects.vectors.BoolVector):
        return bool(value[0])
    if isinstance(value, robjects.vectors.IntVector):
        return int(value[0])
    if isinstance(value, robjects.vectors.FloatVector):
        if isinstance(value, robjects.vectors.POSIXct):
            return next(value.iter_localized_datetime())
        else:
            return float(value[0])
    if isinstance(value, robjects.vectors.StrVector):
        return str(value[0])
    return value


def extract_tuple_from_r(
    output_r_generator: rpy2.robjects.SignatureTranslatedFunction,
    source_operator: bool,
    input_fields: [None, list[str]] = None,
    large_binary_fields: [None, list[str]] = None,
) -> [Tuple, None]:
    output_r_tuple: rpy2.robjects.ListVector = output_r_generator()
    if (
        isinstance(output_r_tuple, rinterface.SexpSymbol)
        and str(output_r_tuple) == ".__exhausted__."
    ) or isinstance(output_r_tuple.names, rpy2.rinterface_lib.sexp.NULLType):
        return None

    output_python_dict: dict[str, object] = {}
    if source_operator:
        output_python_dict = {
            key: output_r_tuple.rx2(key) for key in output_r_tuple.names
        }
    else:
        diff_fields: list[str] = [
            field_name
            for field_name in output_r_tuple.names
            if field_name not in input_fields
        ]
        output_python_dict: dict[str, object] = {
            key: output_r_tuple.rx2(key) for key in (input_fields + diff_fields)
        }

    output_python_dict: dict[str, object] = {
        key: convert_r_to_py(value) for key, value in output_python_dict.items()
    }

    # Convert URI strings back to largebinary objects for LARGE_BINARY fields
    if large_binary_fields:
        for key, value in output_python_dict.items():
            if (
                key in large_binary_fields
                and isinstance(value, str)
                and value.startswith("s3://")
            ):
                # This is a URI string that should be a largebinary object
                output_python_dict[key] = largebinary(value)

    return Tuple(output_python_dict)
