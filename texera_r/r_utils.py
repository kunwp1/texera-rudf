# This file is part of texera-rudf
#
# Copyright (c) 2024 The Texera Team
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
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
    # For source operators, we don't know which fields are large binary ahead of time,
    # so we auto-detect any S3 URIs and convert them to largebinary objects
    if source_operator:
        # Auto-detect and convert any S3 URI strings to largebinary objects
        for key, value in output_python_dict.items():
            if isinstance(value, str) and value.startswith("s3://"):
                output_python_dict[key] = largebinary(value)
    elif large_binary_fields:
        # For non-source operators, only convert known large_binary_fields
        for key, value in output_python_dict.items():
            if (
                key in large_binary_fields
                and isinstance(value, str)
                and value.startswith("s3://")
            ):
                # This is a URI string that should be a largebinary object
                output_python_dict[key] = largebinary(value)

    return Tuple(output_python_dict)
