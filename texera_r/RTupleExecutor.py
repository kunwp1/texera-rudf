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

import datetime
import pickle
import pyarrow as pa
import rpy2
import rpy2.robjects as robjects
import warnings
from rpy2.robjects import default_converter
from rpy2.robjects.conversion import localconverter as local_converter
from rpy2_arrow.arrow import converter as arrow_converter
from typing import Iterator, Optional, Union

# Import from Texera core (must be available at runtime)
from core.models import Tuple, TupleLike, TableLike
from core.models.operator import SourceOperator, TupleOperatorV2
from core.models.schema.attribute_type_utils import (
    TEXERA_TYPE_METADATA_KEY,
    LARGE_BINARY_METADATA_VALUE,
)
from core.models.type.large_binary import largebinary

# Import from this plugin
from texera_r import r_utils

warnings.filterwarnings(action="ignore", category=UserWarning, module=r"rpy2*")


class RTupleExecutor(TupleOperatorV2):
    """
    An operator that can execute R code on R Lists (R's representation of a Tuple)
    """

    is_source = False

    _combine_binary_and_non_binary_lists = robjects.r(
        """
        function(non_binary_list, binary_list) {
            non_binary_list <- as.list(non_binary_list$as_vector())
            return (c(non_binary_list, binary_list))
        }
        """
    )

    @staticmethod
    def _setup_r_large_binary_apis():
        """Set up R functions for large binary S3 operations."""
        import os
        from core.storage.storage_config import StorageConfig

        s3_endpoint = getattr(StorageConfig, "S3_ENDPOINT", None) or os.getenv(
            "STORAGE_S3_ENDPOINT", "http://localhost:9000"
        )
        s3_region = getattr(StorageConfig, "S3_REGION", None) or os.getenv(
            "STORAGE_S3_REGION", "us-west-2"
        )
        s3_username = getattr(StorageConfig, "S3_AUTH_USERNAME", None) or os.getenv(
            "STORAGE_S3_AUTH_USERNAME", "texera_minio"
        )
        s3_password = getattr(StorageConfig, "S3_AUTH_PASSWORD", None) or os.getenv(
            "STORAGE_S3_AUTH_PASSWORD", "password"
        )

        os.environ["STORAGE_S3_ENDPOINT"] = s3_endpoint
        os.environ["STORAGE_S3_REGION"] = s3_region
        os.environ["STORAGE_S3_AUTH_USERNAME"] = s3_username
        os.environ["STORAGE_S3_AUTH_PASSWORD"] = s3_password

        robjects.r("""
        if (!requireNamespace("aws.s3", quietly = TRUE)) {
            stop("Package 'aws.s3' required. Install with: install.packages('aws.s3')")
        }
        library(aws.s3)

        DEFAULT_BUCKET <- "texera-large-binaries"
        
        setup_s3_config <- function() {
            Sys.setenv(
                AWS_ACCESS_KEY_ID = Sys.getenv("STORAGE_S3_AUTH_USERNAME", "texera_minio"),
                AWS_SECRET_ACCESS_KEY = Sys.getenv("STORAGE_S3_AUTH_PASSWORD", "password"),
                AWS_S3_ENDPOINT = Sys.getenv("STORAGE_S3_ENDPOINT", "http://localhost:9000"),
                AWS_DEFAULT_REGION = Sys.getenv("STORAGE_S3_REGION", "us-west-2")
            )
            
            endpoint <- Sys.getenv("STORAGE_S3_ENDPOINT", "http://localhost:9000")
            list(
                base_url = sub("^https?://", "", endpoint),
                use_https = grepl("^https://", endpoint),
                region = "",
                username = Sys.getenv("STORAGE_S3_AUTH_USERNAME", "texera_minio"),
                password = Sys.getenv("STORAGE_S3_AUTH_PASSWORD", "password")
            )
        }
        
        parse_s3_uri <- function(uri) {
            if (!grepl("^s3://", uri)) stop("Invalid S3 URI: ", uri)
            parts <- strsplit(sub("^s3://", "", uri), "/", fixed = TRUE)[[1]]
            if (length(parts) < 2) stop("Invalid S3 URI format: ", uri)
            list(bucket = parts[1], object_key = paste(parts[-1], collapse = "/"))
        }
        
        generate_uuid <- function() {
            paste(sample(c(0:9, letters[1:6]), 32, replace = TRUE), collapse = "")
        }

        largebinary <- function(uri = NULL) {
            if (is.null(uri)) {
                config <- setup_s3_config()
                
                tryCatch(
                    invisible(capture.output(head_bucket(DEFAULT_BUCKET, base_url = config$base_url, 
                        use_https = config$use_https, region = config$region, 
                        key = config$username, secret = config$password))),
                    error = function(e) {
                        tryCatch(
                            invisible(capture.output(put_bucket(DEFAULT_BUCKET, base_url = config$base_url,
                                use_https = config$use_https, region = config$region,
                                key = config$username, secret = config$password))),
                            error = function(e2) {}
                        )
                    }
                )
                
                timestamp_ms_str <- sprintf("%.0f", as.numeric(Sys.time()) * 1000)
                uri <- paste0("s3://", DEFAULT_BUCKET, "/objects/", timestamp_ms_str, "/", generate_uuid())
            } else if (!is.character(uri) || length(uri) != 1) {
                stop("uri must be a single character string")
            } else if (!grepl("^s3://", uri)) {
                stop("largebinary URI must start with 's3://', got: ", uri)
            }
            return(uri)
        }

        LargeBinaryInputStream <- function(uri) {
            if (!is.character(uri)) stop("Expected character URI string")
            
            parsed <- parse_s3_uri(uri)
            config <- setup_s3_config()
            
            temp_file <- tempfile()
            invisible(capture.output(
                save_object(object = parsed$object_key, bucket = parsed$bucket, file = temp_file,
                    base_url = config$base_url, use_https = config$use_https, region = config$region,
                    key = config$username, secret = config$password)
            ))
            
            file_conn <- file(temp_file, open = "rb")
            closed <- FALSE
            
            list(
                read = function(n = -1) {
                    if (closed) stop("I/O operation on closed stream")
                    if (n < 0) {
                        file_size <- file.info(temp_file)$size
                        if (is.na(file_size) || file_size == 0) return(raw(0))
                        current_pos <- seek(file_conn, NA)
                        remaining <- file_size - current_pos
                        if (remaining <= 0) return(raw(0))
                        readBin(file_conn, "raw", n = remaining)
                    } else {
                        readBin(file_conn, "raw", n = n)
                    }
                },
                readline = function(size = -1) {
                    if (closed) stop("I/O operation on closed stream")
                    readLines(file_conn, n = 1, warn = FALSE)
                },
                readable = function() !closed,
                seekable = function() FALSE,
                close = function() {
                    if (!closed) {
                        closed <<- TRUE
                        close(file_conn)
                        unlink(temp_file)
                    }
                },
                closed = function() closed,
                file_path = temp_file,
                file_conn = file_conn
            )
        }

        LargeBinaryOutputStream <- function(uri) {
            if (!is.character(uri)) stop("Expected character URI string")
            
            parsed <- parse_s3_uri(uri)
            config <- setup_s3_config()
            
            temp_file <- tempfile()
            file_conn <- file(temp_file, open = "wb")
            closed <- FALSE
            
            list(
                write = function(data) {
                    if (closed) stop("I/O operation on closed stream")
                    writeBin(data, file_conn)
                    return(length(data))
                },
                writable = function() !closed,
                seekable = function() FALSE,
                flush = function() if (!closed) flush(file_conn),
                closed = function() closed,
                close = function() {
                    if (!closed) {
                        closed <<- TRUE
                        flush(file_conn)
                        close(file_conn)
                        
                        tryCatch({
                            invisible(capture.output(
                                put_object(file = temp_file, object = parsed$object_key, bucket = parsed$bucket,
                                    base_url = config$base_url, use_https = config$use_https, region = config$region,
                                    key = config$username, secret = config$password,
                                    multipart = TRUE, part_size = 50 * 1024 * 1024)
                            ))
                        }, error = function(e) {
                            unlink(temp_file)
                            stop("Failed to upload to S3: ", conditionMessage(e))
                        })
                        unlink(temp_file)
                    }
                },
                file_path = temp_file,
                file_conn = file_conn
            )
        }
        """)

    def __init__(self, r_code: str):
        """
        Initialize the RTupleExecutor with R code.

        Args:
            r_code (str): R code to be executed.
        """
        super().__init__()
        # Set up R large binary APIs before loading user code
        RTupleExecutor._setup_r_large_binary_apis()
        # Use the local converter from rpy2 to load in the R function given by the user
        with local_converter(default_converter):
            self._func = robjects.r(r_code)

    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        """
        Process an input Tuple from the given link.

        :param tuple_: Tuple, a Tuple from an input port to be processed.
        :param port: int, input port index of the current Tuple.
        :return: Iterator[Optional[TupleLike]], producing one TupleLike object at a
            time, or None.
        """
        with local_converter(arrow_converter):
            input_schema: pa.Schema = tuple_._schema.as_arrow_schema()
            input_fields: list[str] = [field.name for field in input_schema]

            # Separate fields by type: regular, binary, and large_binary
            non_binary_fields: list[str] = []
            binary_fields: list[str] = []
            large_binary_fields: list[str] = []

            for field in input_schema:
                # Check if it's LARGE_BINARY by metadata
                is_large_binary = (
                    field.metadata
                    and field.metadata.get(TEXERA_TYPE_METADATA_KEY)
                    == LARGE_BINARY_METADATA_VALUE
                )
                if is_large_binary:
                    large_binary_fields.append(field.name)
                elif field.type == pa.binary():
                    binary_fields.append(field.name)
                else:
                    non_binary_fields.append(field.name)

            non_binary_pyarrow_array: pa.StructArray = pa.array([], type=pa.struct([]))
            if non_binary_fields:
                non_binary_tuple: Tuple = tuple_.get_partial_tuple(non_binary_fields)
                non_binary_tuple_schema: pa.Schema = (
                    non_binary_tuple._schema.as_arrow_schema()
                )
                non_binary_pyarrow_array: pa.StructArray = pa.array(
                    [non_binary_tuple.as_dict()],
                    type=pa.struct(non_binary_tuple_schema),
                )

            binary_r_list: dict[str, object] = {}
            if binary_fields:
                binary_tuple: Tuple = tuple_.get_partial_tuple(binary_fields)
                for k, v in binary_tuple.as_dict().items():
                    if isinstance(v, bytes):
                        binary_r_list[k] = pickle.loads(v[10:])
                    elif isinstance(v, datetime.datetime):
                        binary_r_list[k] = robjects.vectors.POSIXct.sexp_from_datetime(
                            [v]
                        )
                    else:
                        binary_r_list[k] = v

            # Handle LARGE_BINARY fields: convert to URI strings for R
            # R code can use largebinary(uri_string) to create largebinary objects if needed
            large_binary_r_list: dict[str, object] = {}
            if large_binary_fields:
                large_binary_tuple: Tuple = tuple_.get_partial_tuple(
                    large_binary_fields
                )
                for k, v in large_binary_tuple.as_dict().items():
                    if isinstance(v, largebinary):
                        # Convert largebinary object to URI string for R
                        large_binary_r_list[k] = v.uri
                    elif isinstance(v, str):
                        # Already a URI string, pass as-is
                        large_binary_r_list[k] = v
                    else:
                        large_binary_r_list[k] = v

            # Combine binary and large_binary into one list
            all_binary_r_list: dict[str, object] = {
                **binary_r_list,
                **large_binary_r_list,
            }
            binary_r_list: rpy2.robjects.ListVector = robjects.vectors.ListVector(
                all_binary_r_list
            )

            input_r_list: rpy2.robjects.ListVector = (
                RTupleExecutor._combine_binary_and_non_binary_lists(
                    non_binary_pyarrow_array, binary_r_list
                )
            )

            output_r_generator: rpy2.robjects.SignatureTranslatedFunction = self._func(
                input_r_list, port
            )

            while True:
                output_py_tuple: Tuple = r_utils.extract_tuple_from_r(
                    output_r_generator, False, input_fields, large_binary_fields
                )
                yield output_py_tuple if output_py_tuple is not None else None
                if output_py_tuple is None:
                    break


class RTupleSourceExecutor(SourceOperator):
    """
    A source operator that produces a generator that yields R Lists using R code.
    R Lists are R's representation of a Tuple
    """

    is_source = True

    def __init__(self, r_code: str):
        """
        Initialize the RTupleSourceExecutor with R code.

        Args:
            r_code (str): R code to be executed.
        """
        super().__init__()
        # Set up R large binary APIs before loading user code
        RTupleExecutor._setup_r_large_binary_apis()
        # Use the local converter from rpy2 to load in the R function given by the user
        with local_converter(default_converter):
            self._func = robjects.r(r_code)

    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
        """
        Produce Tuples using the provided R generator returned by the UDF.
        The returned R generator is an iterator
        that yields R Lists (R's representation of Tuple)
        Used by the source operator only.

        :return: Iterator[Union[TupleLike, TableLike, None]], producing
            one TupleLike object, one TableLike object, or None, at a time.
        """
        with local_converter(arrow_converter):
            output_r_generator: rpy2.robjects.SignatureTranslatedFunction = self._func()
            while True:
                output_py_tuple: Tuple = r_utils.extract_tuple_from_r(
                    output_r_generator, True
                )
                yield output_py_tuple if output_py_tuple is not None else None
                if output_py_tuple is None:
                    break
