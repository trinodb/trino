/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum IcebergErrorCode
        implements ErrorCodeSupplier
{
    // code ICEBERG_UNKNOWN_TABLE_TYPE(0, EXTERNAL) is deprecated
    ICEBERG_INVALID_METADATA(1, EXTERNAL),
    ICEBERG_TOO_MANY_OPEN_PARTITIONS(2, USER_ERROR),
    ICEBERG_INVALID_PARTITION_VALUE(3, EXTERNAL),
    ICEBERG_BAD_DATA(4, EXTERNAL),
    // ICEBERG_MISSING_DATA(5, EXTERNAL) is deprecated
    ICEBERG_CANNOT_OPEN_SPLIT(6, EXTERNAL),
    ICEBERG_WRITER_OPEN_ERROR(7, EXTERNAL),
    ICEBERG_FILESYSTEM_ERROR(8, EXTERNAL),
    ICEBERG_CURSOR_ERROR(9, EXTERNAL),
    ICEBERG_WRITE_VALIDATION_FAILED(10, INTERNAL_ERROR),
    ICEBERG_INVALID_SNAPSHOT_ID(11, USER_ERROR),
    ICEBERG_COMMIT_ERROR(12, EXTERNAL, true),
    ICEBERG_CATALOG_ERROR(13, EXTERNAL),
    ICEBERG_WRITER_CLOSE_ERROR(14, EXTERNAL),
    ICEBERG_MISSING_METADATA(15, EXTERNAL),
    ICEBERG_WRITER_DATA_ERROR(16, EXTERNAL),
    ICEBERG_UNSUPPORTED_VIEW_DIALECT(17, EXTERNAL)
    /**/;

    private final ErrorCode errorCode;

    IcebergErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0504_0000, name(), type, type == USER_ERROR);
    }

    IcebergErrorCode(int code, ErrorType type, boolean fatal)
    {
        errorCode = new ErrorCode(code + 0x0504_0000, name(), type, fatal);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
