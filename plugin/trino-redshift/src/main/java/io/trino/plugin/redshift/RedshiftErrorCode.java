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
package io.trino.plugin.redshift;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;

public enum RedshiftErrorCode
        implements ErrorCodeSupplier
{
    REDSHIFT_INVALID_TYPE(0, EXTERNAL),
    REDSHIFT_PARQUET_BAD_DATA(1, EXTERNAL),
    REDSHIFT_PARQUET_CURSOR_ERROR(2, EXTERNAL),
    REDSHIFT_FILESYSTEM_ERROR(3, EXTERNAL),
    REDSHIFT_S3_CROSS_REGION_UNSUPPORTED(4, EXTERNAL),
    /**/;

    private final ErrorCode errorCode;

    RedshiftErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0511_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
