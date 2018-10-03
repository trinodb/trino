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
package io.prestosql.plugin.iceberg;

import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.ErrorType;

import static io.prestosql.spi.ErrorType.EXTERNAL;
import static io.prestosql.spi.ErrorType.USER_ERROR;

public enum IcebergErrorCode
        implements ErrorCodeSupplier
{
    ICEBERG_UNKNOWN_TABLE_TYPE(0, EXTERNAL),
    ICEBERG_INVALID_METADATA(1, EXTERNAL),
    ICEBERG_TOO_MANY_OPEN_PARTITIONS(2, USER_ERROR),
    /**/;

    private final ErrorCode errorCode;

    IcebergErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0504_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
