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
package io.trino.plugin.deltalake;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;

public enum DeltaLakeErrorCode
        implements ErrorCodeSupplier
{
    DELTA_LAKE_INVALID_SCHEMA(0, EXTERNAL),
    DELTA_LAKE_INVALID_TABLE(1, EXTERNAL),
    DELTA_LAKE_BAD_DATA(2, EXTERNAL),
    DELTA_LAKE_BAD_WRITE(3, EXTERNAL),
    DELTA_LAKE_FILESYSTEM_ERROR(4, EXTERNAL),
    /**/;

    private final ErrorCode errorCode;

    DeltaLakeErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0506_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
