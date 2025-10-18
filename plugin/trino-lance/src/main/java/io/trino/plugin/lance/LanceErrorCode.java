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
package io.trino.plugin.lance;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum LanceErrorCode
        implements ErrorCodeSupplier
{
    LANCE_TABLE_NOT_FOUND(1, EXTERNAL),
    LANCE_INVALID_METADATA(2, EXTERNAL),
    LANCE_BAD_DATA(3, EXTERNAL),
    LANCE_SPLIT_ERROR(4, EXTERNAL),
    LANCE_INVALID_VERSION_NUMBER(11, USER_ERROR);

    private final ErrorCode errorCode;

    LanceErrorCode(int code, ErrorType errorType)
    {
        errorCode = new ErrorCode(code + 0x0514_0000, name(), errorType, errorType == USER_ERROR);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
