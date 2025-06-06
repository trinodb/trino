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
package io.trino.plugin.google.sheets;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum SheetsErrorCode
        implements ErrorCodeSupplier
{
    SHEETS_BAD_CREDENTIALS_ERROR(0, EXTERNAL),
    SHEETS_METASTORE_ERROR(1, EXTERNAL),
    SHEETS_UNKNOWN_TABLE_ERROR(2, USER_ERROR),
    SHEETS_TABLE_LOAD_ERROR(3, INTERNAL_ERROR),
    SHEETS_INSERT_ERROR(4, INTERNAL_ERROR),
    SHEETS_INVALID_TABLE_FORMAT(5, EXTERNAL)
    /**/;

    private final ErrorCode errorCode;

    SheetsErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0508_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
