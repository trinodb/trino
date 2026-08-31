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
package io.trino.plugin.paimon;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;

public enum PaimonErrorCode
        implements ErrorCodeSupplier
{
    PAIMON_BAD_DATA(0, EXTERNAL),
    PAIMON_CANNOT_OPEN_SPLIT(1, EXTERNAL),
    PAIMON_CURSOR_ERROR(2, EXTERNAL),
    PAIMON_WRITER_DATA_ERROR(3, EXTERNAL),
    PAIMON_WRITER_CLOSE_ERROR(4, EXTERNAL),
    PAIMON_COMMIT_ERROR(5, EXTERNAL),
    PAIMON_METADATA_ERROR(6, EXTERNAL),
    /**/;

    private final ErrorCode errorCode;

    PaimonErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0511_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
