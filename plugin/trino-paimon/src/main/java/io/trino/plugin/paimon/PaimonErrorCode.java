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
    PAIMON_CANNOT_OPEN_SPLIT(0, EXTERNAL),
    PAIMON_METADATA_FETCH_FAILED(1, EXTERNAL);

    private final ErrorCode errorCode;

    PaimonErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0515_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
