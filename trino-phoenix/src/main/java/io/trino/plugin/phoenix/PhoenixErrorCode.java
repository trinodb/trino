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
package io.trino.plugin.phoenix;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;

public enum PhoenixErrorCode
        implements ErrorCodeSupplier
{
    PHOENIX_INTERNAL_ERROR(0, INTERNAL_ERROR),
    PHOENIX_QUERY_ERROR(1, EXTERNAL),
    PHOENIX_CONFIG_ERROR(2, INTERNAL_ERROR),
    PHOENIX_METADATA_ERROR(3, EXTERNAL),
    PHOENIX_SPLIT_ERROR(4, EXTERNAL);

    private final ErrorCode errorCode;

    PhoenixErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0106_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
