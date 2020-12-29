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
package io.prestosql.plugin.kinesis;

import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.ErrorType;

import static io.prestosql.spi.ErrorType.EXTERNAL;
import static io.prestosql.spi.ErrorType.INTERNAL_ERROR;

/**
 * Kinesis connector specific error codes.
 */
public enum KinesisErrorCode
        implements ErrorCodeSupplier
{
    KINESIS_CONVERSION_NOT_SUPPORTED(0, EXTERNAL),
    KINESIS_SPLIT_ERROR(1, INTERNAL_ERROR),
    KINESIS_METADATA_EXCEPTION(2, INTERNAL_ERROR);

    // Connectors can use error codes starting at EXTERNAL
    public static final int StartingErrorCode = 0x0200_0000;

    private final ErrorCode errorCode;

    KinesisErrorCode(int code, ErrorType errorType)
    {
        errorCode = new ErrorCode(code + StartingErrorCode, name(), errorType);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
