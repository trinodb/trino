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
package io.trino.decoder.protobuf;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;

public enum ProtobufErrorCode
        implements ErrorCodeSupplier
{
    INVALID_PROTO_FILE(0, EXTERNAL),
    MESSAGE_NOT_FOUND(1, EXTERNAL),
    INVALID_PROTOBUF_MESSAGE(2, EXTERNAL),
    INVALID_TIMESTAMP(3, EXTERNAL),
    /**/;

    private final ErrorCode errorCode;

    ProtobufErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0606_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
