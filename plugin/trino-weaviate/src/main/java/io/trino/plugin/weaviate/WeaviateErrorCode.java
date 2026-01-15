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
package io.trino.plugin.weaviate;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;

import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum WeaviateErrorCode
        implements ErrorCodeSupplier
{
    WEAVIATE_TABLE_NOT_FOUND(0, USER_ERROR),
    WEAVIATE_UNSUPPORTED_DATA_TYPE(1, INTERNAL_ERROR),
    WEAVIATE_MULTIPLE_DATA_TYPES(2, INTERNAL_ERROR),
    WEAVIATE_SERVER_ERROR(3, INTERNAL_ERROR),
    WEAVIATE_UNSUPPORTED_VECTOR_TYPE(4, INTERNAL_ERROR);

    private final ErrorCode errorCode;

    WeaviateErrorCode(int code, ErrorType type)
    {
        this.errorCode = new ErrorCode(code + 0x0508_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }

    static TrinoException tableNotFound(String tableName)
    {
        return new TrinoException(WEAVIATE_TABLE_NOT_FOUND, "Collection " + tableName + " not found");
    }

    static TrinoException typeNotSupported(String msg)
    {
        return new TrinoException(WEAVIATE_UNSUPPORTED_DATA_TYPE, msg);
    }

    static TrinoException vectorTypeNotSupported(Object vector)
    {
        return new TrinoException(WEAVIATE_UNSUPPORTED_VECTOR_TYPE, "vector type %s is not supported".formatted(vector.getClass()));
    }
}
