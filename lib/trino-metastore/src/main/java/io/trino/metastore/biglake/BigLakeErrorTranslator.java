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
package io.trino.metastore.biglake;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;

/**
 * Translates BigLake failures into Trino errors.
 *
 * <p>A missing object is left to the caller, because the read operations of {@code HiveMetastore}
 * report one as an empty result rather than a failure.
 */
public final class BigLakeErrorTranslator
{
    private BigLakeErrorTranslator() {}

    public static boolean isNotFound(ApiException exception)
    {
        return exception.getStatusCode().getCode() == Code.NOT_FOUND;
    }

    /**
     * Translates a failed BigLake call into the Trino error for {@code operation}.
     */
    public static TrinoException translate(String operation, ApiException exception)
    {
        return switch (exception.getStatusCode().getCode()) {
            case PERMISSION_DENIED, UNAUTHENTICATED -> new TrinoException(
                    PERMISSION_DENIED, message(operation, "BigLake denied the request"), exception);
            case ALREADY_EXISTS -> new TrinoException(
                    ALREADY_EXISTS, message(operation, "the object already exists"), exception);
            case INVALID_ARGUMENT, OUT_OF_RANGE -> new TrinoException(
                    INVALID_ARGUMENTS, message(operation, "BigLake rejected the request"), exception);
            default -> new TrinoException(
                    GENERIC_INTERNAL_ERROR, message(operation, "BigLake request failed"), exception);
        };
    }

    /**
     * Returns the error for every write and DDL operation that the read path excludes.
     */
    public static TrinoException notSupported(String operation)
    {
        return new TrinoException(
                NOT_SUPPORTED, "This BigLake metastore supports read operations only, not " + operation);
    }

    private static String message(String operation, String detail)
    {
        return operation + ": " + detail;
    }
}
