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
package io.trino.metastore.lakehouse;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;

/**
 * Translates Lakehouse failures into Trino errors.
 *
 * <p>A missing object is left to the caller, because the read operations of {@code HiveMetastore}
 * report one as an empty result rather than a failure. Lakehouse reports a missing project, table or
 * database as {@code NOT_FOUND}, but a missing database in a request for its tables and a missing
 * catalog as {@code FAILED_PRECONDITION}.
 */
public final class LakehouseErrorTranslator
{
    private LakehouseErrorTranslator() {}

    public static boolean isNotFound(ApiException exception)
    {
        return exception.getStatusCode().getCode() == Code.NOT_FOUND;
    }

    /**
     * Returns whether Lakehouse reports that the database or catalog of a request does not exist.
     * Only the caller knows which of the two the request addressed.
     */
    public static boolean isMissingParent(ApiException exception)
    {
        return exception.getStatusCode().getCode() == Code.FAILED_PRECONDITION;
    }

    /**
     * Translates a failed Lakehouse call into the Trino error for {@code operation}.
     */
    public static TrinoException translate(String operation, ApiException exception)
    {
        return switch (exception.getStatusCode().getCode()) {
            case PERMISSION_DENIED, UNAUTHENTICATED -> new TrinoException(
                    PERMISSION_DENIED, message(operation, "Lakehouse denied the request", exception), exception);
            case ALREADY_EXISTS -> new TrinoException(
                    ALREADY_EXISTS, message(operation, "the object already exists", exception), exception);
            case INVALID_ARGUMENT, OUT_OF_RANGE -> new TrinoException(
                    INVALID_ARGUMENTS, message(operation, "Lakehouse rejected the request", exception), exception);
            default -> new TrinoException(
                    GENERIC_INTERNAL_ERROR, message(operation, "Lakehouse request failed", exception), exception);
        };
    }

    /**
     * Returns the error for every write and DDL operation that the read path excludes.
     */
    public static TrinoException notSupported(String operation)
    {
        return new TrinoException(
                NOT_SUPPORTED, "This Lakehouse runtime catalog supports read operations only, not " + operation);
    }

    /**
     * Appends the reason Lakehouse gave, which is the message of the status that the client
     * library wraps.
     */
    private static String message(String operation, String detail, ApiException exception)
    {
        String reason = exception.getCause() == null ? exception.getMessage() : exception.getCause().getMessage();
        return operation + ": " + detail + (reason == null || reason.isBlank() ? "" : ": " + reason);
    }
}
