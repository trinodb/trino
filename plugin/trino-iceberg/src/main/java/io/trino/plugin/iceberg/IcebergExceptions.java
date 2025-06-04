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
package io.trino.plugin.iceberg;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import org.apache.iceberg.exceptions.ValidationException;

import java.io.FileNotFoundException;

import static com.google.common.base.Throwables.getCausalChain;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_MISSING_METADATA;

public final class IcebergExceptions
{
    private IcebergExceptions() {}

    private static boolean isNotFoundException(Throwable failure)
    {
        return getCausalChain(failure).stream().anyMatch(e ->
                e instanceof org.apache.iceberg.exceptions.NotFoundException
                        || e instanceof FileNotFoundException);
    }

    public static boolean isFatalException(Throwable failure)
    {
        return isNotFoundException(failure) || failure instanceof ValidationException;
    }

    public static RuntimeException translateMetadataException(Throwable failure, String tableName)
    {
        if (failure instanceof TrinoException trinoException) {
            return trinoException;
        }
        if (isNotFoundException(failure)) {
            throw new TrinoException(ICEBERG_MISSING_METADATA, "Metadata not found in metadata location for table " + tableName, failure);
        }
        if (failure instanceof ValidationException) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Invalid metadata file for table " + tableName, failure);
        }

        return new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Error processing metadata for table " + tableName, failure);
    }
}
