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

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

public record CorruptedIcebergTableHandle(SchemaTableName schemaTableName, TrinoException originalException)
        implements ConnectorTableHandle
{
    public CorruptedIcebergTableHandle
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(originalException, "originalException is null");
    }

    public TrinoException createException()
    {
        // Original exception originates from a different place. Create a new exception not to confuse reader with a stacktrace not matching call site.
        return new TrinoException(originalException.getErrorCode(), originalException.getMessage(), originalException);
    }
}
