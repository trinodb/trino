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
package io.trino.plugin.localfile;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public record LocalFileTableHandle(
        SchemaTableName schemaTableName,
        OptionalInt timestampColumn,
        OptionalInt serverAddressColumn,
        TupleDomain<ColumnHandle> constraint)
        implements ConnectorTableHandle
{
    public LocalFileTableHandle(SchemaTableName schemaTableName, OptionalInt timestampColumn, OptionalInt serverAddressColumn)
    {
        this(schemaTableName, timestampColumn, serverAddressColumn, TupleDomain.all());
    }

    public LocalFileTableHandle
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(timestampColumn, "timestampColumn is null");
        requireNonNull(serverAddressColumn, "serverAddressColumn is null");
        requireNonNull(constraint, "constraint is null");
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaTableName", schemaTableName)
                .toString();
    }
}
