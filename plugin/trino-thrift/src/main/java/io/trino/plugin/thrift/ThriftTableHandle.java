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
package io.trino.plugin.thrift;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public record ThriftTableHandle(String schemaName, String tableName, TupleDomain<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
        implements ConnectorTableHandle
{
    public ThriftTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(constraint, "constraint is null");
        requireNonNull(desiredColumns, "desiredColumns is null");
    }

    public static ThriftTableHandle toThriftTableHandle(SchemaTableName schemaTableName)
    {
        return new ThriftTableHandle(schemaTableName.getSchemaName(), schemaTableName.getTableName(), TupleDomain.all(), Optional.empty());
    }

    @Override
    public String toString()
    {
        return schemaName + ":" + tableName;
    }
}
