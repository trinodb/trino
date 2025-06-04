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
package io.trino.plugin.pinot;

import io.trino.plugin.pinot.query.DynamicTable;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public record PinotTableHandle(
        String schemaName,
        String tableName,
        boolean enableNullHandling,
        TupleDomain<ColumnHandle> constraint,
        OptionalLong limit,
        Optional<DynamicTable> query)
        implements ConnectorTableHandle
{
    public PinotTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(constraint, "constraint is null");
        requireNonNull(limit, "limit is null");
        requireNonNull(query, "query is null");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PinotTableHandle that = (PinotTableHandle) o;
        return Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName);
    }
}
