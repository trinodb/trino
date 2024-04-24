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
package io.trino.plugin.opa.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.CatalogSchemaTableName;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static java.util.Objects.requireNonNull;

@JsonInclude(NON_NULL)
public record TrinoTable(
        String catalogName,
        String schemaName,
        String tableName,
        Set<String> columns,
        Map<String, Optional<Object>> properties)
{
    public TrinoTable
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        if (columns != null) {
            columns = ImmutableSet.copyOf(columns);
        }
        if (properties != null) {
            properties = ImmutableMap.copyOf(properties);
        }
    }

    public TrinoTable(CatalogSchemaTableName table)
    {
        this(table.getCatalogName(), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName());
    }

    public TrinoTable(String catalogName, String schemaName, String tableName)
    {
        this(catalogName, schemaName, tableName, null, null);
    }

    public TrinoTable withColumns(Set<String> newColumns)
    {
        return new TrinoTable(catalogName, schemaName, tableName, newColumns, properties);
    }

    public TrinoTable withProperties(Map<String, Optional<Object>> newProperties)
    {
        return new TrinoTable(catalogName, schemaName, tableName, columns, newProperties);
    }
}
