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
package io.trino.execution;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class EnforcedFilterColumnsConfig
{
    private List<String> enforcedFilterColumns = ImmutableList.of();
    private Map<TableName, Set<String>> enforcedFilterColumnsByTable = ImmutableMap.of();

    @NotNull
    public List<String> getEnforcedFilterColumns()
    {
        return enforcedFilterColumns;
    }

    @Config("query.enforced-filter-columns")
    @ConfigDescription("Comma-separated list of catalog.schema.table.column entries that must be referenced in the WHERE clause")
    public EnforcedFilterColumnsConfig setEnforcedFilterColumns(List<String> enforcedFilterColumns)
    {
        requireNonNull(enforcedFilterColumns, "enforcedFilterColumns is null");
        this.enforcedFilterColumns = enforcedFilterColumns.stream()
                .map(String::trim)
                .filter(enforcedColumn -> !enforcedColumn.isEmpty())
                .collect(toImmutableList());
        this.enforcedFilterColumnsByTable = parseEnforcedFilterColumns(this.enforcedFilterColumns);
        return this;
    }

    public Set<String> getEnforcedFilterColumnsForTable(String catalogName, String schemaName, String tableName)
    {
        return enforcedFilterColumnsByTable.getOrDefault(new TableName(catalogName, schemaName, tableName), ImmutableSet.of());
    }

    private static Map<TableName, Set<String>> parseEnforcedFilterColumns(List<String> enforcedFilterColumns)
    {
        Map<TableName, Set<String>> columnsByTable = new LinkedHashMap<>();
        for (String enforcedColumn : enforcedFilterColumns) {
            List<String> parts = Splitter.on('.').splitToList(enforcedColumn);
            if (parts.size() != 4) {
                throw new IllegalArgumentException(format(
                        "Invalid enforced column '%s': expected catalog.schema.table.column",
                        enforcedColumn));
            }

            String catalogName = parts.get(0).trim();
            String schemaName = parts.get(1).trim();
            String tableName = parts.get(2).trim();
            String columnName = parts.get(3).trim();
            if (catalogName.isEmpty() || schemaName.isEmpty() || tableName.isEmpty() || columnName.isEmpty()) {
                throw new IllegalArgumentException(format(
                        "Invalid enforced column '%s': expected catalog.schema.table.column",
                        enforcedColumn));
            }

            columnsByTable.computeIfAbsent(new TableName(catalogName, schemaName, tableName), _ -> new LinkedHashSet<>())
                    .add(columnName);
        }

        ImmutableMap.Builder<TableName, Set<String>> result = ImmutableMap.builder();
        columnsByTable.forEach((tableName, columns) -> result.put(tableName, ImmutableSet.copyOf(columns)));
        return result.buildOrThrow();
    }

    private record TableName(String catalogName, String schemaName, String tableName)
    {
        private TableName
        {
            catalogName = catalogName.toLowerCase(ENGLISH);
            schemaName = schemaName.toLowerCase(ENGLISH);
            tableName = tableName.toLowerCase(ENGLISH);
        }
    }
}
