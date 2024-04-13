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
package io.trino.plugin.memory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public record TableInfo(
        long id,
        String schemaName,
        String tableName,
        List<ColumnInfo> columns,
        Map<HostAddress, MemoryDataFragment> dataFragments,
        Optional<String> comment)
{
    public TableInfo
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        columns = ImmutableList.copyOf(columns);
        dataFragments = ImmutableMap.copyOf(dataFragments);
        requireNonNull(comment, "comment is null");
    }

    @JsonIgnore
    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @JsonIgnore
    public ConnectorTableMetadata getMetadata()
    {
        return new ConnectorTableMetadata(
                new SchemaTableName(schemaName, tableName),
                columns.stream()
                        .map(ColumnInfo::getMetadata)
                        .collect(Collectors.toList()),
                emptyMap(),
                comment);
    }

    @JsonIgnore
    public ColumnInfo getColumn(ColumnHandle handle)
    {
        return columns.stream()
                .filter(column -> column.handle().equals(handle))
                .collect(onlyElement());
    }
}
