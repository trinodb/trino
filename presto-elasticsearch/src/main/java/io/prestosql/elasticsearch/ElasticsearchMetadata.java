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
package io.prestosql.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayout;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutResult;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ElasticsearchMetadata
        implements ConnectorMetadata
{
    private final ElasticsearchClient client;

    @Inject
    public ElasticsearchMetadata(ElasticsearchClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return client.listSchemas();
    }

    @Override
    public ElasticsearchTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        ElasticsearchTableDescription table = client.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ElasticsearchTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new ElasticsearchTableLayoutHandle(handle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) table;
        SchemaTableName tableName = new SchemaTableName(handle.getSchemaName(), handle.getTableName());
        return getTableMetadata(tableName).get();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return client.listTables(schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ElasticsearchTableHandle handle = (ElasticsearchTableHandle) tableHandle;

        ElasticsearchTableDescription table = client.getTable(handle.getSchemaName(), handle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : client.getColumnMetadata(table)) {
            Map<String, Object> properties = column.getProperties();
            int ordinalPosition = (Integer) properties.get("ordinalPosition");
            int position = ordinalPosition == -1 ? index : ordinalPosition;
            columnHandles.put(column.getName(),
                    new ElasticsearchColumnHandle(
                        column.getName(),
                        column.getType(),
                        String.valueOf(properties.get("jsonPath")),
                        String.valueOf(properties.get("jsonType")),
                        position,
                        (Boolean) properties.get("isList")));
            index++;
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((ElasticsearchColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            Optional<ConnectorTableMetadata> tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata.isPresent()) {
                columns.put(tableName, tableMetadata.get().getColumns());
            }
        }
        return columns.build();
    }

    private Optional<ConnectorTableMetadata> getTableMetadata(SchemaTableName tableName)
    {
        ElasticsearchTableDescription table = client.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return Optional.empty();
        }
        return Optional.of(new ConnectorTableMetadata(tableName, client.getColumnMetadata(table)));
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return prefix.toOptionalSchemaTableName()
                .map(schemaTableName -> (List<SchemaTableName>) ImmutableList.of(schemaTableName))
                .orElseGet(() -> listTables(session, prefix.getSchema()));
    }
}
