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
package io.trino.plugin.dynamodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.dynamodb.DynamoDbService.DEFAULT_SCHEMA;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class DynamoDbMetadata
        implements ConnectorMetadata
{
    private final DynamoDbService service;

    @Inject
    public DynamoDbMetadata(DynamoDbService service)
    {
        this.service = requireNonNull(service, "service is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(DEFAULT_SCHEMA);
    }

    @Override
    public DynamoDbTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        if (!DEFAULT_SCHEMA.equals(tableName.getSchemaName())) {
            return null;
        }
        if (!service.listTableNames().contains(tableName.getTableName())) {
            return null;
        }
        return new DynamoDbTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        DynamoDbTableHandle tableHandle = (DynamoDbTableHandle) table;
        return buildTableMetadata(tableHandle.toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent() && !DEFAULT_SCHEMA.equals(schemaName.get())) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        for (String tableName : service.listTableNames()) {
            tables.add(new SchemaTableName(DEFAULT_SCHEMA, tableName));
        }
        return tables.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DynamoDbTableHandle dynamoDbTableHandle = (DynamoDbTableHandle) tableHandle;
        List<DynamoDbColumnHandle> columns = service.getTableColumns(dynamoDbTableHandle.getTableName());
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (DynamoDbColumnHandle column : columns) {
            columnHandles.put(column.getColumnName(), column);
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = buildTableMetadata(tableName);
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((DynamoDbColumnHandle) columnHandle).getColumnMetadata();
    }

    private ConnectorTableMetadata buildTableMetadata(SchemaTableName tableName)
    {
        if (!DEFAULT_SCHEMA.equals(tableName.getSchemaName())) {
            return null;
        }
        List<DynamoDbColumnHandle> columns;
        try {
            columns = service.getTableColumns(tableName.getTableName());
        }
        catch (ResourceNotFoundException e) {
            throw new TableNotFoundException(tableName);
        }
        ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
        for (DynamoDbColumnHandle column : columns) {
            columnMetadata.add(column.getColumnMetadata());
        }
        return new ConnectorTableMetadata(tableName, columnMetadata.build());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }
}
