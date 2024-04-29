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
package io.trino.plugin.lance;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.lance.internal.LanceClient;
import io.trino.plugin.lance.internal.LanceDynamicTable;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;


public class LanceMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";
    private final LanceClient lanceClient;
    private final LanceConfig lanceConfig;

    @Inject
    public LanceMetadata(
            LanceClient lanceClient,
            LanceConfig lanceConfig)
    {
        this.lanceClient = requireNonNull(lanceClient, "lanceClient is null");
        Function<String, String> identifierQuote = identity();
        this.lanceConfig = lanceConfig;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public LanceTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        // use table name defined with string to describe a sub query plan
        if (tableName.getTableName().trim().contains("select ")) {
            // TODO: create a dynamic table from Lance query result
            // DynamicTable dynamicTable = null;
            // return new LanceTableHandle(dynamicTable);
            throw new UnsupportedOperationException("Unsupported");
        }
        return new LanceTableHandle(tableName.getSchemaName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) table;
        if (lanceTableHandle.getDynamicTable().isPresent()) {
            LanceDynamicTable dynamicTable = lanceTableHandle.getDynamicTable().get();
            ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();
            SchemaTableName schemaTableName = new SchemaTableName(lanceTableHandle.getSchemaName(), dynamicTable.tableName());
            return new ConnectorTableMetadata(schemaTableName, columnMetadataBuilder.build());
        }
        SchemaTableName tableName = new SchemaTableName(lanceTableHandle.getSchemaName(), lanceTableHandle.getTableName());

        return getTableMetadata(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaNameOrNull)
    {
        return lanceClient.listTables(session, schemaNameOrNull.isPresent() ? schemaNameOrNull.get() : SCHEMA_NAME);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;
        // TODO: support dynamic table with projection clause
        return lanceClient.getColumnHandlers(lanceTableHandle.getTableName());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : lanceClient.listTables(session, prefix.toString())) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return lanceClient.getColumnMetadata(columnHandle);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        // TODO: support limit
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        // TODO: support limit
        throw new UnsupportedOperationException("unsupported");
    }

    @VisibleForTesting
    public List<ColumnMetadata> getColumnsMetadata(String tableName)
    {
        Map<String, ColumnHandle> columnHandlers = lanceClient.getColumnHandlers(tableName);
        return columnHandlers.values().stream()
                .map(lanceClient::getColumnMetadata)
                .collect(toImmutableList());
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        return new ConnectorTableMetadata(tableName, getColumnsMetadata(tableName.getTableName()));
    }
}
