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
package io.trino.loki;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.spi.connector.*;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.type.VarcharType;

import java.util.*;
import java.util.function.UnaryOperator;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.connector.RelationColumnsMetadata.forTable;
import static java.util.Objects.requireNonNull;

public class LokiMetadata implements ConnectorMetadata {
    private final LokiClient lokiClient;

    @Inject
    public LokiMetadata(LokiClient lokiClient)
    {
        this.lokiClient = requireNonNull(lokiClient, "lokiClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    private static List<String> listSchemaNames()
    {
        return ImmutableList.copyOf(ImmutableSet.of("default"));
    }


    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(ImmutableSet.of("default")));

        return schemaNames.stream()
                .flatMap(schemaName ->
                        lokiClient.getTableNames(schemaName).stream().map(tableName -> new SchemaTableName(schemaName, tableName)))
                .collect(toImmutableList());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Map<SchemaTableName, RelationColumnsMetadata> relationColumns = new HashMap<>();

        SchemaTablePrefix prefix = schemaName.map(SchemaTablePrefix::new)
                .orElseGet(SchemaTablePrefix::new);
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                relationColumns.put(tableName, forTable(tableName, tableMetadata.getColumns()));
            }
        }

        return relationFilter.apply(relationColumns.keySet()).stream()
                .map(relationColumns::get)
                .iterator();
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (!(handle instanceof LokiTableFunction.QueryHandle queryHandle)) {
            return Optional.empty();
        }

        LokiTableHandle tableHandle = queryHandle.getTableHandle();
        LokiColumnHandle columnHandle = new LokiColumnHandle("a_xxx", VarcharType.VARCHAR, 0);
        List<ColumnHandle> columnHandles = new ArrayList<>();
        columnHandles.add(columnHandle);
        return Optional.of(new TableFunctionApplicationResult<>(tableHandle, columnHandles));
    }

    // TODO this always returns null
    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

        return null;

    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table){
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("a_xxx", VarcharType.VARCHAR));

        return new ConnectorTableMetadata(new SchemaTableName("default", "test"), columns);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return new ColumnMetadata("a_xxx", VarcharType.VARCHAR);
    }
}
