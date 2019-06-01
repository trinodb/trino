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
package io.prestosql.plugin.atop;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.atop.AtopTable.AtopColumn;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ColumnNotFoundException;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.prestosql.plugin.atop.AtopTable.AtopColumn.END_TIME;
import static io.prestosql.plugin.atop.AtopTable.AtopColumn.START_TIME;
import static java.util.Objects.requireNonNull;

public class AtopMetadata
        implements ConnectorMetadata
{
    private static final AtopColumnHandle START_TIME_HANDLE = new AtopColumnHandle(START_TIME.getName());
    private static final AtopColumnHandle END_TIME_HANDLE = new AtopColumnHandle(END_TIME.getName());
    private final TypeManager typeManager;
    private final String environment;

    @Inject
    public AtopMetadata(TypeManager typeManager, Environment environment)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.environment = requireNonNull(environment, "environment is null").toString();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of("default", environment);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");

        String schemaName = tableName.getSchemaName();
        if (!listSchemaNames(session).contains(schemaName)) {
            return null;
        }
        try {
            AtopTable table = AtopTable.valueOf(tableName.getTableName().toUpperCase());
            return new AtopTableHandle(schemaName, table);
        }
        catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        AtopTableHandle atopTableHandle = (AtopTableHandle) tableHandle;
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (AtopColumn column : atopTableHandle.getTable().getColumns()) {
            columns.add(new ColumnMetadata(column.getName(), typeManager.getType(column.getType())));
        }
        SchemaTableName schemaTableName = new SchemaTableName(atopTableHandle.getSchema(), atopTableHandle.getTable().getName());
        return new ConnectorTableMetadata(schemaTableName, columns.build());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (!schemaName.isPresent()) {
            return Stream.of(AtopTable.values())
                    .map(table -> new SchemaTableName(environment, table.getName()))
                    .collect(Collectors.toList());
        }
        if (!listSchemaNames(session).contains(schemaName.get())) {
            return ImmutableList.of();
        }
        return Stream.of(AtopTable.values())
                .map(table -> new SchemaTableName(schemaName.get(), table.getName()))
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        AtopTableHandle atopTableHandle = (AtopTableHandle) tableHandle;
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (AtopColumn column : atopTableHandle.getTable().getColumns()) {
            columnHandles.put(column.getName(), new AtopColumnHandle(column.getName()));
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix.getSchema())) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, getTableHandle(session, tableName));
            columns.put(tableName, tableMetadata.getColumns());
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        String columnName = ((AtopColumnHandle) columnHandle).getName();

        for (ColumnMetadata column : getTableMetadata(session, tableHandle).getColumns()) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }

        AtopTableHandle atopTableHandle = (AtopTableHandle) tableHandle;
        SchemaTableName tableName = new SchemaTableName(atopTableHandle.getSchema(), atopTableHandle.getTable().getName());
        throw new ColumnNotFoundException(tableName, columnName);
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        AtopTableHandle handle = (AtopTableHandle) table;

        Optional<Map<ColumnHandle, Domain>> domains = constraint.getSummary().getDomains();

        Domain oldEndTimeDomain = handle.getEndTimeConstraint();
        Domain oldStartTimeDomain = handle.getStartTimeConstraint();
        Domain newEndTimeDomain = oldEndTimeDomain;
        Domain newStartTimeDomain = oldStartTimeDomain;

        if (domains.isPresent()) {
            if (domains.get().containsKey(START_TIME_HANDLE)) {
                newStartTimeDomain = domains.get().get(START_TIME_HANDLE).intersect(oldStartTimeDomain);
            }
            if (domains.get().containsKey(END_TIME_HANDLE)) {
                newEndTimeDomain = domains.get().get(END_TIME_HANDLE).intersect(oldEndTimeDomain);
            }
        }

        if (oldEndTimeDomain.equals(newEndTimeDomain) && oldStartTimeDomain.equals(newStartTimeDomain)) {
            return Optional.empty();
        }

        handle = new AtopTableHandle(
                handle.getSchema(),
                handle.getTable(),
                newStartTimeDomain,
                newEndTimeDomain);

        return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary()));
    }
}
