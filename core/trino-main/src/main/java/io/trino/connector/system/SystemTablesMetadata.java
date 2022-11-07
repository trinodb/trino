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
package io.trino.connector.system;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.trino.connector.system.jdbc.JdbcTable;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.connector.system.SystemColumnHandle.toSystemColumnHandles;
import static io.trino.metadata.MetadataUtil.findColumnMetadata;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;

public class SystemTablesMetadata
        implements ConnectorMetadata
{
    private final SystemTablesProvider tables;

    public SystemTablesMetadata(SystemTablesProvider tables)
    {
        this.tables = requireNonNull(tables, "tables is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return tables.listSystemTables(session).stream()
                .map(table -> table.getTableMetadata().getTable().getSchemaName())
                .distinct()
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<SystemTable> table = tables.getSystemTable(session, tableName);
        if (table.isEmpty()) {
            return null;
        }
        return SystemTableHandle.fromSchemaTableName(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return checkAndGetTable(session, tableHandle).getTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return tables.listSystemTables(session).stream()
                .map(SystemTable::getTableMetadata)
                .map(ConnectorTableMetadata::getTable)
                .filter(table -> schemaName.isEmpty() || table.getSchemaName().equals(schemaName.get()))
                .collect(toImmutableList());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ConnectorTableMetadata tableMetadata = checkAndGetTable(session, tableHandle).getTableMetadata();

        String columnName = ((SystemColumnHandle) columnHandle).getColumnName();

        ColumnMetadata columnMetadata = findColumnMetadata(tableMetadata, columnName);
        checkArgument(columnMetadata != null, "Column '%s' on table '%s' does not exist", columnName, tableMetadata.getTable());
        return columnMetadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ConnectorTableMetadata tableMetadata = checkAndGetTable(session, tableHandle).getTableMetadata();
        return toSystemColumnHandles(tableMetadata);
    }

    private SystemTable checkAndGetTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SystemTableHandle systemTableHandle = (SystemTableHandle) tableHandle;
        return tables.getSystemTable(session, systemTableHandle.getSchemaTableName())
                // table might disappear in the meantime
                .orElseThrow(() -> new TrinoException(NOT_FOUND, format("Table '%s' not found", systemTableHandle.getSchemaTableName())));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        if (prefix.getTable().isPresent()) {
            // if table is concrete we just use tables.getSystemTable to support tables which are not listable
            SchemaTableName tableName = prefix.toSchemaTableName();
            return tables.getSystemTable(session, tableName)
                    .map(systemTable -> singletonMap(tableName, systemTable.getTableMetadata().getColumns()))
                    .orElseGet(ImmutableMap::of);
        }

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        for (SystemTable table : tables.listSystemTables(session)) {
            ConnectorTableMetadata tableMetadata = table.getTableMetadata();
            if (prefix.matches(tableMetadata.getTable())) {
                builder.put(tableMetadata.getTable(), tableMetadata.getColumns());
            }
        }
        return builder.buildOrThrow();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        SystemTableHandle table = (SystemTableHandle) handle;

        TupleDomain<ColumnHandle> oldDomain = table.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain) && constraint.predicate().isEmpty()) {
            return Optional.empty();
        }

        SystemTable systemTable = checkAndGetTable(session, table);
        if (systemTable instanceof JdbcTable) {
            TupleDomain<ColumnHandle> filtered = ((JdbcTable) systemTable).applyFilter(session, effectiveConstraint(oldDomain, constraint, newDomain));
            newDomain = newDomain.intersect(filtered);
        }

        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        if (newDomain.isNone()) {
            // TODO (https://github.com/trinodb/trino/issues/3647) indicate the table scan is empty
        }
        table = new SystemTableHandle(table.getSchemaName(), table.getTableName(), newDomain);
        return Optional.of(new ConstraintApplicationResult<>(table, constraint.getSummary(), false));
    }

    private Constraint effectiveConstraint(TupleDomain<ColumnHandle> oldDomain, Constraint newConstraint, TupleDomain<ColumnHandle> effectiveDomain)
    {
        if (effectiveDomain.isNone() || newConstraint.predicate().isEmpty()) {
            return new Constraint(effectiveDomain);
        }
        return new Constraint(
                effectiveDomain,
                oldDomain.asPredicate().and(newConstraint.predicate().get()),
                Sets.union(
                        oldDomain.getDomains().orElseThrow().keySet(),
                        newConstraint.getPredicateColumns().orElseThrow()));
    }
}
