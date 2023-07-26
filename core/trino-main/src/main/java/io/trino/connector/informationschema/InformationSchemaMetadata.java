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
package io.trino.connector.informationschema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.connector.informationschema.InformationSchemaTable.COLUMNS;
import static io.trino.connector.informationschema.InformationSchemaTable.INFORMATION_SCHEMA;
import static io.trino.connector.informationschema.InformationSchemaTable.TABLES;
import static io.trino.connector.informationschema.InformationSchemaTable.TABLE_PRIVILEGES;
import static io.trino.connector.informationschema.InformationSchemaTable.VIEWS;
import static io.trino.connector.informationschema.SystemTableFilter.defaultPrefixes;
import static io.trino.metadata.MetadataUtil.findColumnMetadata;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class InformationSchemaMetadata
        implements ConnectorMetadata
{
    private static final InformationSchemaColumnHandle CATALOG_COLUMN_HANDLE = new InformationSchemaColumnHandle("table_catalog");
    private static final InformationSchemaColumnHandle SCHEMA_COLUMN_HANDLE = new InformationSchemaColumnHandle("table_schema");
    private static final InformationSchemaColumnHandle TABLE_NAME_COLUMN_HANDLE = new InformationSchemaColumnHandle("table_name");
    private static final InformationSchemaColumnHandle ROLE_NAME_COLUMN_HANDLE = new InformationSchemaColumnHandle("role_name");
    private static final InformationSchemaColumnHandle GRANTEE_COLUMN_HANDLE = new InformationSchemaColumnHandle("grantee");

    private final String catalogName;
    private final Metadata metadata;
    private final int maxPrefetchedInformationSchemaPrefixes;
    private final AccessControl accessControl;

    public InformationSchemaMetadata(String catalogName, Metadata metadata, AccessControl accessControl, int maxPrefetchedInformationSchemaPrefixes)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.maxPrefetchedInformationSchemaPrefixes = maxPrefetchedInformationSchemaPrefixes;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(INFORMATION_SCHEMA);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName tableName)
    {
        return InformationSchemaTable.of(tableName)
                .map(table -> new InformationSchemaTableHandle(catalogName, table, defaultPrefixes(catalogName), OptionalLong.empty()))
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = (InformationSchemaTableHandle) tableHandle;
        return informationSchemaTableHandle.getTable().getTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent() && !schemaName.get().equals(INFORMATION_SCHEMA)) {
            return ImmutableList.of();
        }
        return Arrays.stream(InformationSchemaTable.values())
                .map(InformationSchemaTable::getSchemaTableName)
                .collect(toImmutableList());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = (InformationSchemaTableHandle) tableHandle;
        ConnectorTableMetadata tableMetadata = informationSchemaTableHandle.getTable().getTableMetadata();

        String columnName = ((InformationSchemaColumnHandle) columnHandle).getColumnName();

        ColumnMetadata columnMetadata = findColumnMetadata(tableMetadata, columnName);
        checkArgument(columnMetadata != null, "Column '%s' on table '%s' does not exist", columnName, tableMetadata.getTable());
        return columnMetadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = (InformationSchemaTableHandle) tableHandle;

        ConnectorTableMetadata tableMetadata = informationSchemaTableHandle.getTable().getTableMetadata();

        return tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableMap(identity(), InformationSchemaColumnHandle::new));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        return Arrays.stream(InformationSchemaTable.values())
                .filter(table -> prefix.matches(table.getSchemaTableName()))
                .collect(toImmutableMap(InformationSchemaTable::getSchemaTableName, table -> table.getTableMetadata().getColumns()));
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        InformationSchemaTableHandle tableHandle = (InformationSchemaTableHandle) table;
        return new ConnectorTableProperties(
                tableHandle.getPrefixes().isEmpty() ? TupleDomain.none() : TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                emptyList());
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        InformationSchemaTableHandle table = (InformationSchemaTableHandle) handle;

        if (table.getLimit().isPresent() && table.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        return Optional.of(new LimitApplicationResult<>(
                new InformationSchemaTableHandle(table.getCatalogName(), table.getTable(), table.getPrefixes(), OptionalLong.of(limit)),
                true,
                false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        InformationSchemaTableHandle table = (InformationSchemaTableHandle) handle;

        Set<QualifiedTablePrefix> prefixes = table.getPrefixes();
        if (isTablesEnumeratingTable(table.getTable()) && table.getPrefixes().equals(defaultPrefixes(catalogName))) {
            SystemTableFilter<ColumnHandle> filter = new SystemTableFilter<>(
                    catalogName,
                    metadata,
                    accessControl,
                    CATALOG_COLUMN_HANDLE,
                    SCHEMA_COLUMN_HANDLE,
                    TABLE_NAME_COLUMN_HANDLE,
                    isColumnsEnumeratingTable(table.getTable()),
                    maxPrefetchedInformationSchemaPrefixes);

            prefixes = filter.getPrefixes(session, constraint.getSummary(), constraint.predicate());
        }

        if (prefixes.equals(table.getPrefixes())) {
            return Optional.empty();
        }

        table = new InformationSchemaTableHandle(table.getCatalogName(), table.getTable(), prefixes, table.getLimit());
        return Optional.of(new ConstraintApplicationResult<>(table, constraint.getSummary(), false));
    }

    public static boolean isTablesEnumeratingTable(InformationSchemaTable table)
    {
        return ImmutableSet.of(COLUMNS, VIEWS, TABLES, TABLE_PRIVILEGES).contains(table);
    }

    private boolean isColumnsEnumeratingTable(InformationSchemaTable table)
    {
        return COLUMNS == table;
    }

    private Map<ColumnHandle, NullableValue> roleAsFixedValues(String schema)
    {
        return ImmutableMap.of(ROLE_NAME_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(schema)));
    }

    private Map<ColumnHandle, NullableValue> granteeAsFixedValues(String schema)
    {
        return ImmutableMap.of(GRANTEE_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(schema)));
    }
}
