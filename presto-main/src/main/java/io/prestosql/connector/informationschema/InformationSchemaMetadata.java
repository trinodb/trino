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
package io.prestosql.connector.informationschema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.slice.Slice;
import io.prestosql.FullConnectorSession;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayout;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.EquatableValueSet;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.compose;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.metadata.MetadataUtil.SchemaMetadataBuilder.schemaMetadataBuilder;
import static io.prestosql.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.prestosql.metadata.MetadataUtil.findColumnMetadata;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class InformationSchemaMetadata
        implements ConnectorMetadata
{
    public static final String INFORMATION_SCHEMA = "information_schema";

    public static final SchemaTableName TABLE_COLUMNS = new SchemaTableName(INFORMATION_SCHEMA, "columns");
    public static final SchemaTableName TABLE_TABLES = new SchemaTableName(INFORMATION_SCHEMA, "tables");
    public static final SchemaTableName TABLE_VIEWS = new SchemaTableName(INFORMATION_SCHEMA, "views");
    public static final SchemaTableName TABLE_SCHEMATA = new SchemaTableName(INFORMATION_SCHEMA, "schemata");
    public static final SchemaTableName TABLE_TABLE_PRIVILEGES = new SchemaTableName(INFORMATION_SCHEMA, "table_privileges");
    public static final SchemaTableName TABLE_ROLES = new SchemaTableName(INFORMATION_SCHEMA, "roles");
    public static final SchemaTableName TABLE_APPLICABLE_ROLES = new SchemaTableName(INFORMATION_SCHEMA, "applicable_roles");
    public static final SchemaTableName TABLE_ENABLED_ROLES = new SchemaTableName(INFORMATION_SCHEMA, "enabled_roles");

    public static final Map<SchemaTableName, ConnectorTableMetadata> TABLES = schemaMetadataBuilder()
            .table(tableMetadataBuilder(TABLE_COLUMNS)
                    .column("table_catalog", createUnboundedVarcharType())
                    .column("table_schema", createUnboundedVarcharType())
                    .column("table_name", createUnboundedVarcharType())
                    .column("column_name", createUnboundedVarcharType())
                    .column("ordinal_position", BIGINT)
                    .column("column_default", createUnboundedVarcharType())
                    .column("is_nullable", createUnboundedVarcharType())
                    .column("data_type", createUnboundedVarcharType())
                    .column("comment", createUnboundedVarcharType())
                    .column("extra_info", createUnboundedVarcharType())
                    .hiddenColumn("column_comment", createUnboundedVarcharType()) // MySQL compatible
                    .build())
            .table(tableMetadataBuilder(TABLE_TABLES)
                    .column("table_catalog", createUnboundedVarcharType())
                    .column("table_schema", createUnboundedVarcharType())
                    .column("table_name", createUnboundedVarcharType())
                    .column("table_type", createUnboundedVarcharType())
                    .hiddenColumn("table_comment", createUnboundedVarcharType()) // MySQL compatible
                    .build())
            .table(tableMetadataBuilder(TABLE_VIEWS)
                    .column("table_catalog", createUnboundedVarcharType())
                    .column("table_schema", createUnboundedVarcharType())
                    .column("table_name", createUnboundedVarcharType())
                    .column("view_definition", createUnboundedVarcharType())
                    .build())
            .table(tableMetadataBuilder(TABLE_SCHEMATA)
                    .column("catalog_name", createUnboundedVarcharType())
                    .column("schema_name", createUnboundedVarcharType())
                    .build())
            .table(tableMetadataBuilder(TABLE_TABLE_PRIVILEGES)
                    .column("grantor", createUnboundedVarcharType())
                    .column("grantor_type", createUnboundedVarcharType())
                    .column("grantee", createUnboundedVarcharType())
                    .column("grantee_type", createUnboundedVarcharType())
                    .column("table_catalog", createUnboundedVarcharType())
                    .column("table_schema", createUnboundedVarcharType())
                    .column("table_name", createUnboundedVarcharType())
                    .column("privilege_type", createUnboundedVarcharType())
                    .column("is_grantable", createUnboundedVarcharType())
                    .column("with_hierarchy", createUnboundedVarcharType())
                    .build())
            .table(tableMetadataBuilder(TABLE_ROLES)
                    .column("role_name", createUnboundedVarcharType())
                    .build())
            .table(tableMetadataBuilder(TABLE_APPLICABLE_ROLES)
                    .column("grantee", createUnboundedVarcharType())
                    .column("grantee_type", createUnboundedVarcharType())
                    .column("role_name", createUnboundedVarcharType())
                    .column("is_grantable", createUnboundedVarcharType())
                    .build())
            .table(tableMetadataBuilder(TABLE_ENABLED_ROLES)
                    .column("role_name", createUnboundedVarcharType())
                    .build())
            .build();

    private static final InformationSchemaColumnHandle SCHEMA_COLUMN_HANDLE = new InformationSchemaColumnHandle("table_schema");
    private static final InformationSchemaColumnHandle TABLE_NAME_COLUMN_HANDLE = new InformationSchemaColumnHandle("table_name");

    private final String catalogName;
    private final Metadata metadata;

    public InformationSchemaMetadata(String catalogName, Metadata metadata)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    private InformationSchemaTableHandle checkTableHandle(ConnectorTableHandle tableHandle)
    {
        InformationSchemaTableHandle handle = (InformationSchemaTableHandle) tableHandle;
        checkArgument(handle.getCatalogName().equals(catalogName), "invalid table handle: expected catalog %s but got %s", catalogName, handle.getCatalogName());
        checkArgument(TABLES.containsKey(handle.getSchemaTableName()), "table %s does not exist", handle.getSchemaTableName());
        return handle;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(INFORMATION_SCHEMA);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName tableName)
    {
        if (!TABLES.containsKey(tableName)) {
            return null;
        }
        return new InformationSchemaTableHandle(catalogName, tableName.getSchemaName(), tableName.getTableName(), Optional.empty(), Optional.empty());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = checkTableHandle(tableHandle);
        return TABLES.get(informationSchemaTableHandle.getSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (!schemaName.isPresent()) {
            return ImmutableList.copyOf(TABLES.keySet());
        }

        return TABLES.keySet().stream()
                .filter(compose(schemaName.get()::equals, SchemaTableName::getSchemaName))
                .collect(toImmutableList());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = checkTableHandle(tableHandle);
        ConnectorTableMetadata tableMetadata = TABLES.get(informationSchemaTableHandle.getSchemaTableName());

        String columnName = ((InformationSchemaColumnHandle) columnHandle).getColumnName();

        ColumnMetadata columnMetadata = findColumnMetadata(tableMetadata, columnName);
        checkArgument(columnMetadata != null, "Column %s on table %s does not exist", columnName, tableMetadata.getTable());
        return columnMetadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = checkTableHandle(tableHandle);

        ConnectorTableMetadata tableMetadata = TABLES.get(informationSchemaTableHandle.getSchemaTableName());

        return tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toMap(identity(), InformationSchemaColumnHandle::new));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        for (Entry<SchemaTableName, ConnectorTableMetadata> entry : TABLES.entrySet()) {
            if (prefix.matches(entry.getKey())) {
                builder.put(entry.getKey(), entry.getValue().getColumns());
            }
        }
        return builder.build();
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
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        InformationSchemaTableHandle table = (InformationSchemaTableHandle) handle;

        // The table is catalog-wise and doesn't have a schema/table name filter.
        if (!isTablesEnumeratingTable(table.getSchemaTableName())) {
            return Optional.empty();
        }

        Optional<Set<String>> schemas = filterString(constraint.getSummary(), SCHEMA_COLUMN_HANDLE);
        Set<String> newSchemas = intersectOptionalSet(table.getSchemas(), schemas).orElseGet(() -> enumerateSchemas(metadata, session, catalogName));

        Optional<Set<String>> tables = filterString(constraint.getSummary(), TABLE_NAME_COLUMN_HANDLE);
        Optional<Set<String>> newTables = intersectOptionalSet(table.getTables(), tables);

        if (constraint.predicate().isPresent()) {
            newSchemas = newSchemas.stream()
                    .filter(schema -> constraint.predicate().get().test(schemaAsFixedValues(schema)))
                    .collect(toImmutableSet());

            if (newTables.isPresent()) {
                newTables = Optional.of(newTables.get().stream()
                .filter(tableName -> constraint.predicate().get().test(tableAsFixedValues(tableName)))
                .collect(toImmutableSet()));
            }
        }

        table = new InformationSchemaTableHandle(table.getCatalogName(), table.getSchemaName(), table.getTableName(), Optional.of(newSchemas), newTables);

        return Optional.of(new ConstraintApplicationResult<>(table, constraint.getSummary()));
    }

    static boolean isTablesEnumeratingTable(SchemaTableName schemaTableName)
    {
        return ImmutableSet.of(TABLE_COLUMNS, TABLE_VIEWS, TABLE_TABLES, TABLE_TABLE_PRIVILEGES).contains(schemaTableName);
    }

    static Set<String> enumerateSchemas(Metadata metadata, ConnectorSession connectorSession, String catalogName)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();
        return metadata.listSchemaNames(session, catalogName).stream()
                .collect(toImmutableSet());
    }

    private <T> Optional<Set<String>> filterString(TupleDomain<T> constraint, T column)
    {
        if (constraint.isNone()) {
            return Optional.of(ImmutableSet.of());
        }

        Domain domain = constraint.getDomains().get().get(column);
        if (domain == null) {
            return Optional.empty();
        }

        if (domain.isSingleValue()) {
            return Optional.of(ImmutableSet.of(((Slice) domain.getSingleValue()).toStringUtf8()));
        }
        if (domain.getValues() instanceof EquatableValueSet) {
            Collection<Object> values = ((EquatableValueSet) domain.getValues()).getValues();
            return Optional.of(values.stream()
                    .map(Slice.class::cast)
                    .map(Slice::toStringUtf8)
                    .collect(toImmutableSet()));
        }
        return Optional.empty();
    }

    private Map<ColumnHandle, NullableValue> schemaAsFixedValues(String schema)
    {
        return ImmutableMap.of(SCHEMA_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(schema)));
    }

    private Map<ColumnHandle, NullableValue> tableAsFixedValues(String table)
    {
        return ImmutableMap.of(TABLE_NAME_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(table)));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    private static Optional<Set<String>> intersectOptionalSet(Optional<Set<String>> set1, Optional<Set<String>> set2)
    {
        if (set1.isPresent() && set2.isPresent()) {
            return Optional.of(Sets.intersection(set1.get(), set2.get()).immutableCopy());
        }
        if (set1.isPresent()) {
            return set1;
        }
        if (set2.isPresent()) {
            return set2;
        }
        return Optional.empty();
    }
}
