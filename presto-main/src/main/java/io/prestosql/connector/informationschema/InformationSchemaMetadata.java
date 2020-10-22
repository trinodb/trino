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
import io.airlift.slice.Slice;
import io.prestosql.FullConnectorSession;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.QualifiedTablePrefix;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.EquatableValueSet;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.connector.informationschema.InformationSchemaTable.COLUMNS;
import static io.prestosql.connector.informationschema.InformationSchemaTable.INFORMATION_SCHEMA;
import static io.prestosql.connector.informationschema.InformationSchemaTable.ROLE_AUTHORIZATION_DESCRIPTORS;
import static io.prestosql.connector.informationschema.InformationSchemaTable.TABLES;
import static io.prestosql.connector.informationschema.InformationSchemaTable.TABLE_PRIVILEGES;
import static io.prestosql.connector.informationschema.InformationSchemaTable.VIEWS;
import static io.prestosql.metadata.MetadataUtil.findColumnMetadata;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class InformationSchemaMetadata
        implements ConnectorMetadata
{
    private static final InformationSchemaColumnHandle CATALOG_COLUMN_HANDLE = new InformationSchemaColumnHandle("table_catalog");
    private static final InformationSchemaColumnHandle SCHEMA_COLUMN_HANDLE = new InformationSchemaColumnHandle("table_schema");
    private static final InformationSchemaColumnHandle TABLE_NAME_COLUMN_HANDLE = new InformationSchemaColumnHandle("table_name");
    private static final InformationSchemaColumnHandle ROLE_NAME_COLUMN_HANDLE = new InformationSchemaColumnHandle("role_name");
    private static final InformationSchemaColumnHandle GRANTEE_COLUMN_HANDLE = new InformationSchemaColumnHandle("grantee");
    private static final int MAX_PREFIXES_COUNT = 100;
    private static final int MAX_ROLE_COUNT = 100;

    private final String catalogName;
    private final Metadata metadata;

    public InformationSchemaMetadata(String catalogName, Metadata metadata)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
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
                .map(table -> new InformationSchemaTableHandle(catalogName, table, defaultPrefixes(catalogName), Optional.empty(), Optional.empty(), OptionalLong.empty()))
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
                .collect(toMap(identity(), InformationSchemaColumnHandle::new));
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
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        InformationSchemaTableHandle tableHandle = (InformationSchemaTableHandle) table;
        return new ConnectorTableProperties(
                tableHandle.getPrefixes().isEmpty() ? TupleDomain.none() : TupleDomain.all(),
                Optional.empty(),
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
                new InformationSchemaTableHandle(table.getCatalogName(), table.getTable(), table.getPrefixes(), table.getRoles(), table.getGrantees(), OptionalLong.of(limit)),
                true));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        InformationSchemaTableHandle table = (InformationSchemaTableHandle) handle;

        Optional<Set<String>> roles = table.getRoles();
        Optional<Set<String>> grantees = table.getGrantees();
        if (ROLE_AUTHORIZATION_DESCRIPTORS.equals(table.getTable()) && table.getRoles().isEmpty() && table.getGrantees().isEmpty()) {
            roles = calculateRoles(session, constraint.getSummary(), constraint.predicate());
            grantees = calculateGrantees(session, constraint.getSummary(), constraint.predicate());
        }

        Set<QualifiedTablePrefix> prefixes = table.getPrefixes();
        if (isTablesEnumeratingTable(table.getTable()) && table.getPrefixes().equals(defaultPrefixes(catalogName))) {
            prefixes = getPrefixes(session, table, constraint);
        }

        if (roles.equals(table.getRoles()) && grantees.equals(table.getGrantees()) && prefixes.equals(table.getPrefixes())) {
            return Optional.empty();
        }

        table = new InformationSchemaTableHandle(table.getCatalogName(), table.getTable(), prefixes, roles, grantees, table.getLimit());
        return Optional.of(new ConstraintApplicationResult<>(table, constraint.getSummary()));
    }

    public static Set<QualifiedTablePrefix> defaultPrefixes(String catalogName)
    {
        return ImmutableSet.of(new QualifiedTablePrefix(catalogName));
    }

    private Set<QualifiedTablePrefix> getPrefixes(ConnectorSession session, InformationSchemaTableHandle table, Constraint constraint)
    {
        if (constraint.getSummary().isNone()) {
            return ImmutableSet.of();
        }

        Optional<Set<String>> catalogs = filterString(constraint.getSummary(), CATALOG_COLUMN_HANDLE);
        if (catalogs.isPresent() && !catalogs.get().contains(table.getCatalogName())) {
            return ImmutableSet.of();
        }

        InformationSchemaTable informationSchemaTable = table.getTable();
        Set<QualifiedTablePrefix> prefixes = calculatePrefixesWithSchemaName(session, constraint.getSummary(), constraint.predicate());
        Set<QualifiedTablePrefix> tablePrefixes = calculatePrefixesWithTableName(informationSchemaTable, session, prefixes, constraint.getSummary(), constraint.predicate());

        if (tablePrefixes.size() <= MAX_PREFIXES_COUNT) {
            prefixes = tablePrefixes;
        }
        if (prefixes.size() > MAX_PREFIXES_COUNT) {
            // in case of high number of prefixes it is better to populate all data and then filter
            prefixes = defaultPrefixes(catalogName);
        }

        return prefixes;
    }

    public static boolean isTablesEnumeratingTable(InformationSchemaTable table)
    {
        return ImmutableSet.of(COLUMNS, VIEWS, TABLES, TABLE_PRIVILEGES).contains(table);
    }

    private Optional<Set<String>> calculateRoles(
            ConnectorSession connectorSession,
            TupleDomain<ColumnHandle> constraint,
            Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate)
    {
        if (constraint.isNone()) {
            return Optional.empty();
        }

        Optional<Set<String>> roles = filterString(constraint, ROLE_NAME_COLUMN_HANDLE);
        if (roles.isPresent()) {
            Set<String> result = roles.get().stream()
                    .filter(this::isLowerCase)
                    .filter(role -> !predicate.isPresent() || predicate.get().test(roleAsFixedValues(role)))
                    .collect(toImmutableSet());

            if (result.isEmpty()) {
                return Optional.empty();
            }
            if (result.size() <= MAX_ROLE_COUNT) {
                return Optional.of(result);
            }
        }

        if (predicate.isEmpty()) {
            return Optional.empty();
        }

        Session session = ((FullConnectorSession) connectorSession).getSession();
        return Optional.of(metadata.listRoles(session, catalogName)
                .stream()
                .filter(role -> predicate.get().test(roleAsFixedValues(role)))
                .collect(toImmutableSet()));
    }

    private Optional<Set<String>> calculateGrantees(
            ConnectorSession connectorSession,
            TupleDomain<ColumnHandle> constraint,
            Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate)
    {
        if (constraint.isNone()) {
            return Optional.empty();
        }

        Optional<Set<String>> grantees = filterString(constraint, GRANTEE_COLUMN_HANDLE);
        if (grantees.isEmpty()) {
            return Optional.empty();
        }

        Set<String> result = grantees.get().stream()
                .filter(this::isLowerCase)
                .filter(role -> !predicate.isPresent() || predicate.get().test(granteeAsFixedValues(role)))
                .collect(toImmutableSet());

        if (!result.isEmpty() && result.size() <= MAX_ROLE_COUNT) {
            return Optional.of(result);
        }

        return Optional.empty();
    }

    private Set<QualifiedTablePrefix> calculatePrefixesWithSchemaName(
            ConnectorSession connectorSession,
            TupleDomain<ColumnHandle> constraint,
            Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate)
    {
        Optional<Set<String>> schemas = filterString(constraint, SCHEMA_COLUMN_HANDLE);
        if (schemas.isPresent()) {
            return schemas.get().stream()
                    .filter(this::isLowerCase)
                    .filter(schema -> predicate.isEmpty() || predicate.get().test(schemaAsFixedValues(schema)))
                    .map(schema -> new QualifiedTablePrefix(catalogName, schema))
                    .collect(toImmutableSet());
        }

        if (predicate.isEmpty()) {
            return ImmutableSet.of(new QualifiedTablePrefix(catalogName));
        }

        Session session = ((FullConnectorSession) connectorSession).getSession();
        return listSchemaNames(session)
                .filter(prefix -> predicate.get().test(schemaAsFixedValues(prefix.getSchemaName().get())))
                .collect(toImmutableSet());
    }

    private Set<QualifiedTablePrefix> calculatePrefixesWithTableName(
            InformationSchemaTable informationSchemaTable,
            ConnectorSession connectorSession,
            Set<QualifiedTablePrefix> prefixes,
            TupleDomain<ColumnHandle> constraint,
            Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate)
    {
        Session session = ((FullConnectorSession) connectorSession).getSession();

        Optional<Set<String>> tables = filterString(constraint, TABLE_NAME_COLUMN_HANDLE);
        if (tables.isPresent()) {
            return prefixes.stream()
                    .peek(prefix -> verify(prefix.asQualifiedObjectName().isEmpty()))
                    .flatMap(prefix -> prefix.getSchemaName()
                            .map(schemaName -> Stream.of(prefix))
                            .orElseGet(() -> listSchemaNames(session)))
                    .flatMap(prefix -> tables.get().stream()
                            .filter(this::isLowerCase)
                            .map(table -> new QualifiedObjectName(catalogName, prefix.getSchemaName().get(), table)))
                    .filter(objectName -> !isColumnsEnumeratingTable(informationSchemaTable) || metadata.getTableHandle(session, objectName).isPresent() || metadata.getView(session, objectName).isPresent())
                    .filter(objectName -> predicate.isEmpty() || predicate.get().test(asFixedValues(objectName)))
                    .map(QualifiedObjectName::asQualifiedTablePrefix)
                    .collect(toImmutableSet());
        }

        if (predicate.isEmpty() || !isColumnsEnumeratingTable(informationSchemaTable)) {
            return prefixes;
        }

        return prefixes.stream()
                .flatMap(prefix -> Stream.concat(
                        metadata.listTables(session, prefix).stream(),
                        metadata.listViews(session, prefix).stream()))
                .filter(objectName -> predicate.get().test(asFixedValues(objectName)))
                .map(QualifiedObjectName::asQualifiedTablePrefix)
                .collect(toImmutableSet());
    }

    private boolean isColumnsEnumeratingTable(InformationSchemaTable table)
    {
        return COLUMNS == table;
    }

    private Stream<QualifiedTablePrefix> listSchemaNames(Session session)
    {
        return metadata.listSchemaNames(session, catalogName).stream()
                .map(schema -> new QualifiedTablePrefix(catalogName, schema));
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
        if (domain.getValues() instanceof SortedRangeSet) {
            ImmutableSet.Builder<String> result = ImmutableSet.builder();
            for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                checkState(!range.isAll()); // Already checked
                if (!range.isSingleValue()) {
                    return Optional.empty();
                }

                result.add(((Slice) range.getSingleValue()).toStringUtf8());
            }

            return Optional.of(result.build());
        }
        return Optional.empty();
    }

    private Map<ColumnHandle, NullableValue> schemaAsFixedValues(String schema)
    {
        return ImmutableMap.of(SCHEMA_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(schema)));
    }

    private Map<ColumnHandle, NullableValue> roleAsFixedValues(String schema)
    {
        return ImmutableMap.of(ROLE_NAME_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(schema)));
    }

    private Map<ColumnHandle, NullableValue> granteeAsFixedValues(String schema)
    {
        return ImmutableMap.of(GRANTEE_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(schema)));
    }

    private Map<ColumnHandle, NullableValue> asFixedValues(QualifiedObjectName objectName)
    {
        return ImmutableMap.of(
                CATALOG_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(objectName.getCatalogName())),
                SCHEMA_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(objectName.getSchemaName())),
                TABLE_NAME_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(objectName.getObjectName())));
    }

    private boolean isLowerCase(String value)
    {
        return value.toLowerCase(ENGLISH).equals(value);
    }
}
