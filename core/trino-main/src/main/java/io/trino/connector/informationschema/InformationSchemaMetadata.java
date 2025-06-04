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
import io.airlift.slice.Slice;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.connector.informationschema.InformationSchemaTable.COLUMNS;
import static io.trino.connector.informationschema.InformationSchemaTable.INFORMATION_SCHEMA;
import static io.trino.connector.informationschema.InformationSchemaTable.TABLES;
import static io.trino.connector.informationschema.InformationSchemaTable.TABLE_PRIVILEGES;
import static io.trino.connector.informationschema.InformationSchemaTable.VIEWS;
import static io.trino.metadata.MetadataUtil.findColumnMetadata;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class InformationSchemaMetadata
        implements ConnectorMetadata
{
    private static final InformationSchemaColumnHandle CATALOG_COLUMN_HANDLE = new InformationSchemaColumnHandle("table_catalog");
    private static final InformationSchemaColumnHandle SCHEMA_COLUMN_HANDLE = new InformationSchemaColumnHandle("table_schema");
    private static final InformationSchemaColumnHandle TABLE_NAME_COLUMN_HANDLE = new InformationSchemaColumnHandle("table_name");

    private final String catalogName;
    private final Metadata metadata;
    private final int maxPrefetchedInformationSchemaPrefixes;

    public InformationSchemaMetadata(String catalogName, Metadata metadata, int maxPrefetchedInformationSchemaPrefixes)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.maxPrefetchedInformationSchemaPrefixes = maxPrefetchedInformationSchemaPrefixes;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(INFORMATION_SCHEMA);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        return InformationSchemaTable.of(tableName)
                .map(table -> new InformationSchemaTableHandle(catalogName, table, defaultPrefixes(catalogName), OptionalLong.empty()))
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = (InformationSchemaTableHandle) tableHandle;
        return informationSchemaTableHandle.table().getTableMetadata();
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
        ConnectorTableMetadata tableMetadata = informationSchemaTableHandle.table().getTableMetadata();

        String columnName = ((InformationSchemaColumnHandle) columnHandle).columnName();

        ColumnMetadata columnMetadata = findColumnMetadata(tableMetadata, columnName);
        checkArgument(columnMetadata != null, "Column '%s' on table '%s' does not exist", columnName, tableMetadata.getTable());
        return columnMetadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        InformationSchemaTableHandle informationSchemaTableHandle = (InformationSchemaTableHandle) tableHandle;

        ConnectorTableMetadata tableMetadata = informationSchemaTableHandle.table().getTableMetadata();

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
                tableHandle.prefixes().isEmpty() ? TupleDomain.none() : TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                emptyList());
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        InformationSchemaTableHandle table = (InformationSchemaTableHandle) handle;

        if (table.limit().isPresent() && table.limit().getAsLong() <= limit) {
            return Optional.empty();
        }

        return Optional.of(new LimitApplicationResult<>(
                new InformationSchemaTableHandle(table.catalogName(), table.table(), table.prefixes(), OptionalLong.of(limit)),
                true,
                false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        InformationSchemaTableHandle table = (InformationSchemaTableHandle) handle;

        Set<QualifiedTablePrefix> prefixes = table.prefixes();
        if (isTablesEnumeratingTable(table.table()) && table.prefixes().equals(defaultPrefixes(catalogName))) {
            prefixes = getPrefixes(session, table, constraint);
        }

        if (prefixes.equals(table.prefixes())) {
            return Optional.empty();
        }

        table = new InformationSchemaTableHandle(table.catalogName(), table.table(), prefixes, table.limit());
        return Optional.of(new ConstraintApplicationResult<>(table, constraint.getSummary(), constraint.getExpression(), false));
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
        if (catalogs.isPresent() && !catalogs.get().contains(table.catalogName())) {
            return ImmutableSet.of();
        }

        InformationSchemaTable informationSchemaTable = table.table();
        Set<QualifiedTablePrefix> schemaPrefixes = calculatePrefixesWithSchemaName(session, constraint.getSummary(), constraint.predicate());
        Set<QualifiedTablePrefix> tablePrefixes = calculatePrefixesWithTableName(informationSchemaTable, session, schemaPrefixes, constraint.getSummary(), constraint.predicate());
        verify(tablePrefixes.size() <= maxPrefetchedInformationSchemaPrefixes, "calculatePrefixesWithTableName returned too many prefixes: %s", tablePrefixes.size());
        return tablePrefixes;
    }

    public static boolean isTablesEnumeratingTable(InformationSchemaTable table)
    {
        return ImmutableSet.of(COLUMNS, VIEWS, TABLES, TABLE_PRIVILEGES).contains(table);
    }

    private Set<QualifiedTablePrefix> calculatePrefixesWithSchemaName(
            ConnectorSession connectorSession,
            TupleDomain<ColumnHandle> constraint,
            Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate)
    {
        Optional<Set<String>> schemas = filterString(constraint, SCHEMA_COLUMN_HANDLE);
        if (schemas.isPresent()) {
            Set<QualifiedTablePrefix> schemasFromPredicate = schemas.get().stream()
                    .filter(this::isLowerCase)
                    .filter(schema -> predicate.isEmpty() || predicate.get().test(schemaAsFixedValues(schema)))
                    .map(schema -> new QualifiedTablePrefix(catalogName, schema))
                    .collect(toImmutableSet());
            if (schemasFromPredicate.size() > maxPrefetchedInformationSchemaPrefixes) {
                return ImmutableSet.of(new QualifiedTablePrefix(catalogName));
            }
            return schemasFromPredicate;
        }

        if (predicate.isEmpty()) {
            return ImmutableSet.of(new QualifiedTablePrefix(catalogName));
        }

        Session session = ((FullConnectorSession) connectorSession).getSession();
        Set<QualifiedTablePrefix> schemaPrefixes = listSchemaNames(session)
                .filter(prefix -> predicate.get().test(schemaAsFixedValues(prefix.getSchemaName().get())))
                .collect(toImmutableSet());
        if (schemaPrefixes.size() > maxPrefetchedInformationSchemaPrefixes) {
            // in case of high number of prefixes it is better to populate all data and then filter
            // TODO this may cause re-running the above filtering upon next applyFilter
            return defaultPrefixes(catalogName);
        }
        return schemaPrefixes;
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
            Set<QualifiedTablePrefix> tablePrefixes = prefixes.stream()
                    .peek(prefix -> verify(prefix.asQualifiedObjectName().isEmpty()))
                    .flatMap(prefix -> prefix.getSchemaName()
                            .map(schemaName -> Stream.of(prefix))
                            .orElseGet(() -> listSchemaNames(session)))
                    .flatMap(prefix -> tables.get().stream()
                            .filter(this::isLowerCase)
                            .map(table -> new QualifiedObjectName(catalogName, prefix.getSchemaName().get(), table)))
                    .filter(objectName -> predicate.isEmpty() || predicate.get().test(asFixedValues(objectName)))
                    .map(QualifiedObjectName::asQualifiedTablePrefix)
                    .distinct()
                    .limit(maxPrefetchedInformationSchemaPrefixes + 1)
                    .collect(toImmutableSet());

            if (tablePrefixes.size() > maxPrefetchedInformationSchemaPrefixes) {
                // in case of high number of prefixes it is better to populate all data and then filter
                // TODO this may cause re-running the above filtering upon next applyFilter
                return defaultPrefixes(catalogName);
            }
            return tablePrefixes;
        }

        if (predicate.isEmpty() || !isColumnsEnumeratingTable(informationSchemaTable)) {
            return prefixes;
        }

        Set<QualifiedTablePrefix> tablePrefixes = prefixes.stream()
                .flatMap(prefix -> metadata.listTables(session, prefix).stream())
                .filter(objectName -> predicate.get().test(asFixedValues(objectName)))
                .map(QualifiedObjectName::asQualifiedTablePrefix)
                .distinct()
                .limit(maxPrefetchedInformationSchemaPrefixes + 1)
                .collect(toImmutableSet());
        if (tablePrefixes.size() > maxPrefetchedInformationSchemaPrefixes) {
            // in case of high number of prefixes it is better to populate all data and then filter
            // TODO this may cause re-running the above filtering upon next applyFilter
            return defaultPrefixes(catalogName);
        }
        return tablePrefixes;
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
        if (domain.getValues() instanceof EquatableValueSet set) {
            return Optional.of(set.getValues().stream()
                    .map(Slice.class::cast)
                    .map(Slice::toStringUtf8)
                    .collect(toImmutableSet()));
        }
        if (domain.getValues() instanceof SortedRangeSet) {
            ImmutableSet.Builder<String> result = ImmutableSet.builder();
            for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
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

    private Map<ColumnHandle, NullableValue> asFixedValues(QualifiedObjectName objectName)
    {
        return ImmutableMap.of(
                CATALOG_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(objectName.catalogName())),
                SCHEMA_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(objectName.schemaName())),
                TABLE_NAME_COLUMN_HANDLE, new NullableValue(createUnboundedVarcharType(), utf8Slice(objectName.objectName())));
    }

    private boolean isLowerCase(String value)
    {
        return value.toLowerCase(ENGLISH).equals(value);
    }
}
