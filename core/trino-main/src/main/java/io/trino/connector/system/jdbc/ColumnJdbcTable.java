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
package io.trino.connector.system.jdbc;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.slice.Slices;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.connector.informationschema.SystemTableFilter;
import io.trino.connector.system.SystemColumnHandle;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.InMemoryRecordSet.Builder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.planner.OptimizerConfig;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SystemSessionProperties.isOmitDateTimeTypePrecision;
import static io.trino.connector.informationschema.SystemTableFilter.TABLE_COUNT_PER_SCHEMA_THRESHOLD;
import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.listCatalogNames;
import static io.trino.metadata.MetadataListing.listTableColumns;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.type.TypeUtils.getDisplayLabel;
import static java.lang.Math.min;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ColumnJdbcTable
        extends JdbcTable
{
    public static final SchemaTableName NAME = new SchemaTableName("jdbc", "columns");

    private static final int MAX_TIMEZONE_LENGTH = ZoneId.getAvailableZoneIds().stream()
            .map(String::length)
            .max(Integer::compareTo)
            .get();

    private static final ColumnHandle TABLE_CATALOG_COLUMN = new SystemColumnHandle("table_cat");
    private static final ColumnHandle TABLE_SCHEMA_COLUMN = new SystemColumnHandle("table_schem");
    private static final ColumnHandle TABLE_NAME_COLUMN = new SystemColumnHandle("table_name");

    public static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
            .column("table_cat", createUnboundedVarcharType())
            .column("table_schem", createUnboundedVarcharType())
            .column("table_name", createUnboundedVarcharType())
            .column("column_name", createUnboundedVarcharType())
            .column("data_type", BIGINT)
            .column("type_name", createUnboundedVarcharType())
            .column("column_size", BIGINT)
            .column("buffer_length", BIGINT)
            .column("decimal_digits", BIGINT)
            .column("num_prec_radix", BIGINT)
            .column("nullable", BIGINT)
            .column("remarks", createUnboundedVarcharType())
            .column("column_def", createUnboundedVarcharType())
            .column("sql_data_type", BIGINT)
            .column("sql_datetime_sub", BIGINT)
            .column("char_octet_length", BIGINT)
            .column("ordinal_position", BIGINT)
            .column("is_nullable", createUnboundedVarcharType())
            .column("scope_catalog", createUnboundedVarcharType())
            .column("scope_schema", createUnboundedVarcharType())
            .column("scope_table", createUnboundedVarcharType())
            .column("source_data_type", BIGINT)
            .column("is_autoincrement", createUnboundedVarcharType())
            .column("is_generatedcolumn", createUnboundedVarcharType())
            .build();

    private final Metadata metadata;
    private final AccessControl accessControl;
    private final int maxPrefetchedInformationSchemaPrefixes;

    @Inject
    public ColumnJdbcTable(Metadata metadata, AccessControl accessControl, OptimizerConfig optimizerConfig)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.maxPrefetchedInformationSchemaPrefixes = optimizerConfig.getMaxPrefetchedInformationSchemaPrefixes();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return METADATA;
    }

    @Override
    public TupleDomain<ColumnHandle> applyFilter(ConnectorSession connectorSession, Constraint constraint)
    {
        TupleDomain<ColumnHandle> tupleDomain = constraint.getSummary();
        if (tupleDomain.isNone() || constraint.predicate().isEmpty()) {
            return tupleDomain;
        }
        Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate = constraint.predicate();
        Set<ColumnHandle> predicateColumns = constraint.getPredicateColumns().orElseThrow(() -> new VerifyException("columns not present for a predicate"));

        boolean hasSchemaPredicate = predicateColumns.contains(TABLE_SCHEMA_COLUMN);
        boolean hasTablePredicate = predicateColumns.contains(TABLE_NAME_COLUMN);
        if (!hasSchemaPredicate && !hasTablePredicate) {
            // No filter on schema name and table name at all.
            return tupleDomain;
        }

        Session session = ((FullConnectorSession) connectorSession).getSession();

        Optional<String> catalogFilter = tryGetSingleVarcharValue(tupleDomain, TABLE_CATALOG_COLUMN);
        List<String> catalogs = listCatalogNames(session, metadata, accessControl, catalogFilter).stream()
                .filter(catalogName -> predicate.get().test(ImmutableMap.of(TABLE_CATALOG_COLUMN, toNullableValue(catalogName))))
                .collect(toImmutableList());

        List<QualifiedTablePrefix> tables = catalogs.stream().flatMap(catalogName -> {
            SystemTableFilter<ColumnHandle> filter = new SystemTableFilter<>(
                    catalogName,
                    metadata,
                    accessControl,
                    TABLE_CATALOG_COLUMN,
                    TABLE_SCHEMA_COLUMN,
                    TABLE_NAME_COLUMN,
                    true,
                    maxPrefetchedInformationSchemaPrefixes);
            return filter.getPrefixes(connectorSession, tupleDomain, predicate).stream();
        }).collect(toImmutableList());

        ImmutableMap.Builder<ColumnHandle, Domain> builder = ImmutableMap.<ColumnHandle, Domain>builder()
                .put(TABLE_CATALOG_COLUMN, tables.stream()
                        .map(QualifiedTablePrefix::getCatalogName)
                        .collect(toVarcharDomain())
                        .simplify(maxPrefetchedInformationSchemaPrefixes));
        Domain schemaDomain = tables.stream()
                .flatMap(table -> table.getSchemaName().stream())
                .collect(toVarcharDomain())
                .simplify(maxPrefetchedInformationSchemaPrefixes);
        Domain tableDomain = tables.stream()
                .flatMap(table -> table.getTableName().stream())
                .collect(toVarcharDomain())
                .simplify(maxPrefetchedInformationSchemaPrefixes * TABLE_COUNT_PER_SCHEMA_THRESHOLD);

        if (!schemaDomain.isNone()) {
            builder.put(TABLE_SCHEMA_COLUMN, schemaDomain);
        }
        if (!tableDomain.isNone()) {
            builder.put(TABLE_NAME_COLUMN, tableDomain);
        }
        return TupleDomain.withColumnDomains(builder.buildOrThrow());
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession connectorSession, TupleDomain<Integer> constraint)
    {
        Builder table = InMemoryRecordSet.builder(METADATA);
        if (constraint.isNone()) {
            return table.build().cursor();
        }

        Session session = ((FullConnectorSession) connectorSession).getSession();
        boolean omitDateTimeTypePrecision = isOmitDateTimeTypePrecision(session);
        Optional<String> catalogFilter = tryGetSingleVarcharValue(constraint, 0);
        Optional<String> schemaFilter = tryGetSingleVarcharValue(constraint, 1);
        Optional<String> tableFilter = tryGetSingleVarcharValue(constraint, 2);

        Domain catalogDomain = constraint.getDomains().get().getOrDefault(0, Domain.all(createUnboundedVarcharType()));

        if (isNonLowercase(schemaFilter) || isNonLowercase(tableFilter)) {
            // Non-lowercase predicate will never match a lowercase name (until TODO https://github.com/trinodb/trino/issues/17)
            return table.build().cursor();
        }

        List<String> catalogs = listCatalogNames(session, metadata, accessControl, catalogFilter).stream()
                .filter(catalog -> catalogDomain.includesNullableValue(utf8Slice(catalog)))
                .collect(toImmutableList());

        List<QualifiedTablePrefix> prefixes = catalogs.stream().flatMap(catalogName -> {
            SystemTableFilter<Integer> filter = new SystemTableFilter<>(
                    catalogName,
                    metadata,
                    accessControl,
                    0,
                    1,
                    2,
                    true,
                    maxPrefetchedInformationSchemaPrefixes);
            return filter.getPrefixes(connectorSession, constraint, Optional.empty()).stream();
        }).collect(toImmutableList());

        prefixes.forEach(prefix -> {
            Map<SchemaTableName, List<ColumnMetadata>> tableColumns = listTableColumns(session, metadata, accessControl, prefix);
            addColumnsRow(table, prefix.getCatalogName(), tableColumns, omitDateTimeTypePrecision);
        });

        return table.build().cursor();
    }

    private static boolean isNonLowercase(Optional<String> filter)
    {
        return filter.filter(value -> !value.equals(value.toLowerCase(ENGLISH))).isPresent();
    }

    private static void addColumnsRow(Builder builder, String catalog, Map<SchemaTableName, List<ColumnMetadata>> columns, boolean isOmitTimestampPrecision)
    {
        for (Entry<SchemaTableName, List<ColumnMetadata>> entry : columns.entrySet()) {
            addColumnRows(builder, catalog, entry.getKey(), entry.getValue(), isOmitTimestampPrecision);
        }
    }

    private static void addColumnRows(Builder builder, String catalog, SchemaTableName tableName, List<ColumnMetadata> columns, boolean isOmitTimestampPrecision)
    {
        int ordinalPosition = 1;
        for (ColumnMetadata column : columns) {
            if (column.isHidden()) {
                continue;
            }
            builder.addRow(
                    // table_cat
                    catalog,
                    // table_schem
                    tableName.getSchemaName(),
                    // table_name
                    tableName.getTableName(),
                    // column_name
                    column.getName(),
                    // data_type
                    jdbcDataType(column.getType()),
                    // type_name
                    getDisplayLabel(column.getType(), isOmitTimestampPrecision),
                    // column_size
                    columnSize(column.getType()),
                    // buffer_length
                    0,
                    // decimal_digits
                    decimalDigits(column.getType()),
                    // num_prec_radix
                    numPrecRadix(column.getType()),
                    // nullable
                    column.isNullable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls,
                    // remarks
                    column.getComment(),
                    // column_def
                    null,
                    // sql_data_type
                    null,
                    // sql_datetime_sub
                    null,
                    // char_octet_length
                    charOctetLength(column.getType()),
                    // ordinal_position
                    ordinalPosition,
                    // is_nullable
                    column.isNullable() ? "YES" : "NO",
                    // scope_catalog
                    null,
                    // scope_schema
                    null,
                    // scope_table
                    null,
                    // source_data_type
                    null,
                    // is_autoincrement
                    null,
                    // is_generatedcolumn
                    null);
            ordinalPosition++;
        }
    }

    static int jdbcDataType(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return Types.BOOLEAN;
        }
        if (type.equals(BIGINT)) {
            return Types.BIGINT;
        }
        if (type.equals(INTEGER)) {
            return Types.INTEGER;
        }
        if (type.equals(SMALLINT)) {
            return Types.SMALLINT;
        }
        if (type.equals(TINYINT)) {
            return Types.TINYINT;
        }
        if (type.equals(REAL)) {
            return Types.REAL;
        }
        if (type.equals(DOUBLE)) {
            return Types.DOUBLE;
        }
        if (type instanceof DecimalType) {
            return Types.DECIMAL;
        }
        if (type instanceof VarcharType) {
            return Types.VARCHAR;
        }
        if (type instanceof CharType) {
            return Types.CHAR;
        }
        if (type.equals(VARBINARY)) {
            return Types.VARBINARY;
        }
        if (type.equals(DATE)) {
            return Types.DATE;
        }
        if (type instanceof TimeType) {
            return Types.TIME;
        }
        if (type instanceof TimeWithTimeZoneType) {
            return Types.TIME_WITH_TIMEZONE;
        }
        if (type instanceof TimestampType) {
            return Types.TIMESTAMP;
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return Types.TIMESTAMP_WITH_TIMEZONE;
        }
        if (type instanceof ArrayType) {
            return Types.ARRAY;
        }
        return Types.JAVA_OBJECT;
    }

    static Integer columnSize(Type type)
    {
        if (type.equals(BIGINT)) {
            return 19;  // 2**63-1
        }
        if (type.equals(INTEGER)) {
            return 10;  // 2**31-1
        }
        if (type.equals(SMALLINT)) {
            return 5;   // 2**15-1
        }
        if (type.equals(TINYINT)) {
            return 3;   // 2**7-1
        }
        if (type instanceof DecimalType) {
            return ((DecimalType) type).getPrecision();
        }
        if (type.equals(REAL)) {
            return 24; // IEEE 754
        }
        if (type.equals(DOUBLE)) {
            return 53; // IEEE 754
        }
        if (type instanceof VarcharType) {
            return ((VarcharType) type).getLength().orElse(VarcharType.UNBOUNDED_LENGTH);
        }
        if (type instanceof CharType) {
            return ((CharType) type).getLength();
        }
        if (type.equals(VARBINARY)) {
            return Integer.MAX_VALUE;
        }
        if (type instanceof TimeType) {
            // 8 characters for "HH:MM:SS"
            // min(p, 1) for the fractional second period (i.e., no period if p == 0)
            // p for the fractional digits
            int precision = ((TimeType) type).getPrecision();
            return 8 + min(precision, 1) + precision;
        }
        if (type instanceof TimeWithTimeZoneType) {
            // 8 characters for "HH:MM:SS"
            // min(p, 1) for the fractional second period (i.e., no period if p == 0)
            // p for the fractional digits
            // 6 for timezone offset
            int precision = ((TimeWithTimeZoneType) type).getPrecision();
            return 8 + min(precision, 1) + precision + 6;
        }
        if (type.equals(DATE)) {
            return 14; // +5881580-07-11 (2**31-1 days)
        }
        if (type instanceof TimestampType) {
            // 1 digit for year sign
            // 5 digits for year
            // 15 characters for "-MM-DD HH:MM:SS"
            // min(p, 1) for the fractional second period (i.e., no period if p == 0)
            // p for the fractional digits
            int precision = ((TimestampType) type).getPrecision();
            return 1 + 5 + 15 + min(precision, 1) + precision;
        }
        if (type instanceof TimestampWithTimeZoneType) {
            // 1 digit for year sign
            // 6 digits for year
            // 15 characters for "-MM-DD HH:MM:SS"
            // min(p, 1) for the fractional second period (i.e., no period if p == 0)
            // p for the fractional digits
            // 1 for space after timestamp
            // MAX_TIMEZONE_LENGTH for timezone
            int precision = ((TimestampWithTimeZoneType) type).getPrecision();
            return 1 + 6 + 15 + min(precision, 1) + precision + 1 + MAX_TIMEZONE_LENGTH;
        }
        return null;
    }

    // DECIMAL_DIGITS is the number of fractional digits
    private static Integer decimalDigits(Type type)
    {
        if (type instanceof DecimalType) {
            return ((DecimalType) type).getScale();
        }
        if (type instanceof TimeType) {
            return ((TimeType) type).getPrecision();
        }
        if (type instanceof TimeWithTimeZoneType) {
            return ((TimeWithTimeZoneType) type).getPrecision();
        }
        if (type instanceof TimestampType) {
            return ((TimestampType) type).getPrecision();
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return ((TimestampWithTimeZoneType) type).getPrecision();
        }
        return null;
    }

    private static Integer charOctetLength(Type type)
    {
        if (type instanceof VarcharType) {
            return ((VarcharType) type).getLength().orElse(VarcharType.UNBOUNDED_LENGTH);
        }
        if (type instanceof CharType) {
            return ((CharType) type).getLength();
        }
        if (type.equals(VARBINARY)) {
            return Integer.MAX_VALUE;
        }
        return null;
    }

    static Integer numPrecRadix(Type type)
    {
        if (type.equals(BIGINT) ||
                type.equals(INTEGER) ||
                type.equals(SMALLINT) ||
                type.equals(TINYINT) ||
                (type instanceof DecimalType)) {
            return 10;
        }
        if (type.equals(REAL) || type.equals(DOUBLE)) {
            return 2;
        }
        return null;
    }

    private static NullableValue toNullableValue(String varcharValue)
    {
        return NullableValue.of(createUnboundedVarcharType(), utf8Slice(varcharValue));
    }

    private static Collector<String, ?, Domain> toVarcharDomain()
    {
        return Collectors.collectingAndThen(toImmutableSet(), set -> {
            if (set.isEmpty()) {
                return Domain.none(createUnboundedVarcharType());
            }
            return Domain.multipleValues(createUnboundedVarcharType(), set.stream()
                    .map(Slices::utf8Slice)
                    .collect(toImmutableList()));
        });
    }
}
