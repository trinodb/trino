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
import io.trino.connector.system.SystemColumnHandle;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.security.AccessControl;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
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

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.time.ZoneId;
import java.util.Collection;
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
import static io.trino.connector.system.jdbc.FilterUtil.isImpossibleObjectName;
import static io.trino.connector.system.jdbc.FilterUtil.tablePrefix;
import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataListing.listCatalogNames;
import static io.trino.metadata.MetadataListing.listSchemas;
import static io.trino.metadata.MetadataListing.listTableColumns;
import static io.trino.metadata.MetadataListing.listTables;
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
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.TypeUtils.getDisplayLabel;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class ColumnJdbcTable
        extends JdbcTable
{
    public static final SchemaTableName NAME = new SchemaTableName("jdbc", "columns");

    private static final int MAX_DOMAIN_SIZE = 100;
    private static final int MAX_TIMEZONE_LENGTH = ZoneId.getAvailableZoneIds().stream()
            .map(String::length)
            .max(Integer::compareTo)
            .get();

    private static final ColumnHandle TABLE_CATALOG_COLUMN = new SystemColumnHandle("table_cat");
    private static final ColumnHandle TABLE_SCHEMA_COLUMN = new SystemColumnHandle("table_schem");
    private static final ColumnHandle TABLE_NAME_COLUMN = new SystemColumnHandle("table_name");

    public static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
            .column("table_cat", VARCHAR)
            .column("table_schem", VARCHAR)
            .column("table_name", VARCHAR)
            .column("column_name", VARCHAR)
            .column("data_type", BIGINT)
            .column("type_name", VARCHAR)
            .column("column_size", BIGINT)
            .column("buffer_length", BIGINT)
            .column("decimal_digits", BIGINT)
            .column("num_prec_radix", BIGINT)
            .column("nullable", BIGINT)
            .column("remarks", VARCHAR)
            .column("column_def", VARCHAR)
            .column("sql_data_type", BIGINT)
            .column("sql_datetime_sub", BIGINT)
            .column("char_octet_length", BIGINT)
            .column("ordinal_position", BIGINT)
            .column("is_nullable", VARCHAR)
            .column("scope_catalog", VARCHAR)
            .column("scope_schema", VARCHAR)
            .column("scope_table", VARCHAR)
            .column("source_data_type", BIGINT)
            .column("is_autoincrement", VARCHAR)
            .column("is_generatedcolumn", VARCHAR)
            .build();

    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public ColumnJdbcTable(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
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
        Predicate<Map<ColumnHandle, NullableValue>> predicate = constraint.predicate().get();
        Set<ColumnHandle> predicateColumns = constraint.getPredicateColumns().orElseThrow(() -> new VerifyException("columns not present for a predicate"));

        boolean hasSchemaPredicate = predicateColumns.contains(TABLE_SCHEMA_COLUMN);
        boolean hasTablePredicate = predicateColumns.contains(TABLE_NAME_COLUMN);
        if (!hasSchemaPredicate && !hasTablePredicate) {
            // No filter on schema name and table name at all.
            return tupleDomain;
        }

        Session session = ((FullConnectorSession) connectorSession).getSession();

        Domain catalogDomain = tupleDomain.getDomain(TABLE_CATALOG_COLUMN, VARCHAR);
        Domain schemaDomain = tupleDomain.getDomain(TABLE_SCHEMA_COLUMN, VARCHAR);
        Domain tableDomain = tupleDomain.getDomain(TABLE_NAME_COLUMN, VARCHAR);

        if (isImpossibleObjectName(catalogDomain) || isImpossibleObjectName(schemaDomain) || isImpossibleObjectName(tableDomain)) {
            return TupleDomain.none();
        }

        Optional<String> schemaFilter = tryGetSingleVarcharValue(schemaDomain);
        Optional<String> tableFilter = tryGetSingleVarcharValue(tableDomain);

        if (schemaFilter.isPresent() && tableFilter.isPresent()) {
            // No need to narrow down the domain.
            return tupleDomain;
        }

        List<String> catalogs = listCatalogNames(session, metadata, accessControl, catalogDomain).stream()
                .filter(catalogName -> predicate.test(ImmutableMap.of(TABLE_CATALOG_COLUMN, toNullableValue(catalogName))))
                .collect(toImmutableList());

        List<CatalogSchemaName> schemas = catalogs.stream()
                .flatMap(catalogName ->
                        listSchemas(session, metadata, accessControl, catalogName, schemaFilter).stream()
                                .filter(schemaName -> !hasSchemaPredicate || predicate.test(ImmutableMap.of(
                                        TABLE_CATALOG_COLUMN, toNullableValue(catalogName),
                                        TABLE_SCHEMA_COLUMN, toNullableValue(schemaName))))
                                .map(schemaName -> new CatalogSchemaName(catalogName, schemaName)))
                .collect(toImmutableList());

        if (!hasTablePredicate) {
            return TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>builder()
                    .put(TABLE_CATALOG_COLUMN, schemas.stream()
                            .map(CatalogSchemaName::getCatalogName)
                            .collect(toVarcharDomain())
                            .simplify(MAX_DOMAIN_SIZE))
                    .put(TABLE_SCHEMA_COLUMN, schemas.stream()
                            .map(CatalogSchemaName::getSchemaName)
                            .collect(toVarcharDomain())
                            .simplify(MAX_DOMAIN_SIZE))
                    .buildOrThrow());
        }

        List<CatalogSchemaTableName> tables = schemas.stream()
                .flatMap(schema -> {
                    QualifiedTablePrefix tablePrefix = tableFilter.isPresent()
                            ? new QualifiedTablePrefix(schema.getCatalogName(), schema.getSchemaName(), tableFilter.get())
                            : new QualifiedTablePrefix(schema.getCatalogName(), schema.getSchemaName());
                    return listTables(session, metadata, accessControl, tablePrefix).stream()
                            .filter(schemaTableName -> predicate.test(ImmutableMap.of(
                                    TABLE_CATALOG_COLUMN, toNullableValue(schema.getCatalogName()),
                                    TABLE_SCHEMA_COLUMN, toNullableValue(schemaTableName.getSchemaName()),
                                    TABLE_NAME_COLUMN, toNullableValue(schemaTableName.getTableName()))))
                            .map(schemaTableName -> new CatalogSchemaTableName(schema.getCatalogName(), schemaTableName.getSchemaName(), schemaTableName.getTableName()));
                })
                .collect(toImmutableList());

        return TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>builder()
                .put(TABLE_CATALOG_COLUMN, tables.stream()
                        .map(CatalogSchemaTableName::getCatalogName)
                        .collect(toVarcharDomain())
                        .simplify(MAX_DOMAIN_SIZE))
                .put(TABLE_SCHEMA_COLUMN, tables.stream()
                        .map(catalogSchemaTableName -> catalogSchemaTableName.getSchemaTableName().getSchemaName())
                        .collect(toVarcharDomain())
                        .simplify(MAX_DOMAIN_SIZE))
                .put(TABLE_NAME_COLUMN, tables.stream()
                        .map(catalogSchemaTableName -> catalogSchemaTableName.getSchemaTableName().getTableName())
                        .collect(toVarcharDomain())
                        .simplify(MAX_DOMAIN_SIZE))
                .buildOrThrow());
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

        Domain catalogDomain = constraint.getDomain(0, VARCHAR);
        Domain schemaDomain = constraint.getDomain(1, VARCHAR);
        Domain tableDomain = constraint.getDomain(2, VARCHAR);

        if (isImpossibleObjectName(catalogDomain) || isImpossibleObjectName(schemaDomain) || isImpossibleObjectName(tableDomain)) {
            return table.build().cursor();
        }

        Optional<String> schemaFilter = tryGetSingleVarcharValue(schemaDomain);
        Optional<String> tableFilter = tryGetSingleVarcharValue(tableDomain);

        for (String catalog : listCatalogNames(session, metadata, accessControl, catalogDomain)) {
            if (!catalogDomain.includesNullableValue(utf8Slice(catalog))) {
                continue;
            }

            if ((schemaDomain.isAll() && tableDomain.isAll()) || schemaFilter.isPresent()) {
                QualifiedTablePrefix tablePrefix = tablePrefix(catalog, schemaFilter, tableFilter);
                Map<SchemaTableName, List<ColumnMetadata>> tableColumns = listTableColumns(session, metadata, accessControl, tablePrefix);
                addColumnsRow(table, catalog, tableColumns, omitDateTimeTypePrecision);
            }
            else {
                Collection<String> schemas = listSchemas(session, metadata, accessControl, catalog, schemaFilter);
                for (String schema : schemas) {
                    if (!schemaDomain.includesNullableValue(utf8Slice(schema))) {
                        continue;
                    }

                    QualifiedTablePrefix tablePrefix = tableFilter.isPresent()
                            ? new QualifiedTablePrefix(catalog, schema, tableFilter.get())
                            : new QualifiedTablePrefix(catalog, schema);
                    Set<SchemaTableName> tables = listTables(session, metadata, accessControl, tablePrefix);
                    for (SchemaTableName schemaTableName : tables) {
                        String tableName = schemaTableName.getTableName();
                        if (!tableDomain.includesNullableValue(utf8Slice(tableName))) {
                            continue;
                        }

                        Map<SchemaTableName, List<ColumnMetadata>> tableColumns = listTableColumns(session, metadata, accessControl, new QualifiedTablePrefix(catalog, schema, tableName));
                        addColumnsRow(table, catalog, tableColumns, omitDateTimeTypePrecision);
                    }
                }
            }
        }
        return table.build().cursor();
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
        if (type instanceof DecimalType decimalType) {
            return decimalType.getPrecision();
        }
        if (type.equals(REAL)) {
            return 24; // IEEE 754
        }
        if (type.equals(DOUBLE)) {
            return 53; // IEEE 754
        }
        if (type instanceof VarcharType varcharType) {
            return varcharType.getLength().orElse(VarcharType.UNBOUNDED_LENGTH);
        }
        if (type instanceof CharType charType) {
            return charType.getLength();
        }
        if (type.equals(VARBINARY)) {
            return Integer.MAX_VALUE;
        }
        if (type instanceof TimeType timeType) {
            // 8 characters for "HH:MM:SS"
            // min(p, 1) for the fractional second period (i.e., no period if p == 0)
            // p for the fractional digits
            int precision = timeType.getPrecision();
            return 8 + min(precision, 1) + precision;
        }
        if (type instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
            // 8 characters for "HH:MM:SS"
            // min(p, 1) for the fractional second period (i.e., no period if p == 0)
            // p for the fractional digits
            // 6 for timezone offset
            int precision = timeWithTimeZoneType.getPrecision();
            return 8 + min(precision, 1) + precision + 6;
        }
        if (type.equals(DATE)) {
            return 14; // +5881580-07-11 (2**31-1 days)
        }
        if (type instanceof TimestampType timestampType) {
            // 1 digit for year sign
            // 5 digits for year
            // 15 characters for "-MM-DD HH:MM:SS"
            // min(p, 1) for the fractional second period (i.e., no period if p == 0)
            // p for the fractional digits
            int precision = timestampType.getPrecision();
            return 1 + 5 + 15 + min(precision, 1) + precision;
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            // 1 digit for year sign
            // 6 digits for year
            // 15 characters for "-MM-DD HH:MM:SS"
            // min(p, 1) for the fractional second period (i.e., no period if p == 0)
            // p for the fractional digits
            // 1 for space after timestamp
            // MAX_TIMEZONE_LENGTH for timezone
            int precision = timestampWithTimeZoneType.getPrecision();
            return 1 + 6 + 15 + min(precision, 1) + precision + 1 + MAX_TIMEZONE_LENGTH;
        }
        return null;
    }

    // DECIMAL_DIGITS is the number of fractional digits
    private static Integer decimalDigits(Type type)
    {
        if (type instanceof DecimalType decimalType) {
            return decimalType.getScale();
        }
        if (type instanceof TimeType timeType) {
            return timeType.getPrecision();
        }
        if (type instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
            return timeWithTimeZoneType.getPrecision();
        }
        if (type instanceof TimestampType timestampType) {
            return timestampType.getPrecision();
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            return timestampWithTimeZoneType.getPrecision();
        }
        return null;
    }

    private static Integer charOctetLength(Type type)
    {
        if (type instanceof VarcharType varcharType) {
            return varcharType.getLength().orElse(VarcharType.UNBOUNDED_LENGTH);
        }
        if (type instanceof CharType charType) {
            return charType.getLength();
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
        return NullableValue.of(VARCHAR, utf8Slice(varcharValue));
    }

    private static Collector<String, ?, Domain> toVarcharDomain()
    {
        return Collectors.collectingAndThen(toImmutableSet(), set -> {
            if (set.isEmpty()) {
                return Domain.none(VARCHAR);
            }
            return Domain.multipleValues(VARCHAR, set.stream()
                    .map(Slices::utf8Slice)
                    .collect(toImmutableList()));
        });
    }
}
