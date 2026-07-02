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
package io.trino.plugin.starrocks;

import com.google.inject.Module;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.plugin.tpch.DecimalTypeMapping;
import io.trino.plugin.tpch.TpchMetadata;
import io.trino.plugin.tpch.TpchRecordSet;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchEntity;
import io.trino.tpch.TpchTable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

final class TestingStarRocksEnvironment
{
    private static final JsonCodec<Object> JSON_CODEC = jsonCodec(Object.class);
    private static final String TPCH_SCHEMA = TpchMetadata.TINY_SCHEMA_NAME;
    private static final double TPCH_SCALE_FACTOR = TpchMetadata.TINY_SCALE_FACTOR;

    private final Map<SchemaTableName, TestingTable> tables;
    private final AtomicReference<Request> lastRequest = new AtomicReference<>();

    public TestingStarRocksEnvironment()
    {
        this(List.of());
    }

    public TestingStarRocksEnvironment(List<TableDefinition> additionalTables)
    {
        tables = createTables(additionalTables);
    }

    public Module createModule()
    {
        TestingStarRocksMetadataClient metadataClient = new TestingStarRocksMetadataClient(tables);
        TestingStarRocksFlightSqlClient flightSqlClient = new TestingStarRocksFlightSqlClient(tables, lastRequest);

        return binder -> {
            binder.bind(StarRocksMetadataClient.class).toInstance(metadataClient);
            binder.bind(StarRocksFlightSqlClient.class).toInstance(flightSqlClient);
        };
    }

    public Optional<Request> getLastRequest()
    {
        return Optional.ofNullable(lastRequest.get());
    }

    private static Map<SchemaTableName, TestingTable> createTables(List<TableDefinition> additionalTables)
    {
        Map<SchemaTableName, TestingTable> tableMap = new LinkedHashMap<>();
        for (TpchTable<?> table : List.of(TpchTable.NATION, TpchTable.REGION, TpchTable.CUSTOMER, TpchTable.ORDERS)) {
            TestingTable testingTable = createTpchTable(table);
            tableMap.put(testingTable.schemaTableName(), testingTable);
        }
        for (TableDefinition additionalTable : additionalTables) {
            TestingTable testingTable = additionalTable.toTestingTable();
            tableMap.put(testingTable.schemaTableName(), testingTable);
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(tableMap));
    }

    private static <E extends TpchEntity> TestingTable createTpchTable(TpchTable<E> table)
    {
        List<TpchColumn<E>> columns = table.getColumns();
        List<StarRocksRemoteColumn> remoteColumns = new ArrayList<>(columns.size());
        for (int index = 0; index < columns.size(); index++) {
            remoteColumns.add(toRemoteColumn(columns.get(index), index + 1));
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        try (RecordCursor cursor = TpchRecordSet.createTpchRecordSet(table, TPCH_SCALE_FACTOR).cursor()) {
            while (cursor.advanceNextPosition()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int index = 0; index < columns.size(); index++) {
                    TpchColumn<E> column = columns.get(index);
                    Type columnType = TpchMetadata.getTrinoType(column, DecimalTypeMapping.DOUBLE);
                    row.put(column.getSimplifiedColumnName(), getCursorValue(cursor, index, columnType));
                }
                rows.add(Map.copyOf(row));
            }
        }

        return new TestingTable(
                new SchemaTableName(TPCH_SCHEMA, table.getTableName()),
                TPCH_SCHEMA,
                table.getTableName(),
                StarRocksRelationType.TABLE,
                remoteColumns,
                rows);
    }

    private static <E extends TpchEntity> StarRocksRemoteColumn toRemoteColumn(TpchColumn<E> column, int ordinalPosition)
    {
        String columnName = column.getSimplifiedColumnName();
        return switch (column.getType().getBase()) {
            case IDENTIFIER -> new StarRocksRemoteColumn(columnName, columnName, "BIGINT", Optional.of(20), Optional.empty(), ordinalPosition, Optional.empty());
            case INTEGER -> new StarRocksRemoteColumn(columnName, columnName, "INT", Optional.of(11), Optional.empty(), ordinalPosition, Optional.empty());
            case DATE -> new StarRocksRemoteColumn(columnName, columnName, "DATE", Optional.empty(), Optional.empty(), ordinalPosition, Optional.empty());
            case DOUBLE -> new StarRocksRemoteColumn(columnName, columnName, "DOUBLE", Optional.empty(), Optional.empty(), ordinalPosition, Optional.empty());
            case VARCHAR -> new StarRocksRemoteColumn(
                    columnName,
                    columnName,
                    "VARCHAR",
                    column.getType().getPrecision().map(Math::toIntExact),
                    Optional.empty(),
                    ordinalPosition,
                    column.getType().getPrecision().map(precision -> "varchar(%s)".formatted(precision)));
        };
    }

    private static Object getCursorValue(RecordCursor cursor, int field, Type columnType)
    {
        if (columnType.getJavaType() == long.class) {
            return cursor.getLong(field);
        }
        if (columnType.getJavaType() == double.class) {
            return cursor.getDouble(field);
        }
        if (columnType.getJavaType() == Slice.class) {
            return cursor.getSlice(field);
        }
        throw new IllegalArgumentException("Unsupported TPCH testing column type: " + columnType);
    }

    public record Request(StarRocksTableHandle tableHandle, List<StarRocksColumnHandle> requestedColumns)
    {
        public Request
        {
            requireNonNull(tableHandle, "tableHandle is null");
            requestedColumns = List.copyOf(requireNonNull(requestedColumns, "requestedColumns is null"));
        }
    }

    public record TableDefinition(
            SchemaTableName schemaTableName,
            String remoteSchemaName,
            String remoteTableName,
            StarRocksRelationType relationType,
            List<StarRocksRemoteColumn> columns,
            List<Map<String, Object>> rows)
    {
        public TableDefinition
        {
            requireNonNull(schemaTableName, "schemaTableName is null");
            requireNonNull(remoteSchemaName, "remoteSchemaName is null");
            requireNonNull(remoteTableName, "remoteTableName is null");
            requireNonNull(relationType, "relationType is null");
            columns = List.copyOf(requireNonNull(columns, "columns is null"));
            rows = List.copyOf(requireNonNull(rows, "rows is null").stream()
                    .map(LinkedHashMap::new)
                    .map(Collections::unmodifiableMap)
                    .toList());
        }

        private TestingTable toTestingTable()
        {
            return new TestingTable(schemaTableName, remoteSchemaName, remoteTableName, relationType, columns, rows);
        }
    }

    private record TestingTable(
            SchemaTableName schemaTableName,
            String remoteSchemaName,
            String remoteTableName,
            StarRocksRelationType relationType,
            List<StarRocksRemoteColumn> columns,
            List<Map<String, Object>> rows)
    {
        private TestingTable
        {
            requireNonNull(schemaTableName, "schemaTableName is null");
            requireNonNull(remoteSchemaName, "remoteSchemaName is null");
            requireNonNull(remoteTableName, "remoteTableName is null");
            requireNonNull(relationType, "relationType is null");
            columns = List.copyOf(requireNonNull(columns, "columns is null"));
            rows = List.copyOf(requireNonNull(rows, "rows is null"));
        }

        private StarRocksRemoteTable toRemoteTable()
        {
            return new StarRocksRemoteTable(schemaTableName, Optional.empty(), remoteSchemaName, remoteTableName, relationType, columns);
        }
    }

    private static final class TestingStarRocksMetadataClient
            implements StarRocksMetadataClient
    {
        private final Map<SchemaTableName, TestingTable> tables;

        private TestingStarRocksMetadataClient(Map<SchemaTableName, TestingTable> tables)
        {
            this.tables = requireNonNull(tables, "tables is null");
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session)
        {
            return tables.keySet().stream()
                    .map(SchemaTableName::getSchemaName)
                    .distinct()
                    .toList();
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
        {
            return tables.keySet().stream()
                    .filter(table -> schemaName.isEmpty() || table.getSchemaName().equals(schemaName.orElseThrow()))
                    .toList();
        }

        @Override
        public Optional<StarRocksRemoteTable> getTable(ConnectorSession session, SchemaTableName tableName)
        {
            return Optional.ofNullable(tables.get(tableName)).map(TestingTable::toRemoteTable);
        }

        @Override
        public OptionalLong getTableRowCount(ConnectorSession session, SchemaTableName tableName)
        {
            return Optional.ofNullable(tables.get(tableName))
                    .map(table -> OptionalLong.of(table.rows().size()))
                    .orElseGet(OptionalLong::empty);
        }
    }

    private static final class TestingStarRocksFlightSqlClient
            implements StarRocksFlightSqlClient
    {
        private final Map<SchemaTableName, TestingTable> tables;
        private final AtomicReference<Request> lastRequest;

        private TestingStarRocksFlightSqlClient(Map<SchemaTableName, TestingTable> tables, AtomicReference<Request> lastRequest)
        {
            this.tables = requireNonNull(tables, "tables is null");
            this.lastRequest = requireNonNull(lastRequest, "lastRequest is null");
        }

        @Override
        public List<StarRocksSplit> getSplits(ConnectorSession session, StarRocksTableHandle tableHandle, List<StarRocksColumnHandle> columns)
        {
            if (tableHandle.aggregation().isPresent() || tableHandle.limit().isPresent() || !tableHandle.sortOrder().isEmpty()) {
                return List.of(new StarRocksSplit());
            }
            return List.of(
                    new StarRocksSplit(Optional.of("0".getBytes(UTF_8))),
                    new StarRocksSplit(Optional.of("1".getBytes(UTF_8))));
        }

        @Override
        public StarRocksFlightSqlResult openStream(ConnectorSession session, StarRocksTableHandle tableHandle, StarRocksSplit split, List<StarRocksColumnHandle> columns)
        {
            lastRequest.set(new Request(tableHandle, columns));

            TestingTable table = tables.get(tableHandle.toSchemaTableName());
            List<Map<String, Object>> rows = table == null ? List.of() : applySplit(split, applyTableHandle(tableHandle, table.rows()));
            return createResult(table, columns, rows);
        }

        private static List<Map<String, Object>> applySplit(StarRocksSplit split, List<Map<String, Object>> rows)
        {
            if (split.partitionDescriptor().isEmpty()) {
                return rows;
            }

            int partition = Integer.parseInt(new String(split.partitionDescriptor().orElseThrow(), UTF_8));
            List<Map<String, Object>> partitionRows = new ArrayList<>();
            for (int index = 0; index < rows.size(); index++) {
                if (index % 2 == partition) {
                    partitionRows.add(rows.get(index));
                }
            }
            return List.copyOf(partitionRows);
        }

        private static List<Map<String, Object>> applyTableHandle(StarRocksTableHandle tableHandle, List<Map<String, Object>> rows)
        {
            List<Map<String, Object>> filteredRows = applyConstraint(tableHandle.constraint(), rows);
            List<Map<String, Object>> projectedRows = tableHandle.aggregation()
                    .map(aggregation -> applyAggregation(aggregation, filteredRows))
                    .orElse(filteredRows);
            List<Map<String, Object>> sortedRows = applySort(tableHandle.sortOrder(), projectedRows);
            if (tableHandle.limit().isEmpty()) {
                return sortedRows;
            }
            return sortedRows.stream()
                    .limit(tableHandle.limit().getAsLong())
                    .toList();
        }

        private static List<Map<String, Object>> applyConstraint(TupleDomain<StarRocksColumnHandle> constraint, List<Map<String, Object>> rows)
        {
            if (constraint.isNone()) {
                return List.of();
            }
            if (constraint.isAll() || constraint.getDomains().isEmpty()) {
                return rows;
            }

            return rows.stream()
                    .filter(row -> matchesConstraint(row, constraint.getDomains().orElseThrow()))
                    .toList();
        }

        private static boolean matchesConstraint(Map<String, Object> row, Map<StarRocksColumnHandle, Domain> domains)
        {
            for (Map.Entry<StarRocksColumnHandle, Domain> entry : domains.entrySet()) {
                StarRocksColumnHandle column = entry.getKey();
                if (!entry.getValue().includesNullableValue(toDomainValue(row.get(column.columnName()), column.columnType()))) {
                    return false;
                }
            }
            return true;
        }

        private static List<Map<String, Object>> applyAggregation(StarRocksAggregation aggregation, List<Map<String, Object>> rows)
        {
            Map<List<Object>, List<Map<String, Object>>> groups = rows.stream()
                    .collect(Collectors.groupingBy(
                            row -> aggregation.groupingColumns().stream()
                                    .map(column -> row.get(column.columnName()))
                                    .toList(),
                            LinkedHashMap::new,
                            Collectors.toList()));
            if (groups.isEmpty() && aggregation.groupingColumns().isEmpty()) {
                groups = Map.of(List.of(), List.of());
            }

            List<Map<String, Object>> results = new ArrayList<>(groups.size());
            for (Map.Entry<List<Object>, List<Map<String, Object>>> group : groups.entrySet()) {
                LinkedHashMap<String, Object> row = new LinkedHashMap<>();
                for (int index = 0; index < aggregation.groupingColumns().size(); index++) {
                    row.put(aggregation.groupingColumns().get(index).columnName(), group.getKey().get(index));
                }
                for (StarRocksAggregateColumn aggregateColumn : aggregation.aggregateColumns()) {
                    row.put(aggregateColumn.columnName(), evaluateAggregate(aggregateColumn, group.getValue()));
                }
                results.add(Collections.unmodifiableMap(row));
            }
            return List.copyOf(results);
        }

        private static Object evaluateAggregate(StarRocksAggregateColumn aggregateColumn, List<Map<String, Object>> rows)
        {
            String expression = aggregateColumn.expression();
            String normalized = expression.trim().toLowerCase(Locale.ENGLISH);
            if (normalized.equals("count(*)")) {
                return rows.size();
            }
            if (normalized.equals("count(null)")) {
                return 0L;
            }
            if (normalized.startsWith("count(`") && normalized.endsWith("`)")) {
                String columnName = quotedAggregationArgument(expression, "count");
                return rows.stream()
                        .filter(row -> row.get(columnName) != null)
                        .count();
            }
            if (normalized.startsWith("sum(`") && normalized.endsWith("`)")) {
                String columnName = quotedAggregationArgument(expression, "sum");
                BigDecimal sum = rows.stream()
                        .map(row -> row.get(columnName))
                        .filter(Objects::nonNull)
                        .map(TestingStarRocksFlightSqlClient::toBigDecimalValue)
                        .reduce(BigDecimal.ZERO, BigDecimal::add);
                if (aggregateColumn.type().getJavaType() == long.class) {
                    return sum.longValueExact();
                }
                if (aggregateColumn.type().getJavaType() == double.class) {
                    return sum.doubleValue();
                }
                return sum;
            }
            if (normalized.startsWith("avg(`") && normalized.endsWith("`)")) {
                String columnName = quotedAggregationArgument(expression, "avg");
                List<BigDecimal> values = rows.stream()
                        .map(row -> row.get(columnName))
                        .filter(Objects::nonNull)
                        .map(TestingStarRocksFlightSqlClient::toBigDecimalValue)
                        .toList();
                if (values.isEmpty()) {
                    return null;
                }
                BigDecimal sum = values.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
                BigDecimal average = sum.divide(BigDecimal.valueOf(values.size()), 12, RoundingMode.HALF_UP);
                if (aggregateColumn.type().getJavaType() == double.class) {
                    return average.doubleValue();
                }
                return average;
            }
            if ((normalized.startsWith("min(`") || normalized.startsWith("max(`")) && normalized.endsWith("`)")) {
                String functionName = normalized.startsWith("min(`") ? "min" : "max";
                String columnName = quotedAggregationArgument(expression, functionName);
                Comparator<Object> comparator = Comparator.comparing(TestingStarRocksFlightSqlClient::toComparableValue, TestingStarRocksFlightSqlClient::compareComparableValues);
                return rows.stream()
                        .map(row -> row.get(columnName))
                        .filter(Objects::nonNull)
                        .min(functionName.equals("min") ? comparator : comparator.reversed())
                        .orElse(null);
            }
            throw new IllegalArgumentException("Unsupported testing aggregate expression: " + expression);
        }

        private static String quotedAggregationArgument(String expression, String functionName)
        {
            return expression.substring((functionName + "(`").length(), expression.length() - "`)".length()).replace("``", "`");
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private static int compareComparableValues(Object left, Object right)
        {
            return ((Comparable) left).compareTo(right);
        }

        private static List<Map<String, Object>> applySort(List<StarRocksSortItem> sortOrder, List<Map<String, Object>> rows)
        {
            if (sortOrder.isEmpty()) {
                return rows;
            }

            Comparator<Map<String, Object>> comparator = (left, right) -> {
                for (StarRocksSortItem sortItem : sortOrder) {
                    int result = compareValues(left.get(sortItem.columnName()), right.get(sortItem.columnName()), sortItem);
                    if (result != 0) {
                        return result;
                    }
                }
                return 0;
            };
            return rows.stream()
                    .sorted(comparator)
                    .toList();
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private static int compareValues(Object left, Object right, StarRocksSortItem sortItem)
        {
            if (left == null || right == null) {
                if (left == right) {
                    return 0;
                }
                return (left == null) == sortItem.sortOrder().isNullsFirst() ? -1 : 1;
            }

            Object comparableLeft = toComparableValue(left);
            Object comparableRight = toComparableValue(right);
            int result = ((Comparable) comparableLeft).compareTo(comparableRight);
            return sortItem.sortOrder().isAscending() ? result : -result;
        }

        private static Object toComparableValue(Object value)
        {
            if (value instanceof Slice slice) {
                return slice.toStringUtf8();
            }
            return value;
        }

        private static Object toDomainValue(Object value, Type type)
        {
            if (value == null) {
                return null;
            }
            if (type == BOOLEAN) {
                return toBooleanValue(value);
            }
            if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT) {
                return toLong(value);
            }
            if (type == REAL) {
                return (long) floatToRawIntBits(((Number) value).floatValue());
            }
            if (type == DOUBLE) {
                return ((Number) value).doubleValue();
            }
            if (type == DATE) {
                return toEpochDay(value);
            }
            if (type instanceof TimestampType timestampType && timestampType.isShort()) {
                return toTimestampMicros(value);
            }
            if (type instanceof DecimalType decimalType) {
                BigDecimal decimal = toBigDecimalValue(value);
                if (decimalType.isShort()) {
                    return encodeShortScaledValue(decimal, decimalType.getScale());
                }
            }
            if (type instanceof VarcharType || type instanceof CharType || type.getBaseName().equals(JSON)) {
                if (value instanceof Slice slice) {
                    return slice;
                }
                return utf8Slice(value.toString());
            }
            return value;
        }

        private static long toTimestampMicros(Object value)
        {
            if (value instanceof LocalDateTime dateTime) {
                return dateTime.toEpochSecond(UTC) * MICROSECONDS_PER_SECOND + dateTime.getLong(ChronoField.MICRO_OF_SECOND);
            }
            String text = value.toString().trim().replace(' ', 'T');
            LocalDateTime dateTime = LocalDateTime.parse(text);
            return dateTime.toEpochSecond(UTC) * MICROSECONDS_PER_SECOND + dateTime.getLong(ChronoField.MICRO_OF_SECOND);
        }

        private static StarRocksFlightSqlResult createResult(TestingTable table, List<StarRocksColumnHandle> columns, List<Map<String, Object>> rows)
        {
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            List<FieldVector> vectors = new ArrayList<>(columns.size());
            Map<String, StarRocksRemoteColumn> remoteColumns = table == null ? Map.of() : table.columns().stream()
                                                                                          .collect(Collectors.toUnmodifiableMap(StarRocksRemoteColumn::columnName, column -> column));

            try {
                for (StarRocksColumnHandle column : columns) {
                    vectors.add(createVector(column, remoteColumns.get(column.columnName()), rows, allocator));
                }

                VectorSchemaRoot root = new VectorSchemaRoot(vectors);
                root.setRowCount(rows.size());
                return new TestingStarRocksFlightSqlResult(allocator, root);
            }
            catch (RuntimeException e) {
                vectors.forEach(FieldVector::close);
                allocator.close();
                throw e;
            }
        }

        private static FieldVector createVector(StarRocksColumnHandle column, StarRocksRemoteColumn remoteColumn, List<Map<String, Object>> rows, RootAllocator allocator)
        {
            String name = column.columnName();
            Type type = column.columnType();
            String remoteType = remoteColumn == null ? "" : normalizeTypeName(remoteColumn.typeName());

            if (type == BOOLEAN) {
                if (remoteType.equals("TINYINT")) {
                    TinyIntVector vector = new TinyIntVector(name, allocator);
                    vector.allocateNew(rows.size());
                    for (int i = 0; i < rows.size(); i++) {
                        Object value = rows.get(i).get(name);
                        if (value == null) {
                            vector.setNull(i);
                        }
                        else {
                            vector.setSafe(i, toBooleanValue(value) ? 1 : 0);
                        }
                    }
                    vector.setValueCount(rows.size());
                    return vector;
                }

                BitVector vector = new BitVector(name, allocator);
                vector.allocateNew(rows.size());
                for (int i = 0; i < rows.size(); i++) {
                    Object value = rows.get(i).get(name);
                    if (value == null) {
                        vector.setNull(i);
                    }
                    else {
                        vector.setSafe(i, toBooleanValue(value) ? 1 : 0);
                    }
                }
                vector.setValueCount(rows.size());
                return vector;
            }
            if (type == TINYINT) {
                TinyIntVector vector = new TinyIntVector(name, allocator);
                vector.allocateNew(rows.size());
                for (int i = 0; i < rows.size(); i++) {
                    Object value = rows.get(i).get(name);
                    if (value == null) {
                        vector.setNull(i);
                    }
                    else {
                        vector.setSafe(i, toIntExact(toLong(value)));
                    }
                }
                vector.setValueCount(rows.size());
                return vector;
            }
            if (type == SMALLINT) {
                SmallIntVector vector = new SmallIntVector(name, allocator);
                vector.allocateNew(rows.size());
                for (int i = 0; i < rows.size(); i++) {
                    Object value = rows.get(i).get(name);
                    if (value == null) {
                        vector.setNull(i);
                    }
                    else {
                        vector.setSafe(i, toIntExact(toLong(value)));
                    }
                }
                vector.setValueCount(rows.size());
                return vector;
            }
            if (type == BIGINT) {
                BigIntVector vector = new BigIntVector(name, allocator);
                vector.allocateNew(rows.size());
                for (int i = 0; i < rows.size(); i++) {
                    Object value = rows.get(i).get(name);
                    if (value == null) {
                        vector.setNull(i);
                    }
                    else {
                        vector.setSafe(i, toLong(value));
                    }
                }
                vector.setValueCount(rows.size());
                return vector;
            }
            if (type == INTEGER) {
                IntVector vector = new IntVector(name, allocator);
                vector.allocateNew(rows.size());
                for (int i = 0; i < rows.size(); i++) {
                    Object value = rows.get(i).get(name);
                    if (value == null) {
                        vector.setNull(i);
                    }
                    else {
                        vector.setSafe(i, toIntExact(toLong(value)));
                    }
                }
                vector.setValueCount(rows.size());
                return vector;
            }
            if (type == REAL) {
                Float4Vector vector = new Float4Vector(name, allocator);
                vector.allocateNew(rows.size());
                for (int i = 0; i < rows.size(); i++) {
                    Object value = rows.get(i).get(name);
                    if (value == null) {
                        vector.setNull(i);
                    }
                    else {
                        vector.setSafe(i, ((Number) value).floatValue());
                    }
                }
                vector.setValueCount(rows.size());
                return vector;
            }
            if (type == DOUBLE) {
                Float8Vector vector = new Float8Vector(name, allocator);
                vector.allocateNew(rows.size());
                for (int i = 0; i < rows.size(); i++) {
                    Object value = rows.get(i).get(name);
                    if (value == null) {
                        vector.setNull(i);
                    }
                    else {
                        vector.setSafe(i, ((Number) value).doubleValue());
                    }
                }
                vector.setValueCount(rows.size());
                return vector;
            }
            if (type == DATE) {
                if (rows.stream()
                        .map(row -> row.get(name))
                        .filter(Objects::nonNull)
                        .anyMatch(value -> value instanceof LocalDate || value instanceof CharSequence)) {
                    VarCharVector vector = new VarCharVector(name, allocator);
                    vector.allocateNew();
                    for (int i = 0; i < rows.size(); i++) {
                        Object value = rows.get(i).get(name);
                        if (value == null) {
                            vector.setNull(i);
                        }
                        else {
                            vector.setSafe(i, toTextBytes(value));
                        }
                    }
                    vector.setValueCount(rows.size());
                    return vector;
                }

                DateDayVector vector = new DateDayVector(name, allocator);
                vector.allocateNew(rows.size());
                for (int i = 0; i < rows.size(); i++) {
                    Object value = rows.get(i).get(name);
                    if (value == null) {
                        vector.setNull(i);
                    }
                    else {
                        vector.setSafe(i, toIntExact(toEpochDay(value)));
                    }
                }
                vector.setValueCount(rows.size());
                return vector;
            }
            if (type instanceof TimestampType timestampType && timestampType.isShort()) {
                VarCharVector vector = new VarCharVector(name, allocator);
                vector.allocateNew();
                for (int i = 0; i < rows.size(); i++) {
                    Object value = rows.get(i).get(name);
                    if (value == null) {
                        vector.setNull(i);
                    }
                    else {
                        vector.setSafe(i, toTextBytes(value));
                    }
                }
                vector.setValueCount(rows.size());
                return vector;
            }
            if (type instanceof DecimalType decimalType) {
                DecimalVector vector = new DecimalVector(name, allocator, decimalType.getPrecision(), decimalType.getScale());
                vector.allocateNew(rows.size());
                for (int i = 0; i < rows.size(); i++) {
                    Object value = rows.get(i).get(name);
                    if (value == null) {
                        vector.setNull(i);
                    }
                    else {
                        vector.setSafe(i, toBigDecimalValue(value));
                    }
                }
                vector.setValueCount(rows.size());
                return vector;
            }
            if (type instanceof VarcharType || type instanceof CharType || type.getBaseName().equals(JSON)) {
                VarCharVector vector = new VarCharVector(name, allocator);
                vector.allocateNew();
                for (int i = 0; i < rows.size(); i++) {
                    Object value = rows.get(i).get(name);
                    if (value == null) {
                        vector.setNull(i);
                    }
                    else {
                        vector.setSafe(i, type.getBaseName().equals(JSON) ? toJsonBytes(value) : toTextBytes(value));
                    }
                }
                vector.setValueCount(rows.size());
                return vector;
            }
            if (type == VARBINARY) {
                VarBinaryVector vector = new VarBinaryVector(name, allocator);
                vector.allocateNew();
                for (int i = 0; i < rows.size(); i++) {
                    Object value = rows.get(i).get(name);
                    if (value == null) {
                        vector.setNull(i);
                    }
                    else {
                        vector.setSafe(i, toBinaryBytes(value));
                    }
                }
                vector.setValueCount(rows.size());
                return vector;
            }

            throw new IllegalArgumentException("Unsupported testing column type: " + type);
        }

        private static boolean toBooleanValue(Object value)
        {
            if (value instanceof Boolean booleanValue) {
                return booleanValue;
            }
            if (value instanceof Number number) {
                return number.longValue() != 0;
            }
            return Boolean.parseBoolean(value.toString());
        }

        private static long toEpochDay(Object value)
        {
            if (value instanceof LocalDate date) {
                return date.toEpochDay();
            }
            return toLong(value);
        }

        private static long toLong(Object value)
        {
            return ((Number) value).longValue();
        }

        private static BigDecimal toBigDecimalValue(Object value)
        {
            if (value instanceof BigDecimal decimal) {
                return decimal;
            }
            if (value instanceof BigInteger integer) {
                return new BigDecimal(integer);
            }
            if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
                return BigDecimal.valueOf(((Number) value).longValue());
            }
            if (value instanceof Number number) {
                return BigDecimal.valueOf(number.doubleValue());
            }
            return new BigDecimal(value.toString());
        }

        private static byte[] toTextBytes(Object value)
        {
            if (value instanceof Slice slice) {
                return slice.getBytes();
            }
            if (value instanceof BigDecimal decimal) {
                return decimal.toPlainString().getBytes(UTF_8);
            }
            if (value instanceof LocalDateTime dateTime) {
                return dateTime.toString().replace('T', ' ').getBytes(UTF_8);
            }
            return value.toString().getBytes(UTF_8);
        }

        private static byte[] toJsonBytes(Object value)
        {
            if (value instanceof Slice slice) {
                return slice.getBytes();
            }
            if (value instanceof CharSequence) {
                return value.toString().getBytes(UTF_8);
            }
            return JSON_CODEC.toJson(value).getBytes(UTF_8);
        }

        private static byte[] toBinaryBytes(Object value)
        {
            if (value instanceof byte[] bytes) {
                return bytes;
            }
            if (value instanceof Slice slice) {
                return slice.getBytes();
            }
            return value.toString().getBytes(UTF_8);
        }

        private static String normalizeTypeName(String dataType)
        {
            return dataType.trim()
                    .toUpperCase(Locale.ENGLISH)
                    .replace(' ', '_');
        }
    }

    private static final class TestingStarRocksFlightSqlResult
            implements StarRocksFlightSqlResult
    {
        private final RootAllocator allocator;
        private final VectorSchemaRoot root;
        private boolean loaded;
        private boolean closed;

        private TestingStarRocksFlightSqlResult(RootAllocator allocator, VectorSchemaRoot root)
        {
            this.allocator = requireNonNull(allocator, "allocator is null");
            this.root = requireNonNull(root, "root is null");
        }

        @Override
        public boolean loadNextBatch()
        {
            if (closed || loaded || root.getRowCount() == 0) {
                return false;
            }
            loaded = true;
            return true;
        }

        @Override
        public VectorSchemaRoot getVectorSchemaRoot()
        {
            return root;
        }

        @Override
        public long getMemoryUsage()
        {
            return root.getFieldVectors().stream()
                    .mapToLong(FieldVector::getBufferSize)
                    .sum();
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            root.close();
            allocator.close();
        }
    }
}
