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
package io.trino.plugin.doris;

import com.google.inject.Module;
import io.airlift.slice.Slice;
import io.trino.plugin.tpch.DecimalTypeMapping;
import io.trino.plugin.tpch.TpchMetadata;
import io.trino.plugin.tpch.TpchRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

final class TestingDorisEnvironment
{
    private static final String TPCH_SCHEMA = TpchMetadata.TINY_SCHEMA_NAME;
    private static final double TPCH_SCALE_FACTOR = TpchMetadata.TINY_SCALE_FACTOR;

    private final Map<SchemaTableName, TestingTable> tables;
    private final AtomicReference<Request> lastRequest = new AtomicReference<>();

    public TestingDorisEnvironment()
    {
        this(List.of());
    }

    public TestingDorisEnvironment(List<TableDefinition> additionalTables)
    {
        this.tables = createTables(additionalTables);
    }

    public Module createModule()
    {
        TestingDorisMetadataClient metadataClient = new TestingDorisMetadataClient(tables);
        TestingDorisSplitPlanner splitPlanner = new TestingDorisSplitPlanner();
        TestingDorisFlightSqlClient flightSqlClient = new TestingDorisFlightSqlClient(tables, lastRequest);

        return binder -> {
            binder.bind(DorisMetadataClient.class).toInstance(metadataClient);
            binder.bind(DorisSplitPlanner.class).toInstance(splitPlanner);
            binder.bind(DorisFlightSqlClient.class).toInstance(flightSqlClient);
        };
    }

    public void clearLastRequest()
    {
        lastRequest.set(null);
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
        return Map.copyOf(tableMap);
    }

    // Reuse the canonical TPCH tiny dataset so BaseConnectorTest sees the values it expects.
    private static <E extends TpchEntity> TestingTable createTpchTable(TpchTable<E> table)
    {
        List<TpchColumn<E>> columns = table.getColumns();
        List<DorisRemoteColumn> remoteColumns = new ArrayList<>(columns.size());
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
                remoteColumns,
                rows);
    }

    private static <E extends TpchEntity> DorisRemoteColumn toRemoteColumn(TpchColumn<E> column, int ordinalPosition)
    {
        String columnName = column.getSimplifiedColumnName();
        return switch (column.getType().getBase()) {
            case IDENTIFIER -> new DorisRemoteColumn(columnName, "BIGINT", Optional.of(20), Optional.empty(), ordinalPosition);
            case INTEGER -> new DorisRemoteColumn(columnName, "INT", Optional.of(11), Optional.empty(), ordinalPosition);
            case DATE -> new DorisRemoteColumn(columnName, "DATEV2", Optional.empty(), Optional.empty(), ordinalPosition);
            case DOUBLE -> new DorisRemoteColumn(columnName, "DOUBLE", Optional.empty(), Optional.empty(), ordinalPosition);
            case VARCHAR -> new DorisRemoteColumn(
                    columnName,
                    "VARCHAR",
                    column.getType().getPrecision().map(Math::toIntExact),
                    Optional.empty(),
                    ordinalPosition);
        };
    }

    private static Object getCursorValue(RecordCursor cursor, int field, Type columnType)
    {
        if (cursor.isNull(field)) {
            return null;
        }
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

    public record Request(DorisTableHandle tableHandle, List<DorisColumnHandle> requestedColumns)
    {
        public Request
        {
            requireNonNull(tableHandle, "tableHandle is null");
            requestedColumns = List.copyOf(requireNonNull(requestedColumns, "requestedColumns is null"));
        }
    }

    public record TableDefinition(SchemaTableName schemaTableName, List<DorisRemoteColumn> columns, List<Map<String, Object>> rows)
    {
        public TableDefinition
        {
            requireNonNull(schemaTableName, "schemaTableName is null");
            columns = List.copyOf(requireNonNull(columns, "columns is null"));
            rows = List.copyOf(requireNonNull(rows, "rows is null").stream()
                    .map(LinkedHashMap::new)
                    .map(Collections::unmodifiableMap)
                    .toList());
        }

        private TestingTable toTestingTable()
        {
            return new TestingTable(schemaTableName, columns, rows);
        }
    }

    private record TestingTable(SchemaTableName schemaTableName, List<DorisRemoteColumn> columns, List<Map<String, Object>> rows)
    {
        private TestingTable
        {
            requireNonNull(schemaTableName, "schemaTableName is null");
            columns = List.copyOf(requireNonNull(columns, "columns is null"));
            rows = List.copyOf(requireNonNull(rows, "rows is null"));
        }

        private DorisRemoteTable toRemoteTable()
        {
            return new DorisRemoteTable(schemaTableName, columns);
        }
    }

    private static final class TestingDorisMetadataClient
            implements DorisMetadataClient
    {
        private final Map<SchemaTableName, TestingTable> tables;

        private TestingDorisMetadataClient(Map<SchemaTableName, TestingTable> tables)
        {
            this.tables = requireNonNull(tables, "tables is null");
        }

        @Override
        public List<String> listSchemaNames()
        {
            return tables.keySet().stream()
                    .map(SchemaTableName::getSchemaName)
                    .distinct()
                    .toList();
        }

        @Override
        public List<SchemaTableName> listTables(Optional<String> schemaName)
        {
            return tables.keySet().stream()
                    .filter(table -> schemaName.isEmpty() || table.getSchemaName().equals(schemaName.get()))
                    .toList();
        }

        @Override
        public Optional<DorisRemoteTable> getTable(SchemaTableName tableName)
        {
            return Optional.ofNullable(tables.get(tableName)).map(TestingTable::toRemoteTable);
        }

        @Override
        public OptionalLong getTableRowCount(SchemaTableName tableName)
        {
            return Optional.ofNullable(tables.get(tableName))
                    .map(table -> OptionalLong.of(table.rows().size()))
                    .orElseGet(OptionalLong::empty);
        }
    }

    private static final class TestingDorisSplitPlanner
            implements DorisSplitPlanner
    {
        @Override
        public List<DorisSplit> planSplits(DorisTableHandle tableHandle)
        {
            return List.of(new DorisSplit(
                    tableHandle.schemaName(),
                    tableHandle.tableName(),
                    "127.0.0.1:9060",
                    List.of(),
                    Optional.empty()));
        }
    }

    private static final class TestingDorisFlightSqlClient
            implements DorisFlightSqlClient
    {
        private final Map<SchemaTableName, TestingTable> tables;
        private final AtomicReference<Request> lastRequest;

        private TestingDorisFlightSqlClient(Map<SchemaTableName, TestingTable> tables, AtomicReference<Request> lastRequest)
        {
            this.tables = requireNonNull(tables, "tables is null");
            this.lastRequest = requireNonNull(lastRequest, "lastRequest is null");
        }

        @Override
        public DorisFlightSqlResult openStream(DorisTableHandle tableHandle, DorisSplit split, List<DorisColumnHandle> columns)
        {
            lastRequest.set(new Request(tableHandle, columns));

            TestingTable table = tables.get(tableHandle.toSchemaTableName());
            List<Map<String, Object>> rows = table == null ? List.of() : table.rows();
            return createResult(table, columns, rows);
        }

        private static DorisFlightSqlResult createResult(TestingTable table, List<DorisColumnHandle> columns, List<Map<String, Object>> rows)
        {
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            List<FieldVector> vectors = new ArrayList<>(columns.size());
            Map<String, DorisRemoteColumn> remoteColumns = table == null ? Map.of() : table.columns().stream()
                                                                                      .collect(Collectors.toUnmodifiableMap(DorisRemoteColumn::columnName, column -> column));

            try {
                for (DorisColumnHandle column : columns) {
                    vectors.add(createVector(column, remoteColumns.get(column.columnName()), rows, allocator));
                }

                VectorSchemaRoot root = new VectorSchemaRoot(vectors);
                root.setRowCount(rows.size());
                return new TestingDorisFlightSqlResult(allocator, root);
            }
            catch (RuntimeException e) {
                vectors.forEach(FieldVector::close);
                allocator.close();
                throw e;
            }
        }

        private static FieldVector createVector(DorisColumnHandle column, DorisRemoteColumn remoteColumn, List<Map<String, Object>> rows, RootAllocator allocator)
        {
            String name = column.columnName();
            Type type = column.columnType();
            String remoteType = remoteColumn == null ? "" : normalizeTypeName(remoteColumn.dataType());

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
                        vector.setSafe(i, (long) value);
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
                        vector.setSafe(i, toIntExact((long) value));
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
                        vector.setSafe(i, (double) value);
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
                        vector.setSafe(i, toIntExact((long) value));
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
            if (type instanceof VarcharType || type instanceof CharType) {
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
            return value.toString().getBytes(UTF_8);
        }

        private static String normalizeTypeName(String dataType)
        {
            return dataType.trim()
                    .toUpperCase(Locale.ENGLISH)
                    .replace(' ', '_');
        }
    }

    private static final class TestingDorisFlightSqlResult
            implements DorisFlightSqlResult
    {
        private final RootAllocator allocator;
        private final VectorSchemaRoot root;
        private boolean loaded;
        private boolean closed;

        private TestingDorisFlightSqlResult(RootAllocator allocator, VectorSchemaRoot root)
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
