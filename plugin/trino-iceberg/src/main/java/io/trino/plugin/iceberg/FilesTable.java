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
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.base.util.JsonUtils;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import jakarta.annotation.Nullable;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetricsUtil.ReadableColMetricsStruct;
import org.apache.iceberg.MetricsUtil.ReadableMetricsStruct;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Streams.mapWithIndex;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionColumnType;
import static io.trino.plugin.iceberg.IcebergUtil.partitionTypes;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataTableType.FILES;
import static org.apache.iceberg.MetadataTableUtils.createMetadataTableInstance;

public class FilesTable
        implements SystemTable
{
    private static final JsonFactory JSON_FACTORY = JsonUtils.jsonFactoryBuilder().build();

    private static final String CONTENT_COLUMN_NAME = "content";
    private static final String FILE_PATH_COLUMN_NAME = "file_path";
    private static final String FILE_FORMAT_COLUMN_NAME = "file_format";
    private static final String SPEC_ID_COLUMN_NAME = "spec_id";
    private static final String PARTITION_COLUMN_NAME = "partition";
    private static final String RECORD_COUNT_COLUMN_NAME = "record_count";
    private static final String FILE_SIZE_IN_BYTES_COLUMN_NAME = "file_size_in_bytes";
    private static final String COLUMN_SIZES_COLUMN_NAME = "column_sizes";
    private static final String VALUE_COUNTS_COLUMN_NAME = "value_counts";
    private static final String NULL_VALUE_COUNTS_COLUMN_NAME = "null_value_counts";
    private static final String NAN_VALUE_COUNTS_COLUMN_NAME = "nan_value_counts";
    private static final String LOWER_BOUNDS_COLUMN_NAME = "lower_bounds";
    private static final String UPPER_BOUNDS_COLUMN_NAME = "upper_bounds";
    private static final String KEY_METADATA_COLUMN_NAME = "key_metadata";
    private static final String SPLIT_OFFSETS_COLUMN_NAME = "split_offsets";
    private static final String EQUALITY_IDS_COLUMN_NAME = "equality_ids";
    private static final String SORT_ORDER_ID_COLUMN_NAME = "sort_order_id";
    private static final String READABLE_METRICS_COLUMN_NAME = "readable_metrics";
    private final ConnectorTableMetadata tableMetadata;
    private final TypeManager typeManager;
    private final Table icebergTable;
    private final Optional<Long> snapshotId;
    private final Optional<IcebergPartitionColumn> partitionColumnType;
    private final Map<Integer, Type.PrimitiveType> idToPrimitiveTypeMapping;
    private final List<Types.NestedField> primitiveFields;
    private final ExecutorService executor;

    public FilesTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable, Optional<Long> snapshotId, ExecutorService executor)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        List<PartitionField> partitionFields = PartitionsTable.getAllPartitionFields(icebergTable);
        partitionColumnType = getPartitionColumnType(partitionFields, icebergTable.schema(), typeManager);
        idToPrimitiveTypeMapping = IcebergUtil.primitiveFieldTypes(icebergTable.schema());
        primitiveFields = IcebergUtil.primitiveFields(icebergTable.schema()).stream()
                .sorted(Comparator.comparing(Types.NestedField::name))
                .collect(toImmutableList());

        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        columns.add(new ColumnMetadata(CONTENT_COLUMN_NAME, INTEGER));
        columns.add(new ColumnMetadata(FILE_PATH_COLUMN_NAME, VARCHAR));
        columns.add(new ColumnMetadata(FILE_FORMAT_COLUMN_NAME, VARCHAR));
        columns.add(new ColumnMetadata(SPEC_ID_COLUMN_NAME, INTEGER));
        partitionColumnType.ifPresent(type -> columns.add(new ColumnMetadata(PARTITION_COLUMN_NAME, type.rowType())));
        columns.add(new ColumnMetadata(RECORD_COUNT_COLUMN_NAME, BIGINT));
        columns.add(new ColumnMetadata(FILE_SIZE_IN_BYTES_COLUMN_NAME, BIGINT));
        columns.add(new ColumnMetadata(COLUMN_SIZES_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))));
        columns.add(new ColumnMetadata(VALUE_COUNTS_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))));
        columns.add(new ColumnMetadata(NULL_VALUE_COUNTS_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))));
        columns.add(new ColumnMetadata(NAN_VALUE_COUNTS_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), BIGINT.getTypeSignature()))));
        columns.add(new ColumnMetadata(LOWER_BOUNDS_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()))));
        columns.add(new ColumnMetadata(UPPER_BOUNDS_COLUMN_NAME, typeManager.getType(mapType(INTEGER.getTypeSignature(), VARCHAR.getTypeSignature()))));
        columns.add(new ColumnMetadata(KEY_METADATA_COLUMN_NAME, VARBINARY));
        columns.add(new ColumnMetadata(SPLIT_OFFSETS_COLUMN_NAME, new ArrayType(BIGINT)));
        columns.add(new ColumnMetadata(EQUALITY_IDS_COLUMN_NAME, new ArrayType(INTEGER)));
        columns.add(new ColumnMetadata(SORT_ORDER_ID_COLUMN_NAME, INTEGER));
        columns.add(new ColumnMetadata(READABLE_METRICS_COLUMN_NAME, typeManager.getType(new TypeSignature(JSON))));

        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), columns.build());
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        List<io.trino.spi.type.Type> types = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        if (snapshotId.isEmpty()) {
            return InMemoryRecordSet.builder(types).build().cursor();
        }

        Map<Integer, Type> idToTypeMapping = getIcebergIdToTypeMapping(icebergTable.schema());
        TableScan tableScan = createMetadataTableInstance(icebergTable, FILES)
                .newScan()
                .useSnapshot(snapshotId.get())
                .includeColumnStats()
                .planWith(executor);

        Map<String, Integer> columnNameToPosition = mapWithIndex(tableScan.schema().columns().stream(),
                (column, position) -> immutableEntry(column.name(), Long.valueOf(position).intValue()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        PlanFilesIterable planFilesIterable = new PlanFilesIterable(
                tableScan.planFiles(),
                primitiveFields,
                idToTypeMapping,
                types,
                columnNameToPosition,
                typeManager,
                partitionColumnType,
                PartitionsTable.getAllPartitionFields(icebergTable),
                idToPrimitiveTypeMapping);
        return planFilesIterable.cursor();
    }

    private static class PlanFilesIterable
            extends CloseableGroup
            implements Iterable<List<Object>>
    {
        private final CloseableIterable<FileScanTask> planFiles;
        private final List<Types.NestedField> primitiveFields;
        private final Map<Integer, Type> idToTypeMapping;
        private final List<io.trino.spi.type.Type> types;
        private final Map<String, Integer> columnNameToPosition;
        private boolean closed;
        private final MapType integerToBigintMapType;
        private final MapType integerToVarcharMapType;
        private final Optional<IcebergPartitionColumn> partitionColumnType;
        private final List<PartitionField> partitionFields;
        private final Map<Integer, Type.PrimitiveType> idToPrimitiveTypeMapping;

        public PlanFilesIterable(
                CloseableIterable<FileScanTask> planFiles,
                List<Types.NestedField> primitiveFields,
                Map<Integer, Type> idToTypeMapping,
                List<io.trino.spi.type.Type> types,
                Map<String, Integer> columnNameToPosition,
                TypeManager typeManager,
                Optional<IcebergPartitionColumn> partitionColumnType,
                List<PartitionField> partitionFields,
                Map<Integer, Type.PrimitiveType> idToPrimitiveTypeMapping)
        {
            this.planFiles = requireNonNull(planFiles, "planFiles is null");
            this.primitiveFields = ImmutableList.copyOf(requireNonNull(primitiveFields, "primitiveFields is null"));
            this.idToTypeMapping = ImmutableMap.copyOf(requireNonNull(idToTypeMapping, "idToTypeMapping is null"));
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.columnNameToPosition = ImmutableMap.copyOf(requireNonNull(columnNameToPosition, "columnNameToPosition is null"));
            this.integerToBigintMapType = new MapType(INTEGER, BIGINT, typeManager.getTypeOperators());
            this.integerToVarcharMapType = new MapType(INTEGER, VARCHAR, typeManager.getTypeOperators());
            this.partitionColumnType = requireNonNull(partitionColumnType, "partitionColumnType is null");
            this.partitionFields = ImmutableList.copyOf(requireNonNull(partitionFields, "partitionFields is null"));
            this.idToPrimitiveTypeMapping = ImmutableMap.copyOf(requireNonNull(idToPrimitiveTypeMapping, "idToPrimitiveTypeMapping is null"));
            addCloseable(planFiles);
        }

        public RecordCursor cursor()
        {
            CloseableIterator<List<Object>> iterator = this.iterator();
            return new InMemoryRecordSet.InMemoryRecordCursor(types, iterator)
            {
                @Override
                public void close()
                {
                    try (iterator) {
                        super.close();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException("Failed to close cursor", e);
                    }
                }
            };
        }

        @Override
        public CloseableIterator<List<Object>> iterator()
        {
            final CloseableIterator<FileScanTask> planFilesIterator = planFiles.iterator();
            addCloseable(planFilesIterator);

            return new CloseableIterator<>()
            {
                private CloseableIterator<StructLike> currentIterator = CloseableIterator.empty();

                @Override
                public boolean hasNext()
                {
                    updateCurrentIterator();
                    return !closed && currentIterator.hasNext();
                }

                @Override
                public List<Object> next()
                {
                    updateCurrentIterator();
                    return getRecord(currentIterator.next());
                }

                private void updateCurrentIterator()
                {
                    try {
                        while (!closed && !currentIterator.hasNext() && planFilesIterator.hasNext()) {
                            currentIterator.close();
                            DataTask dataTask = (DataTask) planFilesIterator.next();
                            currentIterator = dataTask.rows().iterator();
                        }
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }

                @Override
                public void close()
                        throws IOException
                {
                    currentIterator.close();
                    FilesTable.PlanFilesIterable.super.close();
                    closed = true;
                }
            };
        }

        private List<Object> getRecord(StructLike structLike)
        {
            List<Object> columns = new ArrayList<>();
            columns.add(structLike.get(columnNameToPosition.get(CONTENT_COLUMN_NAME), Integer.class));
            columns.add(structLike.get(columnNameToPosition.get(FILE_PATH_COLUMN_NAME), String.class));
            columns.add(structLike.get(columnNameToPosition.get(FILE_FORMAT_COLUMN_NAME), String.class));
            columns.add(structLike.get(columnNameToPosition.get(SPEC_ID_COLUMN_NAME), Integer.class));

            if (columnNameToPosition.containsKey(PARTITION_COLUMN_NAME)) {
                List<Type> partitionTypes = partitionTypes(partitionFields, idToPrimitiveTypeMapping);
                StructLike partitionStruct = structLike.get(columnNameToPosition.get(PARTITION_COLUMN_NAME), org.apache.iceberg.PartitionData.class);
                List<io.trino.spi.type.Type> partitionColumnTypes = partitionColumnType.orElseThrow().rowType().getFields().stream()
                        .map(RowType.Field::getType)
                        .collect(toImmutableList());
                if (partitionStruct != null) {
                    columns.add(buildRowValue(
                            partitionColumnType.get().rowType(),
                            fields -> {
                                for (int i = 0; i < partitionColumnTypes.size(); i++) {
                                    Type type = partitionTypes.get(i);
                                    io.trino.spi.type.Type trinoType = partitionColumnType.get().rowType().getFields().get(i).getType();
                                    Object value = null;
                                    Integer fieldId = partitionColumnType.get().fieldIds().get(i);
                                    if (fieldId != null) {
                                        value = convertIcebergValueToTrino(type, partitionStruct.get(i, type.typeId().javaClass()));
                                    }
                                    writeNativeValue(trinoType, fields.get(i), value);
                                }
                            }));
                }
            }

            columns.add(structLike.get(columnNameToPosition.get(RECORD_COUNT_COLUMN_NAME), Long.class));
            columns.add(structLike.get(columnNameToPosition.get(FILE_SIZE_IN_BYTES_COLUMN_NAME), Long.class));
            columns.add(getIntegerBigintSqlMap(structLike.get(columnNameToPosition.get(COLUMN_SIZES_COLUMN_NAME), Map.class)));
            columns.add(getIntegerBigintSqlMap(structLike.get(columnNameToPosition.get(VALUE_COUNTS_COLUMN_NAME), Map.class)));
            columns.add(getIntegerBigintSqlMap(structLike.get(columnNameToPosition.get(NULL_VALUE_COUNTS_COLUMN_NAME), Map.class)));
            columns.add(getIntegerBigintSqlMap(structLike.get(columnNameToPosition.get(NAN_VALUE_COUNTS_COLUMN_NAME), Map.class)));
            columns.add(getIntegerVarcharSqlMap(structLike.get(columnNameToPosition.get(LOWER_BOUNDS_COLUMN_NAME), Map.class)));
            columns.add(getIntegerVarcharSqlMap(structLike.get(columnNameToPosition.get(UPPER_BOUNDS_COLUMN_NAME), Map.class)));
            columns.add(toVarbinarySlice(structLike.get(columnNameToPosition.get(KEY_METADATA_COLUMN_NAME), ByteBuffer.class)));
            columns.add(toBigintArrayBlock(structLike.get(columnNameToPosition.get(SPLIT_OFFSETS_COLUMN_NAME), List.class)));
            columns.add(toIntegerArrayBlock(structLike.get(columnNameToPosition.get(EQUALITY_IDS_COLUMN_NAME), List.class)));
            columns.add(structLike.get(columnNameToPosition.get(SORT_ORDER_ID_COLUMN_NAME), Integer.class));

            ReadableMetricsStruct readableMetrics = structLike.get(columnNameToPosition.get(READABLE_METRICS_COLUMN_NAME), ReadableMetricsStruct.class);
            columns.add(toJson(readableMetrics, primitiveFields));

            checkArgument(columns.size() == types.size(), "Expected %s types in row, but got %s values", types.size(), columns.size());
            return columns;
        }

        private SqlMap getIntegerBigintSqlMap(Map<Integer, Long> value)
        {
            if (value == null) {
                return null;
            }
            return toIntegerBigintSqlMap(value);
        }

        private SqlMap getIntegerVarcharSqlMap(Map<Integer, ByteBuffer> value)
        {
            if (value == null) {
                return null;
            }
            return toIntegerVarcharSqlMap(
                    value.entrySet().stream()
                            .filter(entry -> idToTypeMapping.containsKey(entry.getKey()))
                            .collect(toImmutableMap(
                                    Map.Entry<Integer, ByteBuffer>::getKey,
                                    entry -> Transforms.identity().toHumanString(
                                            idToTypeMapping.get(entry.getKey()), Conversions.fromByteBuffer(idToTypeMapping.get(entry.getKey()), entry.getValue())))));
        }

        private SqlMap toIntegerBigintSqlMap(Map<Integer, Long> values)
        {
            return buildMapValue(
                    integerToBigintMapType,
                    values.size(),
                    (keyBuilder, valueBuilder) -> values.forEach((key, value) -> {
                        INTEGER.writeLong(keyBuilder, key);
                        BIGINT.writeLong(valueBuilder, value);
                    }));
        }

        private SqlMap toIntegerVarcharSqlMap(Map<Integer, String> values)
        {
            return buildMapValue(
                    integerToVarcharMapType,
                    values.size(),
                    (keyBuilder, valueBuilder) -> values.forEach((key, value) -> {
                        INTEGER.writeLong(keyBuilder, key);
                        VARCHAR.writeString(valueBuilder, value);
                    }));
        }

        @Nullable
        private static Block toIntegerArrayBlock(List<Integer> values)
        {
            if (values == null) {
                return null;
            }
            BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(values.size());
            values.forEach(value -> INTEGER.writeLong(builder, value));
            return builder.build();
        }

        @Nullable
        private static Block toBigintArrayBlock(List<Long> values)
        {
            if (values == null) {
                return null;
            }
            BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.size());
            values.forEach(value -> BIGINT.writeLong(builder, value));
            return builder.build();
        }

        @Nullable
        private static Slice toVarbinarySlice(ByteBuffer value)
        {
            if (value == null) {
                return null;
            }
            return Slices.wrappedHeapBuffer(value);
        }
    }

    static String toJson(ReadableMetricsStruct readableMetrics, List<Types.NestedField> primitiveFields)
    {
        StringWriter writer = new StringWriter();
        try {
            JsonGenerator generator = JSON_FACTORY.createGenerator(writer);
            generator.writeStartObject();

            for (int i = 0; i < readableMetrics.size(); i++) {
                Types.NestedField field = primitiveFields.get(i);
                generator.writeFieldName(field.name());

                generator.writeStartObject();
                ReadableColMetricsStruct columnMetrics = readableMetrics.get(i, ReadableColMetricsStruct.class);

                generator.writeFieldName("column_size");
                Long columnSize = columnMetrics.get(0, Long.class);
                if (columnSize == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeNumber(columnSize);
                }

                generator.writeFieldName("value_count");
                Long valueCount = columnMetrics.get(1, Long.class);
                if (valueCount == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeNumber(valueCount);
                }

                generator.writeFieldName("null_value_count");
                Long nullValueCount = columnMetrics.get(2, Long.class);
                if (nullValueCount == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeNumber(nullValueCount);
                }

                generator.writeFieldName("nan_value_count");
                Long nanValueCount = columnMetrics.get(3, Long.class);
                if (nanValueCount == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeNumber(nanValueCount);
                }

                generator.writeFieldName("lower_bound");
                SingleValueParser.toJson(field.type(), columnMetrics.get(4, Object.class), generator);

                generator.writeFieldName("upper_bound");
                SingleValueParser.toJson(field.type(), columnMetrics.get(5, Object.class), generator);

                generator.writeEndObject();
            }

            generator.writeEndObject();
            generator.flush();
            return writer.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException("JSON conversion failed for: " + readableMetrics, e);
        }
    }

    static Map<Integer, Type> getIcebergIdToTypeMapping(Schema schema)
    {
        ImmutableMap.Builder<Integer, Type> icebergIdToTypeMapping = ImmutableMap.builder();
        for (Types.NestedField field : schema.columns()) {
            populateIcebergIdToTypeMapping(field, icebergIdToTypeMapping);
        }
        return icebergIdToTypeMapping.buildOrThrow();
    }

    private static void populateIcebergIdToTypeMapping(Types.NestedField field, ImmutableMap.Builder<Integer, Type> icebergIdToTypeMapping)
    {
        Type type = field.type();
        icebergIdToTypeMapping.put(field.fieldId(), type);
        if (type instanceof Type.NestedType) {
            type.asNestedType().fields().forEach(child -> populateIcebergIdToTypeMapping(child, icebergIdToTypeMapping));
        }
    }
}
