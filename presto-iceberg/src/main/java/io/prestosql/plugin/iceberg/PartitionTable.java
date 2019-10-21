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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.InMemoryRecordSet;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.iceberg.IcebergUtil.getIdentityPartitions;
import static io.prestosql.plugin.iceberg.IcebergUtil.getTableScan;
import static io.prestosql.plugin.iceberg.TypeConveter.toPrestoType;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toSet;

public class PartitionTable
        implements SystemTable
{
    private final IcebergTableHandle tableHandle;
    private final TypeManager typeManager;
    private final Table icebergTable;
    private Map<Integer, Type.PrimitiveType> idToTypeMapping;
    private List<Types.NestedField> nonPartitionPrimitiveColumns;
    private List<io.prestosql.spi.type.Type> partitionColumnTypes;
    private List<io.prestosql.spi.type.Type> resultTypes;
    private List<RowType> columnMetricTypes;

    public PartitionTable(IcebergTableHandle tableHandle, TypeManager typeManager, Table icebergTable)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        this.idToTypeMapping = icebergTable.schema().columns().stream()
                .filter(column -> column.type().isPrimitiveType())
                .collect(Collectors.toMap(Types.NestedField::fieldId, (column) -> column.type().asPrimitiveType()));

        List<Types.NestedField> columns = icebergTable.schema().columns();
        List<PartitionField> partitionFields = icebergTable.spec().fields();

        ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();

        List<ColumnMetadata> partitionColumnsMetadata = getPartitionColumnsMetadata(partitionFields, icebergTable.schema());
        this.partitionColumnTypes = partitionColumnsMetadata.stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        columnMetadataBuilder.addAll(partitionColumnsMetadata);

        Set<Integer> identityPartitionIds = getIdentityPartitions(icebergTable.spec()).keySet().stream()
                .map(PartitionField::sourceId)
                .collect(toSet());

        this.nonPartitionPrimitiveColumns = columns.stream()
                .filter(column -> !identityPartitionIds.contains(column.fieldId()) && column.type().isPrimitiveType())
                .collect(toImmutableList());

        ImmutableList.of("row_count", "file_count", "total_size")
                .forEach(metric -> columnMetadataBuilder.add(new ColumnMetadata(metric, BIGINT)));

        List<ColumnMetadata> columnMetricsMetadata = getColumnMetadata(nonPartitionPrimitiveColumns);
        columnMetadataBuilder.addAll(columnMetricsMetadata);

        this.columnMetricTypes = columnMetricsMetadata.stream().map(m -> (RowType) m.getType()).collect(toImmutableList());

        ImmutableList<ColumnMetadata> columnMetadata = columnMetadataBuilder.build();
        this.resultTypes = columnMetadata.stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        return new ConnectorTableMetadata(tableHandle.getSchemaTableNameWithType(), columnMetadata);
    }

    private List<ColumnMetadata> getPartitionColumnsMetadata(List<PartitionField> fields, Schema schema)
    {
        return fields.stream()
                .map(field -> new ColumnMetadata(
                        field.name(),
                        toPrestoType(field.transform().getResultType(schema.findType(field.sourceId())), typeManager)))
                .collect(toImmutableList());
    }

    private List<ColumnMetadata> getColumnMetadata(List<Types.NestedField> columns)
    {
        return columns.stream().map(column -> new ColumnMetadata(column.name(),
                RowType.from(ImmutableList.of(
                        new RowType.Field(Optional.of("min"), toPrestoType(column.type(), typeManager)),
                        new RowType.Field(Optional.of("max"), toPrestoType(column.type(), typeManager)),
                        new RowType.Field(Optional.of("null_count"), BIGINT)))))
                .collect(toImmutableList());
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        // TODO instead of cursor use pageSource method.
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            TableScan tableScan = getTableScan(session, TupleDomain.all(), tableHandle.getSnapshotId(), icebergTable).includeColumnStats();
            Map<StructLikeWrapper, Partition> partitions = getPartitions(tableScan);
            return buildRecordCursor(partitions, icebergTable.spec().fields());
        }
    }

    private Map<StructLikeWrapper, Partition> getPartitions(TableScan tableScan)
    {
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            Map<StructLikeWrapper, Partition> partitions = new HashMap<>();

            for (FileScanTask fileScanTask : fileScanTasks) {
                DataFile dataFile = fileScanTask.file();
                StructLike partitionStruct = dataFile.partition();
                StructLikeWrapper partitionWrapper = StructLikeWrapper.wrap(partitionStruct);

                if (!partitions.containsKey(partitionWrapper)) {
                    Partition partition = new Partition(partitionStruct,
                            dataFile.recordCount(),
                            dataFile.fileSizeInBytes(),
                            toMap(dataFile.lowerBounds()),
                            toMap(dataFile.upperBounds()),
                            dataFile.nullValueCounts());
                    partitions.put(partitionWrapper, partition);
                    continue;
                }

                Partition partition = partitions.get(partitionWrapper);
                partition.incrementFileCount();
                partition.incrementRecordCount(dataFile.recordCount());
                partition.incrementSize(dataFile.fileSizeInBytes());
                partition.updateMin(toMap(dataFile.lowerBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                partition.updateMax(toMap(dataFile.upperBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                partition.updateNullCount(dataFile.nullValueCounts());
            }

            return partitions;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private RecordCursor buildRecordCursor(Map<StructLikeWrapper, Partition> partitions, List<PartitionField> partitionFields)
    {
        List<Type> partitionTypes = partitionTypes(partitionFields);
        List<? extends Class<?>> partitionColumnClass = partitionTypes.stream()
                .map(type -> type.typeId().javaClass())
                .collect(toImmutableList());
        int columnCounts = partitionColumnTypes.size() + 3 + columnMetricTypes.size();

        ImmutableList.Builder<List<Object>> records = ImmutableList.builder();

        for (PartitionTable.Partition partition : partitions.values()) {
            List<Object> row = new ArrayList<>(columnCounts);

            // add data for partition columns
            for (int i = 0; i < partitionColumnTypes.size(); i++) {
                row.add(convert(partition.getValues().get(i, partitionColumnClass.get(i)), partitionTypes.get(i)));
            }

            // add the top level metrics.
            row.add(partition.getRecordCount());
            row.add(partition.getFileCount());
            row.add(partition.getSize());

            // add column level metrics
            for (int i = 0; i < columnMetricTypes.size(); i++) {
                Integer fieldId = nonPartitionPrimitiveColumns.get(i).fieldId();
                Type.PrimitiveType type = idToTypeMapping.get(fieldId);
                Object min = convert(partition.getMinValues().get(fieldId), type);
                Object max = convert(partition.getMaxValues().get(fieldId), type);
                Long nullCount = partition.getNullCounts().get(fieldId);
                row.add(getColumnMetricBlock(columnMetricTypes.get(i), min, max, nullCount));
            }

            records.add(row);
        }

        return new InMemoryRecordSet(resultTypes, records.build()).cursor();
    }

    private List<Type> partitionTypes(List<PartitionField> partitionFields)
    {
        ImmutableList.Builder<Type> partitionTypeBuilder = ImmutableList.builder();
        for (PartitionField partitionField : partitionFields) {
            Type.PrimitiveType sourceType = idToTypeMapping.get(partitionField.sourceId());
            Type type = partitionField.transform().getResultType(sourceType);
            partitionTypeBuilder.add(type);
        }
        return partitionTypeBuilder.build();
    }

    private static Block getColumnMetricBlock(RowType columnMetricType, Object min, Object max, Long nullCount)
    {
        BlockBuilder rowBlockBuilder = columnMetricType.createBlockBuilder(null, 1);
        BlockBuilder builder = rowBlockBuilder.beginBlockEntry();
        List<RowType.Field> fields = columnMetricType.getFields();
        TypeUtils.writeNativeValue(fields.get(0).getType(), builder, min);
        TypeUtils.writeNativeValue(fields.get(1).getType(), builder, max);
        TypeUtils.writeNativeValue(fields.get(2).getType(), builder, nullCount);

        rowBlockBuilder.closeEntry();
        return columnMetricType.getObject(rowBlockBuilder, 0);
    }

    private Map<Integer, Object> toMap(Map<Integer, ByteBuffer> idToMetricMap)
    {
        ImmutableMap.Builder<Integer, Object> map = ImmutableMap.builder();
        idToMetricMap.forEach((id, value) -> {
            Type.PrimitiveType type = idToTypeMapping.get(id);
            map.put(id, Conversions.fromByteBuffer(type, value));
        });
        return map.build();
    }

    private static Object convert(Object value, Type type)
    {
        if (value == null) {
            return null;
        }
        if (type instanceof Types.StringType) {
            return value.toString();
        }
        if (type instanceof Types.BinaryType) {
            // TODO the client sees the bytearray's tostring ouput instead of seeing actual bytes, needs to be fixed.
            return ((ByteBuffer) value).array();
        }
        if (type instanceof Types.TimestampType) {
            long utcMillis = TimeUnit.MICROSECONDS.toMillis((Long) value);
            Types.TimestampType timestampType = (Types.TimestampType) type;
            if (timestampType.shouldAdjustToUTC()) {
                return packDateTimeWithZone(utcMillis, TimeZoneKey.UTC_KEY);
            }
            return utcMillis;
        }
        if (type instanceof Types.FloatType) {
            return Float.floatToIntBits((Float) value);
        }
        return value;
    }

    private class Partition
    {
        private final StructLike values;
        private long recordCount;
        private long fileCount;
        private long size;
        private final Map<Integer, Object> minValues;
        private final Map<Integer, Object> maxValues;
        private final Map<Integer, Long> nullCounts;
        private final Set<Integer> corruptedStats;

        public Partition(
                StructLike values,
                long recordCount,
                long size,
                Map<Integer, Object> minValues,
                Map<Integer, Object> maxValues,
                Map<Integer, Long> nullCounts)
        {
            this.values = requireNonNull(values, "values is null");
            this.recordCount = recordCount;
            this.fileCount = 1;
            this.size = size;
            this.minValues = new HashMap<>(requireNonNull(minValues, "minValues is null"));
            this.maxValues = new HashMap<>(requireNonNull(maxValues, "maxValues is null"));
            // we are assuming if minValues is not present, max will be not be present either.
            this.corruptedStats = nonPartitionPrimitiveColumns.stream()
                    .map(Types.NestedField::fieldId)
                    .filter(id -> !minValues.containsKey(id) && (!nullCounts.containsKey(id) || nullCounts.get(id) != recordCount))
                    .collect(toCollection(HashSet::new));
            this.nullCounts = new HashMap<>(nullCounts);
        }

        public StructLike getValues()
        {
            return values;
        }

        public long getRecordCount()
        {
            return recordCount;
        }

        public long getFileCount()
        {
            return fileCount;
        }

        public long getSize()
        {
            return size;
        }

        public Map<Integer, Object> getMinValues()
        {
            return minValues;
        }

        public Map<Integer, Object> getMaxValues()
        {
            return maxValues;
        }

        public Map<Integer, Long> getNullCounts()
        {
            return nullCounts;
        }

        public void incrementRecordCount(long count)
        {
            this.recordCount += count;
        }

        public void incrementFileCount()
        {
            this.fileCount++;
        }

        public void incrementSize(long numberOfBytes)
        {
            this.size += numberOfBytes;
        }

        /**
         * The update logic is built with the following rules:
         * bounds is null => if any file has a missing bound for a column, that bound will not be reported
         * bounds is missing id => not reported in Parquet => that bound will not be reported
         * bound value is null => not an expected case
         * bound value is present => this is the normal case and bounds will be reported correctly
         */
        public void updateMin(Map<Integer, Object> lowerBounds, Map<Integer, Long> nullCounts, long recordCount)
        {
            updateStats(this.minValues, lowerBounds, nullCounts, recordCount, i -> (i > 0));
        }

        public void updateMax(Map<Integer, Object> upperBounds, Map<Integer, Long> nullCounts, long recordCount)
        {
            updateStats(this.maxValues, upperBounds, nullCounts, recordCount, i -> (i < 0));
        }

        private void updateStats(Map<Integer, Object> current, Map<Integer, Object> newStat, Map<Integer, Long> nullCounts, long recordCount, Predicate<Integer> predicate)
        {
            for (Types.NestedField column : nonPartitionPrimitiveColumns) {
                int id = column.fieldId();

                if (corruptedStats.contains(id)) {
                    continue;
                }

                Object newValue = newStat.get(id);
                // it is expected to not have min/max if all values are null for a column in the datafile and it is not a case of corrupted stats.
                if (newValue == null) {
                    Long nullCount = nullCounts.get(id);
                    if ((nullCount == null) || (nullCount != recordCount)) {
                        current.remove(id);
                        corruptedStats.add(id);
                    }
                    continue;
                }

                Object oldValue = current.putIfAbsent(id, newValue);
                if (oldValue != null) {
                    Comparator<Object> comparator = Comparators.forType(idToTypeMapping.get(id));
                    if (predicate.test(comparator.compare(oldValue, newValue))) {
                        current.put(id, newValue);
                    }
                }
            }
        }

        public void updateNullCount(Map<Integer, Long> nullCounts)
        {
            nullCounts.forEach((key, counts) ->
                    this.nullCounts.merge(key, counts, Long::sum));
        }
    }
}
