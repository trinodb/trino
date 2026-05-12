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
package io.trino.plugin.iceberg.system;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.trino.plugin.iceberg.IcebergStatistics;
import io.trino.plugin.iceberg.StructLikeWrapperWithFieldIdToIndex;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.getIdentityPartitions;
import static io.trino.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.trino.plugin.iceberg.StructLikeWrapperWithFieldIdToIndex.createStructLikeWrapper;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getAllPartitionFields;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getPartitionColumnType;
import static io.trino.plugin.iceberg.util.SystemTableUtil.partitionTypes;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class PartitionsTable
        implements SystemTable
{
    private final TypeManager typeManager;
    private final Table icebergTable;
    private final OptionalLong snapshotId;
    private final Map<Integer, Type.PrimitiveType> idToTypeMapping;
    private final List<NestedField> nonPartitionPrimitiveColumns;
    private final Optional<IcebergPartitionColumn> partitionColumnType;
    private final List<PartitionField> partitionFields;
    private final Optional<RowType> dataColumnType;
    private final List<RowType> columnMetricTypes;
    private final List<io.trino.spi.type.Type> resultTypes;
    private final ConnectorTableMetadata connectorTableMetadata;
    private final ExecutorService executor;
    private final int batchSize;

    public PartitionsTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable, OptionalLong snapshotId, ExecutorService executor, int batchSize)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.idToTypeMapping = primitiveFieldTypes(icebergTable.schema());

        List<NestedField> columns = icebergTable.schema().columns();
        this.partitionFields = getAllPartitionFields(icebergTable);

        ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();

        this.partitionColumnType = getPartitionColumnType(typeManager, partitionFields, icebergTable.schema());
        partitionColumnType.ifPresent(icebergPartitionColumn ->
                columnMetadataBuilder.add(new ColumnMetadata("partition", icebergPartitionColumn.rowType())));

        Stream.of("record_count", "file_count", "total_size")
                .forEach(metric -> columnMetadataBuilder.add(new ColumnMetadata(metric, BIGINT)));

        Set<Integer> identityPartitionIds = getIdentityPartitions(icebergTable.spec()).keySet().stream()
                .map(PartitionField::sourceId)
                .collect(toSet());

        this.nonPartitionPrimitiveColumns = columns.stream()
                .filter(column -> !identityPartitionIds.contains(column.fieldId()) && column.type().isPrimitiveType())
                .collect(toImmutableList());

        this.dataColumnType = getMetricsColumnType(typeManager, this.nonPartitionPrimitiveColumns);
        if (dataColumnType.isPresent()) {
            columnMetadataBuilder.add(new ColumnMetadata("data", dataColumnType.get()));
            this.columnMetricTypes = dataColumnType.get().getFields().stream()
                    .map(RowType.Field::getType)
                    .map(RowType.class::cast)
                    .collect(toImmutableList());
        }
        else {
            this.columnMetricTypes = ImmutableList.of();
        }

        List<ColumnMetadata> columnMetadata = columnMetadataBuilder.build();
        this.resultTypes = columnMetadata.stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        this.connectorTableMetadata = new ConnectorTableMetadata(tableName, columnMetadata);
        this.executor = requireNonNull(executor, "executor is null");
        this.batchSize = batchSize;
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return connectorTableMetadata;
    }

    private static Optional<RowType> getMetricsColumnType(TypeManager typeManager, List<NestedField> columns)
    {
        List<RowType.Field> metricColumns = columns.stream()
                .map(column -> RowType.field(
                        column.name(),
                        RowType.from(ImmutableList.of(
                                new RowType.Field(Optional.of("min"), toTrinoType(column.type(), typeManager)),
                                new RowType.Field(Optional.of("max"), toTrinoType(column.type(), typeManager)),
                                new RowType.Field(Optional.of("null_count"), BIGINT),
                                new RowType.Field(Optional.of("nan_count"), BIGINT)))))
                .collect(toImmutableList());
        if (metricColumns.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(RowType.from(metricColumns));
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        if (batchSize == 0) {
            // Fall back to legacy cursor() path; engine will wrap it in a RecordPageSource.
            throw new UnsupportedOperationException();
        }
        if (snapshotId.isEmpty()) {
            return new RecordPageSource(new InMemoryRecordSet(resultTypes, ImmutableList.of()));
        }
        return new BatchedPartitionsPageSource();
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        if (snapshotId.isEmpty()) {
            return new InMemoryRecordSet(resultTypes, ImmutableList.of()).cursor();
        }
        TableScan tableScan = icebergTable.newScan()
                .useSnapshot(snapshotId.getAsLong())
                .includeColumnStats()
                .planWith(executor);
        // TODO make the cursor lazy
        return buildRecordCursor(getStatisticsByPartition(tableScan));
    }

    private Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics> getStatisticsByPartition(TableScan tableScan)
    {
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics.Builder> partitions = new HashMap<>();
            for (FileScanTask fileScanTask : fileScanTasks) {
                DataFile dataFile = fileScanTask.file();
                StructLikeWrapperWithFieldIdToIndex structLikeWrapperWithFieldIdToIndex = createStructLikeWrapper(fileScanTask);

                partitions.computeIfAbsent(
                                structLikeWrapperWithFieldIdToIndex,
                                _ -> new IcebergStatistics.Builder(icebergTable.schema().columns(), typeManager))
                        .acceptDataFile(dataFile, fileScanTask.spec());
            }
            return partitions.entrySet().stream()
                    .collect(toImmutableMap(Entry::getKey, entry -> entry.getValue().build()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private RecordCursor buildRecordCursor(Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics> partitionStatistics)
    {
        List<Type> partitionTypes = partitionTypes(partitionFields, idToTypeMapping);
        List<? extends Class<?>> partitionColumnClass = partitionTypes.stream()
                .map(type -> type.typeId().javaClass())
                .collect(toImmutableList());

        ImmutableList.Builder<List<Object>> records = ImmutableList.builder();

        for (Entry<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics> partitionEntry : partitionStatistics.entrySet()) {
            records.add(buildRow(partitionEntry.getKey(), partitionEntry.getValue(), partitionTypes, partitionColumnClass));
        }

        return new InMemoryRecordSet(resultTypes, records.build()).cursor();
    }

    private class BatchedPartitionsPageSource
            implements ConnectorPageSource
    {
        private final Iterator<List<StructLikeWrapperWithFieldIdToIndex>> batchIterator;
        private final List<Type> batchPartitionTypes;
        private final List<? extends Class<?>> batchPartitionColumnClass;
        private ConnectorPageSource currentBatch;
        private boolean finished;

        BatchedPartitionsPageSource()
        {
            Set<StructLikeWrapperWithFieldIdToIndex> allPartitions = enumeratePartitions();
            List<StructLikeWrapperWithFieldIdToIndex> asList = new ArrayList<>(allPartitions);
            List<List<StructLikeWrapperWithFieldIdToIndex>> batches = asList.isEmpty()
                    ? ImmutableList.of()
                    : Lists.partition(asList, batchSize);
            this.batchIterator = batches.iterator();
            this.batchPartitionTypes = partitionTypes(partitionFields, idToTypeMapping);
            this.batchPartitionColumnClass = batchPartitionTypes.stream()
                    .map(type -> type.typeId().javaClass())
                    .collect(toImmutableList());
        }

        @Override
        public SourcePage getNextSourcePage()
        {
            if (finished) {
                return null;
            }
            while (true) {
                if (currentBatch != null) {
                    SourcePage page = currentBatch.getNextSourcePage();
                    if (page != null) {
                        return page;
                    }
                    closeCurrentBatch();
                }
                if (!batchIterator.hasNext()) {
                    finished = true;
                    return null;
                }
                List<StructLikeWrapperWithFieldIdToIndex> batch = batchIterator.next();
                Set<StructLikeWrapperWithFieldIdToIndex> batchSet = new HashSet<>(batch);
                Expression filter = buildBatchFilter(batch, partitionFields, icebergTable.schema(), batchPartitionTypes);
                Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics> stats = getStatisticsForBatch(batchSet, filter);
                List<List<Object>> rows = new ArrayList<>(stats.size());
                for (StructLikeWrapperWithFieldIdToIndex key : batch) {
                    IcebergStatistics partitionStats = stats.get(key);
                    verify(partitionStats != null, "missing stats for partition in batch: %s", key);
                    rows.add(buildRow(key, partitionStats, batchPartitionTypes, batchPartitionColumnClass));
                }
                currentBatch = new RecordPageSource(new InMemoryRecordSet(resultTypes, rows));
            }
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public void close()
        {
            finished = true;
            closeCurrentBatch();
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        private void closeCurrentBatch()
        {
            if (currentBatch == null) {
                return;
            }
            try {
                currentBatch.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            currentBatch = null;
        }
    }

    private Set<StructLikeWrapperWithFieldIdToIndex> enumeratePartitions()
    {
        TableScan scan = icebergTable.newScan()
                .useSnapshot(snapshotId.getAsLong())
                .planWith(executor);
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            Set<StructLikeWrapperWithFieldIdToIndex> partitions = new HashSet<>();
            for (FileScanTask task : tasks) {
                partitions.add(createStructLikeWrapper(task));
            }
            return partitions;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics> getStatisticsForBatch(
            Set<StructLikeWrapperWithFieldIdToIndex> batchSet,
            Expression filter)
    {
        TableScan scan = icebergTable.newScan()
                .useSnapshot(snapshotId.getAsLong())
                .includeColumnStats()
                .filter(filter)
                .planWith(executor);
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics.Builder> partitions = new HashMap<>();
            for (FileScanTask task : tasks) {
                StructLikeWrapperWithFieldIdToIndex key = createStructLikeWrapper(task);
                if (!batchSet.contains(key)) {
                    continue;
                }
                DataFile dataFile = task.file();
                partitions.computeIfAbsent(
                                key,
                                _ -> new IcebergStatistics.Builder(icebergTable.schema().columns(), typeManager))
                        .acceptDataFile(dataFile, task.spec());
            }
            return partitions.entrySet().stream()
                    .collect(toImmutableMap(Entry::getKey, entry -> entry.getValue().build()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @VisibleForTesting
    public static Expression buildBatchFilter(
            Collection<StructLikeWrapperWithFieldIdToIndex> batch,
            List<PartitionField> partitionFields,
            Schema schema,
            List<Type> partitionTypes)
    {
        if (batch.isEmpty()) {
            return Expressions.alwaysFalse();
        }
        if (partitionFields.isEmpty()) {
            return Expressions.alwaysTrue();
        }
        List<Expression> conjuncts = new ArrayList<>();
        for (int i = 0; i < partitionFields.size(); i++) {
            PartitionField field = partitionFields.get(i);
            // TableScan.filter() evaluates predicates against source columns, then projects them through
            // the partition transform. Only identity transforms are invertible, so only for them can we
            // translate a set of partition values back into a source-column predicate. For bucket/truncate/
            // year/month/day we skip the field; the in-memory tuple check in getStatisticsForBatch still
            // enforces correctness.
            if (!field.transform().isIdentity()) {
                continue;
            }
            String sourceColumnName = schema.findColumnName(field.sourceId());
            if (sourceColumnName == null) {
                continue;
            }
            Class<?> javaClass = partitionTypes.get(i).typeId().javaClass();
            Set<Object> values = new HashSet<>();
            boolean hasNull = false;
            for (StructLikeWrapperWithFieldIdToIndex key : batch) {
                Integer idx = key.getFieldIdToIndex().get(field.fieldId());
                if (idx == null) {
                    // Partition from a spec that doesn't include this field; skip filter construction for this field.
                    return Expressions.alwaysTrue();
                }
                Object value = key.getStructLikeWrapper().get().get(idx, javaClass);
                if (value == null) {
                    hasNull = true;
                }
                else {
                    values.add(value);
                }
            }
            Expression fieldExpr;
            if (hasNull && !values.isEmpty()) {
                fieldExpr = Expressions.or(Expressions.in(sourceColumnName, values), Expressions.isNull(sourceColumnName));
            }
            else if (hasNull) {
                fieldExpr = Expressions.isNull(sourceColumnName);
            }
            else if (!values.isEmpty()) {
                fieldExpr = Expressions.in(sourceColumnName, values);
            }
            else {
                continue;
            }
            conjuncts.add(fieldExpr);
        }
        if (conjuncts.isEmpty()) {
            return Expressions.alwaysTrue();
        }
        Expression result = conjuncts.get(0);
        for (int i = 1; i < conjuncts.size(); i++) {
            result = Expressions.and(result, conjuncts.get(i));
        }
        return result;
    }

    private List<Object> buildRow(
            StructLikeWrapperWithFieldIdToIndex partitionStruct,
            IcebergStatistics icebergStatistics,
            List<Type> partitionTypes,
            List<? extends Class<?>> partitionColumnClass)
    {
        List<Object> row = new ArrayList<>();

        // add data for partition columns
        partitionColumnType.ifPresent(partitionColumnType -> {
            row.add(buildRowValue(partitionColumnType.rowType(), fields -> {
                List<io.trino.spi.type.Type> partitionColumnTypes = partitionColumnType.rowType().getFields().stream()
                        .map(RowType.Field::getType)
                        .collect(toImmutableList());
                for (int i = 0; i < partitionColumnTypes.size(); i++) {
                    io.trino.spi.type.Type trinoType = partitionColumnType.rowType().getFields().get(i).getType();
                    Object value = null;
                    Integer fieldId = partitionColumnType.fieldIds().get(i);
                    if (partitionStruct.getFieldIdToIndex().containsKey(fieldId)) {
                        value = convertIcebergValueToTrino(
                                partitionTypes.get(i),
                                partitionStruct.getStructLikeWrapper().get().get(partitionStruct.getFieldIdToIndex().get(fieldId), partitionColumnClass.get(i)));
                    }
                    writeNativeValue(trinoType, fields.get(i), value);
                }
            }));
        });

        // add the top level metrics.
        row.add(icebergStatistics.recordCount());
        row.add(icebergStatistics.fileCount());
        row.add(icebergStatistics.size());

        // add column level metrics
        dataColumnType.ifPresent(dataColumnType -> {
            row.add(buildRowValue(dataColumnType, fields -> {
                for (int i = 0; i < columnMetricTypes.size(); i++) {
                    Integer fieldId = nonPartitionPrimitiveColumns.get(i).fieldId();
                    Object min = icebergStatistics.minValues().get(fieldId);
                    Object max = icebergStatistics.maxValues().get(fieldId);
                    Long nullCount = icebergStatistics.nullCounts().get(fieldId);
                    Long nanCount = icebergStatistics.nanCounts().get(fieldId);
                    RowType columnMetricType = columnMetricTypes.get(i);
                    if (min == null && max == null && nullCount == null) {
                        fields.get(i).appendNull();
                    }
                    else {
                        columnMetricType.writeObject(fields.get(i), getColumnMetricBlock(columnMetricType, min, max, nullCount, nanCount));
                    }
                }
            }));
        });

        return row;
    }

    private static SqlRow getColumnMetricBlock(RowType columnMetricType, Object min, Object max, Long nullCount, Long nanCount)
    {
        return buildRowValue(columnMetricType, fieldBuilders -> {
            List<RowType.Field> fields = columnMetricType.getFields();
            writeNativeValue(fields.get(0).getType(), fieldBuilders.get(0), min);
            writeNativeValue(fields.get(1).getType(), fieldBuilders.get(1), max);
            writeNativeValue(fields.get(2).getType(), fieldBuilders.get(2), nullCount);
            writeNativeValue(fields.get(3).getType(), fieldBuilders.get(3), nanCount);
        });
    }
}
