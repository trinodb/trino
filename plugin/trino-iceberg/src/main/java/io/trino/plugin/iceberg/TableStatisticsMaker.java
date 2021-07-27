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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergUtil.getColumns;
import static io.trino.plugin.iceberg.IcebergUtil.getIdentityPartitions;
import static io.trino.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.trino.plugin.iceberg.Partition.convertBounds;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class TableStatisticsMaker
{
    private final TypeManager typeManager;
    private final Table icebergTable;

    private TableStatisticsMaker(TypeManager typeManager, Table icebergTable)
    {
        this.typeManager = typeManager;
        this.icebergTable = icebergTable;
    }

    public static TableStatistics getTableStatistics(TypeManager typeManager, Constraint constraint, IcebergTableHandle tableHandle, Table icebergTable)
    {
        return new TableStatisticsMaker(typeManager, icebergTable).makeTableStatistics(tableHandle, constraint);
    }

    private TableStatistics makeTableStatistics(IcebergTableHandle tableHandle, Constraint constraint)
    {
        if (tableHandle.getSnapshotId().isEmpty() || constraint.getSummary().isNone()) {
            return TableStatistics.empty();
        }

        TupleDomain<IcebergColumnHandle> intersection = constraint.getSummary()
                .transformKeys(IcebergColumnHandle.class::cast)
                .intersect(tableHandle.getEnforcedPredicate());

        if (intersection.isNone()) {
            return TableStatistics.empty();
        }

        List<Types.NestedField> columns = icebergTable.schema().columns();

        Map<Integer, Type.PrimitiveType> idToTypeMapping = primitiveFieldTypes(icebergTable.schema());
        List<PartitionField> partitionFields = icebergTable.spec().fields();

        Set<Integer> identityPartitionIds = getIdentityPartitions(icebergTable.spec()).keySet().stream()
                .map(PartitionField::sourceId)
                .collect(toSet());

        List<Types.NestedField> nonPartitionPrimitiveColumns = columns.stream()
                .filter(column -> !identityPartitionIds.contains(column.fieldId()) && column.type().isPrimitiveType())
                .collect(toImmutableList());

        List<Type> icebergPartitionTypes = partitionTypes(partitionFields, idToTypeMapping);
        List<IcebergColumnHandle> columnHandles = getColumns(icebergTable.schema(), typeManager);
        Map<Integer, IcebergColumnHandle> idToColumnHandle = columnHandles.stream()
                .collect(toUnmodifiableMap(IcebergColumnHandle::getId, identity()));

        ImmutableMap.Builder<Integer, ColumnFieldDetails> idToDetailsBuilder = ImmutableMap.builder();
        for (int index = 0; index < partitionFields.size(); index++) {
            PartitionField field = partitionFields.get(index);
            Type type = icebergPartitionTypes.get(index);
            idToDetailsBuilder.put(field.sourceId(), new ColumnFieldDetails(
                    field,
                    idToColumnHandle.get(field.sourceId()),
                    type,
                    toTrinoType(type, typeManager),
                    type.typeId().javaClass()));
        }
        Map<Integer, ColumnFieldDetails> idToDetails = idToDetailsBuilder.build();

        TableScan tableScan = icebergTable.newScan()
                .filter(toIcebergExpression(intersection))
                .useSnapshot(tableHandle.getSnapshotId().get())
                .includeColumnStats();

        Partition summary = null;
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            for (FileScanTask fileScanTask : fileScanTasks) {
                DataFile dataFile = fileScanTask.file();
                if (!dataFileMatches(
                        dataFile,
                        constraint,
                        partitionFields,
                        idToDetails)) {
                    continue;
                }

                if (summary == null) {
                    summary = new Partition(
                            idToTypeMapping,
                            nonPartitionPrimitiveColumns,
                            dataFile.partition(),
                            dataFile.recordCount(),
                            dataFile.fileSizeInBytes(),
                            convertBounds(idToTypeMapping, dataFile.lowerBounds()),
                            convertBounds(idToTypeMapping, dataFile.upperBounds()),
                            dataFile.nullValueCounts(),
                            dataFile.columnSizes());
                }
                else {
                    summary.incrementFileCount();
                    summary.incrementRecordCount(dataFile.recordCount());
                    summary.incrementSize(dataFile.fileSizeInBytes());
                    updateSummaryMin(summary, partitionFields, convertBounds(idToTypeMapping, dataFile.lowerBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                    updateSummaryMax(summary, partitionFields, convertBounds(idToTypeMapping, dataFile.upperBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                    summary.updateNullCount(dataFile.nullValueCounts());
                    updateColumnSizes(summary, dataFile.columnSizes());
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (summary == null) {
            return TableStatistics.empty();
        }

        ImmutableMap.Builder<ColumnHandle, ColumnStatistics> columnHandleBuilder = ImmutableMap.builder();
        double recordCount = summary.getRecordCount();
        for (IcebergColumnHandle columnHandle : idToColumnHandle.values()) {
            int fieldId = columnHandle.getId();
            ColumnStatistics.Builder columnBuilder = new ColumnStatistics.Builder();
            Long nullCount = summary.getNullCounts().get(fieldId);
            if (nullCount != null) {
                columnBuilder.setNullsFraction(Estimate.of(nullCount / recordCount));
            }
            if (summary.getColumnSizes() != null) {
                Long columnSize = summary.getColumnSizes().get(fieldId);
                if (columnSize != null) {
                    columnBuilder.setDataSize(Estimate.of(columnSize));
                }
            }
            Object min = summary.getMinValues().get(fieldId);
            Object max = summary.getMaxValues().get(fieldId);
            if (min instanceof Number && max instanceof Number) {
                columnBuilder.setRange(Optional.of(new DoubleRange(((Number) min).doubleValue(), ((Number) max).doubleValue())));
            }
            columnHandleBuilder.put(columnHandle, columnBuilder.build());
        }
        return new TableStatistics(Estimate.of(recordCount), columnHandleBuilder.build());
    }

    private boolean dataFileMatches(
            DataFile dataFile,
            Constraint constraint,
            List<PartitionField> partitionFields,
            Map<Integer, ColumnFieldDetails> fieldDetails)
    {
        // Currently this method is used only for IcebergMetadata.getTableStatistics and there Constraint never carries a predicate.
        // TODO support pruning with constraint when this changes.
        verify(constraint.predicate().isEmpty(), "Unexpected Constraint predicate");

        TupleDomain<ColumnHandle> constraintSummary = constraint.getSummary();

        Map<ColumnHandle, Domain> domains = constraintSummary.getDomains().get();

        for (int index = 0; index < partitionFields.size(); index++) {
            PartitionField field = partitionFields.get(index);
            int fieldId = field.sourceId();
            ColumnFieldDetails details = fieldDetails.get(fieldId);
            IcebergColumnHandle column = details.getColumnHandle();
            Object value = PartitionTable.convert(dataFile.partition().get(index, details.getJavaClass()), details.getIcebergType());
            Domain allowedDomain = domains.get(column);
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value)) {
                return false;
            }
        }

        return true;
    }

    public List<Type> partitionTypes(List<PartitionField> partitionFields, Map<Integer, Type.PrimitiveType> idToTypeMapping)
    {
        ImmutableList.Builder<Type> partitionTypeBuilder = ImmutableList.builder();
        for (PartitionField partitionField : partitionFields) {
            Type.PrimitiveType sourceType = idToTypeMapping.get(partitionField.sourceId());
            Type type = partitionField.transform().getResultType(sourceType);
            partitionTypeBuilder.add(type);
        }
        return partitionTypeBuilder.build();
    }

    private static class ColumnFieldDetails
    {
        private final PartitionField field;
        private final IcebergColumnHandle columnHandle;
        private final Type icebergType;
        private final io.trino.spi.type.Type trinoType;
        private final Class<?> javaClass;

        public ColumnFieldDetails(PartitionField field, IcebergColumnHandle columnHandle, Type icebergType, io.trino.spi.type.Type trinoType, Class<?> javaClass)
        {
            this.field = requireNonNull(field, "field is null");
            this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
            this.icebergType = requireNonNull(icebergType, "icebergType is null");
            this.trinoType = requireNonNull(trinoType, "trinoType is null");
            this.javaClass = requireNonNull(javaClass, "javaClass is null");
        }

        public PartitionField getField()
        {
            return field;
        }

        public IcebergColumnHandle getColumnHandle()
        {
            return columnHandle;
        }

        public Type getIcebergType()
        {
            return icebergType;
        }

        public io.trino.spi.type.Type getTrinoType()
        {
            return trinoType;
        }

        public Class<?> getJavaClass()
        {
            return javaClass;
        }
    }

    public void updateColumnSizes(Partition summary, Map<Integer, Long> addedColumnSizes)
    {
        Map<Integer, Long> columnSizes = summary.getColumnSizes();
        if (!summary.hasValidColumnMetrics() || columnSizes == null || addedColumnSizes == null) {
            return;
        }
        for (Types.NestedField column : summary.getNonPartitionPrimitiveColumns()) {
            int id = column.fieldId();

            Long addedSize = addedColumnSizes.get(id);
            if (addedSize != null) {
                columnSizes.put(id, addedSize + columnSizes.getOrDefault(id, 0L));
            }
        }
    }

    private void updateSummaryMin(Partition summary, List<PartitionField> partitionFields, Map<Integer, Object> lowerBounds, Map<Integer, Long> nullCounts, long recordCount)
    {
        summary.updateStats(summary.getMinValues(), lowerBounds, nullCounts, recordCount, i -> (i > 0));
        updatePartitionedStats(summary, partitionFields, summary.getMinValues(), lowerBounds, i -> (i > 0));
    }

    private void updateSummaryMax(Partition summary, List<PartitionField> partitionFields, Map<Integer, Object> upperBounds, Map<Integer, Long> nullCounts, long recordCount)
    {
        summary.updateStats(summary.getMaxValues(), upperBounds, nullCounts, recordCount, i -> (i < 0));
        updatePartitionedStats(summary, partitionFields, summary.getMaxValues(), upperBounds, i -> (i < 0));
    }

    private void updatePartitionedStats(
            Partition summary,
            List<PartitionField> partitionFields,
            Map<Integer, Object> current,
            Map<Integer, Object> newStats,
            Predicate<Integer> predicate)
    {
        for (PartitionField field : partitionFields) {
            int id = field.sourceId();
            if (summary.getCorruptedStats().contains(id)) {
                continue;
            }

            Object newValue = newStats.get(id);
            if (newValue == null) {
                continue;
            }

            Object oldValue = current.putIfAbsent(id, newValue);
            if (oldValue != null) {
                Comparator<Object> comparator = Comparators.forType(summary.getIdToTypeMapping().get(id));
                if (predicate.test(comparator.compare(oldValue, newValue))) {
                    current.put(id, newValue);
                }
            }
        }
    }
}
