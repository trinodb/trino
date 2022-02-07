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
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.getColumns;
import static io.trino.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
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

        Schema icebergTableSchema = icebergTable.schema();
        List<Types.NestedField> columns = icebergTableSchema.columns();

        Map<Integer, Type.PrimitiveType> idToTypeMapping = primitiveFieldTypes(icebergTableSchema);
        List<PartitionField> partitionFields = icebergTable.spec().fields();

        List<Type> icebergPartitionTypes = partitionTypes(partitionFields, idToTypeMapping);
        List<IcebergColumnHandle> columnHandles = getColumns(icebergTableSchema, typeManager);
        Map<Integer, IcebergColumnHandle> idToColumnHandle = columnHandles.stream()
                .collect(toUnmodifiableMap(IcebergColumnHandle::getId, identity()));

        ImmutableMap.Builder<Integer, ColumnFieldDetails> idToDetailsBuilder = ImmutableMap.builder();
        for (int index = 0; index < partitionFields.size(); index++) {
            PartitionField field = partitionFields.get(index);
            Type type = icebergPartitionTypes.get(index);
            idToDetailsBuilder.put(field.fieldId(), new ColumnFieldDetails(
                    field,
                    idToColumnHandle.get(field.sourceId()),
                    type,
                    toTrinoType(type, typeManager),
                    type.typeId().javaClass()));
        }
        Map<Integer, ColumnFieldDetails> idToDetails = idToDetailsBuilder.buildOrThrow();

        TableScan tableScan = icebergTable.newScan()
                .filter(toIcebergExpression(intersection))
                .useSnapshot(tableHandle.getSnapshotId().get())
                .includeColumnStats();

        IcebergStatistics.Builder icebergStatisticsBuilder = new IcebergStatistics.Builder(columns, typeManager);
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

                icebergStatisticsBuilder.acceptDataFile(dataFile, fileScanTask.spec());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        IcebergStatistics summary = icebergStatisticsBuilder.build();

        if (summary.getFileCount() == 0) {
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
            if (min != null && max != null) {
                columnBuilder.setRange(DoubleRange.from(columnHandle.getType(), min, max));
            }
            columnHandleBuilder.put(columnHandle, columnBuilder.build());
        }
        return new TableStatistics(Estimate.of(recordCount), columnHandleBuilder.buildOrThrow());
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
            int fieldId = field.fieldId();
            ColumnFieldDetails details = fieldDetails.get(fieldId);
            IcebergColumnHandle column = details.getColumnHandle();
            Object value = convertIcebergValueToTrino(details.getIcebergType(), dataFile.partition().get(index, details.getJavaClass()));
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
}
