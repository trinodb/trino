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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.IcebergStatistics;
import io.trino.plugin.iceberg.StructLikeWrapperWithFieldIdToIndex;
import io.trino.spi.TrinoException;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergTypes.convertTrinoValueToIceberg;
import static io.trino.plugin.iceberg.IcebergUtil.getIdentityPartitions;
import static io.trino.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.trino.plugin.iceberg.StructLikeWrapperWithFieldIdToIndex.createStructLikeWrapper;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getAllPartitionFields;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getPartitionColumnType;
import static io.trino.plugin.iceberg.util.SystemTableUtil.partitionTypes;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class PartitionsTable
        implements SystemTable
{
    // TODO: compute() upon feedback
    //  current logic is
    //  knob-1: estimated-data-file-object-size = 5 kb
    //  knob-2: allowed-budget = 5% of -xmX (e.g. 500 gb = 25 gb )
    //  Formula:
    //                       ( allowed budget ) x 1024 x 1024
    //    - allowed-files = ----------------------------------
    //                           estimated-file-size-in-kb
    private static final long MAX_FILES_PER_PARTITIONS_SCAN = 100L;
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

    public PartitionsTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable, OptionalLong snapshotId, ExecutorService executor)
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
        // expose each identity-transform partitoin field as a top-level column so predicate on those value can be
        // pushed to iceberg scan api
        Schema schema = icebergTable.schema();
        for (PartitionField field : partitionFields) {
            if (field.transform().isIdentity()) {
                Type sourceType = schema.findType(field.sourceId());
                columnMetadataBuilder.add(new ColumnMetadata(field.name(), toTrinoType(sourceType, typeManager)));
            }
        }

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
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        var icebergFilter = convertToIcebergFilter(constraint);
        if (snapshotId.isEmpty() || icebergFilter == Expressions.alwaysFalse()) {
            return new InMemoryRecordSet(resultTypes, ImmutableList.of()).cursor();
        }

        // Pre-flight guard:
        if (icebergFilter == Expressions.alwaysTrue()) {
            var totalDatafiles = Long.parseLong(
                    icebergTable.snapshot(snapshotId.getAsLong())
                            .summary()
                            .getOrDefault("total-data-files", "0"));
            if (totalDatafiles > MAX_FILES_PER_PARTITIONS_SCAN) {
                throw new TrinoException(QUERY_REJECTED,
                        format("Scan on %s would visit %d of data files unfiltered, exceeding threshold(%d). Add a predicate " +
                                        "on top-level partition column to prune the manifest",
                                icebergTable.name(), totalDatafiles, MAX_FILES_PER_PARTITIONS_SCAN));
            }
        }

        TableScan tableScan = icebergTable.newScan()
                .useSnapshot(snapshotId.getAsLong())
                .filter(icebergFilter)
                .includeColumnStats()
                .planWith(executor);
        // TODO make the cursor lazy
        return buildRecordCursor(getStatisticsByPartition(tableScan));
    }

    private Expression convertToIcebergFilter(TupleDomain<Integer> constraint)
    {
        if (constraint.isAll()) {
            return Expressions.alwaysTrue();
        }
        else if (constraint.isNone()) {
            return Expressions.alwaysFalse();
        }

        Map<Integer, Domain> domains = constraint.getDomains().orElseThrow();
        Schema schema = icebergTable.schema();
        int startIdx = partitionColumnType.isPresent() ? 1 : 0;

        Expression result = Expressions.alwaysTrue();
        for (int i = 0; i < partitionFields.size(); i++) {
            var colDomain = domains.get(startIdx + i);
            if (colDomain == null || colDomain.isAll()) {
                continue;
            }
            PartitionField field = partitionFields.get(i);
            var sourceName = schema.findColumnName(field.sourceId());
            var trinoType = toTrinoType(schema.findType(field.sourceId()), typeManager);
            var exp = domainToExpression(sourceName, trinoType, colDomain);
            if (exp.isPresent()) {
                result = Expressions.and(result, exp.get());
            }
        }
        return result;
    }

    private static Optional<Expression> domainToExpression(String fieldName,
            io.trino.spi.type.Type trinoType,
            Domain domain)
    {
        if (domain.isAll()) {
            return Optional.empty();
        }
        else if (domain.isNone()) {
            return Optional.of(Expressions.alwaysFalse());
        }
        else if (domain.isOnlyNull()) {
            return Optional.of(Expressions.isNull(fieldName));
        }
        else if (domain.getValues().isAll() && !domain.isNullAllowed()) {
            return Optional.of(Expressions.notNull(fieldName));
        }
        else {
            var rangeExp = domain.getValues()
                    .getRanges()
                    .getOrderedRanges()
                    .stream()
                    .map(range -> rangeToExpression(fieldName, trinoType, range))
                    .toList();
            var valExp = rangeExp.stream()
                    .reduce(Expressions::or)
                    .orElse(Expressions.alwaysFalse());

            return Optional.of(domain.isNullAllowed()
                    ? Expressions.or(valExp, Expressions.isNull(fieldName))
                    : valExp);
        }
    }

    private static Expression rangeToExpression(String fieldName,
            io.trino.spi.type.Type trinoType,
            Range range)
    {
        if (range.isSingleValue()) {
            var icebergValue = convertTrinoValueToIceberg(trinoType, range.getSingleValue());
            return Expressions.equal(fieldName, icebergValue);
        }

        Expression exp = Expressions.alwaysTrue();
        if (!range.isLowUnbounded()) {
            var low = convertTrinoValueToIceberg(trinoType, range.getLowBoundedValue());
            exp = Expressions.and(exp, range.isLowInclusive()
                    ? Expressions.greaterThanOrEqual(fieldName, low)
                    : Expressions.greaterThan(fieldName, low));
        }

        if (!range.isHighUnbounded()) {
            var high = convertTrinoValueToIceberg(trinoType, range.getHighBoundedValue());
            exp = Expressions.and(exp, range.isHighInclusive()
                    ? Expressions.greaterThanOrEqual(fieldName, high)
                    : Expressions.greaterThan(fieldName, high));
        }
        return exp;
    }

    private Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics> getStatisticsByPartition(TableScan tableScan)
    {
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            Map<StructLikeWrapperWithFieldIdToIndex, IcebergStatistics.Builder> partitions = new HashMap<>();
            long fileCount = 0;
            for (FileScanTask fileScanTask : fileScanTasks) {
                if (fileCount > MAX_FILES_PER_PARTITIONS_SCAN) {
                    throw new TrinoException(QUERY_REJECTED,
                            format("Scan on %s exceeded threshold %d. Add a more selective predicate " +
                                            "on top-level partition column to prune the manifest",
                                    icebergTable.name(), MAX_FILES_PER_PARTITIONS_SCAN));
                }
                DataFile dataFile = fileScanTask.file();
                StructLikeWrapperWithFieldIdToIndex structLikeWrapperWithFieldIdToIndex = createStructLikeWrapper(fileScanTask);

                partitions.computeIfAbsent(
                                structLikeWrapperWithFieldIdToIndex,
                                _ -> new IcebergStatistics.Builder(icebergTable.schema().columns(), typeManager))
                        .acceptDataFile(dataFile, fileScanTask.spec());
                fileCount++;
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
            StructLikeWrapperWithFieldIdToIndex partitionStruct = partitionEntry.getKey();
            IcebergStatistics icebergStatistics = partitionEntry.getValue();
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

            // add top level partition columns
            for (PartitionField field : partitionFields) {
                if (field.transform().isIdentity()) {
                    Type sourceType = icebergTable.schema().findType(field.sourceId());
                    Integer fieldId = field.fieldId();
                    if (partitionStruct.getFieldIdToIndex().containsKey(fieldId)) {
                        row.add(convertIcebergValueToTrino(
                                sourceType,
                                partitionStruct.getStructLikeWrapper()
                                        .get()
                                        .get(partitionStruct.getFieldIdToIndex().get(fieldId),
                                                sourceType.typeId().javaClass())));
                    }
                    else {
                        row.add(null);
                    }
                }
            }

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

            records.add(row);
        }

        return new InMemoryRecordSet(resultTypes, records.build()).cursor();
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
