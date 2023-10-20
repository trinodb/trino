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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.iceberg.IcebergAggregationTableHandle.Aggregation;
import io.trino.plugin.iceberg.IcebergAggregationTableHandle.AggregationType;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.plugin.iceberg.util.DataFileWithDeleteFiles;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.Utils;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.intersection;
import static com.google.common.math.LongMath.saturatedAdd;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergAggregationTableHandle.AggregationType.MIN;
import static io.trino.plugin.iceberg.IcebergColumnHandle.fileModifiedTimeColumnHandle;
import static io.trino.plugin.iceberg.IcebergColumnHandle.pathColumnHandle;
import static io.trino.plugin.iceberg.IcebergEmptySplit.ICEBERG_EMPTY_SPLIT;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getSplitSize;
import static io.trino.plugin.iceberg.IcebergSplitManager.ICEBERG_DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionValues;
import static io.trino.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.iceberg.types.Conversions.fromByteBuffer;

public class IcebergSplitSource
        implements ConnectorSplitSource
{
    private static final ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitBatch(ImmutableList.of(), false);
    private static final ConnectorSplitBatch NO_MORE_SPLITS_BATCH = new ConnectorSplitBatch(ImmutableList.of(), true);

    // TODO what should be the value?
    private static final int MIN_MAX_TRUSTED_VARCHAR_LENGTH_LIMIT = 16;

    private final TrinoFileSystemFactory fileSystemFactory;
    private final ConnectorSession session;
    private final IcebergTableHandle tableHandle;
    private final TableScan tableScan;
    private final Optional<Long> maxScannedFileSizeInBytes;
    private final Map<Integer, Type.PrimitiveType> fieldIdToType;
    private final DynamicFilter dynamicFilter;
    private final long dynamicFilteringWaitTimeoutMillis;
    private final Stopwatch dynamicFilterWaitStopwatch;
    private final Constraint constraint;
    private final TypeManager typeManager;
    private final Closer closer = Closer.create();
    private final double minimumAssignedSplitWeight;
    private final Set<Aggregation> aggregations;
    private final Set<Integer> unconditionallyProjectedBaseColumns;
    private final TupleDomain<IcebergColumnHandle> dataColumnPredicate;
    private final Domain pathDomain;
    private final Domain fileModifiedTimeDomain;
    private final OptionalLong limit;

    private TupleDomain<IcebergColumnHandle> pushedDownDynamicFilterPredicate;
    private CloseableIterable<FileScanTask> fileScanIterable;
    private long targetSplitSize;
    private CloseableIterator<FileScanTask> fileScanIterator;
    private Optional<List<Block>> fileAggregateValues = Optional.empty();
    private Iterator<FileScanTask> fileTasksIterator = emptyIterator();
    private boolean fileHasAnyDeletions;

    private final boolean recordScannedFiles;
    private final ImmutableSet.Builder<DataFileWithDeleteFiles> scannedFiles = ImmutableSet.builder();
    private long outputRowsLowerBound;
    private boolean needOneSplit;

    public IcebergSplitSource(
            TrinoFileSystemFactory fileSystemFactory,
            ConnectorSession session,
            IcebergTableHandle tableHandle,
            TableScan tableScan,
            Optional<DataSize> maxScannedFileSize,
            Set<Aggregation> aggregations,
            Set<Integer> unconditionallyProjectedBaseColumns,
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringWaitTimeout,
            Constraint constraint,
            TypeManager typeManager,
            boolean recordScannedFiles,
            double minimumAssignedSplitWeight)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.session = requireNonNull(session, "session is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.tableScan = requireNonNull(tableScan, "tableScan is null");
        this.maxScannedFileSizeInBytes = maxScannedFileSize.map(DataSize::toBytes);
        this.fieldIdToType = primitiveFieldTypes(tableScan.schema());
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.dynamicFilteringWaitTimeoutMillis = dynamicFilteringWaitTimeout.toMillis();
        this.dynamicFilterWaitStopwatch = Stopwatch.createStarted();
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.aggregations = ImmutableSet.copyOf(requireNonNull(aggregations, "aggregations is null"));
        this.unconditionallyProjectedBaseColumns = ImmutableSet.copyOf(requireNonNull(unconditionallyProjectedBaseColumns, "unconditionallyProjectedBaseColumns is null"));
        this.recordScannedFiles = recordScannedFiles;
        this.minimumAssignedSplitWeight = minimumAssignedSplitWeight;
        this.dataColumnPredicate = tableHandle.getEnforcedPredicate().filter((column, domain) -> !isMetadataColumnId(column.getId()));
        this.pathDomain = getPathDomain(tableHandle.getEnforcedPredicate());
        checkArgument(
                tableHandle.getUnenforcedPredicate().isAll() || tableHandle.getLimit().isEmpty(),
                "Cannot enforce LIMIT %s with unenforced predicate %s present",
                tableHandle.getLimit(),
                tableHandle.getUnenforcedPredicate());
        this.needOneSplit = unconditionallyProjectedBaseColumns.isEmpty() /* global aggregation */ && aggregations.stream().anyMatch(Aggregation::nonNullOnEmpty);
        this.limit = tableHandle.getLimit();
        this.fileModifiedTimeDomain = getFileModifiedTimePathDomain(tableHandle.getEnforcedPredicate());
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        long timeLeft = dynamicFilteringWaitTimeoutMillis - dynamicFilterWaitStopwatch.elapsed(MILLISECONDS);
        if (dynamicFilter.isAwaitable() && timeLeft > 0) {
            return dynamicFilter.isBlocked()
                    .thenApply(ignored -> EMPTY_BATCH)
                    .completeOnTimeout(EMPTY_BATCH, timeLeft, MILLISECONDS);
        }

        if (fileScanIterable == null) {
            // Used to avoid duplicating work if the Dynamic Filter was already pushed down to the Iceberg API
            boolean dynamicFilterIsComplete = dynamicFilter.isComplete();
            this.pushedDownDynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                    .filter((columnHandle, domain) -> !(columnHandle instanceof Aggregation))
                    .transformKeys(IcebergColumnHandle.class::cast);
            TupleDomain<IcebergColumnHandle> fullPredicate = tableHandle.getUnenforcedPredicate()
                    .intersect(pushedDownDynamicFilterPredicate);
            // TODO: (https://github.com/trinodb/trino/issues/9743): Consider removing TupleDomain#simplify
            TupleDomain<IcebergColumnHandle> simplifiedPredicate = fullPredicate.simplify(ICEBERG_DOMAIN_COMPACTION_THRESHOLD);
            boolean usedSimplifiedPredicate = !simplifiedPredicate.equals(fullPredicate);
            if (usedSimplifiedPredicate) {
                // Pushed down predicate was simplified, always evaluate it against individual splits
                this.pushedDownDynamicFilterPredicate = TupleDomain.all();
            }

            TupleDomain<IcebergColumnHandle> effectivePredicate = dataColumnPredicate
                    .intersect(simplifiedPredicate);

            if (effectivePredicate.isNone()) {
                finish();
                return completedFuture(NO_MORE_SPLITS_BATCH);
            }

            Expression filterExpression = toIcebergExpression(effectivePredicate);
            // If the Dynamic Filter will be evaluated against each file, stats are required. Otherwise, skip them.
            boolean requiresColumnStats = usedSimplifiedPredicate || !dynamicFilterIsComplete || !aggregations.isEmpty();
            TableScan scan = tableScan.filter(filterExpression);
            if (requiresColumnStats) {
                scan = scan.includeColumnStats();
            }
            this.fileScanIterable = closer.register(scan.planFiles());
            this.targetSplitSize = getSplitSize(session)
                    .map(DataSize::toBytes)
                    .orElseGet(tableScan::targetSplitSize);
            this.fileScanIterator = closer.register(fileScanIterable.iterator());
            this.fileTasksIterator = emptyIterator();
        }

        TupleDomain<IcebergColumnHandle> dynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                .filter((columnHandle, domain) -> !(columnHandle instanceof Aggregation))
                .transformKeys(IcebergColumnHandle.class::cast);
        if (dynamicFilterPredicate.isNone()) {
            finish();
            if (needOneSplit) {
                needOneSplit = false;
                return completedFuture(new ConnectorSplitBatch(ImmutableList.of(ICEBERG_EMPTY_SPLIT), true));
            }
            return completedFuture(NO_MORE_SPLITS_BATCH);
        }

        List<ConnectorSplit> splits = new ArrayList<>(maxSize);
        while (splits.size() < maxSize && (fileTasksIterator.hasNext() || fileScanIterator.hasNext())) {
            if (!fileTasksIterator.hasNext()) {
                FileScanTask wholeFileTask = fileScanIterator.next();
                this.fileAggregateValues = Optional.empty();
                if (wholeFileTask.deletes().isEmpty() && noDataColumnsProjected(wholeFileTask) && satisfyAggregations(wholeFileTask)) {
                    fileTasksIterator = List.of(wholeFileTask).iterator();
                }
                else {
                    fileTasksIterator = wholeFileTask.split(targetSplitSize).iterator();
                }
                fileHasAnyDeletions = false;
                // In theory, .split() could produce empty iterator, so let's evaluate the outer loop condition again.
                continue;
            }
            FileScanTask scanTask = fileTasksIterator.next();
            fileHasAnyDeletions = fileHasAnyDeletions || !scanTask.deletes().isEmpty();
            if (scanTask.deletes().isEmpty() &&
                    maxScannedFileSizeInBytes.isPresent() &&
                    scanTask.file().fileSizeInBytes() > maxScannedFileSizeInBytes.get()) {
                continue;
            }

            if (!pathDomain.includesNullableValue(utf8Slice(scanTask.file().path().toString()))) {
                continue;
            }
            if (!fileModifiedTimeDomain.isAll()) {
                long fileModifiedTime = getModificationTime(scanTask.file().path().toString());
                if (!fileModifiedTimeDomain.includesNullableValue(packDateTimeWithZone(fileModifiedTime, UTC_KEY))) {
                    continue;
                }
            }
            IcebergSplit icebergSplit = toIcebergSplit(scanTask);

            Schema fileSchema = scanTask.spec().schema();
            Map<Integer, Optional<String>> partitionKeys = getPartitionKeys(scanTask);

            Set<IcebergColumnHandle> identityPartitionColumns = partitionKeys.keySet().stream()
                    .map(fieldId -> getColumnHandle(fileSchema.findField(fieldId), typeManager))
                    .collect(toImmutableSet());

            Supplier<Map<ColumnHandle, NullableValue>> partitionValues = memoize(() -> getPartitionValues(identityPartitionColumns, partitionKeys));

            if (!dynamicFilterPredicate.isAll() && !dynamicFilterPredicate.equals(pushedDownDynamicFilterPredicate)) {
                if (!partitionMatchesPredicate(
                        identityPartitionColumns,
                        partitionValues,
                        dynamicFilterPredicate)) {
                    continue;
                }
                if (!fileMatchesPredicate(
                        fieldIdToType,
                        dynamicFilterPredicate,
                        scanTask.file().lowerBounds(),
                        scanTask.file().upperBounds(),
                        scanTask.file().nullValueCounts())) {
                    continue;
                }
            }
            if (!partitionMatchesConstraint(identityPartitionColumns, partitionValues, constraint)) {
                continue;
            }
            if (recordScannedFiles) {
                // Positional and Equality deletes can only be cleaned up if the whole table has been optimized.
                // Equality deletes may apply to many files, and position deletes may be grouped together. This makes it difficult to know if they are obsolete.
                List<org.apache.iceberg.DeleteFile> fullyAppliedDeletes = tableHandle.getEnforcedPredicate().isAll() ? scanTask.deletes() : ImmutableList.of();
                scannedFiles.add(new DataFileWithDeleteFiles(scanTask.file(), fullyAppliedDeletes));
            }
            if (!fileTasksIterator.hasNext()) {
                // This is the last task for this file
                if (!fileHasAnyDeletions) {
                    // There were no deletions, so we produced splits covering the whole file
                    outputRowsLowerBound = saturatedAdd(outputRowsLowerBound, scanTask.file().recordCount());
                    if (limit.isPresent() && limit.getAsLong() <= outputRowsLowerBound) {
                        finish();
                    }
                }
            }
            splits.add(icebergSplit);
        }
        if (needOneSplit && splits.isEmpty()) {
            splits.add(ICEBERG_EMPTY_SPLIT);
        }
        needOneSplit = false;
        return completedFuture(new ConnectorSplitBatch(splits, isFinished()));
    }

    private boolean noDataColumnsProjected(FileScanTask fileScanTask)
    {
        return fileScanTask.spec().fields().stream()
                .filter(partitionField -> partitionField.transform().isIdentity())
                .map(PartitionField::sourceId)
                .collect(toImmutableSet())
                .containsAll(unconditionallyProjectedBaseColumns);
    }

    private boolean satisfyAggregations(FileScanTask wholeFileTask)
    {
        checkState(fileAggregateValues.isEmpty(), "fileAggregateValues already set");

        // Invoked only for files where unconditionallyProjectedBaseColumns (groupBy) is only partitioning columns
        ImmutableList.Builder<Block> values = ImmutableList.builder();
        for (Aggregation aggregation : aggregations) {
            Optional<Object> value = satisfy(wholeFileTask.file(), aggregation);
            if (value.isEmpty()) {
                return false;
            }
            values.add(Utils.nativeValueToBlock(aggregation.outputType(), value.get()));
        }
        fileAggregateValues = Optional.of(values.build());
        return true;
    }

    private Optional<Object> satisfy(DataFile dataFile, Aggregation aggregation)
    {
        AggregationType aggregationType = aggregation.aggregationType();
        return switch (aggregationType) {
            case COUNT_ALL -> Optional.of(dataFile.recordCount());
            case COUNT_NON_NULL -> {
                IcebergColumnHandle argument = getOnlyElement(aggregation.arguments());
                int fieldId = argument.getId();
                Long valueCount = firstNonNull(dataFile.valueCounts(), ImmutableMap.<Integer, Long>of()).get(fieldId);
                Long nullCount = firstNonNull(dataFile.nullValueCounts(), ImmutableMap.<Integer, Long>of()).get(fieldId);
                if (valueCount == null || nullCount == null) {
                    yield Optional.empty();
                }
                yield Optional.of(valueCount - nullCount);
            }
            case MIN, MAX -> {
                IcebergColumnHandle argument = getOnlyElement(aggregation.arguments());
                io.trino.spi.type.Type argumentTrinoType = argument.getType();
                int fieldId = argument.getId();
                Map<Integer, ByteBuffer> bounds = firstNonNull((aggregationType == MIN) ? dataFile.lowerBounds() : dataFile.upperBounds(), ImmutableMap.of());
                if (!bounds.containsKey(fieldId)) {
                    yield Optional.empty();
                }
                Type icebergType = fieldIdToType.get(fieldId);
                Object value = convertIcebergValueToTrino(icebergType, fromByteBuffer(icebergType, bounds.get(fieldId)));
                if (argumentTrinoType instanceof VarcharType) {
                    // TODO consult org.apache.iceberg.MetricsModes.MetricsMode
                    // TODO this is incorrect for max: truncate("abc<max-code-point><max-code-point>...<max-code-point>") -> "abd", so upper bound value being "abd"
                    //  does not imply the max value is "abd". For this to work Iceberg would need to track upper bound exactness, like Parquet https://github.com/apache/parquet-format/pull/216
                    if (countCodePoints((Slice) value) >= MIN_MAX_TRUSTED_VARCHAR_LENGTH_LIMIT) {
                        // Might be truncated
                        yield Optional.empty();
                    }
                }
                yield Optional.of(value);
            }
        };
    }

    private long getModificationTime(String path)
    {
        try {
            TrinoInputFile inputFile = fileSystemFactory.create(session).newInputFile(Location.of(path));
            return inputFile.lastModified().toEpochMilli();
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to get file modification time: " + path, e);
        }
    }

    private void finish()
    {
        close();
        this.fileScanIterable = CloseableIterable.empty();
        this.fileScanIterator = CloseableIterator.empty();
        this.fileTasksIterator = emptyIterator();
    }

    @Override
    public boolean isFinished()
    {
        return fileScanIterator != null && !fileScanIterator.hasNext() && !fileTasksIterator.hasNext();
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        checkState(isFinished(), "Split source must be finished before TableExecuteSplitsInfo is read");
        if (!recordScannedFiles) {
            return Optional.empty();
        }
        return Optional.of(ImmutableList.copyOf(scannedFiles.build()));
    }

    @Override
    public void close()
    {
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @VisibleForTesting
    static boolean fileMatchesPredicate(
            Map<Integer, Type.PrimitiveType> primitiveTypeForFieldId,
            TupleDomain<IcebergColumnHandle> dynamicFilterPredicate,
            @Nullable Map<Integer, ByteBuffer> lowerBounds,
            @Nullable Map<Integer, ByteBuffer> upperBounds,
            @Nullable Map<Integer, Long> nullValueCounts)
    {
        if (dynamicFilterPredicate.isNone()) {
            return false;
        }
        Map<IcebergColumnHandle, Domain> domains = dynamicFilterPredicate.getDomains().orElseThrow();

        for (Map.Entry<IcebergColumnHandle, Domain> domainEntry : domains.entrySet()) {
            IcebergColumnHandle column = domainEntry.getKey();
            Domain domain = domainEntry.getValue();

            int fieldId = column.getId();
            boolean mayContainNulls;
            if (nullValueCounts == null) {
                mayContainNulls = true;
            }
            else {
                Long nullValueCount = nullValueCounts.get(fieldId);
                mayContainNulls = nullValueCount == null || nullValueCount > 0;
            }
            Type type = primitiveTypeForFieldId.get(fieldId);
            Domain statisticsDomain = domainForStatistics(
                    column,
                    lowerBounds == null ? null : fromByteBuffer(type, lowerBounds.get(fieldId)),
                    upperBounds == null ? null : fromByteBuffer(type, upperBounds.get(fieldId)),
                    mayContainNulls);
            if (!domain.overlaps(statisticsDomain)) {
                return false;
            }
        }
        return true;
    }

    private static Domain domainForStatistics(
            IcebergColumnHandle columnHandle,
            @Nullable Object lowerBound,
            @Nullable Object upperBound,
            boolean mayContainNulls)
    {
        io.trino.spi.type.Type type = columnHandle.getType();
        Type icebergType = toIcebergType(type, columnHandle.getColumnIdentity());
        if (lowerBound == null && upperBound == null) {
            return Domain.create(ValueSet.all(type), mayContainNulls);
        }

        Range statisticsRange;
        if (lowerBound != null && upperBound != null) {
            statisticsRange = Range.range(
                    type,
                    convertIcebergValueToTrino(icebergType, lowerBound),
                    true,
                    convertIcebergValueToTrino(icebergType, upperBound),
                    true);
        }
        else if (upperBound != null) {
            statisticsRange = Range.lessThanOrEqual(type, convertIcebergValueToTrino(icebergType, upperBound));
        }
        else {
            statisticsRange = Range.greaterThanOrEqual(type, convertIcebergValueToTrino(icebergType, lowerBound));
        }
        return Domain.create(ValueSet.ofRanges(statisticsRange), mayContainNulls);
    }

    static boolean partitionMatchesConstraint(
            Set<IcebergColumnHandle> identityPartitionColumns,
            Supplier<Map<ColumnHandle, NullableValue>> partitionValues,
            Constraint constraint)
    {
        // We use Constraint just to pass functional predicate here from DistributedExecutionPlanner
        verify(constraint.getSummary().isAll());

        if (constraint.predicate().isEmpty() ||
                intersection(constraint.getPredicateColumns().orElseThrow(), identityPartitionColumns).isEmpty()) {
            return true;
        }
        return constraint.predicate().get().test(partitionValues.get());
    }

    @VisibleForTesting
    static boolean partitionMatchesPredicate(
            Set<IcebergColumnHandle> identityPartitionColumns,
            Supplier<Map<ColumnHandle, NullableValue>> partitionValues,
            TupleDomain<IcebergColumnHandle> dynamicFilterPredicate)
    {
        if (dynamicFilterPredicate.isNone()) {
            return false;
        }
        Map<IcebergColumnHandle, Domain> domains = dynamicFilterPredicate.getDomains().orElseThrow();

        for (IcebergColumnHandle partitionColumn : identityPartitionColumns) {
            Domain allowedDomain = domains.get(partitionColumn);
            if (allowedDomain != null) {
                if (!allowedDomain.includesNullableValue(partitionValues.get().get(partitionColumn).getValue())) {
                    return false;
                }
            }
        }
        return true;
    }

    private IcebergSplit toIcebergSplit(FileScanTask task)
    {
        return new IcebergSplit(
                task.file().path().toString(),
                task.start(),
                task.length(),
                task.file().fileSizeInBytes(),
                task.file().recordCount(),
                fileAggregateValues,
                IcebergFileFormat.fromIceberg(task.file().format()),
                PartitionSpecParser.toJson(task.spec()),
                PartitionData.toJson(task.file().partition()),
                task.deletes().stream()
                        .map(DeleteFile::fromIceberg)
                        .collect(toImmutableList()),
                SplitWeight.fromProportion(Math.min(Math.max((double) task.length() / tableScan.targetSplitSize(), minimumAssignedSplitWeight), 1.0)));
    }

    private static Domain getPathDomain(TupleDomain<IcebergColumnHandle> effectivePredicate)
    {
        IcebergColumnHandle pathColumn = pathColumnHandle();
        Domain domain = effectivePredicate.getDomains().orElseThrow(() -> new IllegalArgumentException("Unexpected NONE tuple domain"))
                .get(pathColumn);
        if (domain == null) {
            return Domain.all(pathColumn.getType());
        }
        return domain;
    }

    private static Domain getFileModifiedTimePathDomain(TupleDomain<IcebergColumnHandle> effectivePredicate)
    {
        IcebergColumnHandle fileModifiedTimeColumn = fileModifiedTimeColumnHandle();
        Domain domain = effectivePredicate.getDomains().orElseThrow(() -> new IllegalArgumentException("Unexpected NONE tuple domain"))
                .get(fileModifiedTimeColumn);
        if (domain == null) {
            return Domain.all(fileModifiedTimeColumn.getType());
        }
        return domain;
    }
}
