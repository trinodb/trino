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
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.cache.NonEvictableCache;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.plugin.iceberg.util.DataFileWithDeleteFiles;
import io.trino.spi.SplitWeight;
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
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
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
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static com.google.common.math.LongMath.saturatedAdd;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.iceberg.ExpressionConverter.isConvertableToIcebergExpression;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getSplitSize;
import static io.trino.plugin.iceberg.IcebergSplitManager.ICEBERG_DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.getFileModifiedTimePathDomain;
import static io.trino.plugin.iceberg.IcebergUtil.getModificationTime;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionValues;
import static io.trino.plugin.iceberg.IcebergUtil.getPathDomain;
import static io.trino.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.lang.Math.clamp;
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

    private final IcebergFileSystemFactory fileSystemFactory;
    private final ConnectorSession session;
    private final IcebergTableHandle tableHandle;
    private final Map<String, String> fileIoProperties;
    private final TableScan tableScan;
    private final Optional<Long> maxScannedFileSizeInBytes;
    private final Map<Integer, Type.PrimitiveType> fieldIdToType;
    private final DynamicFilter dynamicFilter;
    private final long dynamicFilteringWaitTimeoutMillis;
    private final Stopwatch dynamicFilterWaitStopwatch;
    private final PartitionConstraintMatcher partitionConstraintMatcher;
    private final TypeManager typeManager;
    private final Closer closer = Closer.create();
    private final double minimumAssignedSplitWeight;
    private final Set<Integer> projectedBaseColumns;
    private final TupleDomain<IcebergColumnHandle> dataColumnPredicate;
    private final Domain pathDomain;
    private final Domain fileModifiedTimeDomain;
    private final OptionalLong limit;
    private final Set<Integer> predicatedColumnIds;

    private TupleDomain<IcebergColumnHandle> pushedDownDynamicFilterPredicate;
    private CloseableIterable<FileScanTask> fileScanIterable;
    private long targetSplitSize;
    private CloseableIterator<FileScanTask> fileScanIterator;
    private Iterator<FileScanTask> fileTasksIterator = emptyIterator();
    private TupleDomain<IcebergColumnHandle> fileStatisticsDomain;

    private final boolean recordScannedFiles;
    private final ImmutableSet.Builder<DataFileWithDeleteFiles> scannedFiles = ImmutableSet.builder();
    private long outputRowsLowerBound;
    private final CachingHostAddressProvider cachingHostAddressProvider;

    public IcebergSplitSource(
            IcebergFileSystemFactory fileSystemFactory,
            ConnectorSession session,
            IcebergTableHandle tableHandle,
            Map<String, String> fileIoProperties,
            TableScan tableScan,
            Optional<DataSize> maxScannedFileSize,
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringWaitTimeout,
            Constraint constraint,
            TypeManager typeManager,
            boolean recordScannedFiles,
            double minimumAssignedSplitWeight,
            CachingHostAddressProvider cachingHostAddressProvider)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.session = requireNonNull(session, "session is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.fileIoProperties = requireNonNull(fileIoProperties, "fileIoProperties is null");
        this.tableScan = requireNonNull(tableScan, "tableScan is null");
        this.maxScannedFileSizeInBytes = maxScannedFileSize.map(DataSize::toBytes);
        this.fieldIdToType = primitiveFieldTypes(tableScan.schema());
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.dynamicFilteringWaitTimeoutMillis = dynamicFilteringWaitTimeout.toMillis();
        this.dynamicFilterWaitStopwatch = Stopwatch.createStarted();
        this.partitionConstraintMatcher = new PartitionConstraintMatcher(constraint);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.recordScannedFiles = recordScannedFiles;
        this.minimumAssignedSplitWeight = minimumAssignedSplitWeight;
        this.projectedBaseColumns = tableHandle.getProjectedColumns().stream()
                .map(column -> column.getBaseColumnIdentity().getId())
                .collect(toImmutableSet());
        this.dataColumnPredicate = tableHandle.getEnforcedPredicate().filter((column, domain) -> !isMetadataColumnId(column.getId()));
        this.pathDomain = getPathDomain(tableHandle.getEnforcedPredicate());
        checkArgument(
                tableHandle.getUnenforcedPredicate().isAll() || tableHandle.getLimit().isEmpty(),
                "Cannot enforce LIMIT %s with unenforced predicate %s present",
                tableHandle.getLimit(),
                tableHandle.getUnenforcedPredicate());
        this.limit = tableHandle.getLimit();
        this.predicatedColumnIds = Stream.concat(
                        tableHandle.getUnenforcedPredicate().getDomains().orElse(ImmutableMap.of()).keySet().stream(),
                        dynamicFilter.getColumnsCovered().stream()
                                .map(IcebergColumnHandle.class::cast))
                .map(IcebergColumnHandle::getId)
                .collect(toImmutableSet());
        this.fileModifiedTimeDomain = getFileModifiedTimePathDomain(tableHandle.getEnforcedPredicate());
        this.cachingHostAddressProvider = requireNonNull(cachingHostAddressProvider, "cachingHostAddressProvider is null");
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
            this.pushedDownDynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                    .transformKeys(IcebergColumnHandle.class::cast)
                    .filter((columnHandle, domain) -> isConvertableToIcebergExpression(domain));
            TupleDomain<IcebergColumnHandle> fullPredicate = tableHandle.getUnenforcedPredicate()
                    .intersect(pushedDownDynamicFilterPredicate);
            // TODO: (https://github.com/trinodb/trino/issues/9743): Consider removing TupleDomain#simplify
            TupleDomain<IcebergColumnHandle> simplifiedPredicate = fullPredicate.simplify(ICEBERG_DOMAIN_COMPACTION_THRESHOLD);
            if (!simplifiedPredicate.equals(fullPredicate)) {
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
            // Use stats to populate fileStatisticsDomain if there are predicated columns. Otherwise, skip them.
            boolean requiresColumnStats = !predicatedColumnIds.isEmpty();
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
                .transformKeys(IcebergColumnHandle.class::cast);
        if (dynamicFilterPredicate.isNone()) {
            finish();
            return completedFuture(NO_MORE_SPLITS_BATCH);
        }

        List<ConnectorSplit> splits = new ArrayList<>(maxSize);
        while (splits.size() < maxSize && (fileTasksIterator.hasNext() || fileScanIterator.hasNext())) {
            if (!fileTasksIterator.hasNext()) {
                if (limit.isPresent() && limit.getAsLong() <= outputRowsLowerBound) {
                    finish();
                    break;
                }
                FileScanTask wholeFileTask = fileScanIterator.next();
                boolean fileHasNoDeletions = wholeFileTask.deletes().isEmpty();

                fileStatisticsDomain = createFileStatisticsDomain(wholeFileTask);
                if (pruneFileScanTask(wholeFileTask, fileHasNoDeletions, dynamicFilterPredicate, fileStatisticsDomain)) {
                    continue;
                }

                if (recordScannedFiles) {
                    // Positional and Equality deletes can only be cleaned up if the whole table has been optimized.
                    // Equality deletes may apply to many files, and position deletes may be grouped together. This makes it difficult to know if they are obsolete.
                    List<org.apache.iceberg.DeleteFile> fullyAppliedDeletes = tableHandle.getEnforcedPredicate().isAll() ? wholeFileTask.deletes() : ImmutableList.of();
                    scannedFiles.add(new DataFileWithDeleteFiles(wholeFileTask.file(), fullyAppliedDeletes));
                }

                if (fileHasNoDeletions) {
                    // There were no deletions, so we will produce splits covering the whole file
                    outputRowsLowerBound = saturatedAdd(outputRowsLowerBound, wholeFileTask.file().recordCount());
                }

                if (fileHasNoDeletions && noDataColumnsProjected(wholeFileTask)) {
                    fileTasksIterator = List.of(wholeFileTask).iterator();
                }
                else {
                    fileTasksIterator = wholeFileTask.split(targetSplitSize).iterator();
                }
                // In theory, .split() could produce empty iterator, so let's evaluate the outer loop condition again.
                continue;
            }
            splits.add(toIcebergSplit(fileTasksIterator.next(), fileStatisticsDomain));
        }
        return completedFuture(new ConnectorSplitBatch(splits, isFinished()));
    }

    private boolean pruneFileScanTask(FileScanTask fileScanTask, boolean fileHasNoDeletions, TupleDomain<IcebergColumnHandle> dynamicFilterPredicate, TupleDomain<IcebergColumnHandle> fileStatisticsDomain)
    {
        if (fileHasNoDeletions &&
                maxScannedFileSizeInBytes.isPresent() &&
                fileScanTask.file().fileSizeInBytes() > maxScannedFileSizeInBytes.get()) {
            return true;
        }

        if (!pathDomain.isAll() && !pathDomain.includesNullableValue(utf8Slice(fileScanTask.file().path().toString()))) {
            return true;
        }
        if (!fileModifiedTimeDomain.isAll()) {
            long fileModifiedTime = getModificationTime(fileScanTask.file().path().toString(), fileSystemFactory.create(session.getIdentity(), fileIoProperties));
            if (!fileModifiedTimeDomain.includesNullableValue(packDateTimeWithZone(fileModifiedTime, UTC_KEY))) {
                return true;
            }
        }

        Schema fileSchema = fileScanTask.schema();
        Map<Integer, Optional<String>> partitionKeys = getPartitionKeys(fileScanTask);

        Set<IcebergColumnHandle> identityPartitionColumns = partitionKeys.keySet().stream()
                .map(fieldId -> getColumnHandle(fileSchema.findField(fieldId), typeManager))
                .collect(toImmutableSet());

        Supplier<Map<ColumnHandle, NullableValue>> partitionValues = memoize(() -> getPartitionValues(identityPartitionColumns, partitionKeys));

        if (!dynamicFilterPredicate.isAll() && !dynamicFilterPredicate.equals(pushedDownDynamicFilterPredicate)) {
            if (!partitionMatchesPredicate(
                    identityPartitionColumns,
                    partitionValues,
                    dynamicFilterPredicate)) {
                return true;
            }
            if (!fileStatisticsDomain.overlaps(dynamicFilterPredicate)) {
                return true;
            }
        }

        return !partitionConstraintMatcher.matches(identityPartitionColumns, partitionValues);
    }

    private boolean noDataColumnsProjected(FileScanTask fileScanTask)
    {
        return fileScanTask.spec().fields().stream()
                .filter(partitionField -> partitionField.transform().isIdentity())
                .map(PartitionField::sourceId)
                .collect(toImmutableSet())
                .containsAll(projectedBaseColumns);
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

    private TupleDomain<IcebergColumnHandle> createFileStatisticsDomain(FileScanTask wholeFileTask)
    {
        List<IcebergColumnHandle> predicatedColumns = wholeFileTask.schema().columns().stream()
                .filter(column -> predicatedColumnIds.contains(column.fieldId()))
                .map(column -> getColumnHandle(column, typeManager))
                .collect(toImmutableList());
        return createFileStatisticsDomain(
                fieldIdToType,
                wholeFileTask.file().lowerBounds(),
                wholeFileTask.file().upperBounds(),
                wholeFileTask.file().nullValueCounts(),
                predicatedColumns);
    }

    @VisibleForTesting
    static TupleDomain<IcebergColumnHandle> createFileStatisticsDomain(
            Map<Integer, Type.PrimitiveType> fieldIdToType,
            @Nullable Map<Integer, ByteBuffer> lowerBounds,
            @Nullable Map<Integer, ByteBuffer> upperBounds,
            @Nullable Map<Integer, Long> nullValueCounts,
            List<IcebergColumnHandle> predicatedColumns)
    {
        ImmutableMap.Builder<IcebergColumnHandle, Domain> domainBuilder = ImmutableMap.builder();
        for (IcebergColumnHandle column : predicatedColumns) {
            int fieldId = column.getId();
            boolean mayContainNulls;
            if (nullValueCounts == null) {
                mayContainNulls = true;
            }
            else {
                Long nullValueCount = nullValueCounts.get(fieldId);
                mayContainNulls = nullValueCount == null || nullValueCount > 0;
            }
            Type type = fieldIdToType.get(fieldId);
            domainBuilder.put(
                    column,
                    domainForStatistics(
                            column,
                            lowerBounds == null ? null : fromByteBuffer(type, lowerBounds.get(fieldId)),
                            upperBounds == null ? null : fromByteBuffer(type, upperBounds.get(fieldId)),
                            mayContainNulls));
        }
        return TupleDomain.withColumnDomains(domainBuilder.buildOrThrow());
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

    private static class PartitionConstraintMatcher
    {
        private final NonEvictableCache<Map<ColumnHandle, NullableValue>, Boolean> partitionConstraintResults;
        private final Optional<Predicate<Map<ColumnHandle, NullableValue>>> predicate;
        private final Optional<Set<ColumnHandle>> predicateColumns;

        private PartitionConstraintMatcher(Constraint constraint)
        {
            // We use Constraint just to pass functional predicate here from DistributedExecutionPlanner
            verify(constraint.getSummary().isAll());
            this.predicate = constraint.predicate();
            this.predicateColumns = constraint.getPredicateColumns();
            this.partitionConstraintResults = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));
        }

        boolean matches(
                Set<IcebergColumnHandle> identityPartitionColumns,
                Supplier<Map<ColumnHandle, NullableValue>> partitionValuesSupplier)
        {
            if (predicate.isEmpty()) {
                return true;
            }
            Set<ColumnHandle> predicatePartitionColumns = intersection(predicateColumns.orElseThrow(), identityPartitionColumns);
            if (predicatePartitionColumns.isEmpty()) {
                return true;
            }
            Map<ColumnHandle, NullableValue> partitionValues = partitionValuesSupplier.get();
            return uncheckedCacheGet(
                    partitionConstraintResults,
                    ImmutableMap.copyOf(Maps.filterKeys(partitionValues, predicatePartitionColumns::contains)),
                    () -> predicate.orElseThrow().test(partitionValues));
        }
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

    private IcebergSplit toIcebergSplit(FileScanTask task, TupleDomain<IcebergColumnHandle> fileStatisticsDomain)
    {
        return new IcebergSplit(
                task.file().path().toString(),
                task.start(),
                task.length(),
                task.file().fileSizeInBytes(),
                task.file().recordCount(),
                IcebergFileFormat.fromIceberg(task.file().format()),
                PartitionSpecParser.toJson(task.spec()),
                PartitionData.toJson(task.file().partition()),
                task.deletes().stream()
                        .map(DeleteFile::fromIceberg)
                        .collect(toImmutableList()),
                SplitWeight.fromProportion(clamp((double) task.length() / tableScan.targetSplitSize(), minimumAssignedSplitWeight, 1.0)),
                fileStatisticsDomain,
                fileIoProperties,
                cachingHostAddressProvider.getHosts(task.file().path().toString(), ImmutableList.of()));
    }
}
