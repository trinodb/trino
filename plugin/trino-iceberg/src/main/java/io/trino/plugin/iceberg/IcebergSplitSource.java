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
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
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
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.iceberg.ExpressionConverter.isConvertibleToIcebergExpression;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergExceptions.translateMetadataException;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getSplitSize;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.getFileModifiedTimePathDomain;
import static io.trino.plugin.iceberg.IcebergUtil.getModificationTime;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionDomain;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionValues;
import static io.trino.plugin.iceberg.IcebergUtil.getPathDomain;
import static io.trino.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.trino.plugin.iceberg.StructLikeWrapperWithFieldIdToIndex.createStructLikeWrapper;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.lang.Math.clamp;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.iceberg.FileContent.EQUALITY_DELETES;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.apache.iceberg.types.Conversions.fromByteBuffer;

public class IcebergSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(IcebergSplitSource.class);
    private static final ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitBatch(ImmutableList.of(), false);
    private static final ConnectorSplitBatch NO_MORE_SPLITS_BATCH = new ConnectorSplitBatch(ImmutableList.of(), true);

    private final IcebergFileSystemFactory fileSystemFactory;
    private final ConnectorSession session;
    private final IcebergTableHandle tableHandle;
    private final Map<String, String> fileIoProperties;
    private final Scan<?, FileScanTask, CombinedScanTask> tableScan;
    private final Optional<Long> maxScannedFileSizeInBytes;
    private final Map<Integer, Type.PrimitiveType> fieldIdToType;
    private final DynamicFilter dynamicFilter;
    private final long dynamicFilteringWaitTimeoutMillis;
    private final Stopwatch dynamicFilterWaitStopwatch;
    private final PartitionConstraintMatcher partitionConstraintMatcher;
    private final TypeManager typeManager;
    @GuardedBy("closer")
    private final Closer closer = Closer.create();
    @GuardedBy("closer")
    private boolean closed;
    @GuardedBy("closer")
    private ListenableFuture<ConnectorSplitBatch> currentBatchFuture;
    private final double minimumAssignedSplitWeight;
    private final Set<Integer> projectedBaseColumns;
    private final TupleDomain<IcebergColumnHandle> dataColumnPredicate;
    private final Domain partitionDomain;
    private final Domain pathDomain;
    private final Domain fileModifiedTimeDomain;
    private final OptionalLong limit;
    private final Set<Integer> predicatedColumnIds;
    private final ListeningExecutorService executor;

    @GuardedBy("this")
    private TupleDomain<IcebergColumnHandle> pushedDownDynamicFilterPredicate;
    @GuardedBy("this")
    private CloseableIterable<FileScanTask> fileScanIterable;
    @GuardedBy("this")
    private long targetSplitSize;
    @GuardedBy("this")
    private CloseableIterator<FileScanTask> fileScanIterator;
    @GuardedBy("this")
    private Iterator<FileScanTaskWithDomain> fileTasksIterator = emptyIterator();

    private final boolean recordScannedFiles;
    private final int currentSpecId;
    @GuardedBy("this")
    private final ImmutableSet.Builder<DataFileWithDeleteFiles> scannedFiles = ImmutableSet.builder();
    @GuardedBy("this")
    @Nullable
    private Map<StructLikeWrapperWithFieldIdToIndex, Optional<FileScanTaskWithDomain>> scannedFilesByPartition = new HashMap<>();
    @GuardedBy("this")
    private long outputRowsLowerBound;
    private final CachingHostAddressProvider cachingHostAddressProvider;
    private volatile boolean finished;

    public IcebergSplitSource(
            IcebergFileSystemFactory fileSystemFactory,
            ConnectorSession session,
            IcebergTableHandle tableHandle,
            Table icebergTable,
            Scan<?, FileScanTask, CombinedScanTask> tableScan,
            Optional<DataSize> maxScannedFileSize,
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringWaitTimeout,
            Constraint constraint,
            TypeManager typeManager,
            boolean recordScannedFiles,
            double minimumAssignedSplitWeight,
            CachingHostAddressProvider cachingHostAddressProvider,
            ListeningExecutorService executor)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.session = requireNonNull(session, "session is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.fileIoProperties = requireNonNull(icebergTable.io().properties(), "fileIoProperties is null");
        this.tableScan = requireNonNull(tableScan, "tableScan is null");
        this.maxScannedFileSizeInBytes = maxScannedFileSize.map(DataSize::toBytes);
        this.fieldIdToType = primitiveFieldTypes(tableScan.schema());
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.dynamicFilteringWaitTimeoutMillis = dynamicFilteringWaitTimeout.toMillis();
        this.dynamicFilterWaitStopwatch = Stopwatch.createStarted();
        this.partitionConstraintMatcher = new PartitionConstraintMatcher(constraint);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.recordScannedFiles = recordScannedFiles;
        this.currentSpecId = icebergTable.spec().specId();
        this.minimumAssignedSplitWeight = minimumAssignedSplitWeight;
        this.projectedBaseColumns = tableHandle.getProjectedColumns().stream()
                .map(column -> column.getBaseColumnIdentity().getId())
                .collect(toImmutableSet());
        this.dataColumnPredicate = tableHandle.getEnforcedPredicate().filter((column, domain) -> !isMetadataColumnId(column.getId()));
        this.partitionDomain = getPartitionDomain(tableHandle.getEnforcedPredicate());
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
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        long timeLeft = dynamicFilteringWaitTimeoutMillis - dynamicFilterWaitStopwatch.elapsed(MILLISECONDS);
        if (dynamicFilter.isAwaitable() && timeLeft > 0) {
            return dynamicFilter.isBlocked()
                    .thenApply(_ -> EMPTY_BATCH)
                    .completeOnTimeout(EMPTY_BATCH, timeLeft, MILLISECONDS);
        }

        ListenableFuture<ConnectorSplitBatch> nextBatchFuture;
        synchronized (closer) {
            checkState(!closed, "already closed");
            checkState(currentBatchFuture == null || currentBatchFuture.isDone(), "previous batch future is not done");

            // Avoids blocking the calling (scheduler) thread when producing splits, allowing other split sources to
            // start loading splits in parallel to each other
            nextBatchFuture = executor.submit(() -> getNextBatchInternal(maxSize));
            currentBatchFuture = nextBatchFuture;
        }

        return toCompletableFuture(nextBatchFuture).exceptionally(t -> {
            throw translateMetadataException(t, tableHandle.getSchemaTableName().toString());
        });
    }

    private synchronized ConnectorSplitBatch getNextBatchInternal(int maxSize)
    {
        if (fileScanIterable == null) {
            this.pushedDownDynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                    .transformKeys(IcebergColumnHandle.class::cast)
                    .filter((columnHandle, domain) -> isConvertibleToIcebergExpression(domain));

            TupleDomain<IcebergColumnHandle> effectivePredicate = TupleDomain.intersect(
                    ImmutableList.of(dataColumnPredicate, tableHandle.getUnenforcedPredicate(), pushedDownDynamicFilterPredicate));

            if (effectivePredicate.isNone()) {
                finish();
                return NO_MORE_SPLITS_BATCH;
            }

            Expression filterExpression = toIcebergExpression(effectivePredicate);
            Scan scan = (Scan) tableScan.filter(filterExpression);
            // Use stats to populate fileStatisticsDomain if there are predicated columns. Otherwise, skip them.
            if (!predicatedColumnIds.isEmpty()) {
                Schema schema = tableScan.schema();
                scan = (Scan) scan.includeColumnStats(
                        predicatedColumnIds.stream()
                                .map(schema::findColumnName)
                                // Newly added column may not be found in current snapshot schema until new files are added
                                .filter(Objects::nonNull)
                                .collect(toImmutableList()));
            }

            synchronized (closer) {
                checkState(!closed, "split source is closed");
                this.fileScanIterable = closer.register(scan.planFiles());
                this.targetSplitSize = getSplitSize(session)
                        .map(DataSize::toBytes)
                        .orElseGet(tableScan::targetSplitSize);
                this.fileScanIterator = closer.register(fileScanIterable.iterator());
                this.fileTasksIterator = emptyIterator();
            }
        }

        TupleDomain<IcebergColumnHandle> dynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                .transformKeys(IcebergColumnHandle.class::cast);
        if (dynamicFilterPredicate.isNone()) {
            finish();
            return NO_MORE_SPLITS_BATCH;
        }

        List<ConnectorSplit> splits = new ArrayList<>(maxSize);
        while (splits.size() < maxSize && (fileTasksIterator.hasNext() || fileScanIterator.hasNext())) {
            if (!fileTasksIterator.hasNext()) {
                if (limit.isPresent() && limit.getAsLong() <= outputRowsLowerBound) {
                    finish();
                    break;
                }

                List<FileScanTaskWithDomain> fileScanTasks = processFileScanTask(dynamicFilterPredicate);
                if (fileScanTasks.isEmpty()) {
                    continue;
                }

                fileTasksIterator = prepareFileTasksIterator(fileScanTasks);
                // In theory, .split() could produce empty iterator, so let's evaluate the outer loop condition again.
                continue;
            }
            splits.add(toIcebergSplit(fileTasksIterator.next()));
        }
        if (!fileScanIterator.hasNext() && !fileTasksIterator.hasNext()) {
            finish();
        }
        return new ConnectorSplitBatch(splits, isFinished());
    }

    private synchronized Iterator<FileScanTaskWithDomain> prepareFileTasksIterator(List<FileScanTaskWithDomain> fileScanTasks)
    {
        ImmutableList.Builder<FileScanTaskWithDomain> scanTaskBuilder = ImmutableList.builder();
        for (FileScanTaskWithDomain fileScanTaskWithDomain : fileScanTasks) {
            FileScanTask wholeFileTask = fileScanTaskWithDomain.fileScanTask();
            if (recordScannedFiles) {
                // Equality deletes can only be cleaned up if the whole table has been optimized.
                // Equality and position deletes may apply to many files, however position deletes are always local to a partition
                // https://github.com/apache/iceberg/blob/70c506ebad2dfc6d61b99c05efd59e884282bfa6/core/src/main/java/org/apache/iceberg/deletes/DeleteGranularity.java#L61
                // OPTIMIZE supports only enforced predicates which select whole partitions, so if there is no path or fileModifiedTime predicate, then we can clean up position deletes
                List<org.apache.iceberg.DeleteFile> fullyAppliedDeletes = wholeFileTask.deletes().stream()
                        .filter(deleteFile -> switch (deleteFile.content()) {
                            case POSITION_DELETES -> partitionDomain.isAll() && pathDomain.isAll() && fileModifiedTimeDomain.isAll();
                            case EQUALITY_DELETES -> tableHandle.getEnforcedPredicate().isAll();
                            case DATA -> throw new IllegalStateException("Unexpected delete file: " + deleteFile);
                        })
                        .collect(toImmutableList());
                scannedFiles.add(new DataFileWithDeleteFiles(wholeFileTask.file(), fullyAppliedDeletes));
            }

            boolean fileHasNoDeletions = wholeFileTask.deletes().isEmpty();
            if (fileHasNoDeletions) {
                // There were no deletions, so we will produce splits covering the whole file
                outputRowsLowerBound = saturatedAdd(outputRowsLowerBound, wholeFileTask.file().recordCount());
            }

            if (fileHasNoDeletions && noDataColumnsProjected(wholeFileTask)) {
                scanTaskBuilder.add(fileScanTaskWithDomain);
            }
            else {
                scanTaskBuilder.addAll(fileScanTaskWithDomain.split(targetSplitSize));
            }
        }
        return scanTaskBuilder.build().iterator();
    }

    private synchronized List<FileScanTaskWithDomain> processFileScanTask(TupleDomain<IcebergColumnHandle> dynamicFilterPredicate)
    {
        FileScanTask wholeFileTask = fileScanIterator.next();
        boolean fileHasNoDeletions = wholeFileTask.deletes().isEmpty();
        FileScanTaskWithDomain fileScanTaskWithDomain = createFileScanTaskWithDomain(wholeFileTask);
        if (pruneFileScanTask(fileScanTaskWithDomain, fileHasNoDeletions, dynamicFilterPredicate)) {
            return ImmutableList.of();
        }

        if (!recordScannedFiles || scannedFilesByPartition == null) {
            return ImmutableList.of(fileScanTaskWithDomain);
        }

        // Assess if the partition that wholeFileTask belongs to should be included for OPTIMIZE
        // If file was partitioned under an old spec, OPTIMIZE may be able to merge it with another file under new partitioning spec
        // We don't know which partition of new spec this file belongs to, so we include all files in OPTIMIZE
        if (currentSpecId != wholeFileTask.spec().specId()) {
            Stream<FileScanTaskWithDomain> allQueuedTasks = scannedFilesByPartition.values().stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get);
            scannedFilesByPartition = null;
            return Stream.concat(allQueuedTasks, Stream.of(fileScanTaskWithDomain)).collect(toImmutableList());
        }
        StructLikeWrapperWithFieldIdToIndex structLikeWrapperWithFieldIdToIndex = createStructLikeWrapper(wholeFileTask);
        Optional<FileScanTaskWithDomain> alreadyQueuedFileTask = scannedFilesByPartition.get(structLikeWrapperWithFieldIdToIndex);
        if (alreadyQueuedFileTask != null) {
            // Optional.empty() is a marker for partitions where we've seen enough files to avoid skipping them from OPTIMIZE
            if (alreadyQueuedFileTask.isEmpty()) {
                return ImmutableList.of(fileScanTaskWithDomain);
            }
            scannedFilesByPartition.put(structLikeWrapperWithFieldIdToIndex, Optional.empty());
            return ImmutableList.of(alreadyQueuedFileTask.get(), fileScanTaskWithDomain);
        }
        // If file has no deletions, and it's the only file seen so far for the partition
        // then we skip it from splits generation unless we encounter another file in the same partition
        if (fileHasNoDeletions) {
            scannedFilesByPartition.put(structLikeWrapperWithFieldIdToIndex, Optional.of(fileScanTaskWithDomain));
            return ImmutableList.of();
        }
        scannedFilesByPartition.put(structLikeWrapperWithFieldIdToIndex, Optional.empty());
        return ImmutableList.of(fileScanTaskWithDomain);
    }

    private synchronized boolean pruneFileScanTask(FileScanTaskWithDomain fileScanTaskWithDomain, boolean fileHasNoDeletions, TupleDomain<IcebergColumnHandle> dynamicFilterPredicate)
    {
        BaseFileScanTask fileScanTask = (BaseFileScanTask) fileScanTaskWithDomain.fileScanTask();
        if (fileHasNoDeletions &&
                maxScannedFileSizeInBytes.isPresent() &&
                fileScanTask.file().fileSizeInBytes() > maxScannedFileSizeInBytes.get()) {
            return true;
        }

        if (!partitionDomain.isAll()) {
            String partition = fileScanTask.spec().partitionToPath(fileScanTask.partition());
            if (!partitionDomain.includesNullableValue(utf8Slice(partition))) {
                return true;
            }
        }
        if (!pathDomain.isAll() && !pathDomain.includesNullableValue(utf8Slice(fileScanTask.file().location()))) {
            return true;
        }
        if (!fileModifiedTimeDomain.isAll()) {
            long fileModifiedTime = getModificationTime(fileScanTask.file().location(), fileSystemFactory.create(session.getIdentity(), fileIoProperties));
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
            if (!fileScanTaskWithDomain.fileStatisticsDomain().overlaps(dynamicFilterPredicate)) {
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

    private synchronized void finish()
    {
        closeInternal(false);
        this.finished = true;
        this.fileScanIterable = CloseableIterable.empty();
        this.fileScanIterator = CloseableIterator.empty();
        this.fileTasksIterator = emptyIterator();
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        checkState(isFinished(), "Split source must be finished before TableExecuteSplitsInfo is read");
        if (!recordScannedFiles) {
            return Optional.empty();
        }
        long filesSkipped = 0;
        List<Object> splitsInfo;
        synchronized (this) {
            if (scannedFilesByPartition != null) {
                filesSkipped = scannedFilesByPartition.values().stream()
                        .filter(Optional::isPresent)
                        .count();
                scannedFilesByPartition = null;
            }
            splitsInfo = ImmutableList.copyOf(scannedFiles.build());
        }
        log.info("Generated %d splits, skipped %d files for OPTIMIZE", splitsInfo.size(), filesSkipped);
        return Optional.of(splitsInfo);
    }

    @Override
    public void close()
    {
        closeInternal(true);
    }

    private void closeInternal(boolean interruptIfRunning)
    {
        synchronized (closer) {
            if (!closed) {
                closed = true;
                // don't cancel the current batch future during normal finishing cleanup
                if (interruptIfRunning && currentBatchFuture != null) {
                    currentBatchFuture.cancel(true);
                }
                // release the reference to the current future unconditionally to avoid OOMs
                currentBatchFuture = null;
                try {
                    closer.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    private FileScanTaskWithDomain createFileScanTaskWithDomain(FileScanTask wholeFileTask)
    {
        List<IcebergColumnHandle> predicatedColumns = wholeFileTask.schema().columns().stream()
                .filter(column -> predicatedColumnIds.contains(column.fieldId()))
                .map(column -> getColumnHandle(column, typeManager))
                .collect(toImmutableList());
        return new FileScanTaskWithDomain(
                wholeFileTask,
                createFileStatisticsDomain(
                        fieldIdToType,
                        wholeFileTask.file().lowerBounds(),
                        wholeFileTask.file().upperBounds(),
                        wholeFileTask.file().nullValueCounts(),
                        predicatedColumns));
    }

    private record FileScanTaskWithDomain(FileScanTask fileScanTask, TupleDomain<IcebergColumnHandle> fileStatisticsDomain)
    {
        Iterator<FileScanTaskWithDomain> split(long targetSplitSize)
        {
            return Iterators.transform(
                    fileScanTask().split(targetSplitSize).iterator(),
                    task -> new FileScanTaskWithDomain(task, fileStatisticsDomain));
        }
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

    private IcebergSplit toIcebergSplit(FileScanTaskWithDomain taskWithDomain)
    {
        FileScanTask task = taskWithDomain.fileScanTask();
        Optional<List<Object>> partitionValues = Optional.empty();
        if (tableHandle.getTablePartitioning().isPresent()) {
            PartitionSpec partitionSpec = task.spec();
            StructLike partition = task.file().partition();
            List<PartitionField> fields = partitionSpec.fields();

            partitionValues = Optional.of(tableHandle.getTablePartitioning().get().partitionStructFields().stream()
                    .map(fieldIndex -> convertIcebergValueToTrino(
                            partitionSpec.partitionType().field(fields.get(fieldIndex).fieldId()).type(),
                            partition.get(fieldIndex, Object.class)))
                    .toList());
        }

        return new IcebergSplit(
                task.file().location(),
                task.start(),
                task.length(),
                task.file().fileSizeInBytes(),
                task.file().recordCount(),
                IcebergFileFormat.fromIceberg(task.file().format()),
                partitionValues,
                PartitionSpecParser.toJson(task.spec()),
                PartitionData.toJson(task.file().partition()),
                task.deletes().stream()
                        .map(DeleteFile::fromIceberg)
                        .collect(toImmutableList()),
                SplitWeight.fromProportion(clamp(getSplitWeight(task), minimumAssignedSplitWeight, 1.0)),
                taskWithDomain.fileStatisticsDomain(),
                fileIoProperties,
                cachingHostAddressProvider.getHosts(task.file().location(), ImmutableList.of()),
                task.file().dataSequenceNumber());
    }

    private double getSplitWeight(FileScanTask task)
    {
        double dataWeight = (double) task.length() / tableScan.targetSplitSize();
        double weight = dataWeight;
        if (task.deletes().stream().anyMatch(deleteFile -> deleteFile.content() == POSITION_DELETES)) {
            // Presence of each data position is looked up in a combined bitmap of deleted positions
            weight += dataWeight;
        }

        long equalityDeletes = task.deletes().stream()
                .filter(deleteFile -> deleteFile.content() == EQUALITY_DELETES)
                .mapToLong(ContentFile::recordCount)
                .sum();
        // Every row is a separate equality predicate that must be applied to all data rows
        weight += equalityDeletes * dataWeight;
        return weight;
    }
}
