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
package io.trino.plugin.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Streams;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.HivePartition;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.hive.HiveSplit.BucketConversion;
import io.trino.plugin.hive.HiveSplit.BucketValidation;
import io.trino.plugin.hive.fs.DirectoryLister;
import io.trino.plugin.hive.fs.HiveFileIterator;
import io.trino.plugin.hive.fs.TrinoFileStatus;
import io.trino.plugin.hive.util.AcidTables.AcidState;
import io.trino.plugin.hive.util.AcidTables.ParsedDelta;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.plugin.hive.util.HiveBucketing.HiveBucketFilter;
import io.trino.plugin.hive.util.InternalHiveSplitFactory;
import io.trino.plugin.hive.util.ResumableTask;
import io.trino.plugin.hive.util.ResumableTasks;
import io.trino.plugin.hive.util.ValidWriteIdList;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.hive.formats.HiveClassNames.SYMLINK_TEXT_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_EXCEEDED_PARTITION_LIMIT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveSessionProperties.getMaxInitialSplitSize;
import static io.trino.plugin.hive.HiveSessionProperties.isForceLocalScheduling;
import static io.trino.plugin.hive.HiveSessionProperties.isValidateBucketing;
import static io.trino.plugin.hive.HiveStorageFormat.TEXTFILE;
import static io.trino.plugin.hive.HiveStorageFormat.getHiveStorageFormat;
import static io.trino.plugin.hive.fs.HiveFileIterator.NestedDirectoryPolicy.FAIL;
import static io.trino.plugin.hive.fs.HiveFileIterator.NestedDirectoryPolicy.IGNORED;
import static io.trino.plugin.hive.fs.HiveFileIterator.NestedDirectoryPolicy.RECURSE;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveSchema;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getPartitionLocation;
import static io.trino.plugin.hive.util.AcidTables.getAcidState;
import static io.trino.plugin.hive.util.AcidTables.isFullAcidTable;
import static io.trino.plugin.hive.util.AcidTables.isTransactionalTable;
import static io.trino.plugin.hive.util.AcidTables.readAcidVersionFile;
import static io.trino.plugin.hive.util.HiveBucketing.getBucketingVersion;
import static io.trino.plugin.hive.util.HiveTypeUtil.typeSupported;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.plugin.hive.util.HiveUtil.getFooterCount;
import static io.trino.plugin.hive.util.HiveUtil.getHeaderCount;
import static io.trino.plugin.hive.util.HiveUtil.getInputFormatName;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeyColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.getSerializationLibraryName;
import static io.trino.plugin.hive.util.PartitionMatchSupplier.createPartitionMatchSupplier;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Integer.parseInt;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BackgroundHiveSplitLoader
        implements HiveSplitLoader
{
    // See https://github.com/apache/hive/commit/ffee30e6267e85f00a22767262192abb9681cfb7#diff-5fe26c36b4e029dcd344fc5d484e7347R165
    private static final Pattern BUCKET_WITH_OPTIONAL_ATTEMPT_ID_PATTERN = Pattern.compile("bucket_(\\d+)(_\\d+)?$");

    private static final Iterable<Pattern> BUCKET_PATTERNS = ImmutableList.of(
            // legacy Presto naming pattern (current version matches Hive)
            Pattern.compile("\\d{8}_\\d{6}_\\d{5}_[a-z0-9]{5}_bucket-(\\d+)(?:[-_.].*)?"),
            // Hive naming pattern per `org.apache.hadoop.hive.ql.exec.Utilities#getBucketIdFromFile()`
            Pattern.compile("(\\d+)_\\d+.*"),
            // Hive ACID with optional direct insert attempt id
            BUCKET_WITH_OPTIONAL_ATTEMPT_ID_PATTERN);

    private static final ListenableFuture<Void> COMPLETED_FUTURE = immediateVoidFuture();

    private final Table table;
    private final TupleDomain<? extends ColumnHandle> compactEffectivePredicate;
    private final DynamicFilter dynamicFilter;
    private final long dynamicFilteringWaitTimeoutMillis;
    private final TypeManager typeManager;
    private final Optional<BucketSplitInfo> tableBucketInfo;
    private final DirectoryLister directoryLister;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final int loaderConcurrency;
    private final boolean recursiveDirWalkerEnabled;
    private final boolean ignoreAbsentPartitions;
    private final Executor executor;
    private final ConnectorSession session;
    private final ConcurrentLazyQueue<HivePartitionMetadata> partitions;
    private final Deque<Iterator<InternalHiveSplit>> fileIterators = new ConcurrentLinkedDeque<>();
    private final Optional<ValidWriteIdList> validWriteIds;
    private final Optional<Long> maxSplitFileSize;
    private final int maxPartitions;

    // Purpose of this lock:
    // * Write lock: when you need a consistent view across partitions, fileIterators, and hiveSplitSource.
    // * Read lock: when you need to modify any of the above.
    //   Make sure the lock is held throughout the period during which they may not be consistent with each other.
    // Details:
    // * When write lock is acquired, except the holder, no one can do any of the following:
    // ** poll from (or check empty) partitions
    // ** poll from (or check empty) or push to fileIterators
    // ** push to hiveSplitSource
    // * When any of the above three operations is carried out, either a read lock or a write lock must be held.
    // * When a series of operations involving two or more of the above three operations are carried out, the lock
    //   must be continuously held throughout the series of operations.
    // Implications:
    // * if you hold a read lock but not a write lock, you can do any of the above three operations, but you may
    //   see a series of operations involving two or more of the operations carried out half way.
    private final ReadWriteLock taskExecutionLock = new ReentrantReadWriteLock();

    private HiveSplitSource hiveSplitSource;
    private Stopwatch stopwatch;
    private volatile boolean stopped;
    private final AtomicInteger activeLoaderCount = new AtomicInteger();
    private final AtomicInteger partitionCount = new AtomicInteger();

    public BackgroundHiveSplitLoader(
            Table table,
            Iterator<HivePartitionMetadata> partitions,
            TupleDomain<? extends ColumnHandle> compactEffectivePredicate,
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringWaitTimeout,
            TypeManager typeManager,
            Optional<BucketSplitInfo> tableBucketInfo,
            ConnectorSession session,
            TrinoFileSystemFactory fileSystemFactory,
            DirectoryLister directoryLister,
            Executor executor,
            int loaderConcurrency,
            boolean recursiveDirWalkerEnabled,
            boolean ignoreAbsentPartitions,
            Optional<ValidWriteIdList> validWriteIds,
            Optional<Long> maxSplitFileSize,
            int maxPartitions)
    {
        this.table = table;
        this.compactEffectivePredicate = compactEffectivePredicate;
        this.dynamicFilter = dynamicFilter;
        this.dynamicFilteringWaitTimeoutMillis = dynamicFilteringWaitTimeout.toMillis();
        this.typeManager = typeManager;
        this.tableBucketInfo = tableBucketInfo;
        this.loaderConcurrency = loaderConcurrency;
        checkArgument(loaderConcurrency > 0, "loaderConcurrency must be > 0, found: %s", loaderConcurrency);
        this.session = session;
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.directoryLister = directoryLister;
        this.recursiveDirWalkerEnabled = recursiveDirWalkerEnabled;
        this.ignoreAbsentPartitions = ignoreAbsentPartitions;
        requireNonNull(executor, "executor is null");
        // direct executor is not supported in this implementation due to locking specifics
        checkExecutorIsNotDirectExecutor(executor);
        this.executor = executor;
        this.partitions = new ConcurrentLazyQueue<>(partitions);
        this.validWriteIds = requireNonNull(validWriteIds, "validWriteIds is null");
        this.maxSplitFileSize = requireNonNull(maxSplitFileSize, "maxSplitFileSize is null");
        this.maxPartitions = maxPartitions;
    }

    @Override
    public void start(HiveSplitSource splitSource)
    {
        this.hiveSplitSource = splitSource;
        this.stopwatch = Stopwatch.createStarted();
        addLoaderIfNecessary();
    }

    private void addLoaderIfNecessary()
    {
        // opportunistic check to avoid incrementing indefinitely
        if (activeLoaderCount.get() >= loaderConcurrency) {
            return;
        }
        if (activeLoaderCount.incrementAndGet() > loaderConcurrency) {
            return;
        }
        ListenableFuture<Void> future = ResumableTasks.submit(executor, new HiveSplitLoaderTask());
        // best effort; hiveSplitSource could be already completed
        addExceptionCallback(future, hiveSplitSource::fail);
    }

    @Override
    public void stop()
    {
        stopped = true;
    }

    private class HiveSplitLoaderTask
            implements ResumableTask
    {
        @Override
        public TaskStatus process()
        {
            while (true) {
                if (stopped) {
                    return TaskStatus.finished();
                }
                ListenableFuture<Void> future;
                // Block until one of below conditions is met:
                // 1. Completion of DynamicFilter
                // 2. Timeout after waiting for the configured time
                long timeLeft = dynamicFilteringWaitTimeoutMillis - stopwatch.elapsed(MILLISECONDS);
                if (timeLeft > 0 && dynamicFilter.isAwaitable()) {
                    future = asVoid(toListenableFuture(dynamicFilter.isBlocked()
                            // As isBlocked() returns unmodifiableFuture, we need to create new future for correct propagation of the timeout
                            .thenApply(Function.identity())
                            .orTimeout(timeLeft, MILLISECONDS)));
                    return TaskStatus.continueOn(future);
                }
                taskExecutionLock.readLock().lock();
                try {
                    future = loadSplits();
                }
                catch (Throwable e) {
                    if (e instanceof IOException) {
                        e = new TrinoException(HIVE_FILESYSTEM_ERROR, e);
                    }
                    else if (!(e instanceof TrinoException)) {
                        e = new TrinoException(HIVE_UNKNOWN_ERROR, e);
                    }
                    // Fail the split source before releasing the execution lock
                    // Otherwise, a race could occur where the split source is completed before we fail it.
                    hiveSplitSource.fail(e);
                    checkState(stopped);
                    return TaskStatus.finished();
                }
                finally {
                    taskExecutionLock.readLock().unlock();
                }
                invokeNoMoreSplitsIfNecessary();
                if (!future.isDone()) {
                    return TaskStatus.continueOn(future);
                }
            }
        }
    }

    private void invokeNoMoreSplitsIfNecessary()
    {
        taskExecutionLock.readLock().lock();
        try {
            // This is an opportunistic check to avoid getting the write lock unnecessarily
            if (!partitions.isEmpty() || !fileIterators.isEmpty()) {
                return;
            }
        }
        catch (Exception e) {
            hiveSplitSource.fail(e);
            checkState(stopped, "Task is not marked as stopped even though it failed");
            return;
        }
        finally {
            taskExecutionLock.readLock().unlock();
        }

        taskExecutionLock.writeLock().lock();
        try {
            // the write lock guarantees that no one is operating on the partitions, fileIterators, or hiveSplitSource, or half way through doing so.
            if (partitions.isEmpty() && fileIterators.isEmpty()) {
                // It is legal to call `noMoreSplits` multiple times or after `stop` was called.
                // Nothing bad will happen if `noMoreSplits` implementation calls methods that will try to obtain a read lock because the lock is re-entrant.
                hiveSplitSource.noMoreSplits();
            }
        }
        catch (Exception e) {
            hiveSplitSource.fail(e);
            checkState(stopped, "Task is not marked as stopped even though it failed");
        }
        finally {
            taskExecutionLock.writeLock().unlock();
        }
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }

    private ListenableFuture<Void> loadSplits()
            throws IOException
    {
        Iterator<InternalHiveSplit> splits = fileIterators.poll();
        if (splits == null) {
            HivePartitionMetadata partition = partitions.poll();
            if (partition == null) {
                return COMPLETED_FUTURE;
            }
            if (partitionCount.incrementAndGet() > maxPartitions) {
                throw new TrinoException(HIVE_EXCEEDED_PARTITION_LIMIT, format(
                        "Query over table '%s' can potentially read more than %s partitions",
                        partition.getHivePartition().getTableName(),
                        maxPartitions));
            }
            // this is racy and sometimes more loaders can be added than necessary, but this is fine
            if (!partitions.isEmpty()) {
                addLoaderIfNecessary();
            }
            return loadPartition(partition);
        }

        // this is racy and sometimes more loaders can be added than necessary, but this is fine
        if (!fileIterators.isEmpty()) {
            addLoaderIfNecessary();
        }

        while (splits.hasNext() && !stopped) {
            ListenableFuture<Void> future = hiveSplitSource.addToQueue(splits.next());
            if (!future.isDone()) {
                fileIterators.addFirst(splits);
                return future;
            }
        }

        // No need to put the iterator back, since it's either empty or we've stopped
        return COMPLETED_FUTURE;
    }

    private ListenableFuture<Void> loadPartition(HivePartitionMetadata partition)
            throws IOException
    {
        HivePartition hivePartition = partition.getHivePartition();
        String partitionName = hivePartition.getPartitionId();
        Map<String, String> schema = partition.getPartition()
                .map(value -> getHiveSchema(value, table))
                .orElseGet(() -> getHiveSchema(table));
        List<HivePartitionKey> partitionKeys = getPartitionKeys(table, partition.getPartition());
        TupleDomain<HiveColumnHandle> effectivePredicate = compactEffectivePredicate.transformKeys(HiveColumnHandle.class::cast);

        BooleanSupplier partitionMatchSupplier = createPartitionMatchSupplier(dynamicFilter, hivePartition, getPartitionKeyColumnHandles(table, typeManager));
        if (!partitionMatchSupplier.getAsBoolean()) {
            // Avoid listing files and creating splits from a partition if it has been pruned due to dynamic filters
            return COMPLETED_FUTURE;
        }

        Location location = Location.of(getPartitionLocation(table, partition.getPartition()));

        // Skip header / footer lines are not splittable except for a special case when skip.header.line.count=1
        boolean splittable = getFooterCount(schema) == 0 && getHeaderCount(schema) <= 1;

        if (SYMLINK_TEXT_INPUT_FORMAT_CLASS.equals(getInputFormatName(schema).orElse(null))) {
            if (tableBucketInfo.isPresent()) {
                throw new TrinoException(NOT_SUPPORTED, "Bucketed table in SymlinkTextInputFormat is not yet supported");
            }
            HiveStorageFormat targetStorageFormat = getSymlinkStorageFormat(getSerializationLibraryName(schema));
            ListMultimap<Location, Location> targets = getTargetLocationsByParentFromSymlink(location);

            InternalHiveSplitFactory splitFactory = new InternalHiveSplitFactory(
                    partitionName,
                    targetStorageFormat,
                    schema,
                    partitionKeys,
                    effectivePredicate,
                    partitionMatchSupplier,
                    partition.getHiveColumnCoercions(),
                    Optional.empty(),
                    Optional.empty(),
                    getMaxInitialSplitSize(session),
                    isForceLocalScheduling(session),
                    maxSplitFileSize);

            for (Entry<Location, List<Location>> entry : Multimaps.asMap(targets).entrySet()) {
                fileIterators.addLast(buildManifestFileIterator(splitFactory, entry.getKey(), entry.getValue(), splittable));
            }

            return COMPLETED_FUTURE;
        }

        StorageFormat rawStorageFormat = partition.getPartition()
                .map(Partition::getStorage).orElseGet(table::getStorage).getStorageFormat();
        HiveStorageFormat storageFormat = getHiveStorageFormat(rawStorageFormat)
                .orElseThrow(() -> new TrinoException(HIVE_INVALID_METADATA, "Unsupported storage format: %s %s".formatted(hivePartition, rawStorageFormat)));

        Optional<BucketConversion> bucketConversion = Optional.empty();
        boolean bucketConversionRequiresWorkerParticipation = false;
        if (partition.getPartition().isPresent()) {
            Optional<HiveBucketProperty> partitionBucketProperty = partition.getPartition().get().getStorage().getBucketProperty();
            if (tableBucketInfo.isPresent() && partitionBucketProperty.isPresent()) {
                int tableBucketCount = tableBucketInfo.get().getTableBucketCount();
                // Partition bucketing_version cannot be different from table
                BucketingVersion bucketingVersion = getBucketingVersion(table.getParameters());
                int partitionBucketCount = partitionBucketProperty.get().bucketCount();
                // Validation was done in HiveSplitManager#getPartitionMetadata.
                // Here, it's just trying to see if its needs the BucketConversion.
                if (tableBucketCount != partitionBucketCount) {
                    bucketConversion = Optional.of(new BucketConversion(bucketingVersion, tableBucketCount, partitionBucketCount, tableBucketInfo.get().getBucketColumns()));
                    if (tableBucketCount > partitionBucketCount) {
                        bucketConversionRequiresWorkerParticipation = true;
                    }
                }
            }
        }

        Optional<BucketValidation> bucketValidation = Optional.empty();
        if (isValidateBucketing(session) && tableBucketInfo.isPresent()) {
            BucketSplitInfo info = tableBucketInfo.get();
            bucketValidation = Optional.of(new BucketValidation(info.getBucketingVersion(), info.getTableBucketCount(), info.getBucketColumns()));
        }

        InternalHiveSplitFactory splitFactory = new InternalHiveSplitFactory(
                partitionName,
                storageFormat,
                schema,
                partitionKeys,
                effectivePredicate,
                partitionMatchSupplier,
                partition.getHiveColumnCoercions(),
                bucketConversionRequiresWorkerParticipation ? bucketConversion : Optional.empty(),
                bucketValidation,
                getMaxInitialSplitSize(session),
                isForceLocalScheduling(session),
                maxSplitFileSize);

        if (isTransactionalTable(table.getParameters())) {
            return getTransactionalSplits(location, splittable, bucketConversion, splitFactory);
        }

        TrinoFileSystem trinoFileSystem = fileSystemFactory.create(session);
        // Bucketed partitions are fully loaded immediately since all files must be loaded to determine the file to bucket mapping
        if (tableBucketInfo.isPresent()) {
            List<TrinoFileStatus> files = listBucketFiles(trinoFileSystem, location, splitFactory.getPartitionName());
            return hiveSplitSource.addToQueue(getBucketedSplits(files, splitFactory, tableBucketInfo.get(), bucketConversion, splittable, Optional.empty()));
        }

        fileIterators.addLast(createInternalHiveSplitIterator(trinoFileSystem, location, splitFactory, splittable, Optional.empty()));

        return COMPLETED_FUTURE;
    }

    private List<TrinoFileStatus> listBucketFiles(TrinoFileSystem fs, Location location, String partitionName)
    {
        try {
            HiveFileIterator fileIterator = new HiveFileIterator(table, location, fs, directoryLister, FAIL);
            if (!fileIterator.hasNext() && !ignoreAbsentPartitions) {
                checkPartitionLocationExists(fs, location);
            }
            return ImmutableList.copyOf(fileIterator);
        }
        catch (HiveFileIterator.NestedDirectoryNotAllowedException e) {
            // Fail here to be on the safe side. This seems to be the same as what Hive does
            throw new TrinoException(HIVE_INVALID_BUCKET_FILES, "Hive table '%s' is corrupt. Found sub-directory '%s' in bucket directory for partition: %s"
                    .formatted(table.getSchemaTableName(), e.getNestedDirectoryPath(), partitionName));
        }
    }

    @VisibleForTesting
    Iterator<InternalHiveSplit> buildManifestFileIterator(InternalHiveSplitFactory splitFactory, Location location, List<Location> paths, boolean splittable)
    {
        return createInternalHiveSplitIterator(splitFactory, splittable, Optional.empty(), verifiedFileStatusesStream(location, paths));
    }

    private Stream<TrinoFileStatus> verifiedFileStatusesStream(Location location, List<Location> paths)
    {
        TrinoFileSystem trinoFileSystem = fileSystemFactory.create(session);
        // Check if location is cached BEFORE using the directoryLister
        boolean isCached = directoryLister.isCached(location);

        Map<String, TrinoFileStatus> fileStatuses = new HashMap<>();
        Iterator<TrinoFileStatus> fileStatusIterator = new HiveFileIterator(table, location, trinoFileSystem, directoryLister, RECURSE);
        if (!fileStatusIterator.hasNext()) {
            checkPartitionLocationExists(trinoFileSystem, location);
        }
        fileStatusIterator.forEachRemaining(status -> fileStatuses.put(Location.of(status.getPath()).path(), status));

        // If file statuses came from cache verify that all are present
        if (isCached) {
            boolean missing = paths.stream()
                    .anyMatch(path -> !fileStatuses.containsKey(path.path()));
            // Invalidate the cache and reload
            if (missing) {
                directoryLister.invalidate(location);

                fileStatuses.clear();
                fileStatusIterator = new HiveFileIterator(table, location, trinoFileSystem, directoryLister, RECURSE);
                fileStatusIterator.forEachRemaining(status -> fileStatuses.put(Location.of(status.getPath()).path(), status));
            }
        }

        return paths.stream()
                .map(path -> {
                    TrinoFileStatus status = fileStatuses.get(path.path());
                    if (status == null) {
                        throw new TrinoException(HIVE_FILE_NOT_FOUND, "Manifest file from the location [%s] contains non-existent path: %s".formatted(location, path));
                    }
                    return status;
                });
    }

    private ListenableFuture<Void> getTransactionalSplits(Location path, boolean splittable, Optional<BucketConversion> bucketConversion, InternalHiveSplitFactory splitFactory)
            throws IOException
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        ValidWriteIdList writeIds = validWriteIds.orElseThrow(() -> new IllegalStateException("No validWriteIds present"));
        AcidState acidState = getAcidState(fileSystem, path, writeIds);

        boolean fullAcid = isFullAcidTable(table.getParameters());
        AcidInfo.Builder acidInfoBuilder = AcidInfo.builder(path);

        if (fullAcid) {
            // From Hive version >= 3.0, delta/base files will always have file '_orc_acid_version' with value >= '2'.
            Optional<Location> baseOrDeltaPath = acidState.baseDirectory()
                    .or(() -> acidState.deltas().stream().findFirst()
                            .map(delta -> Location.of(delta.path())));

            if (baseOrDeltaPath.isPresent() && readAcidVersionFile(fileSystem, baseOrDeltaPath.get()) >= 2) {
                // Trino cannot read ORC ACID tables with version < 2 (written by Hive older than 3.0)
                // See https://github.com/trinodb/trino/issues/2790#issuecomment-591901728 for more context

                // We perform initial version check based on _orc_acid_version file here.
                // If we cannot verify the version (the _orc_acid_version file may not exist),
                // we will do extra check based on ORC datafile metadata in OrcPageSourceFactory.
                acidInfoBuilder.setOrcAcidVersionValidated(true);
            }
        }

        // Collect base files, delta files, and delete delta paths
        List<TrinoFileStatus> acidFiles = new ArrayList<>();
        for (FileEntry file : acidState.baseFiles()) {
            acidFiles.add(new TrinoFileStatus(file));
        }

        for (ParsedDelta delta : acidState.deltas()) {
            if (delta.deleteDelta()) {
                if (!fullAcid) {
                    throw new TrinoException(HIVE_BAD_DATA, "Unexpected delete delta for a non full ACID table '%s'. Would be ignored by the reader: %s"
                            .formatted(table.getSchemaTableName(), delta.path()));
                }
                acidInfoBuilder.addDeleteDelta(Location.of(delta.path()));
            }
            else {
                for (FileEntry file : delta.files()) {
                    acidFiles.add(new TrinoFileStatus(file));
                }
            }
        }

        for (FileEntry entry : acidState.originalFiles()) {
            // Hive requires "original" files of transactional tables to conform to the bucketed tables naming pattern, to match them with delete deltas.
            acidInfoBuilder.addOriginalFile(entry.location(), entry.length(), getRequiredBucketNumber(entry.location()));
        }

        if (tableBucketInfo.isPresent()) {
            BucketSplitInfo bucketInfo = tableBucketInfo.get();

            for (FileEntry entry : acidState.originalFiles()) {
                List<TrinoFileStatus> fileStatuses = ImmutableList.of(new TrinoFileStatus(entry));
                Optional<AcidInfo> acidInfo = acidInfoForOriginalFiles(fullAcid, acidInfoBuilder, entry.location());
                hiveSplitSource.addToQueue(getBucketedSplits(fileStatuses, splitFactory, bucketInfo, bucketConversion, splittable, acidInfo));
            }

            Optional<AcidInfo> acidInfo = acidInfo(fullAcid, acidInfoBuilder);
            return hiveSplitSource.addToQueue(getBucketedSplits(acidFiles, splitFactory, bucketInfo, bucketConversion, splittable, acidInfo));
        }

        Optional<AcidInfo> acidInfo = acidInfo(fullAcid, acidInfoBuilder);
        fileIterators.addLast(createInternalHiveSplitIterator(splitFactory, splittable, acidInfo, acidFiles.stream()));

        fileIterators.addLast(generateOriginalFilesSplits(splitFactory, acidState.originalFiles(), splittable, acidInfoBuilder, fullAcid));

        return COMPLETED_FUTURE;
    }

    private static Iterator<InternalHiveSplit> generateOriginalFilesSplits(
            InternalHiveSplitFactory splitFactory,
            List<FileEntry> originalFileLocations,
            boolean splittable,
            AcidInfo.Builder acidInfoBuilder,
            boolean fullAcid)
    {
        return originalFileLocations.stream()
                .map(entry -> createInternalHiveSplit(
                        splitFactory,
                        splittable,
                        acidInfoForOriginalFiles(fullAcid, acidInfoBuilder, entry.location()),
                        new TrinoFileStatus(entry)))
                .flatMap(Optional::stream)
                .iterator();
    }

    private static Optional<AcidInfo> acidInfo(boolean fullAcid, AcidInfo.Builder builder)
    {
        return fullAcid ? builder.build() : Optional.empty();
    }

    private static Optional<AcidInfo> acidInfoForOriginalFiles(boolean fullAcid, AcidInfo.Builder builder, Location location)
    {
        return fullAcid ? Optional.of(builder.buildWithRequiredOriginalFiles(getRequiredBucketNumber(location))) : Optional.empty();
    }

    private Iterator<InternalHiveSplit> createInternalHiveSplitIterator(TrinoFileSystem fileSystem, Location location, InternalHiveSplitFactory splitFactory, boolean splittable, Optional<AcidInfo> acidInfo)
    {
        Iterator<TrinoFileStatus> iterator = new HiveFileIterator(table, location, fileSystem, directoryLister, recursiveDirWalkerEnabled ? RECURSE : IGNORED);
        if (!iterator.hasNext() && !ignoreAbsentPartitions) {
            checkPartitionLocationExists(fileSystem, location);
        }
        return createInternalHiveSplitIterator(splitFactory, splittable, acidInfo, Streams.stream(iterator));
    }

    private static void checkPartitionLocationExists(TrinoFileSystem fileSystem, Location location)
    {
        try {
            if (!fileSystem.directoryExists(location).orElse(true)) {
                throw new TrinoException(HIVE_FILE_NOT_FOUND, "Partition location does not exist: " + location);
            }
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed checking directory path:" + location, e);
        }
    }

    private static Iterator<InternalHiveSplit> createInternalHiveSplitIterator(InternalHiveSplitFactory splitFactory, boolean splittable, Optional<AcidInfo> acidInfo, Stream<TrinoFileStatus> fileStream)
    {
        return fileStream
                .map(file -> createInternalHiveSplit(splitFactory, splittable, acidInfo, file))
                .flatMap(Optional::stream)
                .iterator();
    }

    private static Optional<InternalHiveSplit> createInternalHiveSplit(InternalHiveSplitFactory splitFactory, boolean splittable, Optional<AcidInfo> acidInfo, TrinoFileStatus file)
    {
        return splitFactory.createInternalHiveSplit(file, OptionalInt.empty(), OptionalInt.empty(), splittable, acidInfo);
    }

    private List<InternalHiveSplit> getBucketedSplits(
            List<TrinoFileStatus> files,
            InternalHiveSplitFactory splitFactory,
            BucketSplitInfo bucketSplitInfo,
            Optional<BucketConversion> bucketConversion,
            boolean splittable,
            Optional<AcidInfo> acidInfo)
    {
        int readBucketCount = bucketSplitInfo.getReadBucketCount();
        int tableBucketCount = bucketSplitInfo.getTableBucketCount();
        int partitionBucketCount = bucketConversion.map(BucketConversion::partitionBucketCount).orElse(tableBucketCount);
        int bucketCount = max(readBucketCount, partitionBucketCount);

        checkState(readBucketCount <= tableBucketCount, "readBucketCount(%s) should be less than or equal to tableBucketCount(%s)", readBucketCount, tableBucketCount);

        // build mapping of file name to bucket
        ListMultimap<Integer, TrinoFileStatus> bucketFiles = ArrayListMultimap.create();
        for (TrinoFileStatus file : files) {
            String fileName = Location.of(file.getPath()).fileName();
            OptionalInt bucket = getBucketNumber(fileName);
            if (bucket.isPresent()) {
                bucketFiles.put(bucket.getAsInt(), file);
                continue;
            }

            // legacy mode requires exactly one file per bucket
            if (files.size() != partitionBucketCount) {
                throw new TrinoException(HIVE_INVALID_BUCKET_FILES, format(
                        "Hive table '%s' is corrupt. File '%s' does not match the standard naming pattern, and the number " +
                                "of files in the directory (%s) does not match the declared bucket count (%s) for partition: %s",
                        table.getSchemaTableName(),
                        fileName,
                        files.size(),
                        partitionBucketCount,
                        splitFactory.getPartitionName()));
            }

            // sort FileStatus objects per `org.apache.hadoop.hive.ql.metadata.Table#getSortedPaths()`
            files = files.stream().sorted().toList();

            // use position in sorted list as the bucket number
            bucketFiles.clear();
            for (int i = 0; i < files.size(); i++) {
                bucketFiles.put(i, files.get(i));
            }
            break;
        }

        validateFileBuckets(bucketFiles, partitionBucketCount, table.getSchemaTableName().toString(), splitFactory.getPartitionName());

        // convert files internal splits
        List<InternalHiveSplit> splitList = new ArrayList<>();
        for (int bucketNumber = 0; bucketNumber < bucketCount; bucketNumber++) {
            // Physical bucket #. This determine file name. It also determines the order of splits in the result.
            int partitionBucketNumber = bucketNumber % partitionBucketCount;
            // Logical bucket #. Each logical bucket corresponds to a "bucket" from engine's perspective.
            int readBucketNumber = bucketNumber % readBucketCount;

            boolean containsIneligibleTableBucket = false;
            List<Integer> eligibleTableBucketNumbers = new ArrayList<>();
            for (int tableBucketNumber = bucketNumber % tableBucketCount; tableBucketNumber < tableBucketCount; tableBucketNumber += bucketCount) {
                // table bucket number: this is used for evaluating "$bucket" filters.
                if (bucketSplitInfo.isTableBucketEnabled(tableBucketNumber)) {
                    eligibleTableBucketNumbers.add(tableBucketNumber);
                }
                else {
                    containsIneligibleTableBucket = true;
                }
            }

            if (!eligibleTableBucketNumbers.isEmpty() && containsIneligibleTableBucket) {
                throw new TrinoException(
                        NOT_SUPPORTED,
                        "The bucket filter cannot be satisfied. There are restrictions on the bucket filter when all the following is true: " +
                                "1. a table has a different buckets count as at least one of its partitions that is read in this query; " +
                                "2. the table has a different but compatible bucket number with another table in the query; " +
                                "3. some buckets of the table is filtered out from the query, most likely using a filter on \"$bucket\". " +
                                "(table name: " + table.getTableName() + ", table bucket count: " + tableBucketCount + ", " +
                                "partition bucket count: " + partitionBucketCount + ", effective reading bucket count: " + readBucketCount + ")");
            }
            if (!eligibleTableBucketNumbers.isEmpty()) {
                for (TrinoFileStatus file : bucketFiles.get(partitionBucketNumber)) {
                    // OrcDeletedRows will load only delete delta files matching current bucket id,
                    // so we can pass all delete delta locations here, without filtering.
                    eligibleTableBucketNumbers.stream()
                            .map(tableBucketNumber -> splitFactory.createInternalHiveSplit(file, OptionalInt.of(readBucketNumber), OptionalInt.of(tableBucketNumber), splittable, acidInfo))
                            .flatMap(Optional::stream)
                            .forEach(splitList::add);
                }
            }
        }
        return splitList;
    }

    @VisibleForTesting
    static void validateFileBuckets(ListMultimap<Integer, TrinoFileStatus> bucketFiles, int partitionBucketCount, String tableName, String partitionName)
    {
        if (bucketFiles.isEmpty()) {
            return;
        }

        int highestBucketNumber = max(bucketFiles.keySet());
        // validate the bucket number detected from files, fail the query if the highest bucket number detected from file
        // exceeds the allowed highest number
        if (highestBucketNumber >= partitionBucketCount) {
            throw new TrinoException(HIVE_INVALID_BUCKET_FILES, format(
                    "Hive table '%s' is corrupt. The highest bucket number in the directory (%s) exceeds the bucket number range " +
                            "defined by the declared bucket count (%s) for partition: %s",
                    tableName,
                    highestBucketNumber,
                    partitionBucketCount,
                    partitionName));
        }
    }

    private static int getRequiredBucketNumber(Location location)
    {
        return getBucketNumber(location.fileName())
                .orElseThrow(() -> new IllegalStateException("Cannot get bucket number from location: " + location));
    }

    @VisibleForTesting
    static OptionalInt getBucketNumber(String name)
    {
        for (Pattern pattern : BUCKET_PATTERNS) {
            Matcher matcher = pattern.matcher(name);
            if (matcher.matches()) {
                return OptionalInt.of(parseInt(matcher.group(1)));
            }
        }
        return OptionalInt.empty();
    }

    public static boolean hasAttemptId(String bucketFilename)
    {
        Matcher matcher = BUCKET_WITH_OPTIONAL_ATTEMPT_ID_PATTERN.matcher(bucketFilename);
        return matcher.matches() && matcher.group(2) != null;
    }

    private static HiveStorageFormat getSymlinkStorageFormat(String serde)
    {
        // LazySimpleSerDe is used by TEXTFILE and SEQUENCEFILE. Use TEXTFILE per Hive behavior.
        if (serde.equals(TEXTFILE.getSerde())) {
            return TEXTFILE;
        }
        return Arrays.stream(HiveStorageFormat.values())
                .filter(format -> serde.equals(format.getSerde()))
                .findFirst()
                .orElseThrow(() -> new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Unknown SerDe for SymlinkTextInputFormat: " + serde));
    }

    private ListMultimap<Location, Location> getTargetLocationsByParentFromSymlink(Location symlinkDir)
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        try {
            ListMultimap<Location, Location> targets = ArrayListMultimap.create();
            FileIterator iterator = fileSystem.listFiles(symlinkDir);
            while (iterator.hasNext()) {
                Location location = iterator.next().location();
                String name = location.fileName();
                if (name.startsWith("_") || name.startsWith(".")) {
                    continue;
                }

                try (Reader reader = new InputStreamReader(fileSystem.newInputFile(location).newStream(), UTF_8)) {
                    CharStreams.readLines(reader).stream()
                            .map(Location::of)
                            .forEach(target -> targets.put(target.parentDirectory(), target));
                }
            }
            return targets;
        }
        catch (IOException | IllegalArgumentException e) {
            throw new TrinoException(HIVE_BAD_DATA, "Error parsing symlinks from: " + symlinkDir, e);
        }
    }

    private static List<HivePartitionKey> getPartitionKeys(Table table, Optional<Partition> partition)
    {
        if (partition.isEmpty()) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<HivePartitionKey> partitionKeys = ImmutableList.builder();
        List<Column> keys = table.getPartitionColumns();
        List<String> values = partition.get().getValues();
        checkCondition(keys.size() == values.size(), HIVE_INVALID_METADATA, "Expected %s partition key values, but got %s", keys.size(), values.size());
        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            HiveType hiveType = keys.get(i).getType();
            if (!typeSupported(hiveType.getTypeInfo(), table.getStorage().getStorageFormat())) {
                throw new TrinoException(NOT_SUPPORTED, format("Unsupported Hive type %s found in partition keys of table %s.%s", hiveType, table.getDatabaseName(), table.getTableName()));
            }
            String value = values.get(i);
            checkCondition(value != null, HIVE_INVALID_PARTITION_VALUE, "partition key value cannot be null for field: %s", name);
            partitionKeys.add(new HivePartitionKey(name, value));
        }
        return partitionKeys.build();
    }

    public static class BucketSplitInfo
    {
        private final BucketingVersion bucketingVersion;
        private final List<HiveColumnHandle> bucketColumns;
        private final int tableBucketCount;
        private final int readBucketCount;
        private final IntPredicate bucketFilter;

        public static Optional<BucketSplitInfo> createBucketSplitInfo(Optional<HiveTablePartitioning> tablePartitioning, Optional<HiveBucketFilter> bucketFilter)
        {
            requireNonNull(tablePartitioning, "tablePartitioning is null");
            requireNonNull(bucketFilter, "bucketFilter is null");

            if (tablePartitioning.isEmpty()) {
                checkArgument(bucketFilter.isEmpty(), "tablePartitioning must be present if bucketFilter is present");
                return Optional.empty();
            }

            BucketingVersion bucketingVersion = tablePartitioning.get().partitioningHandle().getBucketingVersion();
            int tableBucketCount = tablePartitioning.get().tableBucketCount();
            int readBucketCount = tablePartitioning.get().partitioningHandle().getBucketCount();

            List<HiveColumnHandle> bucketColumns = tablePartitioning.get().columns();
            IntPredicate predicate = bucketFilter
                    .<IntPredicate>map(filter -> filter.getBucketsToKeep()::contains)
                    .orElse(bucket -> true);
            return Optional.of(new BucketSplitInfo(bucketingVersion, bucketColumns, tableBucketCount, readBucketCount, predicate));
        }

        private BucketSplitInfo(BucketingVersion bucketingVersion, List<HiveColumnHandle> bucketColumns, int tableBucketCount, int readBucketCount, IntPredicate bucketFilter)
        {
            this.bucketingVersion = requireNonNull(bucketingVersion, "bucketingVersion is null");
            this.bucketColumns = ImmutableList.copyOf(requireNonNull(bucketColumns, "bucketColumns is null"));
            this.tableBucketCount = tableBucketCount;
            this.readBucketCount = readBucketCount;
            this.bucketFilter = requireNonNull(bucketFilter, "bucketFilter is null");
        }

        public BucketingVersion getBucketingVersion()
        {
            return bucketingVersion;
        }

        public List<HiveColumnHandle> getBucketColumns()
        {
            return bucketColumns;
        }

        public int getTableBucketCount()
        {
            return tableBucketCount;
        }

        public int getReadBucketCount()
        {
            return readBucketCount;
        }

        /**
         * Evaluates whether the provided table bucket number passes the bucket predicate.
         * A bucket predicate can be present in two cases:
         * <ul>
         * <li>Filter on "$bucket" column. e.g. {@code "$bucket" between 0 and 100}
         * <li>Single-value equality filter on all bucket columns. e.g. for a table with two bucketing columns,
         * {@code bucketCol1 = 'a' AND bucketCol2 = 123}
         * </ul>
         */
        public boolean isTableBucketEnabled(int tableBucketNumber)
        {
            return bucketFilter.test(tableBucketNumber);
        }
    }

    private static void checkExecutorIsNotDirectExecutor(Executor executor)
    {
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        try {
            executor.execute(() -> checkState(!lock.isHeldByCurrentThread(), "executor is a direct executor"));
        }
        finally {
            lock.unlock();
        }
    }
}
