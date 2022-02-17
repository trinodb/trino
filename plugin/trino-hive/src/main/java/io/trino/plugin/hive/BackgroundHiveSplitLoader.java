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
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Streams;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveSplit.BucketConversion;
import io.trino.plugin.hive.HiveSplit.BucketValidation;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.plugin.hive.util.HiveBucketing.HiveBucketFilter;
import io.trino.plugin.hive.util.HiveFileIterator;
import io.trino.plugin.hive.util.InternalHiveSplitFactory;
import io.trino.plugin.hive.util.ResumableTask;
import io.trino.plugin.hive.util.ResumableTasks;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatusWithId;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.MRConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.annotation.Annotation;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.fromProperties;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static io.trino.plugin.hive.HivePartitionManager.partitionMatches;
import static io.trino.plugin.hive.HiveSessionProperties.getMaxInitialSplitSize;
import static io.trino.plugin.hive.HiveSessionProperties.isForceLocalScheduling;
import static io.trino.plugin.hive.HiveSessionProperties.isValidateBucketing;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveSchema;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getPartitionLocation;
import static io.trino.plugin.hive.s3select.S3SelectPushdown.shouldEnablePushdownForTable;
import static io.trino.plugin.hive.util.ConfigurationUtils.toJobConf;
import static io.trino.plugin.hive.util.HiveFileIterator.NestedDirectoryPolicy.FAIL;
import static io.trino.plugin.hive.util.HiveFileIterator.NestedDirectoryPolicy.IGNORED;
import static io.trino.plugin.hive.util.HiveFileIterator.NestedDirectoryPolicy.RECURSE;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.plugin.hive.util.HiveUtil.getFooterCount;
import static io.trino.plugin.hive.util.HiveUtil.getHeaderCount;
import static io.trino.plugin.hive.util.HiveUtil.getInputFormat;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeyColumnHandles;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Integer.parseInt;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Collections.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority;
import static org.apache.hadoop.hive.common.FileUtils.HIDDEN_FILES_PATH_FILTER;

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
    private final AcidTransaction transaction;
    private final TupleDomain<HiveColumnHandle> compactEffectivePredicate;
    private final DynamicFilter dynamicFilter;
    private final long dynamicFilteringWaitTimeoutMillis;
    private final TypeManager typeManager;
    private final Optional<BucketSplitInfo> tableBucketInfo;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final NamenodeStats namenodeStats;
    private final DirectoryLister directoryLister;
    private final int loaderConcurrency;
    private final boolean recursiveDirWalkerEnabled;
    private final boolean ignoreAbsentPartitions;
    private final boolean optimizeSymlinkListing;
    private final Executor executor;
    private final ConnectorSession session;
    private final ConcurrentLazyQueue<HivePartitionMetadata> partitions;
    private final Deque<Iterator<InternalHiveSplit>> fileIterators = new ConcurrentLinkedDeque<>();
    private final Optional<ValidWriteIdList> validWriteIds;
    private final Optional<Long> maxSplitFileSize;

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

    public BackgroundHiveSplitLoader(
            Table table,
            AcidTransaction transaction,
            Iterable<HivePartitionMetadata> partitions,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringWaitTimeout,
            TypeManager typeManager,
            Optional<BucketSplitInfo> tableBucketInfo,
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            NamenodeStats namenodeStats,
            DirectoryLister directoryLister,
            Executor executor,
            int loaderConcurrency,
            boolean recursiveDirWalkerEnabled,
            boolean ignoreAbsentPartitions,
            boolean optimizeSymlinkListing,
            Optional<ValidWriteIdList> validWriteIds,
            Optional<Long> maxSplitFileSize)
    {
        this.table = table;
        this.transaction = requireNonNull(transaction, "transaction is null");
        this.compactEffectivePredicate = compactEffectivePredicate;
        this.dynamicFilter = dynamicFilter;
        this.dynamicFilteringWaitTimeoutMillis = dynamicFilteringWaitTimeout.toMillis();
        this.typeManager = typeManager;
        this.tableBucketInfo = tableBucketInfo;
        this.loaderConcurrency = loaderConcurrency;
        checkArgument(loaderConcurrency > 0, "loaderConcurrency must be > 0, found: %s", loaderConcurrency);
        this.session = session;
        this.hdfsEnvironment = hdfsEnvironment;
        this.namenodeStats = namenodeStats;
        this.directoryLister = directoryLister;
        this.recursiveDirWalkerEnabled = recursiveDirWalkerEnabled;
        this.ignoreAbsentPartitions = ignoreAbsentPartitions;
        this.optimizeSymlinkListing = optimizeSymlinkListing;
        this.executor = executor;
        this.partitions = new ConcurrentLazyQueue<>(partitions);
        this.hdfsContext = new HdfsContext(session);
        this.validWriteIds = requireNonNull(validWriteIds, "validWriteIds is null");
        this.maxSplitFileSize = requireNonNull(maxSplitFileSize, "maxSplitFileSize is null");
    }

    @Override
    public void start(HiveSplitSource splitSource)
    {
        this.hiveSplitSource = splitSource;
        this.stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < loaderConcurrency; i++) {
            ListenableFuture<Void> future = ResumableTasks.submit(executor, new HiveSplitLoaderTask());
            addExceptionCallback(future, hiveSplitSource::fail); // best effort; hiveSplitSource could be already completed
        }
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
            return loadPartition(partition);
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
        Properties schema = getPartitionSchema(table, partition.getPartition());
        List<HivePartitionKey> partitionKeys = getPartitionKeys(table, partition.getPartition());

        List<HiveColumnHandle> partitionColumns = getPartitionKeyColumnHandles(table, typeManager);
        BooleanSupplier partitionMatchSupplier =
                partitionColumns.stream().noneMatch(dynamicFilter.getColumnsCovered()::contains)
                        ? () -> true
                        : () -> partitionMatches(partitionColumns, dynamicFilter.getCurrentPredicate(), hivePartition);
        if (!partitionMatchSupplier.getAsBoolean()) {
            // Avoid listing files and creating splits from a partition if it has been pruned due to dynamic filters
            return COMPLETED_FUTURE;
        }

        Path path = new Path(getPartitionLocation(table, partition.getPartition()));
        Configuration configuration = hdfsEnvironment.getConfiguration(hdfsContext, path);
        InputFormat<?, ?> inputFormat = getInputFormat(configuration, schema, false);
        FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, path);
        boolean s3SelectPushdownEnabled = shouldEnablePushdownForTable(session, table, path.toString(), partition.getPartition());

        // S3 Select pushdown works at the granularity of individual S3 objects,
        // therefore we must not split files when it is enabled.
        // Skip header / footer lines are not splittable except for a special case when skip.header.line.count=1
        boolean splittable = !s3SelectPushdownEnabled && getFooterCount(schema) == 0 && getHeaderCount(schema) <= 1;

        if (inputFormat instanceof SymlinkTextInputFormat) {
            if (tableBucketInfo.isPresent()) {
                throw new TrinoException(NOT_SUPPORTED, "Bucketed table in SymlinkTextInputFormat is not yet supported");
            }
            InputFormat<?, ?> targetInputFormat = getInputFormat(configuration, schema, true);
            List<Path> targetPaths = hdfsEnvironment.doAs(
                    hdfsContext.getIdentity(),
                    () -> getTargetPathsFromSymlink(fs, path));
            Set<Path> parents = targetPaths.stream()
                    .map(Path::getParent)
                    .distinct()
                    .collect(toImmutableSet());
            if (optimizeSymlinkListing && parents.size() == 1 && !recursiveDirWalkerEnabled) {
                Optional<Iterator<InternalHiveSplit>> manifestFileIterator = buildManifestFileIterator(
                        targetInputFormat,
                        partitionName,
                        schema,
                        partitionKeys,
                        compactEffectivePredicate,
                        partitionMatchSupplier,
                        s3SelectPushdownEnabled,
                        partition.getTableToPartitionMapping(),
                        getOnlyElement(parents),
                        targetPaths,
                        splittable);
                if (manifestFileIterator.isPresent()) {
                    fileIterators.addLast(manifestFileIterator.get());
                    return COMPLETED_FUTURE;
                }
            }
            return createHiveSymlinkSplits(
                    partitionName,
                    targetInputFormat,
                    schema,
                    partitionKeys,
                    compactEffectivePredicate,
                    partitionMatchSupplier,
                    s3SelectPushdownEnabled,
                    partition.getTableToPartitionMapping(),
                    targetPaths);
        }

        Optional<BucketConversion> bucketConversion = Optional.empty();
        boolean bucketConversionRequiresWorkerParticipation = false;
        if (partition.getPartition().isPresent()) {
            Optional<HiveBucketProperty> partitionBucketProperty = partition.getPartition().get().getStorage().getBucketProperty();
            if (tableBucketInfo.isPresent() && partitionBucketProperty.isPresent()) {
                int readBucketCount = tableBucketInfo.get().getReadBucketCount();
                BucketingVersion bucketingVersion = partitionBucketProperty.get().getBucketingVersion(); // TODO can partition's bucketing_version be different from table's?
                int partitionBucketCount = partitionBucketProperty.get().getBucketCount();
                // Validation was done in HiveSplitManager#getPartitionMetadata.
                // Here, it's just trying to see if its needs the BucketConversion.
                if (readBucketCount != partitionBucketCount) {
                    bucketConversion = Optional.of(new BucketConversion(bucketingVersion, readBucketCount, partitionBucketCount, tableBucketInfo.get().getBucketColumns()));
                    if (readBucketCount > partitionBucketCount) {
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
                fs,
                partitionName,
                inputFormat,
                schema,
                partitionKeys,
                compactEffectivePredicate,
                partitionMatchSupplier,
                partition.getTableToPartitionMapping(),
                bucketConversionRequiresWorkerParticipation ? bucketConversion : Optional.empty(),
                bucketValidation,
                getMaxInitialSplitSize(session),
                isForceLocalScheduling(session),
                s3SelectPushdownEnabled,
                transaction,
                maxSplitFileSize);

        // To support custom input formats, we want to call getSplits()
        // on the input format to obtain file splits.
        if (shouldUseFileSplitsFromInputFormat(inputFormat)) {
            if (tableBucketInfo.isPresent()) {
                throw new TrinoException(NOT_SUPPORTED, "Trino cannot read bucketed partition in an input format with UseFileSplitsFromInputFormat annotation: " + inputFormat.getClass().getSimpleName());
            }

            if (AcidUtils.isTransactionalTable(table.getParameters())) {
                throw new TrinoException(NOT_SUPPORTED, "Hive transactional tables in an input format with UseFileSplitsFromInputFormat annotation are not supported: " + inputFormat.getClass().getSimpleName());
            }

            JobConf jobConf = toJobConf(configuration);
            FileInputFormat.setInputPaths(jobConf, path);
            // Pass SerDes and Table parameters into input format configuration
            fromProperties(schema).forEach(jobConf::set);
            InputSplit[] splits = hdfsEnvironment.doAs(hdfsContext.getIdentity(), () -> inputFormat.getSplits(jobConf, 0));

            return addSplitsToSource(splits, splitFactory);
        }

        List<Path> readPaths;
        List<HdfsFileStatusWithId> fileStatusOriginalFiles = ImmutableList.of();
        AcidInfo.Builder acidInfoBuilder = AcidInfo.builder(path);
        boolean isFullAcid = AcidUtils.isFullAcidTable(table.getParameters());
        if (AcidUtils.isTransactionalTable(table.getParameters())) {
            AcidUtils.Directory directory = hdfsEnvironment.doAs(hdfsContext.getIdentity(), () -> AcidUtils.getAcidState(
                    path,
                    configuration,
                    validWriteIds.orElseThrow(() -> new IllegalStateException("No validWriteIds present")),
                    false,
                    true));

            if (isFullAcid) {
                // From Hive version >= 3.0, delta/base files will always have file '_orc_acid_version' with value >= '2'.
                Path baseOrDeltaPath = directory.getBaseDirectory() != null
                        ? directory.getBaseDirectory()
                        : (directory.getCurrentDirectories().size() > 0 ? directory.getCurrentDirectories().get(0).getPath() : null);

                if (baseOrDeltaPath != null && AcidUtils.OrcAcidVersion.getAcidVersionFromMetaFile(baseOrDeltaPath, fs) >= 2) {
                    // Trino cannot read ORC ACID tables with version < 2 (written by Hive older than 3.0)
                    // See https://github.com/trinodb/trino/issues/2790#issuecomment-591901728 for more context

                    // We perform initial version check based on _orc_acid_version file here.
                    // If we cannot verify the version (the _orc_acid_version file may not exist),
                    // we will do extra check based on ORC datafile metadata in OrcPageSourceFactory.
                    acidInfoBuilder.setOrcAcidVersionValidated(true);
                }
            }

            readPaths = new ArrayList<>();

            // base
            if (directory.getBaseDirectory() != null) {
                readPaths.add(directory.getBaseDirectory());
            }

            // delta directories
            for (AcidUtils.ParsedDelta delta : directory.getCurrentDirectories()) {
                if (!delta.isDeleteDelta()) {
                    readPaths.add(delta.getPath());
                }
            }

            // Create a registry of delete_delta directories for the partition
            for (AcidUtils.ParsedDelta delta : directory.getCurrentDirectories()) {
                if (delta.isDeleteDelta()) {
                    if (!isFullAcid) {
                        throw new TrinoException(HIVE_BAD_DATA, format(
                                "Unexpected delete delta for a non full ACID table '%s'. Would be ignored by the reader: %s",
                                table.getSchemaTableName(),
                                delta.getPath()));
                    }
                    acidInfoBuilder.addDeleteDelta(delta.getPath());
                }
            }

            // initialize original files status list if present
            fileStatusOriginalFiles = directory.getOriginalFiles();

            for (HdfsFileStatusWithId hdfsFileStatusWithId : fileStatusOriginalFiles) {
                Path originalFilePath = hdfsFileStatusWithId.getFileStatus().getPath();
                long originalFileLength = hdfsFileStatusWithId.getFileStatus().getLen();
                if (originalFileLength == 0) {
                    continue;
                }
                // Hive requires "original" files of transactional tables to conform to the bucketed tables naming pattern, to match them with delete deltas.
                int bucketId = getRequiredBucketNumber(originalFilePath);
                acidInfoBuilder.addOriginalFile(originalFilePath, originalFileLength, bucketId);
            }
        }
        else {
            // TODO https://github.com/trinodb/trino/issues/7603 - we should not referece acidInfoBuilder at allwhen we are not reading from non-ACID table
            acidInfoBuilder.setOrcAcidVersionValidated(true); // no ACID; no further validation needed
            readPaths = ImmutableList.of(path);
        }
        // Bucketed partitions are fully loaded immediately since all files must be loaded to determine the file to bucket mapping
        if (tableBucketInfo.isPresent()) {
            ListenableFuture<Void> lastResult = immediateVoidFuture(); // TODO document in addToQueue() that it is sufficient to hold on to last returned future
            for (Path readPath : readPaths) {
                // list all files in the partition
                List<LocatedFileStatus> files = new ArrayList<>();
                try {
                    Iterators.addAll(files, new HiveFileIterator(table, readPath, fs, directoryLister, namenodeStats, FAIL, ignoreAbsentPartitions));
                }
                catch (HiveFileIterator.NestedDirectoryNotAllowedException e) {
                    // Fail here to be on the safe side. This seems to be the same as what Hive does
                    throw new TrinoException(
                            HIVE_INVALID_BUCKET_FILES,
                            format("Hive table '%s' is corrupt. Found sub-directory '%s' in bucket directory for partition: %s",
                                    table.getSchemaTableName(),
                                    e.getNestedDirectoryPath(),
                                    splitFactory.getPartitionName()));
                }
                Optional<AcidInfo> acidInfo = isFullAcid ? acidInfoBuilder.build() : Optional.empty();
                lastResult = hiveSplitSource.addToQueue(getBucketedSplits(files, splitFactory, tableBucketInfo.get(), bucketConversion, splittable, acidInfo));
            }

            for (HdfsFileStatusWithId hdfsFileStatusWithId : fileStatusOriginalFiles) {
                List<LocatedFileStatus> locatedFileStatuses = ImmutableList.of((LocatedFileStatus) hdfsFileStatusWithId.getFileStatus());
                Optional<AcidInfo> acidInfo = isFullAcid
                        ? Optional.of(acidInfoBuilder.buildWithRequiredOriginalFiles(getRequiredBucketNumber(hdfsFileStatusWithId.getFileStatus().getPath())))
                        : Optional.empty();
                lastResult = hiveSplitSource.addToQueue(getBucketedSplits(locatedFileStatuses, splitFactory, tableBucketInfo.get(), bucketConversion, splittable, acidInfo));
            }

            return lastResult;
        }

        for (Path readPath : readPaths) {
            Optional<AcidInfo> acidInfo = isFullAcid ? acidInfoBuilder.build() : Optional.empty();
            fileIterators.addLast(createInternalHiveSplitIterator(readPath, fs, splitFactory, splittable, acidInfo));
        }

        if (!fileStatusOriginalFiles.isEmpty()) {
            fileIterators.addLast(generateOriginalFilesSplits(splitFactory, fileStatusOriginalFiles, splittable, acidInfoBuilder, isFullAcid));
        }

        return COMPLETED_FUTURE;
    }

    private ListenableFuture<Void> createHiveSymlinkSplits(
            String partitionName,
            InputFormat<?, ?> targetInputFormat,
            Properties schema,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            BooleanSupplier partitionMatchSupplier,
            boolean s3SelectPushdownEnabled,
            TableToPartitionMapping tableToPartitionMapping,
            List<Path> targetPaths)
            throws IOException
    {
        ListenableFuture<Void> lastResult = COMPLETED_FUTURE;
        for (Path targetPath : targetPaths) {
            // the splits must be generated using the file system for the target path
            // get the configuration for the target path -- it may be a different hdfs instance
            FileSystem targetFilesystem = hdfsEnvironment.getFileSystem(hdfsContext, targetPath);
            JobConf targetJob = toJobConf(targetFilesystem.getConf());
            targetJob.setInputFormat(TextInputFormat.class);
            Optional<Principal> principal = hdfsContext.getIdentity().getPrincipal();
            if (principal.isPresent()) {
                targetJob.set(MRConfig.FRAMEWORK_NAME, MRConfig.CLASSIC_FRAMEWORK_NAME);
                targetJob.set(MRConfig.MASTER_USER_NAME, principal.get().getName());
            }
            if (targetInputFormat instanceof JobConfigurable) {
                ((JobConfigurable) targetInputFormat).configure(targetJob);
            }
            FileInputFormat.setInputPaths(targetJob, targetPath);
            InputSplit[] targetSplits = hdfsEnvironment.doAs(
                    hdfsContext.getIdentity(),
                    () -> targetInputFormat.getSplits(targetJob, 0));

            InternalHiveSplitFactory splitFactory = new InternalHiveSplitFactory(
                    targetFilesystem,
                    partitionName,
                    targetInputFormat,
                    schema,
                    partitionKeys,
                    effectivePredicate,
                    partitionMatchSupplier,
                    tableToPartitionMapping,
                    Optional.empty(),
                    Optional.empty(),
                    getMaxInitialSplitSize(session),
                    isForceLocalScheduling(session),
                    s3SelectPushdownEnabled,
                    transaction,
                    maxSplitFileSize);
            lastResult = addSplitsToSource(targetSplits, splitFactory);
            if (stopped) {
                return COMPLETED_FUTURE;
            }
        }
        return lastResult;
    }

    @VisibleForTesting
    Optional<Iterator<InternalHiveSplit>> buildManifestFileIterator(
            InputFormat<?, ?> targetInputFormat,
            String partitionName,
            Properties schema,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            BooleanSupplier partitionMatchSupplier,
            boolean s3SelectPushdownEnabled,
            TableToPartitionMapping tableToPartitionMapping,
            Path parent,
            List<Path> paths,
            boolean splittable)
            throws IOException
    {
        FileSystem targetFilesystem = hdfsEnvironment.getFileSystem(hdfsContext, parent);

        Map<Path, LocatedFileStatus> fileStatuses = new HashMap<>();
        HiveFileIterator fileStatusIterator = new HiveFileIterator(table, parent, targetFilesystem, directoryLister, namenodeStats, IGNORED, false);
        fileStatusIterator.forEachRemaining(status -> fileStatuses.put(getPathWithoutSchemeAndAuthority(status.getPath()), status));

        List<LocatedFileStatus> locatedFileStatuses = new ArrayList<>();
        for (Path path : paths) {
            LocatedFileStatus status = fileStatuses.get(getPathWithoutSchemeAndAuthority(path));
            // This check will catch all directories in the manifest since HiveFileIterator will not return any directories.
            // Some files may not be listed by HiveFileIterator - if those are included in the manifest this check will fail as well.
            if (status == null) {
                return Optional.empty();
            }

            locatedFileStatuses.add(status);
        }

        InternalHiveSplitFactory splitFactory = new InternalHiveSplitFactory(
                targetFilesystem,
                partitionName,
                targetInputFormat,
                schema,
                partitionKeys,
                effectivePredicate,
                partitionMatchSupplier,
                tableToPartitionMapping,
                Optional.empty(),
                Optional.empty(),
                getMaxInitialSplitSize(session),
                isForceLocalScheduling(session),
                s3SelectPushdownEnabled,
                transaction,
                maxSplitFileSize);
        return Optional.of(locatedFileStatuses.stream()
                .map(locatedFileStatus -> splitFactory.createInternalHiveSplit(locatedFileStatus, OptionalInt.empty(), splittable, Optional.empty()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .iterator());
    }

    private Iterator<InternalHiveSplit> generateOriginalFilesSplits(
            InternalHiveSplitFactory splitFactory,
            List<HdfsFileStatusWithId> originalFileLocations,
            boolean splittable,
            AcidInfo.Builder acidInfoBuilder,
            boolean isFullAcid)
    {
        return originalFileLocations.stream()
                .map(HdfsFileStatusWithId::getFileStatus)
                .map(fileStatus -> {
                    Optional<AcidInfo> acidInfo = isFullAcid
                            ? Optional.of(acidInfoBuilder.buildWithRequiredOriginalFiles(getRequiredBucketNumber(fileStatus.getPath())))
                            : Optional.empty();
                    return splitFactory.createInternalHiveSplit(
                            (LocatedFileStatus) fileStatus,
                            OptionalInt.empty(),
                            splittable,
                            acidInfo);
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .iterator();
    }

    private ListenableFuture<Void> addSplitsToSource(InputSplit[] targetSplits, InternalHiveSplitFactory splitFactory)
            throws IOException
    {
        ListenableFuture<Void> lastResult = COMPLETED_FUTURE;
        for (InputSplit inputSplit : targetSplits) {
            Optional<InternalHiveSplit> internalHiveSplit = splitFactory.createInternalHiveSplit((FileSplit) inputSplit);
            if (internalHiveSplit.isPresent()) {
                lastResult = hiveSplitSource.addToQueue(internalHiveSplit.get());
            }
            if (stopped) {
                return COMPLETED_FUTURE;
            }
        }
        return lastResult;
    }

    private static boolean shouldUseFileSplitsFromInputFormat(InputFormat<?, ?> inputFormat)
    {
        return Arrays.stream(inputFormat.getClass().getAnnotations())
                .map(Annotation::annotationType)
                .map(Class::getSimpleName)
                .anyMatch(name -> name.equals("UseFileSplitsFromInputFormat"));
    }

    private Iterator<InternalHiveSplit> createInternalHiveSplitIterator(Path path, FileSystem fileSystem, InternalHiveSplitFactory splitFactory, boolean splittable, Optional<AcidInfo> acidInfo)
    {
        return Streams.stream(new HiveFileIterator(table, path, fileSystem, directoryLister, namenodeStats, recursiveDirWalkerEnabled ? RECURSE : IGNORED, ignoreAbsentPartitions))
                .map(status -> splitFactory.createInternalHiveSplit(status, OptionalInt.empty(), splittable, acidInfo))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .iterator();
    }

    private List<InternalHiveSplit> getBucketedSplits(
            List<LocatedFileStatus> files,
            InternalHiveSplitFactory splitFactory,
            BucketSplitInfo bucketSplitInfo,
            Optional<BucketConversion> bucketConversion,
            boolean splittable,
            Optional<AcidInfo> acidInfo)
    {
        int readBucketCount = bucketSplitInfo.getReadBucketCount();
        int tableBucketCount = bucketSplitInfo.getTableBucketCount();
        int partitionBucketCount = bucketConversion.map(BucketConversion::getPartitionBucketCount).orElse(tableBucketCount);
        int bucketCount = max(readBucketCount, partitionBucketCount);

        // build mapping of file name to bucket
        ListMultimap<Integer, LocatedFileStatus> bucketFiles = ArrayListMultimap.create();
        for (LocatedFileStatus file : files) {
            String fileName = file.getPath().getName();
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
            files.sort(null);

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

            boolean containsEligibleTableBucket = false;
            boolean containsIneligibleTableBucket = false;
            for (int tableBucketNumber = bucketNumber % tableBucketCount; tableBucketNumber < tableBucketCount; tableBucketNumber += bucketCount) {
                // table bucket number: this is used for evaluating "$bucket" filters.
                if (bucketSplitInfo.isTableBucketEnabled(tableBucketNumber)) {
                    containsEligibleTableBucket = true;
                }
                else {
                    containsIneligibleTableBucket = true;
                }
            }

            if (containsEligibleTableBucket && containsIneligibleTableBucket) {
                throw new TrinoException(
                        NOT_SUPPORTED,
                        "The bucket filter cannot be satisfied. There are restrictions on the bucket filter when all the following is true: " +
                                "1. a table has a different buckets count as at least one of its partitions that is read in this query; " +
                                "2. the table has a different but compatible bucket number with another table in the query; " +
                                "3. some buckets of the table is filtered out from the query, most likely using a filter on \"$bucket\". " +
                                "(table name: " + table.getTableName() + ", table bucket count: " + tableBucketCount + ", " +
                                "partition bucket count: " + partitionBucketCount + ", effective reading bucket count: " + readBucketCount + ")");
            }
            if (containsEligibleTableBucket) {
                for (LocatedFileStatus file : bucketFiles.get(partitionBucketNumber)) {
                    // OrcDeletedRows will load only delete delta files matching current bucket id,
                    // so we can pass all delete delta locations here, without filtering.
                    splitFactory.createInternalHiveSplit(file, OptionalInt.of(readBucketNumber), splittable, acidInfo)
                            .ifPresent(splitList::add);
                }
            }
        }
        return splitList;
    }

    @VisibleForTesting
    static void validateFileBuckets(ListMultimap<Integer, LocatedFileStatus> bucketFiles, int partitionBucketCount, String tableName, String partitionName)
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

    private static int getRequiredBucketNumber(Path path)
    {
        return getBucketNumber(path.getName())
                .orElseThrow(() -> new IllegalStateException("Cannot get bucket number from path: " + path));
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

    private static List<Path> getTargetPathsFromSymlink(FileSystem fileSystem, Path symlinkDir)
    {
        try {
            FileStatus[] symlinks = fileSystem.listStatus(symlinkDir, HIDDEN_FILES_PATH_FILTER);
            List<Path> targets = new ArrayList<>();

            for (FileStatus symlink : symlinks) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(symlink.getPath()), StandardCharsets.UTF_8))) {
                    CharStreams.readLines(reader).stream()
                            .map(Path::new)
                            .forEach(targets::add);
                }
            }
            return targets;
        }
        catch (IOException e) {
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
            if (!hiveType.isSupportedType(table.getStorage().getStorageFormat())) {
                throw new TrinoException(NOT_SUPPORTED, format("Unsupported Hive type %s found in partition keys of table %s.%s", hiveType, table.getDatabaseName(), table.getTableName()));
            }
            String value = values.get(i);
            checkCondition(value != null, HIVE_INVALID_PARTITION_VALUE, "partition key value cannot be null for field: %s", name);
            partitionKeys.add(new HivePartitionKey(name, value));
        }
        return partitionKeys.build();
    }

    private static Properties getPartitionSchema(Table table, Optional<Partition> partition)
    {
        if (partition.isEmpty()) {
            return getHiveSchema(table);
        }
        return getHiveSchema(partition.get(), table);
    }

    public static class BucketSplitInfo
    {
        private final BucketingVersion bucketingVersion;
        private final List<HiveColumnHandle> bucketColumns;
        private final int tableBucketCount;
        private final int readBucketCount;
        private final IntPredicate bucketFilter;

        public static Optional<BucketSplitInfo> createBucketSplitInfo(Optional<HiveBucketHandle> bucketHandle, Optional<HiveBucketFilter> bucketFilter)
        {
            requireNonNull(bucketHandle, "bucketHandle is null");
            requireNonNull(bucketFilter, "bucketFilter is null");

            if (bucketHandle.isEmpty()) {
                checkArgument(bucketFilter.isEmpty(), "bucketHandle must be present if bucketFilter is present");
                return Optional.empty();
            }

            BucketingVersion bucketingVersion = bucketHandle.get().getBucketingVersion();
            int tableBucketCount = bucketHandle.get().getTableBucketCount();
            int readBucketCount = bucketHandle.get().getReadBucketCount();

            if (tableBucketCount != readBucketCount && bucketFilter.isPresent()) {
                // TODO: remove when supported
                throw new TrinoException(NOT_SUPPORTED, "Filter on \"$bucket\" is not supported when the table has partitions with different bucket counts");
            }

            List<HiveColumnHandle> bucketColumns = bucketHandle.get().getColumns();
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
}
