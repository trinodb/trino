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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hive.util.ResumableTask;
import io.trino.plugin.hive.util.ResumableTasks;
import io.trino.plugin.iceberg.delete.TrinoDeleteFile;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPartitionHandle;
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
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getMaxSplitSize;
import static io.trino.plugin.iceberg.IcebergSplitManager.ICEBERG_DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static io.trino.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.iceberg.types.Conversions.fromByteBuffer;

public class IcebergSplitSource
        implements ConnectorSplitSource
{
    private static final Object NO_MORE_SPLITS_SIGNAL = new Object();

    private final AtomicBoolean noMoreSplits = new AtomicBoolean();
    // Queue element: IcebergSplit | TrinoException | NO_MORE_SPLITS_SIGNAL
    private final AsyncQueue<Object> queue;
    private final IcebergSplitLoader icebergSplitLoader;

    public IcebergSplitSource(
            ConnectorSession session,
            HiveConfig hiveConfig,
            Executor executor,
            IcebergTableHandle tableHandle,
            TableScan tableScan,
            Optional<DataSize> maxScannedFileSize,
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringWaitTimeout,
            Constraint constraint,
            TypeManager typeManager,
            boolean recordScannedFiles)
    {
        this.queue = new AsyncQueue<>(hiveConfig.getMaxOutstandingSplits(), executor);
        this.icebergSplitLoader = new IcebergSplitLoader(
                queue,
                tableHandle,
                tableScan,
                maxScannedFileSize,
                dynamicFilter,
                dynamicFilteringWaitTimeout,
                constraint,
                typeManager,
                getMaxSplitSize(session),
                recordScannedFiles);
        ListenableFuture<Void> future = ResumableTasks.submit(executor, icebergSplitLoader);
        addExceptionCallback(future, icebergSplitLoader::fail); // best effort; Split Source could be already completed
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        ListenableFuture<List<Object>> future = queue.getBatchAsync(maxSize);
        ListenableFuture<ConnectorSplitBatch> transform = Futures.transform(future, list -> {
            requireNonNull(list, "list is null");
            boolean noMoreSplitsSignal = false;
            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
            for (Object o : list) {
                if (o instanceof TrinoException) {
                    throw (TrinoException) o;
                }
                if (o == NO_MORE_SPLITS_SIGNAL) {
                    noMoreSplitsSignal = true;
                    continue;
                }
                verify(o instanceof IcebergSplit, "element is not split: %s", o);
                verify(!noMoreSplitsSignal, "receive split after no more splits signal");
                splits.add((IcebergSplit) o);
            }
            if (noMoreSplitsSignal) {
                noMoreSplits.set(true);
            }
            return new ConnectorSplitBatch(splits.build(), noMoreSplitsSignal);
        }, directExecutor());

        return toCompletableFuture(transform);
    }

    public static class IcebergSplitLoader
            implements ResumableTask
    {
        private static final ListenableFuture<Void> COMPLETED_FUTURE = immediateVoidFuture();

        private final AsyncQueue<Object> queue;
        private final IcebergTableHandle tableHandle;
        private final TableScan tableScan;
        private final Optional<Long> maxScannedFileSizeInBytes;
        private final Map<Integer, Type.PrimitiveType> fieldIdToType;
        private final DynamicFilter dynamicFilter;
        private final long dynamicFilteringWaitTimeoutMillis;
        private final Stopwatch dynamicFilterWaitStopwatch;
        private final Constraint constraint;
        private final TypeManager typeManager;
        private final long maxSplitSize;

        private volatile boolean stop;

        private CloseableIterable<CombinedScanTask> combinedScanIterable;
        private Iterator<FileScanTask> fileScanIterator;
        private TupleDomain<IcebergColumnHandle> pushedDownDynamicFilterPredicate;

        private final boolean recordScannedFiles;
        private final ImmutableSet.Builder<DataFile> scannedFiles = ImmutableSet.builder();

        public IcebergSplitLoader(
                AsyncQueue<Object> queue,
                IcebergTableHandle tableHandle,
                TableScan tableScan,
                Optional<DataSize> maxScannedFileSize,
                DynamicFilter dynamicFilter,
                Duration dynamicFilteringWaitTimeout,
                Constraint constraint,
                TypeManager typeManager,
                long maxSplitSize,
                boolean recordScannedFiles)
        {
            this.queue = queue;
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
            this.tableScan = requireNonNull(tableScan, "tableScan is null");
            this.maxScannedFileSizeInBytes = requireNonNull(maxScannedFileSize, "maxScannedFileSize is null").map(DataSize::toBytes);
            this.fieldIdToType = primitiveFieldTypes(tableScan.schema());
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
            this.dynamicFilteringWaitTimeoutMillis = requireNonNull(dynamicFilteringWaitTimeout, "dynamicFilteringWaitTimeout is null").toMillis();
            this.dynamicFilterWaitStopwatch = Stopwatch.createStarted();
            this.constraint = requireNonNull(constraint, "constraint is null");
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
            this.maxSplitSize = maxSplitSize;
            this.recordScannedFiles = recordScannedFiles;
        }

        @Override
        public TaskStatus process()
        {
            while (true) {
                try {
                    if (stop) {
                        closeScanner();
                        queue.offer(NO_MORE_SPLITS_SIGNAL);
                        return TaskStatus.finished();
                    }

                    // Block until one of below conditions is met:
                    // 1. Completion of DynamicFilter
                    // 2. Timeout after waiting for the configured time
                    long timeLeft = dynamicFilteringWaitTimeoutMillis - dynamicFilterWaitStopwatch.elapsed(MILLISECONDS);
                    if (timeLeft > 0 && dynamicFilter.isAwaitable()) {
                        ListenableFuture<Void> future = asVoid(toListenableFuture(dynamicFilter.isBlocked()
                                // As isBlocked() returns unmodifiableFuture, we need to create new future for correct propagation of the timeout
                                .thenApply(Function.identity())
                                .orTimeout(timeLeft, MILLISECONDS)));
                        return TaskStatus.continueOn(future);
                    }

                    ListenableFuture<Void> future = loadSplits();
                    if (!future.isDone()) {
                        return TaskStatus.continueOn(future);
                    }
                }
                catch (Throwable e) {
                    if (!(e instanceof TrinoException)) {
                        e = new TrinoException(HIVE_UNKNOWN_ERROR, e);
                    }
                    fail(e);
                    return TaskStatus.finished();
                }
            }
        }

        private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
        {
            return Futures.transform(future, v -> null, directExecutor());
        }

        private ListenableFuture<Void> loadSplits()
        {
            if (combinedScanIterable == null) {
                // Used to avoid duplicating work if the Dynamic Filter was already pushed down to the Iceberg API
                this.pushedDownDynamicFilterPredicate = dynamicFilter.getCurrentPredicate().transformKeys(IcebergColumnHandle.class::cast);
                TupleDomain<IcebergColumnHandle> fullPredicate = tableHandle.getUnenforcedPredicate()
                        .intersect(pushedDownDynamicFilterPredicate);
                // TODO: (https://github.com/trinodb/trino/issues/9743): Consider removing TupleDomain#simplify
                TupleDomain<IcebergColumnHandle> simplifiedPredicate = fullPredicate.simplify(ICEBERG_DOMAIN_COMPACTION_THRESHOLD);
                if (!simplifiedPredicate.equals(fullPredicate)) {
                    // Pushed down predicate was simplified, always evaluate it against individual splits
                    this.pushedDownDynamicFilterPredicate = TupleDomain.all();
                }

                TupleDomain<IcebergColumnHandle> effectivePredicate = tableHandle.getEnforcedPredicate()
                        .intersect(simplifiedPredicate);

                if (effectivePredicate.isNone()) {
                    stop();
                    return COMPLETED_FUTURE;
                }

                Expression filterExpression = toIcebergExpression(effectivePredicate);
                this.combinedScanIterable = tableScan
                        .filter(filterExpression)
                        .includeColumnStats()
                        .option(TableProperties.SPLIT_SIZE, Long.toString(maxSplitSize))
                        .planTasks();
                this.fileScanIterator = Streams.stream(combinedScanIterable)
                        .map(CombinedScanTask::files)
                        .flatMap(Collection::stream)
                        .iterator();
            }

            TupleDomain<IcebergColumnHandle> dynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                    .transformKeys(IcebergColumnHandle.class::cast);
            if (dynamicFilterPredicate.isNone()) {
                stop();
                return COMPLETED_FUTURE;
            }

            while (fileScanIterator.hasNext()) {
                if (stop) {
                    return COMPLETED_FUTURE;
                }

                FileScanTask scanTask = fileScanIterator.next();
                if (maxScannedFileSizeInBytes.isPresent() && scanTask.file().fileSizeInBytes() > maxScannedFileSizeInBytes.get()) {
                    continue;
                }

                IcebergSplit icebergSplit = toIcebergSplit(scanTask);

                Schema fileSchema = scanTask.spec().schema();
                Set<IcebergColumnHandle> identityPartitionColumns = icebergSplit.getPartitionKeys().keySet().stream()
                        .map(fieldId -> getColumnHandle(fileSchema.findField(fieldId), typeManager))
                        .collect(toImmutableSet());

                Supplier<Map<ColumnHandle, NullableValue>> partitionValues = memoize(() -> {
                    Map<ColumnHandle, NullableValue> bindings = new HashMap<>();
                    for (IcebergColumnHandle partitionColumn : identityPartitionColumns) {
                        Object partitionValue = deserializePartitionValue(
                                partitionColumn.getType(),
                                icebergSplit.getPartitionKeys().get(partitionColumn.getId()).orElse(null),
                                partitionColumn.getName());
                        NullableValue bindingValue = new NullableValue(partitionColumn.getType(), partitionValue);
                        bindings.put(partitionColumn, bindingValue);
                    }
                    return bindings;
                });

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
                    scannedFiles.add(scanTask.file());
                }

                ListenableFuture<Void> future = queue.offer(icebergSplit);
                if (!future.isDone()) {
                    return future;
                }
            }

            stop();
            return COMPLETED_FUTURE;
        }

        private void stop()
        {
            stop = true;
        }

        private void fail(Throwable e)
        {
            queue.offerFirst(e);
            try {
                closeScanner();
            }
            catch (Throwable ignored) {
            }
        }

        private void closeScanner() throws IOException
        {
            if (combinedScanIterable != null) {
                combinedScanIterable.close();
            }
            this.combinedScanIterable = CloseableIterable.empty();
            this.fileScanIterator = Collections.emptyIterator();
        }

        private Optional<List<Object>> getTableExecuteSplitsInfo()
        {
            if (!recordScannedFiles) {
                return Optional.empty();
            }
            return Optional.of(ImmutableList.copyOf(scannedFiles.build()));
        }
    }

    @Override
    public boolean isFinished()
    {
        return noMoreSplits.get();
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        checkState(isFinished(), "Split source must be finished before TableExecuteSplitsInfo is read");
        return icebergSplitLoader.getTableExecuteSplitsInfo();
    }

    @Override
    public void close()
    {
        icebergSplitLoader.stop();
        queue.finish();
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
                    column.getType(),
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
            io.trino.spi.type.Type type,
            @Nullable Object lowerBound,
            @Nullable Object upperBound,
            boolean mayContainNulls)
    {
        Type icebergType = toIcebergType(type);
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

    private static IcebergSplit toIcebergSplit(FileScanTask task)
    {
        return new IcebergSplit(
                task.file().path().toString(),
                task.start(),
                task.length(),
                task.file().fileSizeInBytes(),
                IcebergFileFormat.fromIceberg(task.file().format()),
                ImmutableList.of(),
                getPartitionKeys(task),
                task.deletes().stream()
                        .map(TrinoDeleteFile::copyOf)
                        .collect(toImmutableList()));
    }
}
