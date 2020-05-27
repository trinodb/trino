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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.prestosql.plugin.base.splitloader.AbstractAsyncSplitSource;
import io.prestosql.plugin.base.splitloader.AsyncSplitLoader;
import io.prestosql.plugin.base.util.AsyncQueue;
import io.prestosql.plugin.base.util.ThrottledAsyncQueue;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_EXCEEDED_SPLIT_BUFFERING_LIMIT;
import static io.prestosql.plugin.hive.HiveSessionProperties.getMaxInitialSplitSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getMaxSplitSize;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveSplitSource
        extends AbstractAsyncSplitSource<InternalHiveSplit>
{
    private static final Logger log = Logger.get(HiveSplitSource.class);

    private final long maxOutstandingSplitsBytes;
    private final DataSize maxSplitSize;
    private final DataSize maxInitialSplitSize;
    private final AtomicInteger remainingInitialSplits;

    private final AtomicLong estimatedSplitSizeInBytes = new AtomicLong();
    private final CounterStat highMemorySplitSourceCounter;
    private final AtomicBoolean loggedHighMemoryWarning = new AtomicBoolean();

    private HiveSplitSource(
            ConnectorSession session,
            String databaseName,
            String tableName,
            QueueManager<InternalHiveSplit> queues,
            int maxInitialSplits,
            DataSize maxOutstandingSplitsSize,
            AsyncSplitLoader<InternalHiveSplit> splitLoader,
            CounterStat highMemorySplitSourceCounter)
    {
        super(session.getQueryId(),
                databaseName,
                tableName,
                queues,
                splitLoader);
        this.maxSplitSize = getMaxSplitSize(session);
        this.maxInitialSplitSize = getMaxInitialSplitSize(session);
        this.remainingInitialSplits = new AtomicInteger(maxInitialSplits);
        this.maxOutstandingSplitsBytes = requireNonNull(maxOutstandingSplitsSize, "maxOutstandingSplitsSize is null").toBytes();
        this.highMemorySplitSourceCounter = requireNonNull(highMemorySplitSourceCounter, "highMemorySplitSourceCounter is null");
    }

    public static HiveSplitSource allAtOnce(
            ConnectorSession session,
            String databaseName,
            String tableName,
            int maxInitialSplits,
            int maxOutstandingSplits,
            DataSize maxOutstandingSplitsSize,
            int maxSplitsPerSecond,
            AsyncSplitLoader<InternalHiveSplit> splitLoader,
            Executor executor,
            CounterStat highMemorySplitSourceCounter)
    {
        return new HiveSplitSource(
                session,
                databaseName,
                tableName,
                new NonBucketedQueueManager<>(maxSplitsPerSecond, maxOutstandingSplits, executor),
                maxInitialSplits,
                maxOutstandingSplitsSize,
                splitLoader,
                highMemorySplitSourceCounter);
    }

    public static HiveSplitSource bucketed(
            ConnectorSession session,
            String databaseName,
            String tableName,
            int estimatedOutstandingSplitsPerBucket,
            int maxInitialSplits,
            DataSize maxOutstandingSplitsSize,
            int maxSplitsPerSecond,
            AsyncSplitLoader<InternalHiveSplit> splitLoader,
            Executor executor,
            CounterStat highMemorySplitSourceCounter)
    {
        return new HiveSplitSource(
                session,
                databaseName,
                tableName,
                new QueueManager<InternalHiveSplit>()
                {
                    private final Map<Integer, AsyncQueue<InternalHiveSplit>> queues = new ConcurrentHashMap<>();
                    private final AtomicBoolean finished = new AtomicBoolean();

                    @Override
                    public ListenableFuture<?> offer(InternalHiveSplit connectorSplit)
                    {
                        OptionalInt bucketNumber = connectorSplit.getBucketNumber();
                        AsyncQueue<InternalHiveSplit> queue = queueFor(bucketNumber);
                        queue.offer(connectorSplit);
                        // Do not block "offer" when running split discovery in bucketed mode.
                        // A limit is enforced on estimatedSplitSizeInBytes.
                        return immediateFuture(null);
                    }

                    @Override
                    public AsyncQueue<InternalHiveSplit> queueForPartition(ConnectorPartitionHandle partitionHandle)
                    {
                        OptionalInt bucketNumber = toBucketNumber(partitionHandle);
                        return queueFor(bucketNumber);
                    }

                    @Override
                    public void finish()
                    {
                        if (finished.compareAndSet(false, true)) {
                            queues.values().forEach(AsyncQueue::finish);
                        }
                    }

                    @Override
                    public boolean isFinished(ConnectorPartitionHandle partitionHandle)
                    {
                        return queueFor(toBucketNumber(partitionHandle)).isFinished();
                    }

                    public AsyncQueue<InternalHiveSplit> queueFor(OptionalInt bucketNumber)
                    {
                        checkArgument(bucketNumber.isPresent());
                        AtomicBoolean isNew = new AtomicBoolean();
                        AsyncQueue<InternalHiveSplit> queue = queues.computeIfAbsent(bucketNumber.getAsInt(), ignored -> {
                            isNew.set(true);
                            return new ThrottledAsyncQueue<>(maxSplitsPerSecond, estimatedOutstandingSplitsPerBucket, executor);
                        });
                        if (isNew.get() && finished.get()) {
                            // Check `finished` and invoke `queue.finish` after the `queue` is added to the map.
                            // Otherwise, `queue.finish` may not be invoked if `finished` is set while the lambda above is being evaluated.
                            queue.finish();
                        }
                        return queue;
                    }
                },
                maxInitialSplits,
                maxOutstandingSplitsSize,
                splitLoader,
                highMemorySplitSourceCounter);
    }

    @Override
    protected void validateSplit(InternalHiveSplit split)
    {
        if (estimatedSplitSizeInBytes.addAndGet(split.getEstimatedSizeInBytes()) > maxOutstandingSplitsBytes) {
            // TODO: investigate alternative split discovery strategies when this error is hit.
            // This limit should never be hit given there is a limit of maxOutstandingSplits.
            // If it's hit, it means individual splits are huge.
            if (loggedHighMemoryWarning.compareAndSet(false, true)) {
                highMemorySplitSourceCounter.update(1);
                log.warn("Split buffering for %s.%s in query %s exceeded memory limit (%s). %s splits are buffered.",
                        databaseName, tableName, queryId, succinctBytes(maxOutstandingSplitsBytes), getBufferedInternalSplitCount());
            }
            throw new PrestoException(HIVE_EXCEEDED_SPLIT_BUFFERING_LIMIT, format(
                    "Split buffering for %s.%s exceeded memory limit (%s). %s splits are buffered.",
                    databaseName, tableName, succinctBytes(maxOutstandingSplitsBytes), getBufferedInternalSplitCount()));
        }
    }

    @Override
    protected SplitProcessingResult processSplits(List<InternalHiveSplit> splits)
    {
        ImmutableList.Builder<InternalHiveSplit> splitsToInsertBuilder = ImmutableList.builder();
        ImmutableList.Builder<ConnectorSplit> resultBuilder = ImmutableList.builder();
        int removedEstimatedSizeInBytes = 0;
        for (InternalHiveSplit internalSplit : splits) {
            long maxSplitBytes = maxSplitSize.toBytes();
            if (remainingInitialSplits.get() > 0) {
                if (remainingInitialSplits.getAndDecrement() > 0) {
                    maxSplitBytes = maxInitialSplitSize.toBytes();
                }
            }
            InternalHiveSplit.InternalHiveBlock block = internalSplit.currentBlock();
            long splitBytes;
            if (internalSplit.isSplittable()) {
                splitBytes = min(maxSplitBytes, block.getEnd() - internalSplit.getStart());
            }
            else {
                splitBytes = internalSplit.getEnd() - internalSplit.getStart();
            }

            resultBuilder.add(new HiveSplit(
                    databaseName,
                    tableName,
                    internalSplit.getPartitionName(),
                    internalSplit.getPath(),
                    internalSplit.getStart(),
                    splitBytes,
                    internalSplit.getFileSize(),
                    internalSplit.getFileModifiedTime(),
                    internalSplit.getSchema(),
                    internalSplit.getPartitionKeys(),
                    block.getAddresses(),
                    internalSplit.getBucketNumber(),
                    internalSplit.isForceLocalScheduling(),
                    internalSplit.getTableToPartitionMapping(),
                    internalSplit.getBucketConversion(),
                    internalSplit.isS3SelectPushdownEnabled(),
                    internalSplit.getDeleteDeltaLocations()));

            internalSplit.increaseStart(splitBytes);

            if (internalSplit.isDone()) {
                removedEstimatedSizeInBytes += internalSplit.getEstimatedSizeInBytes();
            }
            else {
                splitsToInsertBuilder.add(internalSplit);
            }
        }
        estimatedSplitSizeInBytes.addAndGet(-removedEstimatedSizeInBytes);

        List<InternalHiveSplit> splitsToInsert = splitsToInsertBuilder.build();
        List<ConnectorSplit> result = resultBuilder.build();

        return new SplitProcessingResult(splitsToInsert, result);
    }

    private static OptionalInt toBucketNumber(ConnectorPartitionHandle partitionHandle)
    {
        if (partitionHandle == NOT_PARTITIONED) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(((HivePartitionHandle) partitionHandle).getBucket());
    }
}
