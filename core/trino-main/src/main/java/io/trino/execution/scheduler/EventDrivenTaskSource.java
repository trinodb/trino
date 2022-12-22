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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.io.Closer;
import com.google.common.primitives.ImmutableIntArray;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.execution.TableExecuteContext;
import io.trino.execution.TableExecuteContextManager;
import io.trino.metadata.Split;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource;
import io.trino.spi.exchange.ExchangeSourceHandleSource.ExchangeSourceHandleBatch;
import io.trino.split.RemoteSplit;
import io.trino.split.SplitSource;
import io.trino.split.SplitSource.SplitBatch;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class EventDrivenTaskSource
        implements Closeable
{
    private final QueryId queryId;
    private final TableExecuteContextManager tableExecuteContextManager;
    private final Map<PlanFragmentId, Exchange> sourceExchanges;
    private final Map<PlanFragmentId, PlanNodeId> remoteSources;
    private final Supplier<Map<PlanNodeId, SplitSource>> splitSourceSupplier;
    @GuardedBy("assignerLock")
    private final SplitAssigner assigner;
    @GuardedBy("assignerLock")
    private final Callback callback;
    private final Executor executor;
    private final int splitBatchSize;
    private final long targetExchangeSplitSizeInBytes;
    private final FaultTolerantPartitioningScheme sourcePartitioningScheme;
    private final LongConsumer getSplitTimeRecorder;
    private final SetMultimap<PlanNodeId, PlanFragmentId> remoteSourceFragments;

    @GuardedBy("this")
    private boolean started;
    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private final Closer closer = Closer.create();

    private final Object assignerLock = new Object();

    @GuardedBy("assignerLock")
    private final Set<PlanFragmentId> finishedFragments = new HashSet<>();
    @GuardedBy("assignerLock")
    private final Set<PlanNodeId> allSources = new HashSet<>();
    @GuardedBy("assignerLock")
    private final Set<PlanNodeId> finishedSources = new HashSet<>();

    EventDrivenTaskSource(
            QueryId queryId,
            TableExecuteContextManager tableExecuteContextManager,
            Map<PlanFragmentId, Exchange> sourceExchanges,
            Map<PlanFragmentId, PlanNodeId> remoteSources,
            Supplier<Map<PlanNodeId, SplitSource>> splitSourceSupplier,
            SplitAssigner assigner,
            Callback callback,
            Executor executor,
            int splitBatchSize,
            long targetExchangeSplitSizeInBytes,
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            LongConsumer getSplitTimeRecorder)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
        this.sourceExchanges = ImmutableMap.copyOf(requireNonNull(sourceExchanges, "sourceExchanges is null"));
        this.remoteSources = ImmutableMap.copyOf(requireNonNull(remoteSources, "remoteSources is null"));
        checkArgument(
                sourceExchanges.keySet().equals(remoteSources.keySet()),
                "sourceExchanges and remoteSources are expected to contain the same set of keys: %s != %s",
                sourceExchanges.keySet(),
                remoteSources.keySet());
        this.splitSourceSupplier = requireNonNull(splitSourceSupplier, "splitSourceSupplier is null");
        this.assigner = requireNonNull(assigner, "assigner is null");
        this.callback = requireNonNull(callback, "callback is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.splitBatchSize = splitBatchSize;
        this.targetExchangeSplitSizeInBytes = targetExchangeSplitSizeInBytes;
        this.sourcePartitioningScheme = requireNonNull(sourcePartitioningScheme, "sourcePartitioningScheme is null");
        this.getSplitTimeRecorder = requireNonNull(getSplitTimeRecorder, "getSplitTimeRecorder is null");
        remoteSourceFragments = remoteSources.entrySet().stream()
                .collect(toImmutableSetMultimap(Map.Entry::getValue, Map.Entry::getKey));
    }

    public synchronized void start()
    {
        checkState(!started, "already started");
        checkState(!closed, "already closed");
        started = true;
        try {
            List<SplitLoader> splitLoaders = new ArrayList<>();
            for (Map.Entry<PlanFragmentId, Exchange> entry : sourceExchanges.entrySet()) {
                PlanFragmentId fragmentId = entry.getKey();
                PlanNodeId remoteSourceNodeId = getRemoteSourceNode(fragmentId);
                // doesn't have to be synchronized by assignerLock until the loaders are started
                allSources.add(remoteSourceNodeId);
                ExchangeSourceHandleSource handleSource = closer.register(entry.getValue().getSourceHandles());
                ExchangeSplitSource splitSource = closer.register(new ExchangeSplitSource(handleSource, targetExchangeSplitSizeInBytes));
                SplitLoader splitLoader = closer.register(createExchangeSplitLoader(fragmentId, remoteSourceNodeId, splitSource));
                splitLoaders.add(splitLoader);
            }
            for (Map.Entry<PlanNodeId, SplitSource> entry : splitSourceSupplier.get().entrySet()) {
                PlanNodeId planNodeId = entry.getKey();
                // doesn't have to be synchronized by assignerLock until the loaders are started
                allSources.add(planNodeId);
                SplitLoader splitLoader = closer.register(createTableScanSplitLoader(planNodeId, entry.getValue()));
                splitLoaders.add(splitLoader);
            }
            if (splitLoaders.isEmpty()) {
                executor.execute(() -> {
                    try {
                        synchronized (assignerLock) {
                            assigner.finish().update(callback);
                        }
                    }
                    catch (Throwable t) {
                        fail(t);
                    }
                });
            }
            else {
                splitLoaders.forEach(SplitLoader::start);
            }
        }
        catch (Throwable t) {
            try {
                closer.close();
            }
            catch (Throwable closerFailure) {
                if (closerFailure != t) {
                    t.addSuppressed(closerFailure);
                }
            }
            throw t;
        }
    }

    private SplitLoader createExchangeSplitLoader(PlanFragmentId fragmentId, PlanNodeId remoteSourceNodeId, ExchangeSplitSource splitSource)
    {
        return new SplitLoader(
                splitSource,
                executor,
                ExchangeSplitSource::getSplitPartition,
                new SplitLoader.Callback()
                {
                    @Override
                    public void update(ListMultimap<Integer, Split> splits, boolean noMoreSplitsForFragment)
                    {
                        try {
                            synchronized (assignerLock) {
                                if (noMoreSplitsForFragment) {
                                    finishedFragments.add(fragmentId);
                                }
                                boolean noMoreSplitsForRemoteSource = finishedFragments.containsAll(remoteSourceFragments.get(remoteSourceNodeId));
                                assigner.assign(remoteSourceNodeId, splits, noMoreSplitsForRemoteSource).update(callback);
                                if (noMoreSplitsForRemoteSource) {
                                    finishedSources.add(remoteSourceNodeId);
                                }
                                if (finishedSources.containsAll(allSources)) {
                                    assigner.finish().update(callback);
                                }
                            }
                        }
                        catch (Throwable t) {
                            fail(t);
                        }
                    }

                    @Override
                    public void failed(Throwable t)
                    {
                        fail(t);
                    }
                },
                splitBatchSize,
                getSplitTimeRecorder);
    }

    private SplitLoader createTableScanSplitLoader(PlanNodeId planNodeId, SplitSource splitSource)
    {
        return new SplitLoader(
                splitSource,
                executor,
                this::getSplitPartition,
                new SplitLoader.Callback()
                {
                    @Override
                    public void update(ListMultimap<Integer, Split> splits, boolean noMoreSplits)
                    {
                        try {
                            synchronized (assignerLock) {
                                assigner.assign(planNodeId, splits, noMoreSplits).update(callback);
                                if (noMoreSplits) {
                                    finishedSources.add(planNodeId);

                                    Optional<List<Object>> tableExecuteSplitsInfo = splitSource.getTableExecuteSplitsInfo();
                                    // Here we assume that we can get non-empty tableExecuteSplitsInfo only for queries which facilitate single split source.
                                    tableExecuteSplitsInfo.ifPresent(info -> {
                                        TableExecuteContext tableExecuteContext = tableExecuteContextManager.getTableExecuteContextForQuery(queryId);
                                        tableExecuteContext.setSplitsInfo(info);
                                    });
                                }
                                if (finishedSources.containsAll(allSources)) {
                                    assigner.finish().update(callback);
                                }
                            }
                        }
                        catch (Throwable t) {
                            fail(t);
                        }
                    }

                    @Override
                    public void failed(Throwable t)
                    {
                        fail(t);
                    }
                },
                splitBatchSize,
                getSplitTimeRecorder);
    }

    private PlanNodeId getRemoteSourceNode(PlanFragmentId fragmentId)
    {
        PlanNodeId planNodeId = remoteSources.get(fragmentId);
        verify(planNodeId != null, "remote source not found for fragment: %s", fragmentId);
        return planNodeId;
    }

    private int getSplitPartition(Split split)
    {
        return sourcePartitioningScheme.getPartition(split);
    }

    private void fail(Throwable failure)
    {
        synchronized (assignerLock) {
            callback.failed(failure);
        }
        close();
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    interface Callback
    {
        void partitionsAdded(List<Partition> partitions);

        void noMorePartitions();

        void partitionsUpdated(List<PartitionUpdate> partitionUpdates);

        void partitionsSealed(ImmutableIntArray partitionIds);

        void failed(Throwable t);
    }

    record Partition(int partitionId, NodeRequirements nodeRequirements)
    {
        public Partition
        {
            requireNonNull(nodeRequirements, "nodeRequirements is null");
        }
    }

    record PartitionUpdate(int partitionId, PlanNodeId planNodeId, List<Split> splits, boolean noMoreSplits)
    {
        public PartitionUpdate
        {
            requireNonNull(planNodeId, "planNodeId is null");
            splits = ImmutableList.copyOf(requireNonNull(splits, "splits is null"));
        }
    }

    private static class ExchangeSplitSource
            implements SplitSource
    {
        private final ExchangeSourceHandleSource handleSource;
        private final long targetSplitSizeInBytes;
        private final AtomicBoolean finished = new AtomicBoolean();

        private ExchangeSplitSource(ExchangeSourceHandleSource handleSource, long targetSplitSizeInBytes)
        {
            this.handleSource = requireNonNull(handleSource, "handleSource is null");
            this.targetSplitSizeInBytes = targetSplitSizeInBytes;
        }

        @Override
        public CatalogHandle getCatalogHandle()
        {
            return REMOTE_CATALOG_HANDLE;
        }

        @Override
        public ListenableFuture<SplitBatch> getNextBatch(int maxSize)
        {
            ListenableFuture<ExchangeSourceHandleBatch> sourceHandlesFuture = toListenableFuture(handleSource.getNextBatch());
            return Futures.transform(
                    sourceHandlesFuture,
                    batch -> {
                        List<ExchangeSourceHandle> handles = batch.handles();
                        ListMultimap<Integer, ExchangeSourceHandle> partitionToHandles = handles.stream()
                                .collect(toImmutableListMultimap(ExchangeSourceHandle::getPartitionId, Function.identity()));
                        ImmutableList.Builder<Split> splits = ImmutableList.builder();
                        for (int partition : partitionToHandles.keySet()) {
                            splits.addAll(createRemoteSplits(partitionToHandles.get(partition)));
                        }
                        if (batch.lastBatch()) {
                            finished.set(true);
                        }
                        return new SplitBatch(splits.build(), batch.lastBatch());
                    }, directExecutor());
        }

        private List<Split> createRemoteSplits(List<ExchangeSourceHandle> handles)
        {
            ImmutableList.Builder<Split> result = ImmutableList.builder();
            ImmutableList.Builder<ExchangeSourceHandle> currentSplitHandles = ImmutableList.builder();
            long currentSplitHandlesSize = 0;
            long currentSplitHandlesCount = 0;
            for (ExchangeSourceHandle handle : handles) {
                if (currentSplitHandlesCount > 0 && currentSplitHandlesSize + handle.getDataSizeInBytes() > targetSplitSizeInBytes) {
                    result.add(createRemoteSplit(currentSplitHandles.build()));
                    currentSplitHandles = ImmutableList.builder();
                    currentSplitHandlesSize = 0;
                    currentSplitHandlesCount = 0;
                }
                currentSplitHandles.add(handle);
                currentSplitHandlesSize += handle.getDataSizeInBytes();
                currentSplitHandlesCount++;
            }
            if (currentSplitHandlesCount > 0) {
                result.add(createRemoteSplit(currentSplitHandles.build()));
            }
            return result.build();
        }

        private static Split createRemoteSplit(List<ExchangeSourceHandle> handles)
        {
            return new Split(REMOTE_CATALOG_HANDLE, new RemoteSplit(new SpoolingExchangeInput(handles, Optional.empty())));
        }

        private static int getSplitPartition(Split split)
        {
            RemoteSplit remoteSplit = (RemoteSplit) split.getConnectorSplit();
            SpoolingExchangeInput exchangeInput = (SpoolingExchangeInput) remoteSplit.getExchangeInput();
            List<ExchangeSourceHandle> handles = exchangeInput.getExchangeSourceHandles();
            return handles.get(0).getPartitionId();
        }

        @Override
        public void close()
        {
            handleSource.close();
        }

        @Override
        public boolean isFinished()
        {
            return finished.get();
        }

        @Override
        public Optional<List<Object>> getTableExecuteSplitsInfo()
        {
            return Optional.empty();
        }
    }

    private static class SplitLoader
            implements Closeable
    {
        private final SplitSource splitSource;
        private final Executor executor;
        private final ToIntFunction<Split> splitToPartition;
        private final Callback callback;
        private final int splitBatchSize;
        private final LongConsumer getSplitTimeRecorder;

        @GuardedBy("this")
        private boolean started;
        @GuardedBy("this")
        private boolean closed;
        @GuardedBy("this")
        private ListenableFuture<SplitBatch> splitLoadingFuture;

        public SplitLoader(
                SplitSource splitSource,
                Executor executor,
                ToIntFunction<Split> splitToPartition,
                Callback callback,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder)
        {
            this.splitSource = requireNonNull(splitSource, "splitSource is null");
            this.executor = requireNonNull(executor, "executor is null");
            this.splitToPartition = requireNonNull(splitToPartition, "splitToPartition is null");
            this.callback = requireNonNull(callback, "callback is null");
            this.splitBatchSize = splitBatchSize;
            this.getSplitTimeRecorder = requireNonNull(getSplitTimeRecorder, "getSplitTimeRecorder is null");
        }

        public synchronized void start()
        {
            checkState(!started, "already started");
            checkState(!closed, "already closed");
            started = true;
            processNext();
        }

        private synchronized void processNext()
        {
            if (closed) {
                return;
            }
            verify(splitLoadingFuture == null || splitLoadingFuture.isDone(), "splitLoadingFuture is still running");
            long start = System.nanoTime();
            splitLoadingFuture = splitSource.getNextBatch(splitBatchSize);
            Futures.addCallback(splitLoadingFuture, new FutureCallback<>()
            {
                @Override
                public void onSuccess(SplitBatch result)
                {
                    try {
                        getSplitTimeRecorder.accept(start);
                        ListMultimap<Integer, Split> splits = result.getSplits().stream()
                                .collect(toImmutableListMultimap(splitToPartition::applyAsInt, Function.identity()));
                        boolean finished = result.isLastBatch() && splitSource.isFinished();
                        callback.update(splits, finished);
                        if (!finished) {
                            processNext();
                        }
                    }
                    catch (Throwable t) {
                        callback.failed(t);
                    }
                }

                @Override
                public void onFailure(Throwable t)
                {
                    callback.failed(t);
                }
            }, executor);
        }

        @Override
        public synchronized void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            if (splitLoadingFuture != null) {
                splitLoadingFuture.cancel(true);
                splitLoadingFuture = null;
            }
            splitSource.close();
        }

        public interface Callback
        {
            void update(ListMultimap<Integer, Split> splits, boolean noMoreSplits);

            void failed(Throwable t);
        }
    }
}
