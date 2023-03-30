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
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.execution.TableExecuteContext;
import io.trino.execution.TableExecuteContextManager;
import io.trino.execution.scheduler.SplitAssigner.AssignmentResult;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.airlift.concurrent.MoreFutures.whenAnyCompleteCancelOthers;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class EventDrivenTaskSource
        implements Closeable
{
    private final QueryId queryId;
    private final TableExecuteContextManager tableExecuteContextManager;
    private final Map<PlanFragmentId, Exchange> sourceExchanges;
    private final SetMultimap<PlanNodeId, PlanFragmentId> remoteSources;
    private final Supplier<Map<PlanNodeId, SplitSource>> splitSourceSupplier;
    private final SplitAssigner assigner;
    private final Executor executor;
    private final int splitBatchSize;
    private final long targetExchangeSplitSizeInBytes;
    private final FaultTolerantPartitioningScheme sourcePartitioningScheme;
    private final LongConsumer getSplitTimeRecorder;

    @GuardedBy("this")
    private boolean initialized;
    @GuardedBy("this")
    private List<IdempotentSplitSource> splitSources;
    @GuardedBy("this")
    private final Set<PlanFragmentId> completedFragments = new HashSet<>();

    @GuardedBy("this")
    private ListenableFuture<AssignmentResult> future;
    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private final Closer closer = Closer.create();

    EventDrivenTaskSource(
            QueryId queryId,
            TableExecuteContextManager tableExecuteContextManager,
            Map<PlanFragmentId, Exchange> sourceExchanges,
            SetMultimap<PlanNodeId, PlanFragmentId> remoteSources,
            Supplier<Map<PlanNodeId, SplitSource>> splitSourceSupplier,
            SplitAssigner assigner,
            Executor executor,
            int splitBatchSize,
            long targetExchangeSplitSizeInBytes,
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            LongConsumer getSplitTimeRecorder)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
        this.sourceExchanges = ImmutableMap.copyOf(requireNonNull(sourceExchanges, "sourceExchanges is null"));
        this.remoteSources = ImmutableSetMultimap.copyOf(requireNonNull(remoteSources, "remoteSources is null"));
        this.splitSourceSupplier = requireNonNull(splitSourceSupplier, "splitSourceSupplier is null");
        this.assigner = requireNonNull(assigner, "assigner is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.splitBatchSize = splitBatchSize;
        this.targetExchangeSplitSizeInBytes = targetExchangeSplitSizeInBytes;
        this.sourcePartitioningScheme = requireNonNull(sourcePartitioningScheme, "sourcePartitioningScheme is null");
        this.getSplitTimeRecorder = requireNonNull(getSplitTimeRecorder, "getSplitTimeRecorder is null");
    }

    public synchronized ListenableFuture<AssignmentResult> process()
    {
        checkState(!closed, "closed");
        checkState(future == null || future.isDone(), "still in process");

        if (!initialized) {
            initialize();
            initialized = true;
        }

        future = processNext();
        return future;
    }

    @GuardedBy("this")
    private void initialize()
    {
        Map<PlanFragmentId, PlanNodeId> remoteSourceNodeIds = new HashMap<>();
        remoteSources.forEach((planNodeId, planFragmentId) -> remoteSourceNodeIds.put(planFragmentId, planNodeId));
        ImmutableList.Builder<IdempotentSplitSource> splitSources = ImmutableList.builder();
        for (Map.Entry<PlanFragmentId, Exchange> entry : sourceExchanges.entrySet()) {
            PlanFragmentId sourceFragmentId = entry.getKey();
            PlanNodeId remoteSourceNodeId = remoteSourceNodeIds.get(sourceFragmentId);
            verify(remoteSourceNodeId != null, "remote source not found for fragment: %s", sourceFragmentId);
            ExchangeSourceHandleSource handleSource = closer.register(entry.getValue().getSourceHandles());
            ExchangeSplitSource splitSource = closer.register(new ExchangeSplitSource(handleSource, targetExchangeSplitSizeInBytes));
            splitSources.add(closer.register(new IdempotentSplitSource(queryId, tableExecuteContextManager, remoteSourceNodeId, Optional.of(sourceFragmentId), splitSource, splitBatchSize, getSplitTimeRecorder)));
        }
        for (Map.Entry<PlanNodeId, SplitSource> entry : splitSourceSupplier.get().entrySet()) {
            splitSources.add(closer.register(new IdempotentSplitSource(queryId, tableExecuteContextManager, entry.getKey(), Optional.empty(), closer.register(entry.getValue()), splitBatchSize, getSplitTimeRecorder)));
        }
        this.splitSources = splitSources.build();
    }

    @GuardedBy("this")
    private ListenableFuture<AssignmentResult> processNext()
    {
        List<ListenableFuture<IdempotentSplitSource.SplitBatchReference>> futures = splitSources.stream()
                .map(IdempotentSplitSource::getNext)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());

        if (futures.isEmpty()) {
            return immediateFuture(assigner.finish());
        }

        ListenableFuture<IdempotentSplitSource.SplitBatchReference> firstCompleted = whenAnyCompleteCancelOthers(futures);
        return Futures.transform(firstCompleted, this::process, executor);
    }

    private synchronized AssignmentResult process(IdempotentSplitSource.SplitBatchReference batchReference)
    {
        PlanNodeId sourceNodeId = batchReference.getPlanNodeId();
        Optional<PlanFragmentId> sourceFragmentId = batchReference.getSourceFragmentId();
        SplitBatch splitBatch = batchReference.getSplitBatchAndAdvance();

        boolean noMoreSplits = false;
        if (splitBatch.isLastBatch()) {
            if (sourceFragmentId.isPresent()) {
                completedFragments.add(sourceFragmentId.get());
                noMoreSplits = completedFragments.containsAll(remoteSources.get(sourceNodeId));
            }
            else {
                noMoreSplits = true;
            }
        }

        ListMultimap<Integer, Split> splits = splitBatch.getSplits().stream()
                .collect(toImmutableListMultimap(this::getSplitPartition, Function.identity()));
        return assigner.assign(sourceNodeId, splits, noMoreSplits);
    }

    private int getSplitPartition(Split split)
    {
        if (split.getConnectorSplit() instanceof RemoteSplit remoteSplit) {
            SpoolingExchangeInput exchangeInput = (SpoolingExchangeInput) remoteSplit.getExchangeInput();
            List<ExchangeSourceHandle> handles = exchangeInput.getExchangeSourceHandles();
            return handles.get(0).getPartitionId();
        }
        return sourcePartitioningScheme.getPartition(split);
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

    private static class IdempotentSplitSource
            implements Closeable
    {
        private final QueryId queryId;
        private final TableExecuteContextManager tableExecuteContextManager;
        private final PlanNodeId planNodeId;
        private final Optional<PlanFragmentId> sourceFragmentId;
        private final SplitSource splitSource;
        private final int splitBatchSize;
        private final LongConsumer getSplitTimeRecorder;

        @GuardedBy("this")
        private Optional<CallbackProxyFuture<SplitBatchReference>> future = Optional.empty();
        @GuardedBy("this")
        private boolean closed;
        @GuardedBy("this")
        private boolean finished;

        private IdempotentSplitSource(
                QueryId queryId,
                TableExecuteContextManager tableExecuteContextManager,
                PlanNodeId planNodeId,
                Optional<PlanFragmentId> sourceFragmentId,
                SplitSource splitSource,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceFragmentId = requireNonNull(sourceFragmentId, "sourceFragmentId is null");
            this.splitSource = requireNonNull(splitSource, "splitSource is null");
            this.splitBatchSize = splitBatchSize;
            this.getSplitTimeRecorder = requireNonNull(getSplitTimeRecorder, "getSplitTimeRecorder is null");
        }

        public synchronized Optional<ListenableFuture<SplitBatchReference>> getNext()
        {
            if (future.isEmpty() && !finished) {
                long start = System.nanoTime();
                future = Optional.of(new CallbackProxyFuture<>(Futures.transform(splitSource.getNextBatch(splitBatchSize), batch -> {
                    getSplitTimeRecorder.accept(start);
                    if (batch.isLastBatch()) {
                        Optional<List<Object>> tableExecuteSplitsInfo = splitSource.getTableExecuteSplitsInfo();
                        // Here we assume that we can get non-empty tableExecuteSplitsInfo only for queries which facilitate single split source.
                        tableExecuteSplitsInfo.ifPresent(info -> {
                            TableExecuteContext tableExecuteContext = tableExecuteContextManager.getTableExecuteContextForQuery(queryId);
                            tableExecuteContext.setSplitsInfo(info);
                        });
                    }
                    return new SplitBatchReference(batch);
                }, directExecutor())));
            }
            return future.map(CallbackProxyFuture::addListener);
        }

        @Override
        public synchronized void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            if (future.isPresent() && !future.get().isDone()) {
                future.get().cancel(true);
            }
            splitSource.close();
        }

        private synchronized void advance(boolean lastBatch)
        {
            finished = lastBatch;
            future = Optional.empty();
        }

        public class SplitBatchReference
        {
            private final SplitBatch splitBatch;

            public SplitBatchReference(SplitBatch splitBatch)
            {
                this.splitBatch = requireNonNull(splitBatch, "splitBatch is null");
            }

            public PlanNodeId getPlanNodeId()
            {
                return planNodeId;
            }

            public Optional<PlanFragmentId> getSourceFragmentId()
            {
                return sourceFragmentId;
            }

            public SplitBatch getSplitBatchAndAdvance()
            {
                advance(splitBatch.isLastBatch());
                return splitBatch;
            }
        }
    }

    private static class ExchangeSplitSource
            implements SplitSource
    {
        private final ExchangeSourceHandleSource handleSource;
        private final long targetSplitSizeInBytes;

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

        @Override
        public void close()
        {
            handleSource.close();
        }

        @Override
        public boolean isFinished()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<List<Object>> getTableExecuteSplitsInfo()
        {
            return Optional.empty();
        }
    }

    /**
     * This proxy is necessary to solve the "accumulating callbacks" problem
     */
    private static class CallbackProxyFuture<T>
            extends ForwardingListenableFuture<T>
    {
        private final ListenableFuture<T> delegate;

        @GuardedBy("listeners")
        private final Set<SettableFuture<T>> listeners = Sets.newIdentityHashSet();

        private CallbackProxyFuture(ListenableFuture<T> delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            delegate.addListener(this::propagateIfNecessary, directExecutor());
        }

        @Override
        protected ListenableFuture<T> delegate()
        {
            return delegate;
        }

        @Override
        public void addListener(Runnable listener, Executor executor)
        {
            throw new UnsupportedOperationException();
        }

        public ListenableFuture<T> addListener()
        {
            SettableFuture<T> listener = SettableFuture.create();
            synchronized (listeners) {
                listeners.add(listener);
            }

            listener.addListener(
                    () -> {
                        if (listener.isCancelled()) {
                            synchronized (listeners) {
                                listeners.remove(listener);
                            }
                        }
                    },
                    directExecutor());

            propagateIfNecessary();

            return listener;
        }

        private void propagateIfNecessary()
        {
            if (!delegate.isDone()) {
                return;
            }

            List<SettableFuture<T>> futures;
            synchronized (listeners) {
                futures = ImmutableList.copyOf(listeners);
                listeners.clear();
            }

            for (SettableFuture<T> future : futures) {
                future.setFuture(delegate);
            }
        }
    }
}
