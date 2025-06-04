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
package io.trino.execution.scheduler.faulttolerant;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.execution.TableExecuteContextManager;
import io.trino.execution.scheduler.TestingExchangeSourceHandle;
import io.trino.execution.scheduler.faulttolerant.SplitAssigner.AssignmentResult;
import io.trino.metadata.Split;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource;
import io.trino.spi.metrics.Metrics;
import io.trino.split.RemoteSplit;
import io.trino.split.SplitSource;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestEventDrivenTaskSource
{
    private static final int INVOCATION_COUNT = 20;
    private static final long TIMEOUT = 60;

    private static final PlanNodeId PLAN_NODE_1 = new PlanNodeId("plan-node-1");
    private static final PlanNodeId PLAN_NODE_2 = new PlanNodeId("plan-node-2");
    private static final PlanNodeId PLAN_NODE_3 = new PlanNodeId("plan-node-3");
    private static final PlanNodeId PLAN_NODE_4 = new PlanNodeId("plan-node-3");

    private static final PlanFragmentId FRAGMENT_1 = new PlanFragmentId("fragment-1");
    private static final PlanFragmentId FRAGMENT_2 = new PlanFragmentId("fragment-2");
    private static final PlanFragmentId FRAGMENT_3 = new PlanFragmentId("fragment-3");

    private final AtomicInteger nextId = new AtomicInteger();

    private ListeningScheduledExecutorService executor;

    @BeforeAll
    public void setUp()
    {
        executor = listeningDecorator(newScheduledThreadPool(10, daemonThreadsNamed(getClass().getName())));
    }

    @AfterAll
    public void tearDown()
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    @RepeatedTest(INVOCATION_COUNT)
    @Timeout(TIMEOUT)
    public void testHappyPath()
            throws Exception
    {
        // no inputs
        testStageTaskSourceSuccess(
                ImmutableListMultimap.of(),
                ImmutableMap.of(),
                ImmutableListMultimap.of());
        // single split
        testStageTaskSourceSuccess(
                ImmutableListMultimap.of(),
                ImmutableMap.of(),
                ImmutableListMultimap.of(PLAN_NODE_1, createSplit(0)));
        // multiple splits
        testStageTaskSourceSuccess(
                ImmutableListMultimap.of(),
                ImmutableMap.of(),
                ImmutableListMultimap.<PlanNodeId, ConnectorSplit>builder()
                        .putAll(PLAN_NODE_1, createSplit(0), createSplit(0), createSplit(1))
                        .build());
        testStageTaskSourceSuccess(
                ImmutableListMultimap.of(),
                ImmutableMap.of(),
                ImmutableListMultimap.<PlanNodeId, ConnectorSplit>builder()
                        .putAll(PLAN_NODE_1, createSplit(0))
                        .putAll(PLAN_NODE_2, createSplit(0))
                        .build());
        testStageTaskSourceSuccess(
                ImmutableListMultimap.of(),
                ImmutableMap.of(),
                ImmutableListMultimap.<PlanNodeId, ConnectorSplit>builder()
                        .putAll(PLAN_NODE_1, createSplit(0))
                        .putAll(PLAN_NODE_2, createSplit(0), createSplit(1))
                        .build());
        testStageTaskSourceSuccess(
                ImmutableListMultimap.of(),
                ImmutableMap.of(),
                ImmutableListMultimap.<PlanNodeId, ConnectorSplit>builder()
                        .putAll(PLAN_NODE_1, createSplit(0), createSplit(3), createSplit(4))
                        .putAll(PLAN_NODE_2, createSplit(0), createSplit(1))
                        .build());
        // single source handle
        testStageTaskSourceSuccess(
                ImmutableListMultimap.of(FRAGMENT_1, createSourceHandle(1)),
                ImmutableMap.of(FRAGMENT_1, PLAN_NODE_1),
                ImmutableListMultimap.of());
        // multiple source handles
        testStageTaskSourceSuccess(
                ImmutableListMultimap.<PlanFragmentId, ExchangeSourceHandle>builder()
                        .putAll(FRAGMENT_1, createSourceHandle(1), createSourceHandle(1))
                        .build(),
                ImmutableMap.of(FRAGMENT_1, PLAN_NODE_1),
                ImmutableListMultimap.of());
        testStageTaskSourceSuccess(
                ImmutableListMultimap.<PlanFragmentId, ExchangeSourceHandle>builder()
                        .putAll(FRAGMENT_1, createSourceHandle(1), createSourceHandle(1))
                        .build(),
                ImmutableMap.<PlanFragmentId, PlanNodeId>builder()
                        .put(FRAGMENT_1, PLAN_NODE_1)
                        .put(FRAGMENT_2, PLAN_NODE_2)
                        .buildOrThrow(),
                ImmutableListMultimap.of());
        testStageTaskSourceSuccess(
                ImmutableListMultimap.<PlanFragmentId, ExchangeSourceHandle>builder()
                        .putAll(FRAGMENT_1, createSourceHandle(1), createSourceHandle(1))
                        .putAll(FRAGMENT_2, createSourceHandle(1), createSourceHandle(3))
                        .build(),
                ImmutableMap.<PlanFragmentId, PlanNodeId>builder()
                        .put(FRAGMENT_1, PLAN_NODE_1)
                        .put(FRAGMENT_2, PLAN_NODE_2)
                        .buildOrThrow(),
                ImmutableListMultimap.of());
        testStageTaskSourceSuccess(
                ImmutableListMultimap.<PlanFragmentId, ExchangeSourceHandle>builder()
                        .putAll(FRAGMENT_1, createSourceHandle(1), createSourceHandle(1))
                        .putAll(FRAGMENT_2, createSourceHandle(1), createSourceHandle(3))
                        .putAll(FRAGMENT_3, createSourceHandle(4))
                        .build(),
                ImmutableMap.<PlanFragmentId, PlanNodeId>builder()
                        .put(FRAGMENT_1, PLAN_NODE_1)
                        .put(FRAGMENT_2, PLAN_NODE_1)
                        .put(FRAGMENT_3, PLAN_NODE_2)
                        .buildOrThrow(),
                ImmutableListMultimap.of());
        // multiple source handles and splits
        testStageTaskSourceSuccess(
                ImmutableListMultimap.<PlanFragmentId, ExchangeSourceHandle>builder()
                        .putAll(FRAGMENT_1, createSourceHandle(1), createSourceHandle(1))
                        .build(),
                ImmutableMap.of(FRAGMENT_1, PLAN_NODE_1),
                ImmutableListMultimap.<PlanNodeId, ConnectorSplit>builder()
                        .putAll(PLAN_NODE_3, createSplit(0))
                        .putAll(PLAN_NODE_4, createSplit(0))
                        .build());
        testStageTaskSourceSuccess(
                ImmutableListMultimap.<PlanFragmentId, ExchangeSourceHandle>builder()
                        .putAll(FRAGMENT_1, createSourceHandle(1), createSourceHandle(1))
                        .putAll(FRAGMENT_2, createSourceHandle(1), createSourceHandle(3))
                        .build(),
                ImmutableMap.<PlanFragmentId, PlanNodeId>builder()
                        .put(FRAGMENT_1, PLAN_NODE_3)
                        .put(FRAGMENT_2, PLAN_NODE_4)
                        .buildOrThrow(),
                ImmutableListMultimap.<PlanNodeId, ConnectorSplit>builder()
                        .putAll(PLAN_NODE_1, createSplit(0), createSplit(3), createSplit(4))
                        .putAll(PLAN_NODE_2, createSplit(0), createSplit(1))
                        .build());
        testStageTaskSourceSuccess(
                ImmutableListMultimap.<PlanFragmentId, ExchangeSourceHandle>builder()
                        .putAll(FRAGMENT_1, createSourceHandle(1), createSourceHandle(1))
                        .putAll(FRAGMENT_2, createSourceHandle(1), createSourceHandle(3))
                        .putAll(FRAGMENT_3, createSourceHandle(4))
                        .build(),
                ImmutableMap.<PlanFragmentId, PlanNodeId>builder()
                        .put(FRAGMENT_1, PLAN_NODE_1)
                        .put(FRAGMENT_2, PLAN_NODE_1)
                        .put(FRAGMENT_3, PLAN_NODE_2)
                        .buildOrThrow(),
                ImmutableListMultimap.<PlanNodeId, ConnectorSplit>builder()
                        .putAll(PLAN_NODE_3, createSplit(0), createSplit(3), createSplit(4))
                        .putAll(PLAN_NODE_4, createSplit(0), createSplit(1))
                        .build());
    }

    @RepeatedTest(INVOCATION_COUNT)
    @Timeout(TIMEOUT)
    public void stressTest()
            throws Exception
    {
        Set<PlanFragmentId> allFragments = ImmutableSet.of(FRAGMENT_1, FRAGMENT_2, FRAGMENT_3);
        Map<PlanFragmentId, PlanNodeId> remoteSources = ImmutableMap.of(FRAGMENT_1, PLAN_NODE_1, FRAGMENT_2, PLAN_NODE_1, FRAGMENT_3, PLAN_NODE_2);
        Set<PlanNodeId> splitSources = ImmutableSet.of(PLAN_NODE_3, PLAN_NODE_4);

        ListMultimap<PlanFragmentId, ExchangeSourceHandle> sourceHandles = ArrayListMultimap.create();
        for (PlanFragmentId fragmentId : allFragments) {
            int numberOfHandles = ThreadLocalRandom.current().nextInt(100);
            for (int i = 0; i < numberOfHandles; i++) {
                int partition = ThreadLocalRandom.current().nextInt(10);
                sourceHandles.put(fragmentId, createSourceHandle(partition));
            }
        }

        ListMultimap<PlanNodeId, ConnectorSplit> splits = ArrayListMultimap.create();
        for (PlanNodeId planNodeId : splitSources) {
            int numberOfSplits = ThreadLocalRandom.current().nextInt(100);
            for (int i = 0; i < numberOfSplits; i++) {
                int partition = ThreadLocalRandom.current().nextInt(10);
                splits.put(planNodeId, createSplit(partition));
            }
        }

        testStageTaskSourceSuccess(sourceHandles, remoteSources, splits);
    }

    private void testStageTaskSourceSuccess(
            ListMultimap<PlanFragmentId, ExchangeSourceHandle> sourceHandles,
            Map<PlanFragmentId, PlanNodeId> remoteSources,
            ListMultimap<PlanNodeId, ConnectorSplit> splits)
            throws Exception
    {
        List<TestingExchangeSourceHandleSource> handleSources = new ArrayList<>();
        Map<PlanFragmentId, Exchange> exchanges = new HashMap<>();
        Multimaps.asMap(sourceHandles).forEach((fragmentId, handles) -> {
            TestingExchangeSourceHandleSource handleSource = new TestingExchangeSourceHandleSource(executor, handles);
            handleSources.add(handleSource);
            exchanges.put(fragmentId, new TestingExchange(handleSource));
        });
        remoteSources.keySet().forEach(fragmentId -> {
            if (!exchanges.containsKey(fragmentId)) {
                TestingExchangeSourceHandleSource handleSource = new TestingExchangeSourceHandleSource(executor, ImmutableList.of());
                handleSources.add(handleSource);
                exchanges.put(fragmentId, new TestingExchange(handleSource));
            }
        });

        Map<PlanNodeId, SplitSource> splitSources = new HashMap<>();
        Multimaps.asMap(splits).forEach((planNodeId, connectorSplits) -> splitSources.put(planNodeId, new TestingSplitSource(executor, connectorSplits)));

        SplitAssignerTester tester = new SplitAssignerTester();
        int partitionCount = getPartitionCount(sourceHandles.values(), splits.values());
        FaultTolerantPartitioningScheme partitioningScheme = createPartitioningScheme(partitionCount);
        AtomicLong getSplitInvocations = new AtomicLong();
        Set<PlanNodeId> allSources = ImmutableSet.<PlanNodeId>builder()
                .addAll(remoteSources.values())
                .addAll(splits.keySet())
                .build();
        List<TaskDescriptor> taskDescriptors;
        TestingSplitAssigner testingSplitAssigner = new TestingSplitAssigner(allSources);
        try (EventDrivenTaskSource taskSource = new EventDrivenTaskSource(
                new QueryId("query"),
                new TableExecuteContextManager(),
                exchanges,
                remoteSources.entrySet().stream()
                        .collect(toImmutableSetMultimap(Map.Entry::getValue, Map.Entry::getKey)),
                () -> splitSources,
                testingSplitAssigner,
                executor,
                1,
                1,
                partitioningScheme,
                (_, _, _) -> getSplitInvocations.incrementAndGet())) {
            while (tester.getTaskDescriptors().isEmpty()) {
                AssignmentResult result = taskSource.process().orElseThrow().get(10, SECONDS);
                tester.update(result);
            }
            assertThat(taskSource.process()).as("Expected taskSource to be finished").isEmpty();
            taskDescriptors = tester.getTaskDescriptors().get();
        }

        for (TestingExchangeSourceHandleSource handleSource : handleSources) {
            assertThat(handleSource.isClosed()).isTrue();
        }
        for (SplitSource splitSource : splitSources.values()) {
            if (splitSource instanceof TestingSplitSource source) {
                assertThat(source.isClosed()).isTrue();
            }
            else {
                fail("unexpected split source: " + splitSource.getClass());
            }
        }

        assertThat(taskDescriptors)
                .isNotNull()
                .isNotEmpty();

        Map<Integer, SetMultimap<PlanNodeId, TestingExchangeSourceHandle>> expectedHandles = new HashMap<>();
        Map<Integer, SetMultimap<PlanNodeId, TestingConnectorSplit>> expectedSplits = new HashMap<>();
        for (Map.Entry<PlanFragmentId, ExchangeSourceHandle> entry : sourceHandles.entries()) {
            TestingExchangeSourceHandle handle = (TestingExchangeSourceHandle) entry.getValue();
            PlanNodeId planNodeId = remoteSources.get(entry.getKey());
            expectedHandles.computeIfAbsent(handle.getPartitionId(), key -> HashMultimap.create()).put(planNodeId, handle);
        }
        for (Map.Entry<PlanNodeId, ConnectorSplit> entry : splits.entries()) {
            TestingConnectorSplit split = (TestingConnectorSplit) entry.getValue();
            expectedSplits.computeIfAbsent(split.getBucket().orElseThrow(), key -> HashMultimap.create()).put(entry.getKey(), split);
        }

        Map<Integer, SetMultimap<PlanNodeId, TestingExchangeSourceHandle>> actualHandles = new HashMap<>();
        Map<Integer, SetMultimap<PlanNodeId, TestingConnectorSplit>> actualSplits = new HashMap<>();
        for (TaskDescriptor taskDescriptor : taskDescriptors) {
            int partitionId = taskDescriptor.getPartitionId();
            for (Map.Entry<PlanNodeId, Split> entry : taskDescriptor.getSplits().getSplitsFlat().entries()) {
                if (entry.getValue().getCatalogHandle().equals(REMOTE_CATALOG_HANDLE)) {
                    RemoteSplit remoteSplit = (RemoteSplit) entry.getValue().getConnectorSplit();
                    SpoolingExchangeInput input = (SpoolingExchangeInput) remoteSplit.getExchangeInput();
                    for (ExchangeSourceHandle handle : input.getExchangeSourceHandles()) {
                        assertThat(handle.getPartitionId()).isEqualTo(partitionId);
                        actualHandles.computeIfAbsent(partitionId, key -> HashMultimap.create()).put(entry.getKey(), (TestingExchangeSourceHandle) handle);
                    }
                }
                else {
                    TestingConnectorSplit split = (TestingConnectorSplit) entry.getValue().getConnectorSplit();
                    assertThat(split.getBucket().orElseThrow()).isEqualTo(partitionId);
                    actualSplits.computeIfAbsent(partitionId, key -> HashMultimap.create()).put(entry.getKey(), split);
                }
            }
        }

        assertThat(actualHandles).isEqualTo(expectedHandles);
        assertThat(actualSplits).isEqualTo(expectedSplits);
    }

    private static FaultTolerantPartitioningScheme createPartitioningScheme(int partitionCount)
    {
        return new FaultTolerantPartitioningScheme(
                partitionCount,
                Optional.of(IntStream.range(0, partitionCount).toArray()),
                Optional.of(split -> ((TestingConnectorSplit) split.getConnectorSplit()).getBucket().orElseThrow()),
                Optional.empty());
    }

    private static int getPartitionCount(Collection<ExchangeSourceHandle> sourceHandles, Collection<ConnectorSplit> splits)
    {
        int maxPartitionId = sourceHandles.stream()
                .mapToInt(ExchangeSourceHandle::getPartitionId)
                .max()
                .orElse(-1);
        maxPartitionId = max(maxPartitionId, splits.stream()
                .map(TestingConnectorSplit.class::cast)
                .map(TestingConnectorSplit::getBucket)
                .mapToInt(OptionalInt::orElseThrow)
                .max()
                .orElse(-1));
        return max(maxPartitionId + 1, 1);
    }

    private TestingExchangeSourceHandle createSourceHandle(int partitionId)
    {
        return new TestingExchangeSourceHandle(nextId.getAndIncrement(), partitionId, 0);
    }

    private TestingConnectorSplit createSplit(int partitionId)
    {
        return new TestingConnectorSplit(nextId.getAndIncrement(), OptionalInt.of(partitionId), Optional.empty());
    }

    private static class TestingSplitSource
            implements SplitSource
    {
        private final ScheduledExecutorService executor;
        @GuardedBy("this")
        private final Queue<ConnectorSplit> remainingSplits;
        @GuardedBy("this")
        private SettableFuture<SplitBatch> currentFuture;
        @GuardedBy("this")
        private boolean finished;
        @GuardedBy("this")
        private boolean closed;

        public TestingSplitSource(ScheduledExecutorService executor, List<ConnectorSplit> splits)
        {
            this.executor = requireNonNull(executor, "executor is null");
            remainingSplits = new LinkedList<>(splits);
        }

        @Override
        public CatalogHandle getCatalogHandle()
        {
            return TEST_CATALOG_HANDLE;
        }

        @Override
        public synchronized ListenableFuture<SplitBatch> getNextBatch(int maxSize)
        {
            checkState(!closed, "closed");
            checkState(currentFuture == null || currentFuture.isDone(), "currentFuture is still running");
            currentFuture = SettableFuture.create();
            long delay = ThreadLocalRandom.current().nextInt(3);
            if (delay == 0) {
                setNextBatch();
            }
            else {
                executor.schedule(this::setNextBatch, delay, MILLISECONDS);
            }
            return currentFuture;
        }

        private void setNextBatch()
        {
            SettableFuture<SplitBatch> future;
            SplitBatch batch;
            synchronized (this) {
                future = currentFuture;
                ConnectorSplit split = remainingSplits.poll();
                boolean lastBatch = remainingSplits.isEmpty();
                batch = new SplitBatch(split == null ? ImmutableList.of() : ImmutableList.of(new Split(TEST_CATALOG_HANDLE, split)), lastBatch);
                if (lastBatch) {
                    finished = true;
                }
            }
            if (future != null) {
                future.set(batch);
            }
        }

        @Override
        public synchronized void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            if (currentFuture != null) {
                currentFuture.cancel(true);
                currentFuture = null;
            }
            remainingSplits.clear();
        }

        @Override
        public synchronized boolean isFinished()
        {
            return finished || closed;
        }

        @Override
        public Optional<List<Object>> getTableExecuteSplitsInfo()
        {
            return Optional.empty();
        }

        @Override
        public Metrics getMetrics()
        {
            return Metrics.EMPTY;
        }

        public synchronized boolean isClosed()
        {
            return closed;
        }
    }

    private static class TestingExchangeSourceHandleSource
            implements ExchangeSourceHandleSource
    {
        private final ScheduledExecutorService executor;
        @GuardedBy("this")
        private final Queue<ExchangeSourceHandle> remainingHandles;
        @GuardedBy("this")
        private CompletableFuture<ExchangeSourceHandleBatch> currentFuture;
        @GuardedBy("this")
        private boolean closed;

        private TestingExchangeSourceHandleSource(ScheduledExecutorService executor, List<ExchangeSourceHandle> handles)
        {
            this.executor = requireNonNull(executor, "executor is null");
            this.remainingHandles = new LinkedList<>(requireNonNull(handles, "handles is null"));
        }

        @Override
        public synchronized CompletableFuture<ExchangeSourceHandleBatch> getNextBatch()
        {
            checkState(!closed, "closed");
            checkState(currentFuture == null || currentFuture.isDone(), "currentFuture is still running");
            currentFuture = new CompletableFuture<>();
            long delay = ThreadLocalRandom.current().nextInt(3);
            if (delay == 0) {
                setNextBatch();
            }
            else {
                executor.schedule(this::setNextBatch, delay, MILLISECONDS);
            }
            return currentFuture;
        }

        private void setNextBatch()
        {
            CompletableFuture<ExchangeSourceHandleBatch> future;
            ExchangeSourceHandleBatch batch;
            synchronized (this) {
                future = currentFuture;
                ExchangeSourceHandle handle = remainingHandles.poll();
                boolean lastBatch = remainingHandles.isEmpty();
                batch = new ExchangeSourceHandleBatch(handle == null ? ImmutableList.of() : ImmutableList.of(handle), lastBatch);
            }
            if (future != null) {
                future.complete(batch);
            }
        }

        @Override
        public synchronized void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            if (currentFuture != null) {
                currentFuture.cancel(true);
                currentFuture = null;
            }
            remainingHandles.clear();
        }

        public synchronized boolean isClosed()
        {
            return closed;
        }
    }

    private static class TestingExchange
            implements Exchange
    {
        @GuardedBy("this")
        private ExchangeSourceHandleSource exchangeSourceHandleSource;
        @GuardedBy("this")
        private boolean closed;

        public TestingExchange(ExchangeSourceHandleSource exchangeSourceHandleSource)
        {
            this.exchangeSourceHandleSource = requireNonNull(exchangeSourceHandleSource, "exchangeSourceHandleSource is null");
        }

        @Override
        public ExchangeId getId()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExchangeSinkHandle addSink(int taskPartitionId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void noMoreSinks()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<ExchangeSinkInstanceHandle> instantiateSink(ExchangeSinkHandle sinkHandle, int taskAttemptId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<ExchangeSinkInstanceHandle> updateSinkInstanceHandle(ExchangeSinkHandle sinkHandle, int taskAttemptId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void sinkFinished(ExchangeSinkHandle sinkHandle, int taskAttemptId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void allRequiredSinksFinished()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized ExchangeSourceHandleSource getSourceHandles()
        {
            checkState(!closed, "already closed");
            checkState(exchangeSourceHandleSource != null, "already retrieved");
            ExchangeSourceHandleSource result = exchangeSourceHandleSource;
            exchangeSourceHandleSource = null;
            return result;
        }

        @Override
        public synchronized void close()
        {
            closed = true;
        }
    }

    private static class TestingSplitAssigner
            implements SplitAssigner
    {
        private final Set<PlanNodeId> allSources;

        private final Set<Integer> partitions = new HashSet<>();
        private final Set<PlanNodeId> finishedSources = new HashSet<>();

        private boolean finished;

        private TestingSplitAssigner(Set<PlanNodeId> allSources)
        {
            this.allSources = ImmutableSet.copyOf(requireNonNull(allSources, "allSources is null"));
        }

        @Override
        public AssignmentResult assign(PlanNodeId planNodeId, ListMultimap<Integer, Split> splitsMap, boolean noMoreSplits)
        {
            checkState(!finished, "finished is set");
            AssignmentResult.Builder result = AssignmentResult.builder();
            Multimaps.asMap(splitsMap).forEach((partition, splits) -> {
                if (partitions.add(partition)) {
                    result.addPartition(new Partition(partition, new NodeRequirements(Optional.empty(), Optional.empty(), true)));
                    for (PlanNodeId finishedSource : finishedSources) {
                        result.updatePartition(new PartitionUpdate(partition, finishedSource, false, ImmutableListMultimap.of(), true));
                    }
                }

                // todo - why are mixing source and task partition ids here
                ListMultimap<Integer, Split> partitionSplits = ImmutableListMultimap.<Integer, Split>builder().putAll(partition, splits).build();
                result.updatePartition(new PartitionUpdate(partition, planNodeId, true, partitionSplits, noMoreSplits));
            });
            if (noMoreSplits) {
                finishedSources.add(planNodeId);
                for (Integer partition : partitions) {
                    result.updatePartition(new PartitionUpdate(partition, planNodeId, false, ImmutableListMultimap.of(), true));
                }
            }
            if (finishedSources.containsAll(allSources)) {
                partitions.forEach(result::sealPartition);
            }
            return result.build();
        }

        @Override
        public AssignmentResult finish()
        {
            AssignmentResult.Builder result = AssignmentResult.builder();
            if (finished) {
                return result.build();
            }
            finished = true;

            checkState(finishedSources.containsAll(allSources));
            if (partitions.isEmpty()) {
                partitions.add(0);
                result
                        .addPartition(new Partition(0, new NodeRequirements(Optional.empty(), Optional.empty(), true)))
                        .sealPartition(0);
            }
            return result.setNoMorePartitions()
                    .build();
        }
    }
}
