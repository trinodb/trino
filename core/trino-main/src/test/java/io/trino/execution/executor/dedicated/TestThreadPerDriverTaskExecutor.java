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
package io.trino.execution.executor.dedicated;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.execution.SplitRunner;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.executor.TaskHandle;
import io.trino.execution.executor.scheduler.FairScheduler;
import io.trino.execution.executor.scheduler.Group;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.util.EmbedVersion.testingVersionEmbedder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestThreadPerDriverTaskExecutor
{
    @Test
    @Timeout(10)
    public void testCancellationWhileProcessing()
            throws ExecutionException, InterruptedException
    {
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(new TaskManagerConfig(), noopTracer(), testingVersionEmbedder());
        executor.start();
        try {
            TaskId taskId = new TaskId(new StageId("query", 1), 1, 1);
            TaskHandle task = executor.addTask(taskId, () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());

            CountDownLatch started = new CountDownLatch(1);

            SplitRunner split = new TestingSplitRunner(ImmutableList.of(_ -> {
                started.countDown();
                try {
                    Thread.currentThread().join();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                return Futures.immediateVoidFuture();
            }));

            ListenableFuture<Void> splitDone = executor.enqueueSplits(task, false, ImmutableList.of(split)).get(0);

            started.await();
            executor.removeTask(task);

            splitDone.get();
            assertThat(split.isFinished()).isTrue();
        }
        finally {
            executor.stop();
        }
    }

    @Test
    @Timeout(10)
    public void testSplitsAcrossPipelines()
            throws ExecutionException, InterruptedException
    {
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(new TaskManagerConfig(), noopTracer(), testingVersionEmbedder());
        executor.start();
        try {
            TaskId taskId = new TaskId(new StageId("query", 1), 1, 1);
            TaskHandle task = executor.addTask(taskId, () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());

            // Splits from several pipelines are scheduled under distinct per-pipeline groups; all run.
            ImmutableList.Builder<SplitRunner> splits = ImmutableList.builder();
            for (int pipeline = 0; pipeline < 4; pipeline++) {
                for (int i = 0; i < 5; i++) {
                    splits.add(new TestingSplitRunner(pipeline, ImmutableList.of(_ -> Futures.immediateVoidFuture())));
                }
            }

            List<ListenableFuture<Void>> done = executor.enqueueSplits(task, false, splits.build());
            Futures.allAsList(done).get();

            assertThat(done).hasSize(20);
        }
        finally {
            executor.stop();
        }
    }

    @Test
    @Timeout(10)
    public void testProbeDonatesToBuildPipeline()
            throws ExecutionException, InterruptedException
    {
        int buildPipeline = 1;
        int probePipeline = 2;

        FairScheduler scheduler = FairScheduler.newInstance(4);
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(noopTracer(), testingVersionEmbedder(), scheduler, 3, 10, 8);
        executor.start();
        try {
            TaskId taskId = new TaskId(new StageId("query", 1), 1, 1);
            TaskHandle task = executor.addTask(taskId, () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());

            TestFuture buildGate = new TestFuture();
            TestFuture lookupSource = new TestFuture();

            // Build split: parks on a gate, so its pipeline group exists and is running.
            SplitRunner build = new TestingSplitRunner(buildPipeline, ImmutableList.of(
                    _ -> buildGate,
                    _ -> Futures.immediateVoidFuture()));
            // Probe split: blocks on the lookup source and names the build pipeline as its producer.
            SplitRunner probe = new TestingSplitRunner(probePipeline, OptionalInt.of(buildPipeline), ImmutableList.of(
                    _ -> lookupSource,
                    _ -> Futures.immediateVoidFuture()));

            ListenableFuture<Void> buildDone = executor.enqueueSplits(task, true, ImmutableList.of(build)).get(0);
            buildGate.awaitListenerAdded();

            ListenableFuture<Void> probeDone = executor.enqueueSplits(task, true, ImmutableList.of(probe)).get(0);
            lookupSource.awaitListenerAdded();

            // The probe's block donated priority to the whole build pipeline.
            Group buildGroup = executor.pipelineGroup(taskId, buildPipeline).orElseThrow();
            assertEventually(() -> assertThat(scheduler.pipelineBoostCount(buildGroup)).isEqualTo(1));

            lookupSource.set(null);
            probeDone.get();
            assertThat(scheduler.pipelineBoostCount(buildGroup)).isZero();

            buildGate.set(null);
            buildDone.get();
        }
        finally {
            executor.stop();
        }
    }

    @Test
    @Timeout(10)
    public void testDonatesToCoLocatedProducerTask()
            throws ExecutionException, InterruptedException
    {
        FairScheduler scheduler = FairScheduler.newInstance(4);
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(noopTracer(), testingVersionEmbedder(), scheduler, 3, 10, 8);
        executor.start();
        try {
            TaskId producerId = new TaskId(new StageId("query", 1), 1, 1);
            TaskId consumerId = new TaskId(new StageId("query", 2), 1, 1);
            TaskHandle producer = executor.addTask(producerId, () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());
            TaskHandle consumer = executor.addTask(consumerId, () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());

            TestFuture producerGate = new TestFuture();
            TestFuture consumerData = new TestFuture();

            // Producer task's split parks, so its task group exists and it stays alive to be boosted.
            SplitRunner producerSplit = new TestingSplitRunner(ImmutableList.of(
                    _ -> producerGate,
                    _ -> Futures.immediateVoidFuture()));
            // Consumer task's split blocks and names the co-located producer task.
            SplitRunner consumerSplit = new TestingSplitRunner(0, List.of(producerId), ImmutableList.of(
                    _ -> consumerData,
                    _ -> Futures.immediateVoidFuture()));

            ListenableFuture<Void> producerDone = executor.enqueueSplits(producer, true, ImmutableList.of(producerSplit)).get(0);
            producerGate.awaitListenerAdded();

            ListenableFuture<Void> consumerDone = executor.enqueueSplits(consumer, true, ImmutableList.of(consumerSplit)).get(0);
            consumerData.awaitListenerAdded();

            // The consumer's block donated priority to the co-located producer task's group.
            Group producerGroup = executor.taskDonationGroup(producerId).orElseThrow();
            assertEventually(() -> assertThat(scheduler.pipelineBoostCount(producerGroup)).isEqualTo(1));

            consumerData.set(null);
            consumerDone.get();
            assertThat(scheduler.pipelineBoostCount(producerGroup)).isZero();

            producerGate.set(null);
            producerDone.get();
        }
        finally {
            executor.stop();
        }
    }

    @Test
    @Timeout(10)
    public void testBlocking()
            throws ExecutionException, InterruptedException
    {
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(new TaskManagerConfig(), noopTracer(), testingVersionEmbedder());
        executor.start();

        try {
            TaskId taskId = new TaskId(new StageId("query", 1), 1, 1);
            TaskHandle task = executor.addTask(taskId, () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());

            TestFuture blocked = new TestFuture();

            SplitRunner split = new TestingSplitRunner(ImmutableList.of(
                    _ -> blocked,
                    _ -> Futures.immediateVoidFuture()));

            ListenableFuture<Void> splitDone = executor.enqueueSplits(task, false, ImmutableList.of(split)).get(0);

            blocked.awaitListenerAdded();
            blocked.set(null); // unblock the split

            splitDone.get();
            assertThat(split.isFinished()).isTrue();
        }
        finally {
            executor.stop();
        }
    }

    @Test
    @Timeout(10)
    public void testYielding()
            throws ExecutionException, InterruptedException
    {
        TestingTicker ticker = new TestingTicker();
        FairScheduler scheduler = new FairScheduler(1, "Runner-%d", ticker);
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(noopTracer(), testingVersionEmbedder(), scheduler, 1, Integer.MAX_VALUE, Integer.MAX_VALUE);
        executor.start();

        try {
            TaskId taskId = new TaskId(new StageId("query", 1), 1, 1);
            TaskHandle task = executor.addTask(taskId, () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());

            Phaser phaser = new Phaser(2);
            SplitRunner split = new TestingSplitRunner(ImmutableList.of(
                    _ -> {
                        phaser.arriveAndAwaitAdvance(); // wait to start
                        phaser.arriveAndAwaitAdvance(); // wait to advance time
                        return Futures.immediateVoidFuture();
                    },
                    _ -> {
                        phaser.arriveAndAwaitAdvance();
                        return Futures.immediateVoidFuture();
                    }));

            ListenableFuture<Void> splitDone = executor.enqueueSplits(task, false, ImmutableList.of(split)).get(0);

            phaser.arriveAndAwaitAdvance(); // wait for split to start

            // cause the task to yield
            ticker.increment(FairScheduler.QUANTUM_NANOS * 2, TimeUnit.NANOSECONDS);
            phaser.arriveAndAwaitAdvance();

            // wait for reschedule
            assertThat(phaser.arriveAndAwaitAdvance()).isEqualTo(3); // wait for reschedule

            splitDone.get();
            assertThat(split.isFinished()).isTrue();
        }
        finally {
            executor.stop();
        }
    }

    private static class TestFuture
            extends AbstractFuture<Void>
    {
        private final CountDownLatch listenerAdded = new CountDownLatch(1);

        @Override
        public void addListener(Runnable listener, Executor executor)
        {
            super.addListener(listener, executor);
            listenerAdded.countDown();
        }

        @Override
        public boolean set(Void value)
        {
            return super.set(value);
        }

        public void awaitListenerAdded()
                throws InterruptedException
        {
            listenerAdded.await();
        }
    }

    private static class TestingSplitRunner
            implements SplitRunner
    {
        private final int pipelineId;
        private final OptionalInt blockedProducerPipeline;
        private final List<TaskId> blockedProducerTasks;
        private final List<Function<Duration, ListenableFuture<Void>>> invocations;
        private int invocation;
        private volatile boolean finished;
        private volatile Thread runnerThread;

        public TestingSplitRunner(List<Function<Duration, ListenableFuture<Void>>> invocations)
        {
            this(0, invocations);
        }

        public TestingSplitRunner(int pipelineId, List<Function<Duration, ListenableFuture<Void>>> invocations)
        {
            this(pipelineId, OptionalInt.empty(), List.of(), invocations);
        }

        public TestingSplitRunner(int pipelineId, OptionalInt blockedProducerPipeline, List<Function<Duration, ListenableFuture<Void>>> invocations)
        {
            this(pipelineId, blockedProducerPipeline, List.of(), invocations);
        }

        public TestingSplitRunner(int pipelineId, List<TaskId> blockedProducerTasks, List<Function<Duration, ListenableFuture<Void>>> invocations)
        {
            this(pipelineId, OptionalInt.empty(), blockedProducerTasks, invocations);
        }

        public TestingSplitRunner(int pipelineId, OptionalInt blockedProducerPipeline, List<TaskId> blockedProducerTasks, List<Function<Duration, ListenableFuture<Void>>> invocations)
        {
            this.pipelineId = pipelineId;
            this.blockedProducerPipeline = blockedProducerPipeline;
            this.blockedProducerTasks = blockedProducerTasks;
            this.invocations = invocations;
        }

        @Override
        public final int getPipelineId()
        {
            return pipelineId;
        }

        @Override
        public OptionalInt getBlockedProducerPipeline()
        {
            return blockedProducerPipeline;
        }

        @Override
        public List<TaskId> getBlockedProducerTasks()
        {
            return blockedProducerTasks;
        }

        @Override
        public final Span getPipelineSpan()
        {
            return Span.getInvalid();
        }

        @Override
        public final boolean isFinished()
        {
            return finished;
        }

        @Override
        public final ListenableFuture<Void> processFor(Duration duration)
        {
            ListenableFuture<Void> blocked;

            runnerThread = Thread.currentThread();
            try {
                blocked = invocations.get(invocation).apply(duration);
            }
            finally {
                runnerThread = null;
            }

            invocation++;

            if (invocation == invocations.size()) {
                finished = true;
            }

            return blocked;
        }

        @Override
        public final String getInfo()
        {
            return "";
        }

        @Override
        public final void close()
        {
            finished = true;

            Thread runnerThread = this.runnerThread;

            if (runnerThread != null) {
                runnerThread.interrupt();
            }
        }
    }
}
