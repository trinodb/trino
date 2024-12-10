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
import io.trino.execution.executor.ExecutionPriority;
import io.trino.execution.executor.TaskHandle;
import io.trino.execution.executor.scheduler.FairScheduler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.scheduleAsync;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.airlift.units.Duration.succinctDuration;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.util.EmbedVersion.testingVersionEmbedder;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
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

            SplitRunner split = new TestingSplitRunner(ImmutableList.of(duration -> {
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
                    duration -> blocked,
                    duration -> Futures.immediateVoidFuture()));

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
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(noopTracer(), testingVersionEmbedder(), scheduler, 0.01, 1, Integer.MAX_VALUE, Integer.MAX_VALUE);
        executor.start();

        try {
            TaskId taskId = new TaskId(new StageId("query", 1), 1, 1);
            TaskHandle task = executor.addTask(taskId, () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());

            Phaser phaser = new Phaser(2);
            SplitRunner split = new TestingSplitRunner(ImmutableList.of(
                    duration -> {
                        phaser.arriveAndAwaitAdvance(); // wait to start
                        phaser.arriveAndAwaitAdvance(); // wait to advance time
                        return Futures.immediateVoidFuture();
                    },
                    duration -> {
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

    @Test
    @Timeout(60)
    public void testLowPriorityTask()
            throws ExecutionException, InterruptedException
    {
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(new TaskManagerConfig().setMaxWorkerThreads("1"), noopTracer(), testingVersionEmbedder());
        executor.start();

        try (ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1, daemonThreadsNamed("thread-per-driver-executor-test"))) {
            TaskHandle normalPriorityTask = executor.addTask(new TaskId(new StageId("normal", 1), 1, 1),
                    ExecutionPriority.NORMAL,
                    () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());

            TaskHandle lowPriorityTask = executor.addTask(new TaskId(new StageId("low", 1), 1, 1),
                    ExecutionPriority.fromResourcePercentage(0.01),
                    () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());

            List<SplitRunner> normalPrioritySplits = splits(scheduledExecutorService, 20, 100);
            SplitRunner lowPrioritySplit = split(scheduledExecutorService, 20);

            ListenableFuture<Void> lowPrioritySplitDone = executor.enqueueSplits(lowPriorityTask, false, ImmutableList.of(lowPrioritySplit)).get(0);
            List<ListenableFuture<Void>> normalPrioritySplitsDone = executor.enqueueSplits(normalPriorityTask, false, normalPrioritySplits);

            for (ListenableFuture<Void> normalPrioritySplitDone : normalPrioritySplitsDone) {
                normalPrioritySplitDone.get();
            }
            for (SplitRunner normalPrioritySplit : normalPrioritySplits) {
                assertThat(normalPrioritySplit.isFinished()).isTrue();
            }
            assertThat(lowPrioritySplit.isFinished()).isFalse();

            lowPrioritySplitDone.get();
            assertThat(lowPrioritySplit.isFinished()).isTrue();
        }
        finally {
            executor.stop();
        }
    }

    @Test
    @Timeout(20)
    public void testLowPriorityTaskSplitQueue()
    {
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(
                new TaskManagerConfig().setMaxWorkerThreads("8").setMinDrivers(16),
                noopTracer(),
                testingVersionEmbedder());
        executor.start();

        try (ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(17, daemonThreadsNamed("thread-per-driver-executor-test"))) {
            TaskHandle normalPriorityTask = executor.addTask(new TaskId(new StageId("normal", 1), 1, 1),
                    ExecutionPriority.NORMAL,
                    () -> 0, 16, new Duration(1, MILLISECONDS), OptionalInt.empty());

            TaskHandle lowPriorityTask = executor.addTask(new TaskId(new StageId("low", 1), 1, 1),
                    ExecutionPriority.fromResourcePercentage(0.01),
                    () -> 0, 16, new Duration(1, MILLISECONDS), OptionalInt.empty());

            List<ListenableFuture<Void>> normalPrioritySplitsFutures = executor.enqueueSplits(normalPriorityTask, false, splits(scheduledExecutorService, 60, 20));

            List<SplitRunner> lowPrioritySplits = splits(scheduledExecutorService, 50, 20);
            executor.enqueueSplits(lowPriorityTask, false, lowPrioritySplits);
            AtomicInteger normalSplitsDoneCount = new AtomicInteger(0);
            for (ListenableFuture<Void> normalPrioritySplitsFuture : normalPrioritySplitsFutures) {
                normalPrioritySplitsFuture.addListener(normalSplitsDoneCount::incrementAndGet, scheduledExecutorService);
            }
            // Wait for at least 50 normal priority splits to finish. Last 10 are left to block low priority splits from running while we do the assertions
            assertEventually(
                    succinctDuration(30, SECONDS),
                    succinctDuration(10, MILLISECONDS),
                    () -> assertThat(normalSplitsDoneCount).hasValueGreaterThanOrEqualTo(50));
            assertThat(lowPrioritySplits.stream().filter(SplitRunner::isFinished).count()).isLessThanOrEqualTo(0);
        }
        finally {
            executor.stop();
        }
    }

    private static List<SplitRunner> splits(ScheduledExecutorService scheduledExecutorService, int splitCount, int invocations)
    {
        return IntStream.range(0, splitCount)
                .mapToObj(_ -> split(scheduledExecutorService, invocations))
                .collect(toImmutableList());
    }

    private static TestingSplitRunner split(ScheduledExecutorService scheduledExecutorService, int invocations)
    {
        return new TestingSplitRunner(IntStream.range(0, invocations)
                .mapToObj(_ -> (Function<Duration, ListenableFuture<Void>>) _ -> {
                    sleepUninterruptibly(10, MILLISECONDS);
                    // return blocked future to make sure the split yields
                    return scheduleAsync(Futures::immediateVoidFuture, 10, MICROSECONDS, scheduledExecutorService);
                })
                .collect(toImmutableList()));
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
        private final List<Function<Duration, ListenableFuture<Void>>> invocations;
        private int invocation;
        private volatile boolean finished;
        private volatile Thread runnerThread;

        public TestingSplitRunner(List<Function<Duration, ListenableFuture<Void>>> invocations)
        {
            this.invocations = invocations;
        }

        @Override
        public final int getPipelineId()
        {
            return 0;
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
