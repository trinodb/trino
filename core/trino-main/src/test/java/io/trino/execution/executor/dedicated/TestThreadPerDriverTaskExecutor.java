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

import com.google.common.base.Ticker;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.airlift.tracing.Tracing.noopTracer;
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

    @Test
    @Timeout(60)
    public void testAllSplitsRunWhenGlobalCapacityIsScarce()
            throws ExecutionException, InterruptedException
    {
        int taskCount = 8;
        int splitsPerTask = 6;

        FairScheduler scheduler = new FairScheduler(4, "Runner-%d", Ticker.systemTicker());
        // guarantee 1 driver per task, cap each task at 2, and allow only 3 leaf drivers globally
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(noopTracer(), testingVersionEmbedder(), scheduler, 1, 2, 3);
        executor.start();

        try {
            List<ListenableFuture<Void>> allDone = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                TaskId taskId = new TaskId(new StageId("query", 1), 1, i);
                TaskHandle task = executor.addTask(taskId, () -> 0, 1, new Duration(1, MILLISECONDS), OptionalInt.empty());

                List<SplitRunner> splits = new ArrayList<>();
                for (int j = 0; j < splitsPerTask; j++) {
                    splits.add(new TestingSplitRunner(ImmutableList.of(_ -> Futures.immediateVoidFuture())));
                }
                allDone.addAll(executor.enqueueSplits(task, false, splits));
            }

            // every split has to be handed a driver eventually, even though far more splits are
            // queued than the worker can run at once
            Futures.allAsList(allDone).get();
        }
        finally {
            executor.stop();
        }
    }

    @Test
    @Timeout(60)
    public void testPerTaskGuaranteeIgnoresGlobalTarget()
            throws InterruptedException
    {
        int taskCount = 3;
        int guaranteed = 2;

        FairScheduler scheduler = new FairScheduler(16, "Runner-%d", Ticker.systemTicker());
        // guarantee 2 drivers per task even though the global target is only 1
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(noopTracer(), testingVersionEmbedder(), scheduler, guaranteed, Integer.MAX_VALUE, 1);
        executor.start();

        try {
            CountDownLatch running = new CountDownLatch(taskCount * guaranteed);

            for (int i = 0; i < taskCount; i++) {
                TaskId taskId = new TaskId(new StageId("query", 1), 1, i);
                TaskHandle task = executor.addTask(taskId, () -> 0, 1, new Duration(1, MILLISECONDS), OptionalInt.empty());

                List<SplitRunner> splits = new ArrayList<>();
                for (int j = 0; j < 4; j++) {
                    splits.add(new TestingSplitRunner(ImmutableList.of(_ -> {
                        running.countDown();
                        return new TestFuture(); // never completes, so the driver stays occupied
                    })));
                }
                executor.enqueueSplits(task, false, splits);
            }

            assertThat(running.await(30, TimeUnit.SECONDS))
                    .describedAs("Every task got its guaranteed drivers")
                    .isTrue();
        }
        finally {
            executor.stop();
        }
    }

    @Test
    @Timeout(10)
    public void testConcurrencyAdjustmentIsRateLimited()
    {
        TestingTicker ticker = new TestingTicker();
        try (FairScheduler scheduler = new FairScheduler(4, "Runner-%d", ticker)) {
            TaskEntry task = new TaskEntry(
                    new TaskId(new StageId("query", 1), 1, 1),
                    scheduler,
                    testingVersionEmbedder(),
                    noopTracer(),
                    4,
                    new Duration(100, MILLISECONDS),
                    () -> 1.0, // fully utilized, so every permitted adjustment lowers the target
                    ticker);

            assertThat(task.targetConcurrency()).isEqualTo(4);

            // ticks inside the adjustment interval are ignored no matter how many arrive
            for (int i = 0; i < 10; i++) {
                ticker.increment(5, MILLISECONDS);
                task.updateConcurrency();
            }
            assertThat(task.targetConcurrency()).isEqualTo(4);

            // crossing the interval permits exactly one adjustment
            ticker.increment(60, MILLISECONDS);
            task.updateConcurrency();
            task.updateConcurrency();
            assertThat(task.targetConcurrency()).isEqualTo(3);
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
