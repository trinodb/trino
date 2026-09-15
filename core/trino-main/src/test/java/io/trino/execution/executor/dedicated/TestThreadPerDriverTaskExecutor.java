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
import com.google.common.util.concurrent.SettableFuture;
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

import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.util.EmbedVersion.testingVersionEmbedder;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestThreadPerDriverTaskExecutor
{
    @Test
    @Timeout(30)
    public void testSlowSplitStartDoesNotBlockTaskManagement()
            throws Exception
    {
        // A worker under load takes a long time to create a thread. While a split is being started,
        // tasks must still be able to come and go, otherwise the worker cannot shed the load that
        // made thread creation slow in the first place.
        AtomicBoolean stallNextThread = new AtomicBoolean();
        CountDownLatch threadCreationStarted = new CountDownLatch(1);
        CountDownLatch releaseThreadCreation = new CountDownLatch(1);

        ThreadFactory threadFactory = runnable -> {
            if (stallNextThread.compareAndSet(true, false)) {
                threadCreationStarted.countDown();
                awaitUninterruptibly(releaseThreadCreation);
            }
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            return thread;
        };

        FairScheduler scheduler = new FairScheduler(3, threadFactory, Ticker.systemTicker());
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(noopTracer(), testingVersionEmbedder(), scheduler, 1, 1, Integer.MAX_VALUE);
        // Do not start the executor's periodic scheduler: the replacement below must be started by
        // the completion callback whose progress this test verifies.
        scheduler.start();
        ExecutorService submitter = newSingleThreadExecutor(daemonThreadsNamed("submitter"));
        try {
            TaskHandle task = executor.addTask(new TaskId(new StageId("query", 1), 1, 1), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());
            TestFuture firstBlocked = new TestFuture();
            ListenableFuture<Void> firstDone = executor.enqueueSplits(
                            task,
                            false,
                            ImmutableList.of(new TestingSplitRunner(ImmutableList.of(
                                    _ -> firstBlocked,
                                    _ -> Futures.immediateVoidFuture()))))
                    .getFirst();
            firstBlocked.awaitListenerAdded();

            CountDownLatch replacementStarted = new CountDownLatch(1);
            ListenableFuture<Void> replacementDone = executor.enqueueSplits(
                            task,
                            false,
                            ImmutableList.of(new TestingSplitRunner(ImmutableList.of(_ -> {
                                replacementStarted.countDown();
                                return Futures.immediateVoidFuture();
                            }))))
                    .getFirst();

            TaskHandle stalledTask = executor.addTask(new TaskId(new StageId("query", 1), 2, 1), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());
            SplitRunner stalledSplit = new TestingSplitRunner(ImmutableList.of(_ -> Futures.immediateVoidFuture()));

            stallNextThread.set(true);
            Future<?> stalled = submitter.submit(() -> executor.enqueueSplits(stalledTask, false, ImmutableList.of(stalledSplit)));
            threadCreationStarted.await();

            // Registering, removing and completing a leaf split must not wait for the stalled
            // thread creation.
            TaskId otherTaskId = new TaskId(new StageId("query", 1), 3, 1);
            TaskHandle other = executor.addTask(otherTaskId, () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());
            executor.removeTask(other);
            firstBlocked.set(null);
            firstDone.get(10, TimeUnit.SECONDS);
            assertThat(replacementStarted.await(10, TimeUnit.SECONDS)).isTrue();
            replacementDone.get(10, TimeUnit.SECONDS);

            releaseThreadCreation.countDown();
            stalled.get();
        }
        finally {
            releaseThreadCreation.countDown();
            submitter.shutdownNow();
            executor.stop();
        }
    }

    @Test
    @Timeout(30)
    public void testFailureToStartSplitRequeuesItsClaim()
            throws Exception
    {
        // Starting a split creates its thread, which an overloaded worker fails to do with
        // OutOfMemoryError. The leaf driver accounting has to survive that, otherwise the worker
        // permanently loses the capacity of every split it failed to start.
        AtomicBoolean failNextThread = new AtomicBoolean();
        ThreadFactory threadFactory = runnable -> {
            if (failNextThread.compareAndSet(true, false)) {
                throw new OutOfMemoryError("unable to create native thread");
            }
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            return thread;
        };

        FairScheduler scheduler = new FairScheduler(2, threadFactory, Ticker.systemTicker());
        // no minimum guarantee and a global budget of one leaf driver, so that a leaked claim is
        // observable: it would leave targetGlobalLeafDrivers - runningLeafDrivers at zero and no
        // later split could be scheduled
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(noopTracer(), testingVersionEmbedder(), scheduler, 0, Integer.MAX_VALUE, 1);
        // start the scheduler but not the executor's background tasks, so that splits are only
        // scheduled by the explicit enqueueSplits calls below
        scheduler.start();
        try {
            TaskId taskId = new TaskId(new StageId("query", 1), 1, 1);
            TaskEntry task = (TaskEntry) executor.addTask(taskId, () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());

            // Queue the split without scheduling it, then fail its first start attempt.
            ListenableFuture<Void> done = task.enqueueLeafSplit(new TestingSplitRunner(ImmutableList.of(_ -> Futures.immediateVoidFuture())));

            failNextThread.set(true);
            executor.scheduleMoreLeafSplits();

            assertThat(done).isNotDone();
            assertThat(executor.getTotalRunningLeafSplits()).isEqualTo(0);
            assertThat(executor.getTotalRunningSplits()).isEqualTo(0);
            assertThat(executor.getTotalPendingLeafSplits()).isEqualTo(1);

            // A later pass retries the same split after thread capacity becomes available.
            executor.scheduleMoreLeafSplits();
            done.get();
        }
        finally {
            executor.stop();
        }
    }

    @Test
    @Timeout(30)
    public void testFailureToStartSecondSplitPreservesStartedClaim()
            throws Exception
    {
        AtomicInteger threads = new AtomicInteger();
        ThreadFactory threadFactory = runnable -> {
            if (threads.incrementAndGet() == 2) {
                throw new OutOfMemoryError("unable to create native thread");
            }
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            return thread;
        };

        FairScheduler scheduler = new FairScheduler(2, threadFactory, Ticker.systemTicker());
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(noopTracer(), testingVersionEmbedder(), scheduler, 1, Integer.MAX_VALUE, 2);
        scheduler.start();
        try {
            TaskEntry firstTask = (TaskEntry) executor.addTask(new TaskId(new StageId("query", 1), 1, 1), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());
            TaskEntry secondTask = (TaskEntry) executor.addTask(new TaskId(new StageId("query", 1), 2, 1), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());
            SettableFuture<Void> blocked = SettableFuture.create();
            CountDownLatch started = new CountDownLatch(1);
            ListenableFuture<Void> firstDone = firstTask.enqueueLeafSplit(new TestingSplitRunner(ImmutableList.of(_ -> {
                started.countDown();
                return blocked;
            }, _ -> Futures.immediateVoidFuture())));
            ListenableFuture<Void> secondDone = secondTask.enqueueLeafSplit(new TestingSplitRunner(ImmutableList.of(_ -> {
                started.countDown();
                return blocked;
            }, _ -> Futures.immediateVoidFuture())));

            executor.scheduleMoreLeafSplits();
            started.await();

            assertThat(executor.getTotalRunningLeafSplits()).isEqualTo(1);
            assertThat(executor.getTotalRunningSplits()).isEqualTo(1);
            assertThat(executor.getTotalPendingLeafSplits()).isEqualTo(1);

            blocked.set(null);
            executor.scheduleMoreLeafSplits();
            firstDone.get();
            secondDone.get();
        }
        finally {
            executor.stop();
        }
    }

    @Test
    @Timeout(30)
    public void testStartRegistersResilientSchedulingTask()
            throws Exception
    {
        AtomicBoolean failNextThread = new AtomicBoolean(true);
        ThreadFactory threadFactory = runnable -> {
            if (failNextThread.compareAndSet(true, false)) {
                throw new OutOfMemoryError("unable to create native thread");
            }
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            return thread;
        };

        FairScheduler scheduler = new FairScheduler(1, threadFactory, Ticker.systemTicker());
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(noopTracer(), testingVersionEmbedder(), scheduler, 0, Integer.MAX_VALUE, 1);
        TaskEntry task = (TaskEntry) executor.addTask(new TaskId(new StageId("query", 1), 1, 1), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());
        ListenableFuture<Void> done = task.enqueueLeafSplit(new TestingSplitRunner(ImmutableList.of(_ -> Futures.immediateVoidFuture())));

        executor.start();
        try {
            // The immediate pass fails. The wrapped fixed-delay task must remain registered so the
            // 100ms retry can start the split.
            done.get(10, TimeUnit.SECONDS);
        }
        finally {
            executor.stop();
        }
    }

    @Test
    @Timeout(30)
    public void testLeafCompletionSchedulesReplacementWhenCloseFails()
            throws Exception
    {
        FairScheduler scheduler = FairScheduler.newInstance(1);
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(noopTracer(), testingVersionEmbedder(), scheduler, 1, 1, 1);
        try {
            TaskEntry task = (TaskEntry) executor.addTask(new TaskId(new StageId("query", 1), 1, 1), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());
            ListenableFuture<Void> failingDone = task.enqueueLeafSplit(new CloseFailingSplitRunner());
            CountDownLatch replacementStarted = new CountDownLatch(1);
            ListenableFuture<Void> replacementDone = task.enqueueLeafSplit(new TestingSplitRunner(ImmutableList.of(_ -> {
                replacementStarted.countDown();
                return Futures.immediateVoidFuture();
            })));

            executor.scheduleMoreLeafSplits();

            failingDone.get(10, TimeUnit.SECONDS);
            assertThat(replacementStarted.await(10, TimeUnit.SECONDS)).isTrue();
            replacementDone.get(10, TimeUnit.SECONDS);
        }
        finally {
            executor.stop();
        }
    }

    @Test
    @Timeout(30)
    public void testLeafCompletionSchedulesAnotherTaskWhenTaskDrains()
            throws Exception
    {
        FairScheduler scheduler = FairScheduler.newInstance(1);
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(noopTracer(), testingVersionEmbedder(), scheduler, 0, 1, 1);
        try {
            TaskEntry firstTask = (TaskEntry) executor.addTask(new TaskId(new StageId("query", 1), 1, 1), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());
            TestFuture firstBlocked = new TestFuture();
            ListenableFuture<Void> firstDone = firstTask.enqueueLeafSplit(new TestingSplitRunner(ImmutableList.of(
                    _ -> firstBlocked,
                    _ -> Futures.immediateVoidFuture())));

            executor.scheduleMoreLeafSplits();
            firstBlocked.awaitListenerAdded();

            TaskEntry secondTask = (TaskEntry) executor.addTask(new TaskId(new StageId("query", 1), 2, 1), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());
            CountDownLatch secondStarted = new CountDownLatch(1);
            ListenableFuture<Void> secondDone = secondTask.enqueueLeafSplit(new TestingSplitRunner(ImmutableList.of(_ -> {
                secondStarted.countDown();
                return Futures.immediateVoidFuture();
            })));

            firstBlocked.set(null);
            firstDone.get(10, TimeUnit.SECONDS);
            assertThat(secondStarted.await(10, TimeUnit.SECONDS)).isTrue();
            secondDone.get(10, TimeUnit.SECONDS);
        }
        finally {
            executor.stop();
        }
    }

    @Test
    public void testDestroyCompletesEveryLeafFutureWhenCloseFails()
            throws Exception
    {
        FairScheduler scheduler = FairScheduler.newInstance(1);
        TaskEntry task = new TaskEntry(
                new TaskId(new StageId("query", 1), 1, 1),
                scheduler,
                testingVersionEmbedder(),
                noopTracer(),
                1,
                () -> 0);
        TestingSplitRunner failing = new CloseFailingSplitRunner();
        TestingSplitRunner pending = new TestingSplitRunner(ImmutableList.of(_ -> Futures.immediateVoidFuture()));
        ListenableFuture<Void> claimedDone = task.enqueueLeafSplit(failing);
        ListenableFuture<Void> pendingDone = task.enqueueLeafSplit(pending);
        task.claimLeafSplit();

        try {
            assertThatThrownBy(task::destroy)
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("close failed");

            claimedDone.get();
            pendingDone.get();
            assertThat(failing.isFinished()).isTrue();
            assertThat(pending.isFinished()).isTrue();

            TestingSplitRunner afterDestroy = new TestingSplitRunner(ImmutableList.of(_ -> Futures.immediateVoidFuture()));
            task.enqueueLeafSplit(afterDestroy).get();
            assertThat(afterDestroy.isFinished()).isTrue();
        }
        finally {
            scheduler.close();
        }
    }

    @Test
    public void testMaintenanceSurvivesFailure()
    {
        // scheduleWithFixedDelay stops rescheduling a task that throws, so the wrapper must swallow
        // the failure. It has to catch Error too: the failure an overloaded worker produces is
        // OutOfMemoryError from creating a split's thread.
        AtomicInteger runs = new AtomicInteger();
        Runnable task = ThreadPerDriverTaskExecutor.maintenance(
                () -> {
                    if (runs.incrementAndGet() == 1) {
                        throw new OutOfMemoryError("unable to create native thread");
                    }
                },
                "Error in test task");

        task.run();
        task.run();

        assertThat(runs.get()).isEqualTo(2);
    }

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
        public void close()
        {
            finished = true;

            Thread runnerThread = this.runnerThread;

            if (runnerThread != null) {
                runnerThread.interrupt();
            }
        }
    }

    private static class CloseFailingSplitRunner
            extends TestingSplitRunner
    {
        public CloseFailingSplitRunner()
        {
            super(ImmutableList.of(_ -> Futures.immediateVoidFuture()));
        }

        @Override
        public void close()
        {
            super.close();
            throw new RuntimeException("close failed");
        }
    }
}
