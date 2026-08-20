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

        FairScheduler scheduler = new FairScheduler(2, threadFactory, Ticker.systemTicker());
        ThreadPerDriverTaskExecutor executor = new ThreadPerDriverTaskExecutor(noopTracer(), testingVersionEmbedder(), scheduler, 1, Integer.MAX_VALUE, Integer.MAX_VALUE);
        executor.start();
        ExecutorService submitter = newSingleThreadExecutor(daemonThreadsNamed("submitter"));
        try {
            TaskHandle task = executor.addTask(new TaskId(new StageId("query", 1), 1, 1), () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());
            SplitRunner split = new TestingSplitRunner(ImmutableList.of(_ -> Futures.immediateVoidFuture()));

            stallNextThread.set(true);
            Future<?> stalled = submitter.submit(() -> executor.enqueueSplits(task, true, ImmutableList.of(split)));
            threadCreationStarted.await();

            // registering and removing a task must not wait for the stalled thread creation
            TaskId otherTaskId = new TaskId(new StageId("query", 1), 2, 1);
            TaskHandle other = executor.addTask(otherTaskId, () -> 0, 10, new Duration(1, MILLISECONDS), OptionalInt.empty());
            executor.removeTask(other);

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
    public void testFailureToStartSplitReleasesItsClaim()
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

            // queue the split without scheduling it, so that the failing start happens in a later
            // call and the caller is holding the split's future when it does
            ListenableFuture<Void> done = task.enqueueLeafSplit(new TestingSplitRunner(ImmutableList.of(_ -> Futures.immediateVoidFuture())));

            failNextThread.set(true);
            assertThatThrownBy(() -> executor.enqueueSplits(task, false, ImmutableList.of()))
                    .isInstanceOf(OutOfMemoryError.class);

            // the task must be failed rather than left waiting on a split that never ran
            assertThatThrownBy(done::get)
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOf(OutOfMemoryError.class);

            assertThat(executor.getTotalRunningLeafSplits()).isEqualTo(0);
            assertThat(executor.getTotalRunningSplits()).isEqualTo(0);

            // a leaked claim would permanently consume the global leaf driver budget, leaving no
            // capacity to schedule this one
            SplitRunner next = new TestingSplitRunner(ImmutableList.of(_ -> Futures.immediateVoidFuture()));
            executor.enqueueSplits(task, false, ImmutableList.of(next)).get(0).get();
            assertThat(next.isFinished()).isTrue();
        }
        finally {
            executor.stop();
        }
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
