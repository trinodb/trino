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
package io.trino.execution.executor.scheduler;

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.testing.TestingTicker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFairScheduler
{
    @Test
    public void testBasic()
            throws ExecutionException, InterruptedException
    {
        try (FairScheduler scheduler = FairScheduler.newInstance(1)) {
            Group group = scheduler.createGroup("G1");

            AtomicBoolean ran = new AtomicBoolean();
            ListenableFuture<Void> done = scheduler.submit(group, 1, _ -> ran.set(true));

            done.get();
            assertThat(ran.get())
                    .describedAs("Ran task")
                    .isTrue();
        }
    }

    @Test
    @Timeout(5)
    public void testYield()
            throws ExecutionException, InterruptedException
    {
        TestingTicker ticker = new TestingTicker();
        try (FairScheduler scheduler = FairScheduler.newInstance(1, ticker)) {
            Group group = scheduler.createGroup("G");

            CountDownLatch task1Started = new CountDownLatch(1);
            AtomicBoolean task2Ran = new AtomicBoolean();

            ListenableFuture<Void> task1 = scheduler.submit(group, 1, context -> {
                task1Started.countDown();
                while (!task2Ran.get()) {
                    if (!context.maybeYield()) {
                        return;
                    }
                }
            });

            task1Started.await();

            ListenableFuture<Void> task2 = scheduler.submit(group, 2, _ -> {
                task2Ran.set(true);
            });

            while (!task2.isDone()) {
                ticker.increment(FairScheduler.QUANTUM_NANOS * 2, TimeUnit.NANOSECONDS);
            }

            task1.get();
        }
    }

    @Test
    public void testBlocking()
            throws InterruptedException, ExecutionException
    {
        try (FairScheduler scheduler = FairScheduler.newInstance(1)) {
            Group group = scheduler.createGroup("G");

            CountDownLatch task1Started = new CountDownLatch(1);
            CountDownLatch task2Submitted = new CountDownLatch(1);
            CountDownLatch task2Started = new CountDownLatch(1);
            AtomicBoolean task2Ran = new AtomicBoolean();

            SettableFuture<Void> task1Blocked = SettableFuture.create();

            ListenableFuture<Void> task1 = scheduler.submit(group, 1, context -> {
                try {
                    task1Started.countDown();
                    task2Submitted.await();

                    assertThat(task2Ran.get())
                            .describedAs("Task 2 run")
                            .isFalse();

                    context.block(task1Blocked);

                    assertThat(task2Ran.get())
                            .describedAs("Task 2 run")
                            .isTrue();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });

            task1Started.await();

            ListenableFuture<Void> task2 = scheduler.submit(group, 2, _ -> {
                task2Started.countDown();
                task2Ran.set(true);
            });

            task2Submitted.countDown();
            task2Started.await();

            // unblock task 1
            task1Blocked.set(null);

            task1.get();
            task2.get();
        }
    }

    @Test
    public void testCancelWhileYielding()
            throws InterruptedException, ExecutionException
    {
        TestingTicker ticker = new TestingTicker();
        try (FairScheduler scheduler = FairScheduler.newInstance(1, ticker)) {
            Group group = scheduler.createGroup("G");

            CountDownLatch task1Started = new CountDownLatch(1);
            CountDownLatch task1TimeAdvanced = new CountDownLatch(1);
            CountDownLatch cancelled = new CountDownLatch(1);

            ListenableFuture<Void> task1 = scheduler.submit(group, 1, context -> {
                try {
                    task1Started.countDown();
                    task1TimeAdvanced.await();

                    cancelled.await();
                    assertThat(context.maybeYield())
                            .describedAs("Cancelled while yielding")
                            .isFalse();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });

            task1Started.await();
            scheduler.pause(); // prevent rescheduling after yield

            ticker.increment(FairScheduler.QUANTUM_NANOS * 2, TimeUnit.NANOSECONDS);
            task1TimeAdvanced.countDown();

            scheduler.removeGroup(group); // cause a cancellation
            cancelled.countDown();
            task1.get();
        }
    }

    @Test
    public void testCancelWhileBlocking()
            throws InterruptedException, ExecutionException
    {
        TestingTicker ticker = new TestingTicker();
        try (FairScheduler scheduler = FairScheduler.newInstance(1, ticker)) {
            Group group = scheduler.createGroup("G");

            CountDownLatch task1Started = new CountDownLatch(1);
            TestFuture task1Blocked = new TestFuture();

            ListenableFuture<Void> task1 = scheduler.submit(group, 1, context -> {
                task1Started.countDown();

                assertThat(context.block(task1Blocked))
                        .describedAs("Cancelled while blocking")
                        .isFalse();
            });

            task1Started.await();

            task1Blocked.awaitListenerAdded(); // When the listener is added, we know the task is blocked

            scheduler.removeGroup(group);
            task1.get();
        }
    }

    @Test
    @Timeout(30)
    public void testUnblockedTaskResumesWithoutScheduler()
            throws InterruptedException, ExecutionException
    {
        // With spare capacity and nothing runnable, an unblocked task must resume on its own
        // thread rather than waiting for the scheduler thread to hand it a slot
        try (FairScheduler scheduler = FairScheduler.newInstance(4)) {
            Group group = scheduler.createGroup("G");

            int blocks = 100;
            AtomicInteger completed = new AtomicInteger();
            ListenableFuture<Void> task = scheduler.submit(group, 1, context -> {
                for (int i = 0; i < blocks; i++) {
                    if (!context.block(immediateVoidFuture())) {
                        return;
                    }
                }
                completed.set(blocks);
            });

            task.get();

            assertThat(completed.get()).isEqualTo(blocks);
            assertThat(scheduler.getBypassedResumeCount() + scheduler.getScheduledResumeCount())
                    .describedAs("Every block is accounted for as either a bypassed or scheduled resume")
                    .isEqualTo(blocks);
            assertThat(scheduler.getBypassedResumeCount())
                    .describedAs("Bypassed resumes")
                    .isPositive();
        }
    }

    @Test
    @Timeout(30)
    public void testPausedSchedulerIsNotBypassed()
            throws InterruptedException, ExecutionException
    {
        // pause() is not synchronous: a scheduler thread already parked in dequeue() is past the
        // gate and can still hand out a slot. What must hold is that a task unblocking while
        // paused does not resume itself, since that would skip the gate altogether.
        try (FairScheduler scheduler = FairScheduler.newInstance(4)) {
            Group group = scheduler.createGroup("G");

            CountDownLatch started = new CountDownLatch(1);
            TestFuture blocked = new TestFuture();

            ListenableFuture<Void> task = scheduler.submit(group, 1, context -> {
                started.countDown();
                context.block(blocked);
            });

            started.await();
            blocked.awaitListenerAdded(); // the task is now parked in block()

            scheduler.pause();
            blocked.set(null);

            // the counters are bumped as the resume path is chosen, before the task parks, so
            // this waits for the decision itself rather than for the task to run
            while (scheduler.getBypassedResumeCount() + scheduler.getScheduledResumeCount() == 0) {
                Thread.onSpinWait();
            }

            assertThat(scheduler.getBypassedResumeCount())
                    .describedAs("Bypassed resumes while paused")
                    .isEqualTo(0);

            scheduler.resume();
            task.get();
        }
    }

    @Test
    @Timeout(30)
    public void testBypassDoesNotStarveRunnableTasks()
            throws InterruptedException, ExecutionException
    {
        // A task that blocks and unblocks in a tight loop must not monopolize the scheduler
        // while another task is waiting to be scheduled
        try (FairScheduler scheduler = FairScheduler.newInstance(1)) {
            Group group = scheduler.createGroup("G");

            CountDownLatch spinnerStarted = new CountDownLatch(1);
            AtomicBoolean otherRan = new AtomicBoolean();

            ListenableFuture<Void> spinner = scheduler.submit(group, 1, context -> {
                spinnerStarted.countDown();
                while (!otherRan.get()) {
                    if (!context.block(immediateVoidFuture())) {
                        return;
                    }
                }
            });

            spinnerStarted.await();

            ListenableFuture<Void> other = scheduler.submit(group, 2, _ -> otherRan.set(true));

            other.get();
            spinner.get();

            assertThat(otherRan.get()).isTrue();
        }
    }

    @Test
    @Timeout(60)
    public void testShardedScheduling()
            throws InterruptedException, ExecutionException
    {
        int groups = 16;
        int tasksPerGroup = 4;
        int blocksPerTask = 10;

        try (FairScheduler scheduler = FairScheduler.newInstance(4, 4, Ticker.systemTicker())) {
            assertThat(scheduler.getShardCount()).isEqualTo(4);

            AtomicInteger completed = new AtomicInteger();
            List<ListenableFuture<Void>> tasks = new ArrayList<>();

            for (int i = 0; i < groups; i++) {
                Group group = scheduler.createGroup("G" + i);
                for (int j = 0; j < tasksPerGroup; j++) {
                    tasks.add(scheduler.submit(group, j, context -> {
                        for (int k = 0; k < blocksPerTask; k++) {
                            if (!context.block(immediateVoidFuture())) {
                                return;
                            }
                        }
                        completed.incrementAndGet();
                    }));
                }
            }

            Futures.allAsList(tasks).get();

            assertThat(completed.get())
                    .describedAs("Completed tasks")
                    .isEqualTo(groups * tasksPerGroup);
        }
    }

    @Test
    public void testCleanupAfterFinish()
            throws InterruptedException, ExecutionException
    {
        TestingTicker ticker = new TestingTicker();
        try (FairScheduler scheduler = FairScheduler.newInstance(1, ticker)) {
            Group group = scheduler.createGroup("G");

            AtomicInteger counter = new AtomicInteger();
            ListenableFuture<Void> task1 = scheduler.submit(group, 1, _ -> {
                counter.incrementAndGet();
            });

            task1.get();
            assertThat(counter.get()).isEqualTo(1);
            assertThat(scheduler.getTasks(group)).isEmpty();
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
}
