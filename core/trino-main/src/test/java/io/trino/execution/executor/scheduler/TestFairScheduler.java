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

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.testing.TestingTicker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
            ListenableFuture<Void> done = scheduler.submit(group, 1, context -> ran.set(true));

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

            ListenableFuture<Void> task2 = scheduler.submit(group, 2, context -> {
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

            ListenableFuture<Void> task2 = scheduler.submit(group, 2, context -> {
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

            ListenableFuture<Void> task1 = scheduler.submit(group, 1, context -> {
                try {
                    task1Started.countDown();
                    task1TimeAdvanced.await();

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

            scheduler.removeGroup(group);
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
