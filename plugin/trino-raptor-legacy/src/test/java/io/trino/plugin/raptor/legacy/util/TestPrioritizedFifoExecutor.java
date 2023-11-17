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
package io.trino.plugin.raptor.legacy.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
public class TestPrioritizedFifoExecutor
{
    private static final Comparator<Runnable> DUMMY_COMPARATOR = (o1, o2) -> 0;

    private ExecutorService executor;

    @BeforeEach
    public void setUp()
    {
        executor = newCachedThreadPool();
    }

    @AfterEach
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testCounter()
            throws Exception
    {
        // enforce single thread
        PrioritizedFifoExecutor<Runnable> executor = new PrioritizedFifoExecutor<>(this.executor, 1, DUMMY_COMPARATOR);

        int totalTasks = 100_000;
        AtomicInteger counter = new AtomicInteger();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(totalTasks);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < totalTasks; i++) {
            int taskNumber = i;
            futures.add(executor.submit(() -> {
                try {
                    // wait for the go signal

                    assertThat(awaitUninterruptibly(startLatch, 1, TimeUnit.MINUTES)).isTrue();
                    assertThat(futures.get(taskNumber).isDone()).isFalse();

                    // intentional distinct read and write calls
                    int initialCount = counter.get();
                    counter.set(initialCount + 1);
                }
                finally {
                    completeLatch.countDown();
                }
            }));
        }

        for (Future<?> future : futures) {
            assertThat(future.isDone()).isFalse();
        }

        // signal go and wait for tasks to complete
        startLatch.countDown();
        assertThat(awaitUninterruptibly(completeLatch, 1, TimeUnit.MINUTES)).isTrue();

        assertThat(counter.get()).isEqualTo(totalTasks);
        // since this is a fifo executor with one thread and completeLatch is decremented inside the future,
        // the last future may not be done yet, but all the rest must be
        futures.get(futures.size() - 1).get(1, TimeUnit.MINUTES);
        for (Future<?> future : futures) {
            assertThat(future.isDone()).isTrue();
        }
    }

    @Test
    public void testSingleThreadBound()
    {
        testBound(1, 100_000);
    }

    @Test
    public void testDoubleThreadBound()
    {
        testBound(2, 100_000);
    }

    private void testBound(int maxThreads, int totalTasks)
    {
        PrioritizedFifoExecutor<Runnable> boundedExecutor = new PrioritizedFifoExecutor<>(executor, maxThreads, DUMMY_COMPARATOR);

        AtomicInteger activeThreadCount = new AtomicInteger();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(totalTasks);
        AtomicBoolean failed = new AtomicBoolean();

        for (int i = 0; i < totalTasks; i++) {
            boundedExecutor.submit(() -> {
                try {
                    // wait for the go signal
                    awaitUninterruptibly(startLatch);

                    int count = activeThreadCount.incrementAndGet();
                    if (count < 1 || count > maxThreads) {
                        failed.set(true);
                    }
                    activeThreadCount.decrementAndGet();
                }
                finally {
                    completeLatch.countDown();
                }
            });
        }

        // signal go and wait for tasks to complete
        startLatch.countDown();
        assertThat(awaitUninterruptibly(completeLatch, 1, TimeUnit.MINUTES)).isTrue();

        assertThat(failed.get()).isFalse();
    }
}
