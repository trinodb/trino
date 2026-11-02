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

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStaggeredExitSynchronousQueue
{
    @Test
    public void testExitSlotsAreOneIntervalApart()
    {
        StaggeredExitSynchronousQueue queue = new StaggeredExitSynchronousQueue(10, MILLISECONDS);
        long now = System.nanoTime();
        assertThat(queue.reserveExitDelay(now)).isEqualTo(0);
        assertThat(queue.reserveExitDelay(now)).isEqualTo(MILLISECONDS.toNanos(10));
        assertThat(queue.reserveExitDelay(now)).isEqualTo(MILLISECONDS.toNanos(20));
        assertThat(queue.reserveExitDelay(now + MILLISECONDS.toNanos(5))).isEqualTo(MILLISECONDS.toNanos(25));
        assertThat(queue.reserveExitDelay(now + SECONDS.toNanos(1))).isEqualTo(0);
    }

    @Test
    public void testIdleThreadWaitingToExitRunsNewTask()
            throws Exception
    {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 1, MILLISECONDS, new StaggeredExitSynchronousQueue(1, MINUTES));
        Set<Thread> workers = ConcurrentHashMap.newKeySet();
        CountDownLatch release = new CountDownLatch(1);
        for (int i = 0; i < 3; i++) {
            executor.execute(() -> {
                workers.add(Thread.currentThread());
                awaitUninterruptibly(release);
            });
        }
        assertEventually(() -> assertThat(workers).hasSize(3));
        release.countDown();

        assertEventually(() -> assertThat(executor.getPoolSize()).isEqualTo(2));
        Thread reused = executor.submit(Thread::currentThread).get(10, SECONDS);
        assertThat(workers).contains(reused);
        assertThat(executor.getPoolSize()).isEqualTo(2);

        executor.shutdownNow();
        assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
    }
}
