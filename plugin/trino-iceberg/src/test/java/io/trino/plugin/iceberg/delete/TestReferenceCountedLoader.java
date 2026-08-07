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
package io.trino.plugin.iceberg.delete;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestReferenceCountedLoader
{
    // in seconds
    private static final long TIMEOUT = 5;

    private ExecutorService executor;

    @BeforeEach
    void setUp()
    {
        executor = Executors.newFixedThreadPool(4);
    }

    @AfterEach
    void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    void testConcurrentLoadsOfTheSameKeyRunOnce()
            throws Exception
    {
        ReferenceCountedLoader loader = new ReferenceCountedLoader(executor);
        BlockingLoad load = new BlockingLoad();

        ListenableFuture<?> first = loader.load("key", load);
        load.awaitStarted();
        ListenableFuture<?> second = loader.load("key", load);

        load.release();
        first.get(TIMEOUT, SECONDS);
        second.get(TIMEOUT, SECONDS);
        assertThat(load.runs()).isEqualTo(1);
    }

    @Test
    void testCancellingOneReferenceKeepsTheLoadRunning()
            throws Exception
    {
        ReferenceCountedLoader loader = new ReferenceCountedLoader(executor);
        BlockingLoad load = new BlockingLoad();

        ListenableFuture<?> cancelled = loader.load("key", load);
        load.awaitStarted();
        ListenableFuture<?> survivor = loader.load("key", load);

        cancelled.cancel(true);
        load.release();

        survivor.get(TIMEOUT, SECONDS);
        assertThat(load.wasInterrupted()).isFalse();
    }

    /**
     * A load that has started is never interrupted, so it cannot leave partial state behind.
     */
    @Test
    void testCancellingLastReferenceLetsARunningLoadFinish()
            throws Exception
    {
        ReferenceCountedLoader loader = new ReferenceCountedLoader(executor);
        BlockingLoad load = new BlockingLoad();

        ListenableFuture<?> firstFuture = loader.load("key", load);
        ListenableFuture<?> secondFuture = loader.load("key", load);
        load.awaitStarted();

        // dropping one of the two references leaves the load running, and does not cancel the other reference
        firstFuture.cancel(true);
        assertThat(secondFuture.isCancelled()).isFalse();
        assertThat(load.wasInterrupted()).isFalse();

        // dropping the last reference does not interrupt a load that has already started
        secondFuture.cancel(true);
        load.release();
        load.awaitFinished();
        assertThat(load.wasInterrupted()).isFalse();

        // the load was never abandoned, so a later caller shares the completed load
        loader.load("key", load).get(TIMEOUT, SECONDS);
        assertThat(load.runs()).isEqualTo(1);
    }

    /**
     * A load dropped before it started leaves no partial state behind.
     */
    @Test
    void testAbandonedQueuedLoadIsRunAgainForALaterCaller()
            throws Exception
    {
        BlockingLoad pinnedLoad = new BlockingLoad();
        AtomicInteger runs = new AtomicInteger();

        // A single thread keeps the second load queued behind the pinned one
        try (ExecutorService singleThread = Executors.newSingleThreadExecutor()) {
            ReferenceCountedLoader loader = new ReferenceCountedLoader(singleThread);

            ListenableFuture<?> pinned = loader.load("pinned", pinnedLoad);
            pinnedLoad.awaitStarted();

            ListenableFuture<?> queued = loader.load("queued", runs::incrementAndGet);
            queued.cancel(true);
            pinnedLoad.release();
            pinned.get(TIMEOUT, SECONDS);

            // the abandoned load was dropped, so this caller starts a fresh one rather than waiting on a canceled load
            loader.load("queued", runs::incrementAndGet).get(TIMEOUT, SECONDS);
            assertThat(runs.get()).isEqualTo(1);
        }
    }

    @Test
    void testCompletedLoadIsNotRunAgain()
            throws Exception
    {
        ReferenceCountedLoader loader = new ReferenceCountedLoader(executor);
        AtomicInteger runs = new AtomicInteger();

        loader.load("key", runs::incrementAndGet).get(TIMEOUT, SECONDS);
        loader.load("key", runs::incrementAndGet).get(TIMEOUT, SECONDS);

        assertThat(runs.get()).isEqualTo(1);
    }

    /**
     * A failed load is shared like any other, so every caller sees the failure rather than re-reading a file
     * whose read is already failing. The cache lives on the query's DeleteManager, so retries happen at task
     * level, above this loader.
     */
    @Test
    void testFailedLoadIsNotRunAgain()
    {
        ReferenceCountedLoader loader = new ReferenceCountedLoader(executor);
        AtomicInteger runs = new AtomicInteger();

        Runnable failing = () -> {
            runs.incrementAndGet();
            throw new RuntimeException("load failed");
        };

        assertThatThrownBy(() -> loader.load("key", failing).get(TIMEOUT, SECONDS))
                .hasMessageContaining("load failed");
        assertThatThrownBy(() -> loader.load("key", failing).get(TIMEOUT, SECONDS))
                .hasMessageContaining("load failed");
        assertThat(runs.get()).isEqualTo(1);
    }

    /**
     * A load that has not started yet must never run once it is canceled, so that a caller giving up does not
     * leave a queue of loads for the executor to work through.
     */
    @Test
    void testQueuedLoadIsNotRunWhenCancelled()
            throws Exception
    {
        BlockingLoad pinnedLoad = new BlockingLoad();
        AtomicBoolean queuedRan = new AtomicBoolean();

        // A single thread makes the ordering deterministic
        try (ExecutorService singleThread = Executors.newSingleThreadExecutor()) {
            ReferenceCountedLoader loader = new ReferenceCountedLoader(singleThread);

            ListenableFuture<?> pinned = loader.load("pinned", pinnedLoad);
            pinnedLoad.awaitStarted();

            ListenableFuture<?> queued = loader.load("queued", () -> queuedRan.set(true));
            queued.cancel(true);
            pinnedLoad.release();
            pinned.get(TIMEOUT, SECONDS);

            // The pool runs in FIFO order, so once this barrier task has run the canceled load has been skipped.
            singleThread.submit(() -> {}).get(TIMEOUT, SECONDS);
            assertThat(queuedRan.get()).isFalse();
        }
    }

    /**
     * A load that could not be scheduled must not be left behind, otherwise a later caller waits forever on a
     * load that will never run.
     */
    @Test
    void testRejectedLoadIsNotKept()
    {
        ExecutorService shutdownExecutor = Executors.newFixedThreadPool(1);
        shutdownExecutor.shutdown();
        ReferenceCountedLoader loader = new ReferenceCountedLoader(shutdownExecutor);

        assertThatThrownBy(() -> loader.load("key", () -> {}))
                .isInstanceOf(RejectedExecutionException.class);
        assertThatThrownBy(() -> loader.load("key", () -> {}))
                .isInstanceOf(RejectedExecutionException.class);
    }

    /**
     * A load that blocks until it is released. Exposes methods to control the behavior of the load.
     */
    private static final class BlockingLoad
            implements Runnable
    {
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);
        private final CountDownLatch finished = new CountDownLatch(1);
        private final CountDownLatch interrupted = new CountDownLatch(1);
        private final AtomicInteger runs = new AtomicInteger();

        @Override
        public void run()
        {
            runs.incrementAndGet();
            started.countDown();
            try {
                released.await();
                finished.countDown();
            }
            catch (InterruptedException e) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
            }
        }

        public void awaitStarted()
                throws InterruptedException
        {
            assertThat(started.await(TIMEOUT, SECONDS)).isTrue();
        }

        public void release()
        {
            released.countDown();
        }

        public void awaitFinished()
                throws InterruptedException
        {
            assertThat(finished.await(TIMEOUT, SECONDS)).isTrue();
        }

        public boolean wasInterrupted()
        {
            return interrupted.getCount() == 0;
        }

        public int runs()
        {
            return runs.get();
        }
    }
}
