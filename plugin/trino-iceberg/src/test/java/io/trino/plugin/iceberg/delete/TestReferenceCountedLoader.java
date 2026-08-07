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

import java.io.Closeable;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.trino.testing.assertions.Assert.assertEventually;
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
    void testLoadOfSameKeyRunsOnce()
            throws Exception
    {
        ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();
        BlockingOpen open = new BlockingOpen();

        ListenableFuture<?> first = load(loader, "key", open);
        open.awaitStarted();
        ListenableFuture<?> second = load(loader, "key", open);

        open.release();
        first.get(TIMEOUT, SECONDS);
        second.get(TIMEOUT, SECONDS);
        assertThat(open.runs()).isEqualTo(1);
    }

    @Test
    void testConcurrentLoadsOfSameKeyRunOnce()
            throws Exception
    {
        ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();
        AtomicInteger opens = new AtomicInteger();
        AtomicInteger decodes = new AtomicInteger();

        // both callers ask for the key at the same time, racing to create the load
        CyclicBarrier barrier = new CyclicBarrier(2);
        Callable<Object> caller = () -> {
            barrier.await(TIMEOUT, SECONDS);
            return loader.load("key", executor, countingOpen(opens), directExecutor(), countingDecode(decodes)).get(TIMEOUT, SECONDS);
        };
        Future<?> first = executor.submit(caller);
        Future<?> second = executor.submit(caller);
        first.get(TIMEOUT, SECONDS);
        second.get(TIMEOUT, SECONDS);
        assertThat(opens.get()).isEqualTo(1);
        assertThat(decodes.get()).isEqualTo(1);
    }

    @Test
    void testOpenRunsOnOpenExecutorAndDecodeOnDecodeExecutor()
            throws Exception
    {
        ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();
        ThreadlessExecutor split = new ThreadlessExecutor();
        AtomicReference<Thread> openThread = new AtomicReference<>();
        AtomicReference<Thread> decodeThread = new AtomicReference<>();
        Opened opened = new Opened();

        ListenableFuture<?> future = loader.load(
                "key",
                executor,
                () -> {
                    openThread.set(Thread.currentThread());
                    return opened;
                },
                split,
                value -> {
                    assertThat(value).isSameAs(opened);
                    decodeThread.set(Thread.currentThread());
                });

        split.taskAvailable().get(TIMEOUT, SECONDS);
        assertThat(future.isDone()).isFalse();
        split.runPending();

        future.get(TIMEOUT, SECONDS);
        assertThat(openThread.get()).isNotSameAs(Thread.currentThread());
        assertThat(decodeThread.get()).isSameAs(Thread.currentThread());
        assertThat(opened.isClosed()).isTrue();
    }

    /**
     * Every caller can run the decode, so the first one to get to it decodes, and the others share the result.
     */
    @Test
    void testDecodeRunsOnceOnTheFirstCallerToRunIt()
            throws Exception
    {
        ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();
        ThreadlessExecutor firstSplit = new ThreadlessExecutor();
        ThreadlessExecutor secondSplit = new ThreadlessExecutor();
        AtomicInteger decodes = new AtomicInteger();
        BlockingOpen open = new BlockingOpen();

        ListenableFuture<?> first = loader.load("key", executor, open, firstSplit, countingDecode(decodes));
        ListenableFuture<?> second = loader.load("key", executor, open, secondSplit, countingDecode(decodes));
        open.release();

        // the second split runs first, and decodes for both
        secondSplit.taskAvailable().get(TIMEOUT, SECONDS);
        secondSplit.runPending();
        first.get(TIMEOUT, SECONDS);
        second.get(TIMEOUT, SECONDS);

        firstSplit.runPending();
        assertThat(decodes.get()).isEqualTo(1);
        assertThat(open.opened().isClosed()).isTrue();
    }

    /**
     * A caller whose split is closed never runs its decode, which must not leave the other callers waiting.
     */
    @Test
    void testClosedCallerDoesNotHoldBackOtherCallers()
            throws Exception
    {
        ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();
        ThreadlessExecutor closedSplit = new ThreadlessExecutor();
        ThreadlessExecutor liveSplit = new ThreadlessExecutor();
        AtomicInteger decodes = new AtomicInteger();
        BlockingOpen open = new BlockingOpen();

        ListenableFuture<?> closed = loader.load("key", executor, open, closedSplit, countingDecode(decodes));
        ListenableFuture<?> live = loader.load("key", executor, open, liveSplit, countingDecode(decodes));
        closedSplit.close();
        closed.cancel(true);
        open.release();

        liveSplit.taskAvailable().get(TIMEOUT, SECONDS);
        liveSplit.runPending();
        live.get(TIMEOUT, SECONDS);
        assertThat(decodes.get()).isEqualTo(1);
    }

    @Test
    void testCancellingOneReferenceKeepsTheLoadRunning()
            throws Exception
    {
        ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();
        BlockingOpen open = new BlockingOpen();

        ListenableFuture<?> cancelled = load(loader, "key", open);
        open.awaitStarted();
        ListenableFuture<?> survivor = load(loader, "key", open);

        cancelled.cancel(true);
        open.release();

        survivor.get(TIMEOUT, SECONDS);
        assertThat(open.wasInterrupted()).isFalse();
    }

    /**
     * Opening writes no shared state, so once every caller gave up a running open is interrupted, and the load is
     * dropped so that a later caller loads the key again.
     */
    @Test
    void testCancellingLastReferenceInterruptsARunningOpen()
            throws Exception
    {
        ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();
        BlockingOpen open = new BlockingOpen();
        AtomicInteger decodes = new AtomicInteger();

        ListenableFuture<?> firstFuture = loader.load("key", executor, open, directExecutor(), countingDecode(decodes));
        ListenableFuture<?> secondFuture = loader.load("key", executor, open, directExecutor(), countingDecode(decodes));
        open.awaitStarted();

        // dropping one of the two references leaves the open running, and does not cancel the other reference
        firstFuture.cancel(true);
        assertThat(secondFuture.isCancelled()).isFalse();
        assertThat(open.wasInterrupted()).isFalse();

        // dropping the last reference interrupts the running open
        secondFuture.cancel(true);
        open.awaitInterrupted();
        assertThat(decodes.get()).isZero();

        // the load was abandoned before it decoded, so a later caller loads the key again
        open.release();
        loader.load("key", executor, open, directExecutor(), countingDecode(decodes)).get(TIMEOUT, SECONDS);
        assertThat(open.runs()).isEqualTo(2);
        assertThat(decodes.get()).isEqualTo(1);
    }

    /**
     * An open that completes after every caller gave up, as the interrupt came too late, closes what it opened.
     */
    @Test
    void testOpenCompletingAfterAbandonClosesItsValue()
            throws Exception
    {
        ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Opened opened = new Opened();
        AtomicInteger decodes = new AtomicInteger();

        ListenableFuture<?> future = loader.load("key", executor, () -> {
            started.countDown();
            // ignores the interrupt, as an open blocked in IO that cannot be interrupted would
            awaitUninterruptibly(release);
            return opened;
        }, directExecutor(), countingDecode(decodes));
        assertThat(started.await(TIMEOUT, SECONDS)).isTrue();

        future.cancel(true);
        release.countDown();

        // the load was abandoned, so a later caller loads the key again once the stale open has closed its value
        AtomicInteger opens = new AtomicInteger();
        loader.load("key", executor, countingOpen(opens), directExecutor(), countingDecode(decodes)).get(TIMEOUT, SECONDS);
        assertThat(opens.get()).isEqualTo(1);
        assertEventually(() -> assertThat(opened.isClosed()).isTrue());
        assertThat(decodes.get()).isEqualTo(1);
    }

    /**
     * A load whose value was opened but not decoded when every caller gave up closes the value and is opened again later.
     */
    @Test
    void testAbandonedOpenedLoadClosesValueAndIsLoadedAgain()
            throws Exception
    {
        ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();
        ThreadlessExecutor split = new ThreadlessExecutor();
        Opened opened = new Opened();
        AtomicInteger decodes = new AtomicInteger();

        ListenableFuture<?> abandoned = loader.load("key", executor, () -> opened, split, countingDecode(decodes));
        // the open is done once the decode is queued
        split.taskAvailable().get(TIMEOUT, SECONDS);
        abandoned.cancel(true);
        assertThat(opened.isClosed()).isTrue();

        // the decode was canceled, so running it does nothing
        split.runPending();
        assertThat(decodes.get()).isZero();

        AtomicInteger opens = new AtomicInteger();
        loader.load("key", executor, countingOpen(opens), directExecutor(), countingDecode(decodes)).get(TIMEOUT, SECONDS);
        assertThat(opens.get()).isEqualTo(1);
        assertThat(decodes.get()).isEqualTo(1);
    }

    /**
     * A decode that has started always runs to completion, so it cannot leave partial state behind.
     */
    @Test
    void testCancellingDuringDecodeLetsTheDecodeFinish()
            throws Exception
    {
        ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();
        ThreadlessExecutor split = new ThreadlessExecutor();
        AtomicReference<ListenableFuture<?>> future = new AtomicReference<>();
        AtomicBoolean decoded = new AtomicBoolean();
        Opened opened = new Opened();

        future.set(loader.load("key", executor, () -> opened, split, _ -> {
            // the caller gives up in the middle of the decode
            future.get().cancel(true);
            decoded.set(true);
        }));
        split.taskAvailable().get(TIMEOUT, SECONDS);
        split.runPending();

        assertThat(decoded.get()).isTrue();
        assertThat(opened.isClosed()).isTrue();

        // the load completed, so a later caller shares it
        AtomicInteger opens = new AtomicInteger();
        loader.load("key", executor, countingOpen(opens), directExecutor(), _ -> {}).get(TIMEOUT, SECONDS);
        assertThat(opens.get()).isZero();
    }

    /**
     * A load dropped before its open started leaves no partial state behind.
     */
    @Test
    void testAbandonedQueuedLoadIsRunAgainForALaterCaller()
            throws Exception
    {
        BlockingOpen pinnedOpen = new BlockingOpen();
        AtomicInteger opens = new AtomicInteger();

        // A single thread keeps the second open queued behind the pinned one
        try (ExecutorService singleThread = Executors.newSingleThreadExecutor()) {
            ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();

            ListenableFuture<?> pinned = loader.load("pinned", singleThread, pinnedOpen, directExecutor(), _ -> {});
            pinnedOpen.awaitStarted();

            ListenableFuture<?> queued = loader.load("queued", singleThread, countingOpen(opens), directExecutor(), _ -> {});
            queued.cancel(true);
            pinnedOpen.release();
            pinned.get(TIMEOUT, SECONDS);

            // the abandoned load was dropped, so this caller starts a fresh one rather than waiting on a canceled load
            loader.load("queued", singleThread, countingOpen(opens), directExecutor(), _ -> {}).get(TIMEOUT, SECONDS);
            assertThat(opens.get()).isEqualTo(1);
        }
    }

    @Test
    void testCompletedLoadIsNotRunAgain()
            throws Exception
    {
        ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();
        AtomicInteger opens = new AtomicInteger();
        AtomicInteger decodes = new AtomicInteger();

        loader.load("key", executor, countingOpen(opens), directExecutor(), countingDecode(decodes)).get(TIMEOUT, SECONDS);
        loader.load("key", executor, countingOpen(opens), directExecutor(), countingDecode(decodes)).get(TIMEOUT, SECONDS);

        assertThat(opens.get()).isEqualTo(1);
        assertThat(decodes.get()).isEqualTo(1);
    }

    /**
     * A failed load is shared like any other, so every caller sees the failure rather than re-reading a file
     * whose read is already failing. The cache lives on the query's DeleteManager, so retries happen at task
     * level, above this loader.
     */
    @Test
    void testFailedOpenIsNotRunAgain()
    {
        ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();
        AtomicInteger opens = new AtomicInteger();
        AtomicInteger decodes = new AtomicInteger();

        Callable<Opened> failing = () -> {
            opens.incrementAndGet();
            throw new RuntimeException("open failed");
        };
        Consumer<Opened> decode = _ -> {
            decodes.incrementAndGet();
        };

        assertThatThrownBy(() -> loader.load("key", executor, failing, directExecutor(), decode).get(TIMEOUT, SECONDS))
                .hasMessageContaining("open failed");
        assertThatThrownBy(() -> loader.load("key", executor, failing, directExecutor(), decode).get(TIMEOUT, SECONDS))
                .hasMessageContaining("open failed");
        assertThat(opens.get()).isEqualTo(1);
        assertThat(decodes.get()).isZero();
    }

    @Test
    void testFailedDecodeIsNotRunAgainAndClosesValue()
    {
        ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();
        AtomicInteger decodes = new AtomicInteger();
        Opened opened = new Opened();

        Consumer<Opened> failing = _ -> {
            decodes.incrementAndGet();
            throw new RuntimeException("decode failed");
        };

        assertThatThrownBy(() -> loader.load("key", executor, () -> opened, directExecutor(), failing).get(TIMEOUT, SECONDS))
                .hasMessageContaining("decode failed");
        assertThatThrownBy(() -> loader.load("key", executor, () -> opened, directExecutor(), failing).get(TIMEOUT, SECONDS))
                .hasMessageContaining("decode failed");
        assertThat(decodes.get()).isEqualTo(1);
        assertThat(opened.isClosed()).isTrue();
    }

    /**
     * A load that has not started yet must never run once it is canceled, so that a caller giving up does not
     * leave a queue of loads for the executor to work through.
     */
    @Test
    void testQueuedOpenIsNotRunWhenCancelled()
            throws Exception
    {
        BlockingOpen pinnedOpen = new BlockingOpen();
        AtomicBoolean queuedRan = new AtomicBoolean();

        // A single thread makes the ordering deterministic
        try (ExecutorService singleThread = Executors.newSingleThreadExecutor()) {
            ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();

            ListenableFuture<?> pinned = loader.load("pinned", singleThread, pinnedOpen, directExecutor(), _ -> {});
            pinnedOpen.awaitStarted();

            ListenableFuture<?> queued = loader.load("queued", singleThread, () -> {
                queuedRan.set(true);
                return new Opened();
            }, directExecutor(), _ -> {});
            queued.cancel(true);
            pinnedOpen.release();
            pinned.get(TIMEOUT, SECONDS);

            // The pool runs in FIFO order, so once this barrier task has run the canceled open has been skipped.
            singleThread.submit(() -> {}).get(TIMEOUT, SECONDS);
            assertThat(queuedRan.get()).isFalse();
        }
    }

    /**
     * A load that could not be scheduled must be abandoned and canceled
     */
    @Test
    void testRejectedLoadIsNotKept()
    {
        ExecutorService shutdownExecutor = Executors.newFixedThreadPool(1);
        shutdownExecutor.shutdown();
        ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();

        ListenableFuture<?> loadOne = loader.load("key", shutdownExecutor, Opened::new, directExecutor(), _ -> {});
        assertThatThrownBy(loadOne::get).isInstanceOf(CancellationException.class);
        ListenableFuture<?> loadTwo = loader.load("key", shutdownExecutor, Opened::new, directExecutor(), _ -> {});
        assertThatThrownBy(loadTwo::get).isInstanceOf(CancellationException.class);
    }

    /**
     * A load can be concurrently abandoned between the moment a caller publishes it and calls start on it.
     */
    @Test
    void testStartToleratesConcurrentAbandonment()
    {
        ReferenceCountedLoader<Opened> loader = new ReferenceCountedLoader<>();

        ReferenceCountedLoader<Opened>.Load load = loader.new Load("key", executor, Opened::new, _ -> {});
        load.tryAcquire(directExecutor()).orElseThrow().cancel(true);
        assertThat(load.start(directExecutor())).isEmpty();
    }

    private ListenableFuture<?> load(ReferenceCountedLoader<Opened> loader, String key, Callable<Opened> open)
    {
        return loader.load(key, executor, open, directExecutor(), _ -> {});
    }

    private static Consumer<Opened> countingDecode(AtomicInteger decodes)
    {
        return _ -> {
            decodes.incrementAndGet();
        };
    }

    private static Callable<Opened> countingOpen(AtomicInteger opens)
    {
        return () -> {
            opens.incrementAndGet();
            return new Opened();
        };
    }

    private static final class Opened
            implements Closeable
    {
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public void close()
        {
            closed.set(true);
        }

        public boolean isClosed()
        {
            return closed.get();
        }
    }

    /**
     * An open that blocks until it is released. Exposes methods to control the behavior of the open.
     */
    private static final class BlockingOpen
            implements Callable<Opened>
    {
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);
        private final CountDownLatch finished = new CountDownLatch(1);
        private final CountDownLatch interrupted = new CountDownLatch(1);
        private final AtomicInteger runs = new AtomicInteger();
        private final AtomicReference<Opened> opened = new AtomicReference<>();

        @Override
        public Opened call()
        {
            runs.incrementAndGet();
            started.countDown();
            Opened value = new Opened();
            opened.set(value);
            try {
                released.await();
                finished.countDown();
            }
            catch (InterruptedException e) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
            }
            return value;
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

        public void awaitInterrupted()
                throws InterruptedException
        {
            assertThat(interrupted.await(TIMEOUT, SECONDS)).isTrue();
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

        public Opened opened()
        {
            return opened.get();
        }
    }
}
