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
package io.trino.plugin.deltalake.transactionlog.checkpoint;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestParallelUnorderedIterator
{
    @Test
    public void testAcceptsEmptyListOfStreams()
    {
        try (ExecutorService executor = newFixedThreadPool(1, daemonThreadsNamed("test-parallel-iterator-empty-%s"))) {
            try (Stream<Integer> merged = ParallelUnorderedIterator.stream(ImmutableList.of(), executor)) {
                assertThat(merged.collect(toImmutableList())).isEmpty();
            }
        }
    }

    @Test
    public void testMergesArbitraryElementTypeFromStreams()
    {
        List<Supplier<Stream<Integer>>> streamSuppliers = ImmutableList.of(
                () -> ImmutableList.of(1, 2, 3).stream(),
                () -> ImmutableList.of(4, 5).stream(),
                () -> ImmutableList.of(6).stream());

        try (ExecutorService executor = newFixedThreadPool(3, daemonThreadsNamed("test-parallel-iterator-%s"))) {
            try (Stream<Integer> merged = ParallelUnorderedIterator.stream(streamSuppliers, executor)) {
                List<Integer> values = merged.collect(toImmutableList());
                assertThat(values).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6);
            }
        }
    }

    @Test
    public void testSubmitsOneTaskPerStreamToExecutor()
    {
        List<Supplier<Stream<Integer>>> streamSuppliers = ImmutableList.of(
                () -> ImmutableList.of(1, 2, 3).stream(),
                () -> ImmutableList.of(4, 5).stream(),
                () -> ImmutableList.of(6).stream());

        try (RecordingExecutor executor = new RecordingExecutor(3, "test-parallel-iterator-submissions-%s")) {
            try (Stream<Integer> merged = ParallelUnorderedIterator.stream(streamSuppliers, executor)) {
                assertThat(merged.collect(toImmutableList())).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6);
            }

            assertThat(executor.getSubmittedTaskCount()).isEqualTo(streamSuppliers.size());
        }
    }

    @Test
    public void testPropagatesProducerFailure()
    {
        List<Supplier<Stream<Integer>>> streamSuppliers = ImmutableList.of(
                () -> ImmutableList.of(1, 2).stream(),
                () -> {
                    throw new IllegalStateException("boom");
                });

        try (ExecutorService executor = newFixedThreadPool(2, daemonThreadsNamed("test-parallel-iterator-failure-%s"))) {
            try (Stream<Integer> merged = ParallelUnorderedIterator.stream(streamSuppliers, executor)) {
                assertThatThrownBy(() -> merged.collect(toImmutableList()))
                        .hasCauseInstanceOf(IllegalStateException.class)
                        .cause()
                        .hasMessageContaining("boom");
            }
        }
    }

    @Test
    public void testPropagatesProducerFailureDuringIteration()
    {
        List<Supplier<Stream<Integer>>> streamSuppliers = ImmutableList.of(() -> failingStreamAfterFirstElement());

        try (ExecutorService executor = newFixedThreadPool(1, daemonThreadsNamed("test-parallel-iterator-iteration-failure-%s"))) {
            try (Stream<Integer> merged = ParallelUnorderedIterator.stream(streamSuppliers, executor)) {
                Iterator<Integer> iterator = merged.iterator();

                assertThat(iterator.hasNext()).isTrue();
                assertThat(iterator.next()).isEqualTo(1);
                assertThatThrownBy(iterator::hasNext)
                        .hasCauseInstanceOf(IllegalStateException.class)
                        .cause()
                        .hasMessageContaining("boom during iteration");
            }
        }
    }

    @Test
    public void testRejectsNullElement()
    {
        List<Supplier<Stream<Integer>>> streamSuppliers = ImmutableList.of(() -> Stream.of(1, null));

        try (ExecutorService executor = newFixedThreadPool(1, daemonThreadsNamed("test-parallel-iterator-null-%s"))) {
            try (Stream<Integer> merged = ParallelUnorderedIterator.stream(streamSuppliers, executor)) {
                assertThatThrownBy(() -> merged.collect(toImmutableList()))
                        .hasCauseInstanceOf(NullPointerException.class)
                        .cause()
                        .hasMessageContaining("entry is null");
            }
        }
    }

    @Test
    public void testMergesMixedEmptyAndNonEmptyStreams()
    {
        List<Supplier<Stream<Integer>>> streamSuppliers = ImmutableList.of(
                Stream::<Integer>empty,
                () -> ImmutableList.of(1, 2).stream(),
                Stream::<Integer>empty,
                () -> ImmutableList.of(3).stream());

        try (ExecutorService executor = newFixedThreadPool(4, daemonThreadsNamed("test-parallel-iterator-empty-mixed-%s"))) {
            try (Stream<Integer> merged = ParallelUnorderedIterator.stream(streamSuppliers, executor)) {
                assertThat(merged.collect(toImmutableList()))
                        .containsExactlyInAnyOrder(1, 2, 3);
            }
        }
    }

    @Test
    public void testCloseUnblocksBlockedProducer()
            throws InterruptedException
    {
        CountDownLatch secondElementRead = new CountDownLatch(1);
        CountDownLatch streamClosed = new CountDownLatch(1);

        List<Supplier<Stream<Integer>>> streamSuppliers = ImmutableList.of(() -> interruptibleStream(secondElementRead, streamClosed));

        try (ExecutorService executor = newFixedThreadPool(1, daemonThreadsNamed("test-parallel-iterator-close-%s"))) {
            Stream<Integer> merged = ParallelUnorderedIterator.stream(streamSuppliers, executor, 1);
            try {
                assertThat(secondElementRead.await(5, SECONDS)).isTrue();

                merged.close();

                assertThat(streamClosed.await(5, SECONDS)).isTrue();
            }
            finally {
                merged.close();
            }
        }
    }

    @Test
    public void testPropagatesUnexpectedProducerInterrupt()
            throws InterruptedException
    {
        CountDownLatch secondElementRead = new CountDownLatch(1);
        AtomicReference<Thread> producerThread = new AtomicReference<>();
        ThreadFactory delegate = daemonThreadsNamed("test-parallel-iterator-interrupt-%s");

        List<Supplier<Stream<Integer>>> streamSuppliers = ImmutableList.of(() -> interruptibleStream(secondElementRead));

        try (ExecutorService executor = newFixedThreadPool(1, runnable -> {
            Thread thread = delegate.newThread(runnable);
            producerThread.set(thread);
            return thread;
        })) {
            try (Stream<Integer> merged = ParallelUnorderedIterator.stream(streamSuppliers, executor, 1)) {
                assertThat(secondElementRead.await(5, SECONDS)).isTrue();
                assertThat(producerThread.get()).isNotNull();
                producerThread.get().interrupt();

                assertThatThrownBy(() -> merged.collect(toImmutableList()))
                        .hasCauseInstanceOf(RuntimeException.class)
                        .cause()
                        .hasMessageContaining("Interrupted while producing elements");
            }
        }
    }

    private static Stream<Integer> interruptibleStream(CountDownLatch secondElementRead)
    {
        return interruptibleStream(secondElementRead, new CountDownLatch(0));
    }

    private static Stream<Integer> interruptibleStream(CountDownLatch secondElementRead, CountDownLatch streamClosed)
    {
        AtomicInteger index = new AtomicInteger();
        Iterator<Integer> iterator = new Iterator<>()
        {
            @Override
            public boolean hasNext()
            {
                return index.get() < 2;
            }

            @Override
            public Integer next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                int value = index.incrementAndGet();
                if (value == 2) {
                    secondElementRead.countDown();
                }
                return value;
            }
        };
        return stream(iterator)
                .onClose(streamClosed::countDown);
    }

    private static Stream<Integer> failingStreamAfterFirstElement()
    {
        AtomicInteger index = new AtomicInteger();
        Iterator<Integer> iterator = new Iterator<>()
        {
            @Override
            public boolean hasNext()
            {
                return index.get() < 2;
            }

            @Override
            public Integer next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                if (index.getAndIncrement() == 0) {
                    return 1;
                }
                throw new IllegalStateException("boom during iteration");
            }
        };
        return stream(iterator);
    }

    private static final class RecordingExecutor
            implements Executor, AutoCloseable
    {
        private final AtomicInteger submittedTaskCount = new AtomicInteger();
        private final ExecutorService delegate;

        private RecordingExecutor(int threads, String threadNameFormat)
        {
            delegate = newFixedThreadPool(threads, daemonThreadsNamed(threadNameFormat));
        }

        @Override
        public void execute(Runnable command)
        {
            submittedTaskCount.incrementAndGet();
            delegate.execute(command);
        }

        public int getSubmittedTaskCount()
        {
            return submittedTaskCount.get();
        }

        @Override
        public void close()
        {
            delegate.shutdownNow();
        }
    }
}
