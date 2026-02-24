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

import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.DynamicSizeBoundQueue;
import io.airlift.concurrent.ExecutorServiceAdapter;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.MoreFutures.allAsListWithCancellationOnFailure;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static java.util.Objects.requireNonNull;

public class ParallelUnorderedIterator<T>
        extends AbstractIterator<T>
        implements AutoCloseable
{
    private static final int DEFAULT_QUEUE_CAPACITY = 1_024;

    private sealed interface QueueElement
            permits Complete, Failure, Entry {}

    private record Complete()
            implements QueueElement {}

    private record Failure(Throwable throwable)
            implements QueueElement
    {
        private Failure
        {
            requireNonNull(throwable, "throwable is null");
        }
    }

    private record Entry<T>(T value)
            implements QueueElement
    {
        private Entry
        {
            requireNonNull(value, "value is null");
        }
    }

    private static final Complete COMPLETE = new Complete();

    private final SettableFuture<Void> allStreamsCompletionFuture = SettableFuture.create();

    private final Executor executor;
    private final DynamicSizeBoundQueue<QueueElement> queue;
    private volatile boolean cancellationRequested;

    public static <T> Stream<T> stream(
            List<Supplier<Stream<T>>> streamSuppliers,
            Executor executor)
    {
        return stream(streamSuppliers, executor, DEFAULT_QUEUE_CAPACITY);
    }

    public static <T> Stream<T> stream(
            List<Supplier<Stream<T>>> streamSuppliers,
            Executor executor,
            int queueCapacity)
    {
        ParallelUnorderedIterator<T> iterator = new ParallelUnorderedIterator<>(executor, queueCapacity);
        iterator.start(streamSuppliers);
        return com.google.common.collect.Streams.stream(iterator).onClose(iterator::close);
    }

    private ParallelUnorderedIterator(
            Executor executor,
            int queueCapacity)
    {
        this.executor = requireNonNull(executor, "executor is null");
        queue = new DynamicSizeBoundQueue<>(queueCapacity, _ -> 1);
    }

    private void start(List<Supplier<Stream<T>>> streamSuppliers)
    {
        ListeningExecutorService listeningExecutorService = listeningDecorator(ExecutorServiceAdapter.from(executor));

        List<ListenableFuture<Void>> streamFutures = streamSuppliers
                .stream()
                .map(streamSupplier -> asVoid(listeningExecutorService.submit(() -> runProducer(streamSupplier))))
                .collect(toImmutableList());

        ListenableFuture<Void> allStreamsFuture = asVoid(allAsListWithCancellationOnFailure(streamFutures));
        allStreamsCompletionFuture.setFuture(allStreamsFuture);

        Futures.addCallback(
                allStreamsFuture,
                new FutureCallback<>()
                {
                    @Override
                    public void onSuccess(Void result)
                    {
                        queue.forcePut(COMPLETE);
                    }

                    @Override
                    public void onFailure(Throwable throwable)
                    {
                        queue.forcePut(new Failure(throwable));
                    }
                },
                directExecutor());
    }

    @Override
    protected T computeNext()
    {
        try {
            QueueElement element = queue.take();

            return switch (element) {
                case Entry<?> entry -> {
                    @SuppressWarnings("unchecked")
                    T value = (T) entry.value();
                    yield value;
                }
                case Failure(Throwable throwable) -> throw new RuntimeException("Error in nested iterator within ParallelUnorderedIterator", throwable);
                case Complete _ -> endOfData();
            };
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        cancellationRequested = true;
        queue.forcePut(COMPLETE);
        allStreamsCompletionFuture.cancel(true);
    }

    private void runProducer(Supplier<Stream<T>> supplier)
    {
        try (Stream<T> stream = requireNonNull(supplier.get(), "supplier returned null stream")) {
            Iterator<T> iterator = stream.iterator();

            while (iterator.hasNext()) {
                queue.put(new Entry<>(requireNonNull(iterator.next(), "entry is null")));
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (cancellationRequested) {
                return;
            }
            throw new RuntimeException("Interrupted while producing elements", e);
        }
    }
}
