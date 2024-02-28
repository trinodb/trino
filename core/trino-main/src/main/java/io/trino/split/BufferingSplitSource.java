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
package io.trino.split;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.opentelemetry.context.Context;
import io.trino.metadata.Split;
import io.trino.spi.connector.CatalogHandle;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class BufferingSplitSource
        implements SplitSource
{
    private final int bufferSize;
    private final SplitSource source;
    private final Executor executor;

    public BufferingSplitSource(SplitSource source, Executor executor, int bufferSize)
    {
        this.source = requireNonNull(source, "source is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.bufferSize = bufferSize;
    }

    @Override
    public CatalogHandle getCatalogHandle()
    {
        return source.getCatalogHandle();
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(int maxSize)
    {
        checkArgument(maxSize > 0, "Cannot fetch a batch of zero size");
        return GetNextBatch.fetchNextBatchAsync(source, executor, Math.min(bufferSize, maxSize), maxSize);
    }

    @Override
    public void close()
    {
        source.close();
    }

    @Override
    public boolean isFinished()
    {
        return source.isFinished();
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        return source.getTableExecuteSplitsInfo();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bufferSize", bufferSize)
                .add("source", source)
                .toString();
    }

    private static class GetNextBatch
            extends AbstractFuture<SplitBatch>
    {
        private final Context context = Context.current();
        private final SplitSource splitSource;
        private final Executor executor;
        private final int min;
        private final int max;

        @GuardedBy("this")
        private final List<Split> splits = new ArrayList<>();
        @GuardedBy("this")
        private ListenableFuture<SplitBatch> nextBatchFuture;

        public static ListenableFuture<SplitBatch> fetchNextBatchAsync(
                SplitSource splitSource,
                Executor executor,
                int min,
                int max)
        {
            GetNextBatch getNextBatch = new GetNextBatch(splitSource, executor, min, max);
            getNextBatch.fetchSplits();
            return getNextBatch;
        }

        private GetNextBatch(SplitSource splitSource, Executor executor, int min, int max)
        {
            this.splitSource = requireNonNull(splitSource, "splitSource is null");
            this.executor = requireNonNull(executor, "executor is null");
            checkArgument(min <= max, "Min splits greater than max splits");
            this.min = min;
            this.max = max;
        }

        private synchronized void fetchSplits()
        {
            checkState(nextBatchFuture == null || nextBatchFuture.isDone(), "nextBatchFuture is expected to be done");

            try (var ignored = context.makeCurrent()) {
                nextBatchFuture = splitSource.getNextBatch(max - splits.size());
                // If the split source returns completed futures, we process them on
                // directExecutor without chaining to avoid the overhead of going through separate executor
                while (nextBatchFuture.isDone()) {
                    addCallback(
                            nextBatchFuture,
                            new FutureCallback<>()
                            {
                                @Override
                                public void onSuccess(SplitBatch splitBatch)
                                {
                                    processBatch(splitBatch);
                                }

                                @Override
                                public void onFailure(Throwable throwable)
                                {
                                    setException(throwable);
                                }
                            },
                            directExecutor());
                    if (isDone()) {
                        return;
                    }
                    nextBatchFuture = splitSource.getNextBatch(max - splits.size());
                }
            }

            addCallback(
                    nextBatchFuture,
                    new FutureCallback<>()
                    {
                        @Override
                        public void onSuccess(SplitBatch splitBatch)
                        {
                            synchronized (GetNextBatch.this) {
                                if (processBatch(splitBatch)) {
                                    return;
                                }
                                fetchSplits();
                            }
                        }

                        @Override
                        public void onFailure(Throwable throwable)
                        {
                            setException(throwable);
                        }
                    },
                    executor);
        }

        // Accumulates splits from the returned batch and returns whether
        // sufficient splits have been buffered to satisfy min batch size
        private synchronized boolean processBatch(SplitBatch splitBatch)
        {
            splits.addAll(splitBatch.getSplits());
            boolean isLastBatch = splitBatch.isLastBatch();
            if (splits.size() >= min || isLastBatch) {
                set(new SplitBatch(ImmutableList.copyOf(splits), isLastBatch));
                splits.clear();
                return true;
            }
            return false;
        }
    }
}
