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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.opentelemetry.context.Context;
import io.trino.metadata.Split;
import io.trino.spi.connector.CatalogHandle;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class BufferingSplitSource
        implements SplitSource
{
    private final int bufferSize;
    private final SplitSource source;

    public BufferingSplitSource(SplitSource source, int bufferSize)
    {
        this.source = requireNonNull(source, "source is null");
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
        return GetNextBatch.fetchNextBatchAsync(source, Math.min(bufferSize, maxSize), maxSize);
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

    private static class GetNextBatch
    {
        private final Context context = Context.current();
        private final SplitSource splitSource;
        private final int min;
        private final int max;

        private final List<Split> splits = new ArrayList<>();
        private boolean noMoreSplits;

        public static ListenableFuture<SplitBatch> fetchNextBatchAsync(
                SplitSource splitSource,
                int min,
                int max)
        {
            GetNextBatch getNextBatch = new GetNextBatch(splitSource, min, max);
            ListenableFuture<Void> future = getNextBatch.fetchSplits();
            return Futures.transform(future, ignored -> new SplitBatch(getNextBatch.splits, getNextBatch.noMoreSplits), directExecutor());
        }

        private GetNextBatch(SplitSource splitSource, int min, int max)
        {
            this.splitSource = requireNonNull(splitSource, "splitSource is null");
            checkArgument(min <= max, "Min splits greater than max splits");
            this.min = min;
            this.max = max;
        }

        private ListenableFuture<Void> fetchSplits()
        {
            if (splits.size() >= min) {
                return immediateVoidFuture();
            }
            ListenableFuture<SplitBatch> future;
            try (var ignored = context.makeCurrent()) {
                future = splitSource.getNextBatch(max - splits.size());
            }
            return Futures.transformAsync(future, splitBatch -> {
                splits.addAll(splitBatch.getSplits());
                if (splitBatch.isLastBatch()) {
                    noMoreSplits = true;
                    return immediateVoidFuture();
                }
                return fetchSplits();
            }, directExecutor());
        }
    }
}
