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
package io.trino.plugin.bigquery;

import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.StreamSupport.stream;

public class LazySplitSource
        implements ConnectorSplitSource
{
    private final ExecutorService executor;
    private final Supplier<Iterable<? extends ConnectorSplit>> splitsSupplier;
    private List<ConnectorSplit> splits;
    private int offset;

    public LazySplitSource(ExecutorService executor, Supplier<Iterable<? extends ConnectorSplit>> splitsSupplier)
    {
        this.executor = requireNonNull(executor, "executor is null");
        this.splitsSupplier = requireNonNull(splitsSupplier, "splitSupplier is null");
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        if (splits == null) {
            return CompletableFuture.supplyAsync(() -> {
                splits = stream(splitsSupplier.get().spliterator(), false).collect(toUnmodifiableList());
                return new ConnectorSplitBatch(prepareNextBatch(maxSize), isFinished());
            }, executor);
        }

        return completedFuture(new ConnectorSplitBatch(prepareNextBatch(maxSize), isFinished()));
    }

    private List<ConnectorSplit> prepareNextBatch(int maxSize)
    {
        requireNonNull(splits, "splits is null");
        int nextOffset = Math.min(splits.size(), offset + maxSize);
        List<ConnectorSplit> results = splits.subList(offset, nextOffset);
        offset = nextOffset;
        return results;
    }

    @Override
    public boolean isFinished()
    {
        return splits != null && offset >= splits.size();
    }

    @Override
    public void close()
    {
    }
}
