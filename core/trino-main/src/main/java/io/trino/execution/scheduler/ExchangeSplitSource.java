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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.metadata.Split;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource;
import io.trino.spi.metrics.Metrics;
import io.trino.split.RemoteSplit;
import io.trino.split.SplitSource;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static java.util.Objects.requireNonNull;

public class ExchangeSplitSource
        implements SplitSource
{
    private final ExchangeSourceHandleSource handleSource;
    private final long targetSplitSizeInBytes;

    public ExchangeSplitSource(ExchangeSourceHandleSource handleSource, long targetSplitSizeInBytes)
    {
        this.handleSource = requireNonNull(handleSource, "handleSource is null");
        this.targetSplitSizeInBytes = targetSplitSizeInBytes;
    }

    @Override
    public CatalogHandle getCatalogHandle()
    {
        return REMOTE_CATALOG_HANDLE;
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(int maxSize)
    {
        ListenableFuture<ExchangeSourceHandleSource.ExchangeSourceHandleBatch> sourceHandlesFuture = toListenableFuture(handleSource.getNextBatch());
        return Futures.transform(
                sourceHandlesFuture,
                batch -> {
                    List<ExchangeSourceHandle> handles = batch.handles();
                    ListMultimap<Integer, ExchangeSourceHandle> partitionToHandles = handles.stream()
                            .collect(toImmutableListMultimap(ExchangeSourceHandle::getPartitionId, Function.identity()));
                    ImmutableList.Builder<Split> splits = ImmutableList.builder();
                    for (int partition : partitionToHandles.keySet()) {
                        splits.addAll(createRemoteSplits(partitionToHandles.get(partition)));
                    }
                    return new SplitBatch(splits.build(), batch.lastBatch());
                }, directExecutor());
    }

    private List<Split> createRemoteSplits(List<ExchangeSourceHandle> handles)
    {
        ImmutableList.Builder<Split> result = ImmutableList.builder();
        ImmutableList.Builder<ExchangeSourceHandle> currentSplitHandles = ImmutableList.builder();
        long currentSplitHandlesSize = 0;
        long currentSplitHandlesCount = 0;
        for (ExchangeSourceHandle handle : handles) {
            if (currentSplitHandlesCount > 0 && currentSplitHandlesSize + handle.getDataSizeInBytes() > targetSplitSizeInBytes) {
                result.add(createRemoteSplit(currentSplitHandles.build()));
                currentSplitHandles = ImmutableList.builder();
                currentSplitHandlesSize = 0;
                currentSplitHandlesCount = 0;
            }
            currentSplitHandles.add(handle);
            currentSplitHandlesSize += handle.getDataSizeInBytes();
            currentSplitHandlesCount++;
        }
        if (currentSplitHandlesCount > 0) {
            result.add(createRemoteSplit(currentSplitHandles.build()));
        }
        return result.build();
    }

    private static Split createRemoteSplit(List<ExchangeSourceHandle> handles)
    {
        return new Split(REMOTE_CATALOG_HANDLE, new RemoteSplit(new SpoolingExchangeInput(handles, Optional.empty())));
    }

    @Override
    public void close()
    {
        handleSource.close();
    }

    @Override
    public boolean isFinished()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        return Optional.empty();
    }

    @Override
    public Metrics getMetrics()
    {
        return Metrics.EMPTY;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("handleSource", handleSource)
                .add("targetSplitSizeInBytes", targetSplitSizeInBytes)
                .toString();
    }
}
