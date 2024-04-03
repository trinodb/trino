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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.annotation.NotThreadSafe;
import io.trino.metadata.Split;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorSplitSource.ConnectorSplitBatch;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Objects.requireNonNull;

/**
 * Adapts {@link ConnectorSplitSource} to {@link SplitSource} interface.
 * <p>
 * Thread-safety: the implementations is not thread-safe
 *
 * Note: The implementation is internally not thread-safe but also {@link ConnectorSplitSource} is
 * not required to be thread-safe.
 */
@NotThreadSafe
public class ConnectorAwareSplitSource
        implements SplitSource
{
    private final CatalogHandle catalogHandle;
    private final String sourceToString;

    @Nullable
    private ConnectorSplitSource source;
    private boolean finished;
    private Optional<Optional<List<Object>>> tableExecuteSplitsInfo = Optional.empty();

    public ConnectorAwareSplitSource(CatalogHandle catalogHandle, ConnectorSplitSource source)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.source = requireNonNull(source, "source is null");
        this.sourceToString = source.toString();
    }

    @Override
    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(int maxSize)
    {
        checkState(source != null, "Already finished or closed");
        ListenableFuture<ConnectorSplitBatch> nextBatch = toListenableFuture(source.getNextBatch(maxSize));
        return Futures.transform(nextBatch, splitBatch -> {
            List<ConnectorSplit> connectorSplits = splitBatch.getSplits();
            ImmutableList.Builder<Split> result = ImmutableList.builderWithExpectedSize(connectorSplits.size());
            for (ConnectorSplit connectorSplit : connectorSplits) {
                result.add(new Split(catalogHandle, connectorSplit));
            }
            boolean noMoreSplits = splitBatch.isNoMoreSplits();
            if (noMoreSplits) {
                finished = true;
                tableExecuteSplitsInfo = Optional.of(source.getTableExecuteSplitsInfo());
                closeSource();
            }
            return new SplitBatch(result.build(), noMoreSplits);
        }, directExecutor());
    }

    @Override
    public void close()
    {
        closeSource();
    }

    private void closeSource()
    {
        if (source != null) {
            try {
                source.close();
            }
            finally {
                source = null;
            }
        }
    }

    @Override
    public boolean isFinished()
    {
        if (!finished) {
            checkState(source != null, "Already closed");
            if (source.isFinished()) {
                finished = true;
                tableExecuteSplitsInfo = Optional.of(source.getTableExecuteSplitsInfo());
                closeSource();
            }
        }
        return finished;
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        return tableExecuteSplitsInfo.orElseThrow(() -> new IllegalStateException("Not finished yet"));
    }

    @Override
    public String toString()
    {
        return catalogHandle + ":" + firstNonNull(source, sourceToString);
    }
}
