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
import io.trino.metadata.Split;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorSplitSource.ConnectorSplitBatch;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Objects.requireNonNull;

public class ConnectorAwareSplitSource
        implements SplitSource
{
    private final CatalogHandle catalogHandle;
    private final ConnectorSplitSource source;

    public ConnectorAwareSplitSource(CatalogHandle catalogHandle, ConnectorSplitSource source)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.source = requireNonNull(source, "source is null");
    }

    @Override
    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(int maxSize)
    {
        ListenableFuture<ConnectorSplitBatch> nextBatch = toListenableFuture(source.getNextBatch(maxSize));
        return Futures.transform(nextBatch, splitBatch -> {
            ImmutableList.Builder<Split> result = ImmutableList.builder();
            for (ConnectorSplit connectorSplit : splitBatch.getSplits()) {
                result.add(new Split(catalogHandle, connectorSplit));
            }
            return new SplitBatch(result.build(), splitBatch.isNoMoreSplits());
        }, directExecutor());
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
        return catalogHandle + ":" + source;
    }
}
