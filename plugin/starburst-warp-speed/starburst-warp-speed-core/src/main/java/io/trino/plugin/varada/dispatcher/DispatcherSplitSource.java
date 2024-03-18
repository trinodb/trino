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
package io.trino.plugin.varada.dispatcher;

import io.airlift.log.Logger;
import io.trino.plugin.varada.storage.splits.ConnectorSplitNodeDistributor;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class DispatcherSplitSource
        implements ConnectorSplitSource
{
    private static final Logger logger = Logger.get(DispatcherSplitSource.class);

    private final ConnectorSplitSource proxiedConnectorSplitSource;
    private final ConnectorSession session;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final ConnectorSplitNodeDistributor connectorSplitNodeDistributor;
    private final DispatcherTableHandle dispatcherTableHandle;

    public DispatcherSplitSource(
            ConnectorSplitSource proxiedConnectorSplitSource,
            DispatcherTableHandle dispatcherTableHandle,
            ConnectorSession session,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            ConnectorSplitNodeDistributor connectorSplitNodeDistributor)
    {
        this.proxiedConnectorSplitSource = requireNonNull(proxiedConnectorSplitSource);
        this.dispatcherTableHandle = requireNonNull(dispatcherTableHandle);
        this.session = requireNonNull(session);
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.connectorSplitNodeDistributor = requireNonNull(connectorSplitNodeDistributor);
        this.connectorSplitNodeDistributor.updateNodeBucketsIfNeeded();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        logger.debug("getNextBatch started isFinished [%b]", isFinished());
        CompletableFuture<ConnectorSplitBatch> nextBatch = proxiedConnectorSplitSource.getNextBatch(maxSize);
        return nextBatch.thenApply(connectorSplitBatch -> {
            List<ConnectorSplit> splits = connectorSplitBatch.getSplits()
                    .stream()
                    .map(connectorSplit -> dispatcherProxiedConnectorTransformer.createDispatcherSplit(
                            connectorSplit,
                            dispatcherTableHandle,
                            connectorSplitNodeDistributor,
                            session))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            logger.debug("getNextBatch ended isFinished [%b] - num splits %d", isFinished(), splits.size());
            return new ConnectorSplitBatch(splits, connectorSplitBatch.isNoMoreSplits());
        });
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        return proxiedConnectorSplitSource.getTableExecuteSplitsInfo();
    }

    @Override
    public void close()
    {
        proxiedConnectorSplitSource.close();
    }

    @Override
    public boolean isFinished()
    {
        return proxiedConnectorSplitSource.isFinished();
    }
}
