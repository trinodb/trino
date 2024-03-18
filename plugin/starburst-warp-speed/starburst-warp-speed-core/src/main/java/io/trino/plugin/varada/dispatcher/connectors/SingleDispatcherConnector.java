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
package io.trino.plugin.varada.dispatcher.connectors;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.varada.CoordinatorNodeManager;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.dispatcher.DispatcherAlternativeChooser;
import io.trino.plugin.varada.dispatcher.DispatcherNodePartitioningProvider;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;

import static java.util.Objects.requireNonNull;

@Singleton
public class SingleDispatcherConnector
        extends DispatcherConnectorBase
{
    private final CoordinatorDispatcherConnector coordinatorDispatcherConnector;
    private final WorkerDispatcherConnector workerDispatcherConnector;
    private final CoordinatorNodeManager coordinatorNodeManager;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;

    @Inject
    public SingleDispatcherConnector(@ForWarp Connector proxiedConnector,
            VaradaSessionProperties varadaSessionProperties,
            LifeCycleManager lifeCycleManager,
            ConnectorTaskExecutor connectorTaskExecutor,
            CoordinatorDispatcherConnector coordinatorDispatcherConnector,
            WorkerDispatcherConnector workerDispatcherConnector,
            CoordinatorNodeManager coordinatorNodeManager,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        super(proxiedConnector, varadaSessionProperties, lifeCycleManager, connectorTaskExecutor);
        this.coordinatorDispatcherConnector = coordinatorDispatcherConnector;
        this.workerDispatcherConnector = workerDispatcherConnector;
        this.coordinatorNodeManager = requireNonNull(coordinatorNodeManager);
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
    }

    /**
     * Guaranteed to be called at most once per transaction. The returned metadata will only be accessed
     * in a single threaded context.
     */
    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return coordinatorDispatcherConnector.getMetadata(session, transactionHandle);
    }

    @Override
    public DispatcherAlternativeChooser getAlternativeChooser()
    {
        return workerDispatcherConnector.getAlternativeChooser();
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        coordinatorDispatcherConnector.commit(transactionHandle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        coordinatorDispatcherConnector.rollback(transactionHandle);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return coordinatorDispatcherConnector.getSplitManager();
    }

    @Override
    public ConnectorCacheMetadata getCacheMetadata()
    {
        return coordinatorDispatcherConnector.getCacheMetadata();
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return new DispatcherNodePartitioningProvider(proxiedConnector.getNodePartitioningProvider(), coordinatorNodeManager, dispatcherProxiedConnectorTransformer);
    }
}
