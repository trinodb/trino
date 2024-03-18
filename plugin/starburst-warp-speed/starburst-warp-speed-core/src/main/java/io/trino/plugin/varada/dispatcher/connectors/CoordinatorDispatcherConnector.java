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
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorCacheMetadata;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.varada.CoordinatorNodeManager;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.dispatcher.DispatcherCacheMetadata;
import io.trino.plugin.varada.dispatcher.DispatcherMetadata;
import io.trino.plugin.varada.dispatcher.DispatcherMetadataFactory;
import io.trino.plugin.varada.dispatcher.DispatcherNodePartitioningProvider;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherSplitManager;
import io.trino.plugin.varada.dispatcher.DispatcherTransactionManager;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;

import static java.util.Objects.requireNonNull;

@Singleton
public class CoordinatorDispatcherConnector
        extends DispatcherConnectorBase
{
    private final DispatcherMetadataFactory dispatcherMetadataFactory;
    private final ClassLoaderSafeConnectorCacheMetadata dispatcherCacheMetadata;
    private final DispatcherSplitManager dispatcherSplitManager;
    private final DispatcherTransactionManager dispatcherTransactionManager;
    private final CoordinatorNodeManager coordinatorNodeManager;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;

    @Inject
    public CoordinatorDispatcherConnector(@ForWarp Connector proxiedConnector,
            DispatcherMetadataFactory dispatcherMetadataFactory,
            DispatcherCacheMetadata dispatcherCacheMetadata,
            DispatcherSplitManager dispatcherSplitManager,
            DispatcherTransactionManager dispatcherTransactionManager,
            VaradaSessionProperties varadaSessionProperties,
            LifeCycleManager lifeCycleManager,
            ConnectorTaskExecutor connectorTaskExecutor,
            CoordinatorNodeManager coordinatorNodeManager,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        super(proxiedConnector, varadaSessionProperties, lifeCycleManager, connectorTaskExecutor);
        this.dispatcherMetadataFactory = requireNonNull(dispatcherMetadataFactory);
        this.dispatcherCacheMetadata = new ClassLoaderSafeConnectorCacheMetadata(requireNonNull(dispatcherCacheMetadata), getClass().getClassLoader());
        this.dispatcherSplitManager = requireNonNull(dispatcherSplitManager);
        this.dispatcherTransactionManager = requireNonNull(dispatcherTransactionManager);
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
        DispatcherMetadata dispatcherMetadata = dispatcherMetadataFactory.createMetadata(proxiedConnector.getMetadata(session, transactionHandle));

        dispatcherTransactionManager.put(transactionHandle, dispatcherMetadata);
        return new ClassLoaderSafeConnectorMetadata(dispatcherMetadata, this.getClass().getClassLoader());
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        try {
            proxiedConnector.commit(transactionHandle);
        }
        finally {
            dispatcherTransactionManager.remove(transactionHandle);
        }
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        try {
            proxiedConnector.rollback(transactionHandle);
        }
        finally {
            dispatcherTransactionManager.remove(transactionHandle);
        }
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return dispatcherSplitManager;
    }

    @Override
    public ConnectorCacheMetadata getCacheMetadata()
    {
        return dispatcherCacheMetadata;
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return new DispatcherNodePartitioningProvider(proxiedConnector.getNodePartitioningProvider(), coordinatorNodeManager, dispatcherProxiedConnectorTransformer);
    }
}
