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
package io.trino.plugin.storage;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class StorageConnector
        implements Connector
{
    private static final Logger log = Logger.get(StorageConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final StorageMetadata metadata;
    private final StorageSplitManager splitManager;
    private final StoragePageSourceProvider pageSourceProvider;
    private final Set<ConnectorTableFunction> connectorTableFunctions;

    @Inject
    public StorageConnector(
            LifeCycleManager lifeCycleManager,
            StorageMetadata metadata,
            StorageSplitManager splitManager,
            StoragePageSourceProvider pageSourceProvider,
            Set<ConnectorTableFunction> connectorTableFunctions)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.connectorTableFunctions = ImmutableSet.copyOf(requireNonNull(connectorTableFunctions, "connectorTableFunctions is null"));
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return StorageTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return connectorTableFunctions;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
