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
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class OpenSearchConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final OpenSearchMetadata metadata;
    private final OpenSearchSplitManager splitManager;
    private final OpenSearchPageSourceProvider pageSourceProvider;
    private final NodesSystemTable nodesSystemTable;
    private final Set<ConnectorTableFunction> connectorTableFunctions;
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public OpenSearchConnector(
            LifeCycleManager lifeCycleManager,
            OpenSearchMetadata metadata,
            OpenSearchSplitManager splitManager,
            OpenSearchPageSourceProvider pageSourceProvider,
            NodesSystemTable nodesSystemTable,
            Set<ConnectorTableFunction> connectorTableFunctions,
            Set<SessionPropertiesProvider> sessionPropertiesProviders)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.nodesSystemTable = requireNonNull(nodesSystemTable, "nodesSystemTable is null");
        this.connectorTableFunctions = ImmutableSet.copyOf(requireNonNull(connectorTableFunctions, "connectorTableFunctions is null"));
        this.sessionProperties = sessionPropertiesProviders.stream()
                .flatMap(sessionPropertiesProvider -> sessionPropertiesProvider.getSessionProperties().stream())
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return OpenSearchTransactionHandle.INSTANCE;
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
    public Set<SystemTable> getSystemTables()
    {
        return ImmutableSet.of(nodesSystemTable);
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return connectorTableFunctions;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }
}
