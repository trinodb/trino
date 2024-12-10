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
package io.trino.plugin.neo4j;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Set;

import static io.trino.plugin.neo4j.Neo4jTransactionHandle.INSTANCE;
import static java.util.Objects.requireNonNull;

public class Neo4jConnector
        implements Connector
{
    private static final Logger log = Logger.get(Neo4jConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final Neo4jMetadata metadata;
    private final Neo4jSplitManager splitManager;
    private final Set<ConnectorTableFunction> connectorTableFunctions;
    private final Neo4jRecordSetProvider recordSetProvider;

    @Inject
    public Neo4jConnector(
            LifeCycleManager lifeCycleManager,
            Neo4jMetadata metadata,
            Neo4jSplitManager splitManager,
            Set<ConnectorTableFunction> connectorTableFunctions,
            Neo4jRecordSetProvider recordSetProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.connectorTableFunctions = requireNonNull(connectorTableFunctions, "connectorTableFunctions is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return this.splitManager;
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return this.connectorTableFunctions;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return this.metadata;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return this.recordSetProvider;
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
