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
package io.trino.plugin.warp;

import io.trino.plugin.varada.dispatcher.connectors.DispatcherConnectorBase;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorAlternativeChooser;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorIndexProvider;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class StarburstWarpConnector
        implements Connector
{
    private final DispatcherConnectorBase warpConnector;

    public StarburstWarpConnector(Connector warpConnector)
    {
        this.warpConnector = (DispatcherConnectorBase) requireNonNull(warpConnector);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return warpConnector.beginTransaction(isolationLevel, readOnly, autoCommit);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return warpConnector.getMetadata(session, transactionHandle);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return warpConnector.getSplitManager();
    }

    @Override
    public ConnectorCacheMetadata getCacheMetadata()
    {
        return warpConnector.getCacheMetadata();
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return warpConnector.getPageSourceProvider();
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return warpConnector.getRecordSetProvider();
    }

    @Override
    public ConnectorAlternativeChooser getAlternativeChooser()
    {
        return warpConnector.getAlternativeChooser();
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return warpConnector.getPageSinkProvider();
    }

    @Override
    public ConnectorIndexProvider getIndexProvider()
    {
        return warpConnector.getIndexProvider();
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return warpConnector.getNodePartitioningProvider();
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return warpConnector.getSystemTables();
    }

    @SuppressWarnings("TrinoExperimentalSpi") // FunctionProvider
    @Override
    public Optional<FunctionProvider> getFunctionProvider()
    {
        return warpConnector.getFunctionProvider();
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return warpConnector.getProcedures();
    }

    @Override
    public Set<TableProcedureMetadata> getTableProcedures()
    {
        return warpConnector.getTableProcedures();
    }

    @SuppressWarnings("TrinoExperimentalSpi") // ConnectorTableFunction
    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return warpConnector.getTableFunctions();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return warpConnector.getSessionProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return warpConnector.getSchemaProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getAnalyzeProperties()
    {
        return warpConnector.getAnalyzeProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return warpConnector.getTableProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return warpConnector.getMaterializedViewProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return warpConnector.getColumnProperties();
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return warpConnector.getAccessControl();
    }

    @Override
    public Iterable<EventListener> getEventListeners()
    {
        return warpConnector.getEventListeners();
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        warpConnector.commit(transactionHandle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        warpConnector.rollback(transactionHandle);
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return warpConnector.isSingleStatementWritesOnly();
    }

    @Override
    public void shutdown()
    {
        warpConnector.shutdown();
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return warpConnector.getCapabilities();
    }
}
