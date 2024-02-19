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
package io.trino.plugin.base;

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

public abstract class ForwardingConnector
        implements Connector
{
    protected abstract Connector delegate();

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return delegate().beginTransaction(isolationLevel, readOnly, autoCommit);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return delegate().getMetadata(session, transactionHandle);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return delegate().getSplitManager();
    }

    @Override
    public ConnectorCacheMetadata getCacheMetadata()
    {
        return delegate().getCacheMetadata();
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return delegate().getPageSourceProvider();
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return delegate().getRecordSetProvider();
    }

    @Override
    public ConnectorAlternativeChooser getAlternativeChooser()
    {
        return delegate().getAlternativeChooser();
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return delegate().getPageSinkProvider();
    }

    @Override
    public ConnectorIndexProvider getIndexProvider()
    {
        return delegate().getIndexProvider();
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return delegate().getNodePartitioningProvider();
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return delegate().getSystemTables();
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return delegate().getProcedures();
    }

    @Override
    public Set<TableProcedureMetadata> getTableProcedures()
    {
        return delegate().getTableProcedures();
    }

    @Override
    public Optional<FunctionProvider> getFunctionProvider()
    {
        return delegate().getFunctionProvider();
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return delegate().getTableFunctions();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return delegate().getSessionProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return delegate().getSchemaProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getAnalyzeProperties()
    {
        return delegate().getAnalyzeProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return delegate().getTableProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return delegate().getMaterializedViewProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return delegate().getColumnProperties();
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return delegate().getAccessControl();
    }

    @Override
    public Iterable<EventListener> getEventListeners()
    {
        return delegate().getEventListeners();
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        delegate().commit(transactionHandle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        delegate().rollback(transactionHandle);
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return delegate().isSingleStatementWritesOnly();
    }

    @Override
    public void shutdown()
    {
        delegate().shutdown();
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return delegate().getCapabilities();
    }
}
