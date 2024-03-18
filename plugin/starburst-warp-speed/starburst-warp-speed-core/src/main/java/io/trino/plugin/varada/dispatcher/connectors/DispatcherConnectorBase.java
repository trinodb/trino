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

import com.google.common.collect.ImmutableList;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.dispatcher.DispatcherIndexProvider;
import io.trino.plugin.varada.dispatcher.DispatcherPageSinkProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorIndexProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public abstract class DispatcherConnectorBase
        implements Connector
{
    private static final Logger logger = Logger.get(DispatcherConnectorBase.class);

    protected final Connector proxiedConnector;
    private final ImmutableList<PropertyMetadata<?>> sessionProperties;
    private final LifeCycleManager lifeCycleManager;
    private final ConnectorTaskExecutor connectorTaskExecutor;

    public DispatcherConnectorBase(
            @ForWarp Connector proxiedConnector,
            VaradaSessionProperties varadaSessionProperties,
            LifeCycleManager lifeCycleManager,
            ConnectorTaskExecutor connectorTaskExecutor)
    {
        this.proxiedConnector = requireNonNull(proxiedConnector);
        this.sessionProperties = ImmutableList.copyOf(requireNonNull(varadaSessionProperties.getSessionProperties()));
        this.lifeCycleManager = requireNonNull(lifeCycleManager);
        this.connectorTaskExecutor = requireNonNull(connectorTaskExecutor);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        // @TODO ignore isolation level, readOnly for now
        return proxiedConnector.beginTransaction(isolationLevel, readOnly, autoCommit);
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return proxiedConnector.getTableProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return proxiedConnector.getMaterializedViewProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return proxiedConnector.getSchemaProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        List<PropertyMetadata<?>> ret = new ArrayList<>(sessionProperties);

        ret.addAll(proxiedConnector.getSessionProperties());
        return ret;
    }

    @Override
    public List<PropertyMetadata<?>> getAnalyzeProperties()
    {
        return proxiedConnector.getAnalyzeProperties();
    }

    @Override
    public void shutdown()
    {
        proxiedConnector.shutdown();
        lifeCycleManager.stop();
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return new DispatcherPageSinkProvider(proxiedConnector.getPageSinkProvider());
    }

    @Override
    public ConnectorIndexProvider getIndexProvider()
    {
        return new DispatcherIndexProvider(proxiedConnector.getIndexProvider());
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return proxiedConnector.getRecordSetProvider();
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return proxiedConnector.getSystemTables();
    }

    @Override
    public Optional<FunctionProvider> getFunctionProvider()
    {
        return proxiedConnector.getFunctionProvider();
    }

    @Override
    public Set<TableProcedureMetadata> getTableProcedures()
    {
        return proxiedConnector.getTableProcedures();
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return proxiedConnector.getTableFunctions();
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return proxiedConnector.getColumnProperties();
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return proxiedConnector.getAccessControl();
    }

    @Override
    public Iterable<EventListener> getEventListeners()
    {
        return proxiedConnector.getEventListeners();
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return proxiedConnector.isSingleStatementWritesOnly();
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return proxiedConnector.getCapabilities();
    }

    /**
     * this method is used by SEP plugin, do not remove
     */
    public Object executeConnectorTask(String taskName, String taskJsonData, String httpMethod)
    {
        try {
            return connectorTaskExecutor.executeTask(taskName, taskJsonData, httpMethod);
        }
        catch (TrinoException pe) {
            logger.warn(pe, "failed to execute connector task %s", taskJsonData);
            throw pe;
        }
        catch (Exception e) {
            logger.warn(e, "failed to execute connector task %s", taskJsonData);
            throw new RuntimeException(String.format("failed to execute connector task %s", taskJsonData));
        }
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return Set.copyOf(proxiedConnector.getProcedures());
    }
}
