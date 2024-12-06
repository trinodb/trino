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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class HiveConnector
        implements Connector
{
    private final Injector injector;
    private final LifeCycleManager lifeCycleManager;
    private final ConnectorSplitManager splitManager;
    private final ConnectorCacheMetadata cacheMetadata;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final ConnectorPageSinkProvider pageSinkProvider;
    private final ConnectorNodePartitioningProvider nodePartitioningProvider;
    private final Set<Procedure> procedures;
    private final Set<TableProcedureMetadata> tableProcedures;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final List<PropertyMetadata<?>> schemaProperties;
    private final List<PropertyMetadata<?>> tableProperties;
    private final List<PropertyMetadata<?>> viewProperties;
    private final List<PropertyMetadata<?>> columnProperties;
    private final List<PropertyMetadata<?>> analyzeProperties;
    private final List<PropertyMetadata<?>> materializedViewProperties;

    private final Optional<ConnectorAccessControl> accessControl;
    private final ClassLoader classLoader;

    private final HiveTransactionManager transactionManager;
    private final Set<ConnectorTableFunction> connectorTableFunctions;
    private final FunctionProvider functionProvider;
    private final boolean singleStatementWritesOnly;

    public HiveConnector(
            Injector injector,
            LifeCycleManager lifeCycleManager,
            HiveTransactionManager transactionManager,
            ConnectorSplitManager splitManager,
            ConnectorCacheMetadata cacheMetadata,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorPageSinkProvider pageSinkProvider,
            ConnectorNodePartitioningProvider nodePartitioningProvider,
            Set<Procedure> procedures,
            Set<TableProcedureMetadata> tableProcedures,
            Set<SessionPropertiesProvider> sessionPropertiesProviders,
            List<PropertyMetadata<?>> schemaProperties,
            List<PropertyMetadata<?>> tableProperties,
            List<PropertyMetadata<?>> viewProperties,
            List<PropertyMetadata<?>> columnProperties,
            List<PropertyMetadata<?>> analyzeProperties,
            List<PropertyMetadata<?>> materializedViewProperties,
            Optional<ConnectorAccessControl> accessControl,
            Set<ConnectorTableFunction> connectorTableFunctions,
            FunctionProvider functionProvider,
            boolean singleStatementWritesOnly,
            ClassLoader classLoader)
    {
        this.injector = requireNonNull(injector, "injector is null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.cacheMetadata = requireNonNull(cacheMetadata, "cacheMetadata is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.nodePartitioningProvider = requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
        this.tableProcedures = ImmutableSet.copyOf(requireNonNull(tableProcedures, "tableProcedures is null"));
        this.sessionProperties = sessionPropertiesProviders.stream()
                .flatMap(sessionPropertiesProvider -> sessionPropertiesProvider.getSessionProperties().stream())
                .collect(toImmutableList());
        this.schemaProperties = ImmutableList.copyOf(requireNonNull(schemaProperties, "schemaProperties is null"));
        this.tableProperties = ImmutableList.copyOf(requireNonNull(tableProperties, "tableProperties is null"));
        this.viewProperties = ImmutableList.copyOf(requireNonNull(viewProperties, "viewProperties is null"));
        this.columnProperties = ImmutableList.copyOf(requireNonNull(columnProperties, "columnProperties is null"));
        this.analyzeProperties = ImmutableList.copyOf(requireNonNull(analyzeProperties, "analyzeProperties is null"));
        this.materializedViewProperties = requireNonNull(materializedViewProperties, "materializedViewProperties is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.connectorTableFunctions = ImmutableSet.copyOf(requireNonNull(connectorTableFunctions, "connectorTableFunctions is null"));
        this.functionProvider = requireNonNull(functionProvider, "functionProvider is null");
        this.singleStatementWritesOnly = singleStatementWritesOnly;
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
    {
        ConnectorMetadata metadata = transactionManager.get(transaction, session.getIdentity());
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return new ClassLoaderSafeConnectorMetadata(metadata, classLoader);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorCacheMetadata getCacheMetadata()
    {
        return cacheMetadata;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return nodePartitioningProvider;
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return schemaProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getAnalyzeProperties()
    {
        return analyzeProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getViewProperties()
    {
        return viewProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return this.columnProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return materializedViewProperties;
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return accessControl.orElseThrow(UnsupportedOperationException::new);
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return singleStatementWritesOnly;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        checkConnectorSupports(READ_UNCOMMITTED, isolationLevel);
        ConnectorTransactionHandle transaction = new HiveTransactionHandle(autoCommit);
        transactionManager.begin(transaction);
        return transaction;
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        transactionManager.commit(transaction);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction)
    {
        transactionManager.rollback(transaction);
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }

    @Override
    public Optional<FunctionProvider> getFunctionProvider()
    {
        return Optional.of(functionProvider);
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return connectorTableFunctions;
    }

    @Override
    public Set<TableProcedureMetadata> getTableProcedures()
    {
        return tableProcedures;
    }

    public Injector getInjector()
    {
        return injector;
    }
}
