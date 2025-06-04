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
package io.trino.plugin.iceberg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.immutableEnumSet;
import static io.trino.spi.connector.ConnectorCapabilities.MATERIALIZED_VIEW_GRACE_PERIOD;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.spi.transaction.IsolationLevel.SERIALIZABLE;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class IcebergConnector
        implements Connector
{
    private final Injector injector;
    private final LifeCycleManager lifeCycleManager;
    private final IcebergTransactionManager transactionManager;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProviderFactory pageSourceProviderFactory;
    private final ConnectorPageSinkProvider pageSinkProvider;
    private final ConnectorNodePartitioningProvider nodePartitioningProvider;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final List<PropertyMetadata<?>> schemaProperties;
    private final List<PropertyMetadata<?>> tableProperties;
    private final List<PropertyMetadata<?>> materializedViewProperties;
    private final List<PropertyMetadata<?>> analyzeProperties;
    private final Optional<ConnectorAccessControl> accessControl;
    private final Set<Procedure> procedures;
    private final Set<TableProcedureMetadata> tableProcedures;
    private final Set<ConnectorTableFunction> tableFunctions;
    private final FunctionProvider functionProvider;
    private final Set<SystemTable> systemTables;

    @Inject
    public IcebergConnector(
            Injector injector,
            LifeCycleManager lifeCycleManager,
            IcebergTransactionManager transactionManager,
            ConnectorSplitManager splitManager,
            ConnectorPageSourceProviderFactory pageSourceProviderFactory,
            ConnectorPageSinkProvider pageSinkProvider,
            ConnectorNodePartitioningProvider nodePartitioningProvider,
            Set<SessionPropertiesProvider> sessionPropertiesProviders,
            IcebergSchemaProperties schemaProperties,
            IcebergTableProperties tableProperties,
            IcebergMaterializedViewProperties materializedViewProperties,
            IcebergAnalyzeProperties analyzeProperties,
            Optional<ConnectorAccessControl> accessControl,
            Set<Procedure> procedures,
            Set<TableProcedureMetadata> tableProcedures,
            Set<ConnectorTableFunction> tableFunctions,
            FunctionProvider functionProvider,
            Set<SystemTable> systemTables)
    {
        this.injector = requireNonNull(injector, "injector is null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProviderFactory = requireNonNull(pageSourceProviderFactory, "pageSourceProviderFactory is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.nodePartitioningProvider = requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
        this.sessionProperties = sessionPropertiesProviders.stream()
                .flatMap(sessionPropertiesProvider -> sessionPropertiesProvider.getSessionProperties().stream())
                .collect(toImmutableList());
        this.schemaProperties = ImmutableList.copyOf(requireNonNull(schemaProperties, "schemaProperties is null").getSchemaProperties());
        this.tableProperties = ImmutableList.copyOf(requireNonNull(tableProperties, "tableProperties is null").getTableProperties());
        this.materializedViewProperties = ImmutableList.copyOf(requireNonNull(materializedViewProperties, "materializedViewProperties is null").getMaterializedViewProperties());
        this.analyzeProperties = ImmutableList.copyOf(requireNonNull(analyzeProperties, "analyzeProperties is null").getAnalyzeProperties());
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
        this.tableProcedures = ImmutableSet.copyOf(requireNonNull(tableProcedures, "tableProcedures is null"));
        this.tableFunctions = ImmutableSet.copyOf(requireNonNull(tableFunctions, "tableFunctions is null"));
        this.functionProvider = requireNonNull(functionProvider, "functionProvider is null");
        this.systemTables = ImmutableSet.copyOf(systemTables);
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return immutableEnumSet(
                NOT_NULL_COLUMN_CONSTRAINT,
                MATERIALIZED_VIEW_GRACE_PERIOD);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
    {
        ConnectorMetadata metadata = transactionManager.get(transaction, session.getIdentity());
        return new ClassLoaderSafeConnectorMetadata(metadata, getClass().getClassLoader());
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProviderFactory getPageSourceProviderFactory()
    {
        return pageSourceProviderFactory;
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
    public Set<TableProcedureMetadata> getTableProcedures()
    {
        return tableProcedures;
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return tableFunctions;
    }

    @Override
    public Optional<FunctionProvider> getFunctionProvider()
    {
        return Optional.of(functionProvider);
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return systemTables;
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
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return materializedViewProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getAnalyzeProperties()
    {
        return analyzeProperties;
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return accessControl.orElseThrow(UnsupportedOperationException::new);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        checkConnectorSupports(SERIALIZABLE, isolationLevel);
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

    @VisibleForTesting
    public Injector getInjector()
    {
        return injector;
    }
}
