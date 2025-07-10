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
package io.trino.plugin.lakehouse;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.hive.HiveSchemaProperties;
import io.trino.plugin.iceberg.IcebergMaterializedViewProperties;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.immutableEnumSet;
import static io.trino.spi.connector.ConnectorCapabilities.MATERIALIZED_VIEW_GRACE_PERIOD;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class LakehouseConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final LakehouseTransactionManager transactionManager;
    private final LakehouseSplitManager splitManager;
    private final LakehousePageSourceProviderFactory pageSourceProviderFactory;
    private final LakehousePageSinkProvider pageSinkProvider;
    private final LakehouseNodePartitioningProvider nodePartitioningProvider;
    private final LakehouseSessionProperties sessionProperties;
    private final LakehouseTableProperties tableProperties;
    private final IcebergMaterializedViewProperties materializedViewProperties;

    @Inject
    public LakehouseConnector(
            LifeCycleManager lifeCycleManager,
            LakehouseTransactionManager transactionManager,
            LakehouseSplitManager splitManager,
            LakehousePageSourceProviderFactory pageSourceProviderFactory,
            LakehousePageSinkProvider pageSinkProvider,
            LakehouseNodePartitioningProvider nodePartitioningProvider,
            LakehouseSessionProperties sessionProperties,
            LakehouseTableProperties tableProperties,
            IcebergMaterializedViewProperties materializedViewProperties)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProviderFactory = requireNonNull(pageSourceProviderFactory, "pageSourceProviderFactory is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.nodePartitioningProvider = requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
        this.materializedViewProperties = requireNonNull(materializedViewProperties, "materializedViewProperties is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        checkConnectorSupports(READ_UNCOMMITTED, isolationLevel);
        return transactionManager.begin();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return transactionManager.get(transactionHandle, session.getIdentity());
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
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        transactionManager.commit(transactionHandle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        transactionManager.rollback(transactionHandle);
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties.getSessionProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return HiveSchemaProperties.SCHEMA_PROPERTIES;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties.getTableProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return materializedViewProperties.getMaterializedViewProperties();
    }

    @Override
    public void shutdown()
    {
        lifeCycleManager.stop();
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return immutableEnumSet(NOT_NULL_COLUMN_CONSTRAINT, MATERIALIZED_VIEW_GRACE_PERIOD);
    }
}
