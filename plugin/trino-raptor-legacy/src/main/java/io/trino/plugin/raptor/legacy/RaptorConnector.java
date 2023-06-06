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
package io.trino.plugin.raptor.legacy;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.trino.plugin.raptor.legacy.metadata.ForMetadata;
import io.trino.plugin.raptor.legacy.metadata.MetadataDao;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import jakarta.annotation.PostConstruct;
import org.jdbi.v3.core.Jdbi;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.raptor.legacy.util.DatabaseUtil.onDemandDao;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RaptorConnector
        implements Connector
{
    private static final Logger log = Logger.get(RaptorConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final RaptorMetadataFactory metadataFactory;
    private final RaptorSplitManager splitManager;
    private final RaptorPageSourceProvider pageSourceProvider;
    private final RaptorPageSinkProvider pageSinkProvider;
    private final RaptorNodePartitioningProvider nodePartitioningProvider;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final List<PropertyMetadata<?>> tableProperties;
    private final Set<SystemTable> systemTables;
    private final MetadataDao dao;
    private final Optional<ConnectorAccessControl> accessControl;
    private final boolean coordinator;

    private final ConcurrentMap<ConnectorTransactionHandle, RaptorMetadata> transactions = new ConcurrentHashMap<>();

    private final ScheduledExecutorService unblockMaintenanceExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("raptor-unblock-maintenance"));

    @GuardedBy("this")
    private final SetMultimap<Long, UUID> deletions = HashMultimap.create();

    @Inject
    public RaptorConnector(
            LifeCycleManager lifeCycleManager,
            NodeManager nodeManager,
            RaptorMetadataFactory metadataFactory,
            RaptorSplitManager splitManager,
            RaptorPageSourceProvider pageSourceProvider,
            RaptorPageSinkProvider pageSinkProvider,
            RaptorNodePartitioningProvider nodePartitioningProvider,
            RaptorSessionProperties sessionProperties,
            RaptorTableProperties tableProperties,
            Set<SystemTable> systemTables,
            Optional<ConnectorAccessControl> accessControl,
            @ForMetadata Jdbi dbi)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.nodePartitioningProvider = requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
        this.sessionProperties = sessionProperties.getSessionProperties();
        this.tableProperties = tableProperties.getTableProperties();
        this.systemTables = requireNonNull(systemTables, "systemTables is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.dao = onDemandDao(dbi, MetadataDao.class);
        this.coordinator = nodeManager.getCurrentNode().isCoordinator();
    }

    @PostConstruct
    public void start()
    {
        if (coordinator) {
            dao.unblockAllMaintenance();
        }
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        RaptorTransactionHandle transaction = new RaptorTransactionHandle();
        transactions.put(transaction, metadataFactory.create(tableId -> beginDelete(tableId, transaction.getUuid())));
        return transaction;
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        checkArgument(transactions.remove(transaction) != null, "no such transaction: %s", transaction);
        finishDelete(((RaptorTransactionHandle) transaction).getUuid());
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction)
    {
        RaptorMetadata metadata = transactions.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        finishDelete(((RaptorTransactionHandle) transaction).getUuid());
        metadata.rollback();
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
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
    {
        RaptorMetadata metadata = transactions.get(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return nodePartitioningProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return systemTables;
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return accessControl.orElseThrow(UnsupportedOperationException::new);
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }

    private synchronized void beginDelete(long tableId, UUID transactionId)
    {
        dao.blockMaintenance(tableId);
        verify(deletions.put(tableId, transactionId));
    }

    private synchronized void finishDelete(UUID transactionId)
    {
        deletions.entries().stream()
                .filter(entry -> entry.getValue().equals(transactionId))
                .findFirst()
                .ifPresent(entry -> {
                    long tableId = entry.getKey();
                    deletions.remove(tableId, transactionId);
                    if (!deletions.containsKey(tableId)) {
                        unblockMaintenance(tableId);
                    }
                });
    }

    private void unblockMaintenance(long tableId)
    {
        try {
            dao.unblockMaintenance(tableId);
        }
        catch (Throwable t) {
            log.warn(t, "Failed to unblock maintenance for table ID %s, will retry", tableId);
            unblockMaintenanceExecutor.schedule(() -> unblockMaintenance(tableId), 2, SECONDS);
        }
    }
}
