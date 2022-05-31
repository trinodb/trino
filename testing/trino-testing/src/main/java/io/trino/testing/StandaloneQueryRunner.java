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
package io.trino.testing;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.cost.StatsCalculator;
import io.trino.execution.FailureInjector.InjectedFailureType;
import io.trino.metadata.AllNodes;
import io.trino.metadata.FunctionBundle;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SessionPropertyManager;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.ErrorType;
import io.trino.spi.Plugin;
import io.trino.spi.type.TypeManager;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.transaction.TransactionManager;
import org.intellij.lang.annotations.Language;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.airlift.testing.Closeables.closeAll;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class StandaloneQueryRunner
        implements QueryRunner
{
    private final TestingTrinoServer server;

    private final TestingTrinoClient trinoClient;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public StandaloneQueryRunner(Session defaultSession)
    {
        requireNonNull(defaultSession, "defaultSession is null");

        this.server = createTestingTrinoServer();
        this.trinoClient = new TestingTrinoClient(server, defaultSession);

        refreshNodes();

        server.addFunctions(AbstractTestQueries.CUSTOM_FUNCTIONS);
    }

    @Override
    public MaterializedResult execute(@Language("SQL") String sql)
    {
        lock.readLock().lock();
        try {
            return trinoClient.execute(sql).getResult();
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public MaterializedResult execute(Session session, @Language("SQL") String sql)
    {
        lock.readLock().lock();
        try {
            return trinoClient.execute(session, sql).getResult();
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close()
    {
        try {
            closeAll(trinoClient, server);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getNodeCount()
    {
        return 1;
    }

    @Override
    public Session getDefaultSession()
    {
        return trinoClient.getDefaultSession();
    }

    @Override
    public TransactionManager getTransactionManager()
    {
        return server.getTransactionManager();
    }

    @Override
    public Metadata getMetadata()
    {
        return server.getMetadata();
    }

    @Override
    public TypeManager getTypeManager()
    {
        return server.getTypeManager();
    }

    @Override
    public QueryExplainer getQueryExplainer()
    {
        return server.getQueryExplainer();
    }

    @Override
    public SessionPropertyManager getSessionPropertyManager()
    {
        return server.getSessionPropertyManager();
    }

    @Override
    public FunctionManager getFunctionManager()
    {
        return server.getFunctionManager();
    }

    @Override
    public SplitManager getSplitManager()
    {
        return server.getSplitManager();
    }

    @Override
    public PageSourceManager getPageSourceManager()
    {
        return server.getPageSourceManager();
    }

    @Override
    public NodePartitioningManager getNodePartitioningManager()
    {
        return server.getNodePartitioningManager();
    }

    @Override
    public StatsCalculator getStatsCalculator()
    {
        return server.getStatsCalculator();
    }

    @Override
    public TestingGroupProvider getGroupProvider()
    {
        return server.getGroupProvider();
    }

    @Override
    public TestingAccessControlManager getAccessControl()
    {
        return server.getAccessControl();
    }

    public TestingTrinoServer getServer()
    {
        return server;
    }

    public void refreshNodes()
    {
        AllNodes allNodes;

        do {
            try {
                MILLISECONDS.sleep(10);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            allNodes = server.refreshNodes();
        }
        while (allNodes.getActiveNodes().isEmpty());
    }

    private void refreshNodes(CatalogName catalogName)
    {
        Set<InternalNode> activeNodesWithConnector;

        do {
            try {
                MILLISECONDS.sleep(10);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            activeNodesWithConnector = server.getActiveNodesWithConnector(catalogName);
        }
        while (activeNodesWithConnector.isEmpty());
    }

    @Override
    public void installPlugin(Plugin plugin)
    {
        server.installPlugin(plugin);
    }

    @Override
    public void addFunctions(FunctionBundle functionBundle)
    {
        server.addFunctions(functionBundle);
    }

    public void createCatalog(String catalogName, String connectorName)
    {
        createCatalog(catalogName, connectorName, ImmutableMap.of());
    }

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        CatalogName catalog = server.createCatalog(catalogName, connectorName, properties);

        refreshNodes(catalog);
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
    {
        lock.readLock().lock();
        try {
            return trinoClient.listTables(session, catalog, schema);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean tableExists(Session session, String table)
    {
        lock.readLock().lock();
        try {
            return trinoClient.tableExists(session, table);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Lock getExclusiveLock()
    {
        return lock.writeLock();
    }

    @Override
    public void injectTaskFailure(
            String traceToken,
            int stageId,
            int partitionId,
            int attemptId,
            InjectedFailureType injectionType,
            Optional<ErrorType> errorType)
    {
        server.injectTaskFailure(
                traceToken,
                stageId,
                partitionId,
                attemptId,
                injectionType,
                errorType);
    }

    @Override
    public void loadExchangeManager(String name, Map<String, String> properties)
    {
        server.loadExchangeManager(name, properties);
    }

    private static TestingTrinoServer createTestingTrinoServer()
    {
        return TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("query.client.timeout", "10m")
                        .put("exchange.http-client.idle-timeout", "1h")
                        .put("node-scheduler.min-candidates", "1")
                        .buildOrThrow())
                .build();
    }
}
