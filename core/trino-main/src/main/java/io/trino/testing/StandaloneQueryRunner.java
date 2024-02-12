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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.trino.Session;
import io.trino.cost.StatsCalculator;
import io.trino.execution.FailureInjector.InjectedFailureType;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.FunctionBundle;
import io.trino.metadata.MetadataUtil;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.SessionPropertyManager;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.ErrorType;
import io.trino.spi.Plugin;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.Plan;
import io.trino.sql.tree.Statement;
import io.trino.transaction.TransactionManager;
import org.intellij.lang.annotations.Language;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static java.util.Objects.requireNonNull;

public final class StandaloneQueryRunner
        implements QueryRunner
{
    private final Session defaultSession;
    private final TestingTrinoServer server;
    private final DirectTrinoClient trinoClient;
    private final InMemorySpanExporter spanExporter = InMemorySpanExporter.create();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicInteger concurrentQueries = new AtomicInteger();
    private volatile boolean spansValid = true;

    public StandaloneQueryRunner(Session defaultSession)
    {
        this(defaultSession, builder -> {});
    }

    public StandaloneQueryRunner(Session defaultSession, Consumer<TestingTrinoServer.Builder> serverProcessor)
    {
        this.defaultSession = requireNonNull(defaultSession, "defaultSession is null");
        TestingTrinoServer.Builder builder = TestingTrinoServer.builder()
                .setSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("query.client.timeout", "10m")
                        .put("exchange.http-client.idle-timeout", "1h")
                        .put("node-scheduler.min-candidates", "1")
                        .buildOrThrow());
        serverProcessor.accept(builder);
        this.server = builder.build();

        this.trinoClient = new DirectTrinoClient(
                server.getDispatchManager(),
                server.getQueryManager(),
                server.getInstance(Key.get(DirectExchangeClientSupplier.class)),
                server.getInstance(Key.get(BlockEncodingSerde.class)));
    }

    @Override
    public List<SpanData> getSpans()
    {
        checkState(spansValid, "No valid spans, queries were executing concurrently");
        return spanExporter.getFinishedSpanItems();
    }

    @Override
    public MaterializedResult execute(Session session, @Language("SQL") String sql)
    {
        return executeInternal(session, sql).result();
    }

    @Override
    public MaterializedResultWithPlan executeWithPlan(Session session, String sql)
    {
        DirectTrinoClient.Result result = executeInternal(session, sql);
        return new MaterializedResultWithPlan(result.queryId(), server.getQueryPlan(result.queryId()), result.result());
    }

    private DirectTrinoClient.Result executeInternal(Session session, @Language("SQL") String sql)
    {
        lock.readLock().lock();
        try {
            spansValid = concurrentQueries.incrementAndGet() == 1;
            try {
                spanExporter.reset();
                return trinoClient.execute(session, sql);
            }
            finally {
                concurrentQueries.decrementAndGet();
            }
        }
        catch (Throwable e) {
            e.addSuppressed(new Exception("SQL: " + sql));
            throw e;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Plan createPlan(Session session, String sql)
    {
        // session must be in a transaction registered with the transaction manager in this query runner
        getTransactionManager().getTransactionInfo(session.getRequiredTransactionId());

        spansValid = concurrentQueries.incrementAndGet() == 1;
        try {
            spanExporter.reset();
            Statement statement = server.getInstance(Key.get(SqlParser.class)).createStatement(sql);
            return server.getQueryExplainer().getLogicalPlan(session, statement, ImmutableList.of(), WarningCollector.NOOP, createPlanOptimizersStatsCollector());
        }
        finally {
            concurrentQueries.decrementAndGet();
        }
    }

    @Override
    public void close()
    {
        try {
            server.close();
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
        return defaultSession;
    }

    @Override
    public TransactionManager getTransactionManager()
    {
        return server.getTransactionManager();
    }

    @Override
    public PlannerContext getPlannerContext()
    {
        return server.getPlannerContext();
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
    public TestingGroupProviderManager getGroupProvider()
    {
        return server.getGroupProvider();
    }

    @Override
    public TestingAccessControlManager getAccessControl()
    {
        return server.getAccessControl();
    }

    @Override
    public TestingTrinoServer getCoordinator()
    {
        return server;
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

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        server.createCatalog(catalogName, connectorName, properties);
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
    {
        lock.readLock().lock();
        try {
            return inTransaction(session, transactionSession ->
                    server.getPlannerContext().getMetadata().listTables(transactionSession, new QualifiedTablePrefix(catalog, schema)));
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
            return inTransaction(session, transactionSession ->
                    MetadataUtil.tableExists(server.getPlannerContext().getMetadata(), transactionSession, table));
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

    @Override
    public void loadSpoolingManager(String name, Map<String, String> properties)
    {
        server.loadSpoolingManager(name, properties);
    }
}
