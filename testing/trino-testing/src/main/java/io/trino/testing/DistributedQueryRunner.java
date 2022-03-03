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
import com.google.common.io.Closer;
import com.google.inject.Module;
import io.airlift.discovery.server.testing.TestingDiscoveryServer;
import io.airlift.log.Logger;
import io.airlift.testing.Assertions;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.Session.SessionBuilder;
import io.trino.connector.CatalogName;
import io.trino.cost.StatsCalculator;
import io.trino.execution.FailureInjector.InjectedFailureType;
import io.trino.execution.QueryManager;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AllNodes;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.SqlFunction;
import io.trino.server.BasicQueryInfo;
import io.trino.server.SessionPropertyDefaults;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.ErrorType;
import io.trino.spi.Plugin;
import io.trino.spi.QueryId;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.type.TypeManager;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.Plan;
import io.trino.transaction.TransactionManager;
import org.intellij.lang.annotations.Language;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DistributedQueryRunner
        implements QueryRunner
{
    private static final Logger log = Logger.get(DistributedQueryRunner.class);
    private static final String ENVIRONMENT = "testing";

    private final TestingDiscoveryServer discoveryServer;
    private final TestingTrinoServer coordinator;
    private final Optional<TestingTrinoServer> backupCoordinator;
    private List<TestingTrinoServer> servers;

    private final Closer closer = Closer.create();

    private final TestingTrinoClient trinoClient;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public static Builder<?> builder(Session defaultSession)
    {
        return new Builder<>(defaultSession);
    }

    private DistributedQueryRunner(
            Session defaultSession,
            int nodeCount,
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            Optional<Map<String, String>> backupCoordinatorProperties,
            String environment,
            Module additionalModule,
            Optional<Path> baseDataDir,
            List<SystemAccessControl> systemAccessControls,
            List<EventListener> eventListeners)
            throws Exception
    {
        requireNonNull(defaultSession, "defaultSession is null");

        if (backupCoordinatorProperties.isPresent()) {
            checkArgument(nodeCount >= 2, "the nodeCount must be greater than or equal to two!");
        }

        try {
            long start = System.nanoTime();
            discoveryServer = new TestingDiscoveryServer(environment);
            closer.register(() -> closeUnchecked(discoveryServer));
            log.info("Created TestingDiscoveryServer in %s", nanosSince(start).convertToMostSuccinctTimeUnit());

            ImmutableList.Builder<TestingTrinoServer> servers = ImmutableList.builder();

            for (int i = backupCoordinatorProperties.isEmpty() ? 1 : 2; i < nodeCount; i++) {
                TestingTrinoServer worker = closer.register(createTestingTrinoServer(
                        discoveryServer.getBaseUrl(),
                        false,
                        extraProperties,
                        environment,
                        additionalModule,
                        baseDataDir,
                        ImmutableList.of(),
                        ImmutableList.of()));
                servers.add(worker);
            }

            Map<String, String> extraCoordinatorProperties = new HashMap<>();
            extraCoordinatorProperties.putAll(extraProperties);
            extraCoordinatorProperties.putAll(coordinatorProperties);

            if (!extraCoordinatorProperties.containsKey("web-ui.authentication.type")) {
                // Make it possible to use Trino UI when running multiple tests (or tests and SomeQueryRunner.main) at once.
                // This is necessary since cookies are shared (don't discern port number) and logging into one instance logs you out from others.
                extraCoordinatorProperties.put("web-ui.authentication.type", "fixed");
                extraCoordinatorProperties.put("web-ui.user", "admin");
            }

            coordinator = closer.register(createTestingTrinoServer(
                    discoveryServer.getBaseUrl(),
                    true,
                    extraCoordinatorProperties,
                    environment,
                    additionalModule,
                    baseDataDir,
                    systemAccessControls,
                    eventListeners));
            servers.add(coordinator);
            if (backupCoordinatorProperties.isPresent()) {
                Map<String, String> extraBackupCoordinatorProperties = new HashMap<>();
                extraBackupCoordinatorProperties.putAll(extraProperties);
                extraBackupCoordinatorProperties.putAll(backupCoordinatorProperties.get());
                backupCoordinator = Optional.of(closer.register(createTestingTrinoServer(
                        discoveryServer.getBaseUrl(),
                        true,
                        extraBackupCoordinatorProperties,
                        environment,
                        additionalModule,
                        baseDataDir,
                        systemAccessControls,
                        eventListeners)));
                servers.add(backupCoordinator.get());
            }
            else {
                backupCoordinator = Optional.empty();
            }

            this.servers = servers.build();
        }
        catch (Exception e) {
            try {
                throw closer.rethrow(e, Exception.class);
            }
            finally {
                closer.close();
            }
        }

        // copy session using property manager in coordinator
        defaultSession = defaultSession.toSessionRepresentation().toSession(coordinator.getSessionPropertyManager(), defaultSession.getIdentity().getExtraCredentials());
        this.trinoClient = closer.register(new TestingTrinoClient(coordinator, defaultSession));

        waitForAllNodesGloballyVisible();

        long start = System.nanoTime();
        for (TestingTrinoServer server : servers) {
            server.getMetadata().addFunctions(AbstractTestQueries.CUSTOM_FUNCTIONS);
        }
        log.info("Added functions in %s", nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private static TestingTrinoServer createTestingTrinoServer(
            URI discoveryUri,
            boolean coordinator,
            Map<String, String> extraProperties,
            String environment,
            Module additionalModule,
            Optional<Path> baseDataDir,
            List<SystemAccessControl> systemAccessControls,
            List<EventListener> eventListeners)
    {
        long start = System.nanoTime();
        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder()
                .put("internal-communication.shared-secret", "test-secret")
                .put("query.client.timeout", "10m")
                // Use few threads in tests to preserve resources on CI
                .put("discovery.http-client.min-threads", "1") // default 8
                .put("exchange.http-client.min-threads", "1") // default 8
                .put("node-manager.http-client.min-threads", "1") // default 8
                .put("exchange.page-buffer-client.max-callback-threads", "5") // default 25
                .put("exchange.http-client.idle-timeout", "1h")
                .put("task.max-index-memory", "16kB") // causes index joins to fault load
                .put("distributed-index-joins-enabled", "true");
        if (coordinator) {
            propertiesBuilder.put("node-scheduler.include-coordinator", "true");
            propertiesBuilder.put("join-distribution-type", "PARTITIONED");

            // Use few threads in tests to preserve resources on CI
            propertiesBuilder.put("failure-detector.http-client.min-threads", "1"); // default 8
            propertiesBuilder.put("memoryManager.http-client.min-threads", "1"); // default 8
            propertiesBuilder.put("scheduler.http-client.min-threads", "1"); // default 8
            propertiesBuilder.put("workerInfo.http-client.min-threads", "1"); // default 8
        }
        HashMap<String, String> properties = new HashMap<>(propertiesBuilder.buildOrThrow());
        properties.putAll(extraProperties);

        TestingTrinoServer server = TestingTrinoServer.builder()
                .setCoordinator(coordinator)
                .setProperties(properties)
                .setEnvironment(environment)
                .setDiscoveryUri(discoveryUri)
                .setAdditionalModule(additionalModule)
                .setBaseDataDir(baseDataDir)
                .setSystemAccessControls(systemAccessControls)
                .setEventListeners(eventListeners)
                .build();

        String nodeRole = coordinator ? "coordinator" : "worker";
        log.info("Created %s TestingTrinoServer in %s: %s", nodeRole, nanosSince(start).convertToMostSuccinctTimeUnit(), server.getBaseUrl());

        return server;
    }

    public void addServers(int nodeCount)
            throws Exception
    {
        ImmutableList.Builder<TestingTrinoServer> serverBuilder = ImmutableList.<TestingTrinoServer>builder()
                .addAll(servers);
        for (int i = 0; i < nodeCount; i++) {
            TestingTrinoServer server = closer.register(createTestingTrinoServer(
                    discoveryServer.getBaseUrl(),
                    false,
                    ImmutableMap.of(),
                    ENVIRONMENT,
                    EMPTY_MODULE,
                    Optional.empty(),
                    ImmutableList.of(),
                    ImmutableList.of()));
            serverBuilder.add(server);
            // add functions
            server.getMetadata().addFunctions(AbstractTestQueries.CUSTOM_FUNCTIONS);
        }
        servers = serverBuilder.build();
        waitForAllNodesGloballyVisible();
    }

    private void waitForAllNodesGloballyVisible()
            throws InterruptedException
    {
        long start = System.nanoTime();
        while (!allNodesGloballyVisible()) {
            Assertions.assertLessThan(nanosSince(start), new Duration(10, SECONDS));
            MILLISECONDS.sleep(10);
        }
        log.info("Announced servers in %s", nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private boolean allNodesGloballyVisible()
    {
        for (TestingTrinoServer server : servers) {
            AllNodes allNodes = server.refreshNodes();
            if (!allNodes.getInactiveNodes().isEmpty() ||
                    (allNodes.getActiveNodes().size() != servers.size())) {
                return false;
            }
        }
        return true;
    }

    public TestingTrinoClient getClient()
    {
        return trinoClient;
    }

    @Override
    public int getNodeCount()
    {
        return servers.size();
    }

    @Override
    public Session getDefaultSession()
    {
        return trinoClient.getDefaultSession();
    }

    @Override
    public TransactionManager getTransactionManager()
    {
        return coordinator.getTransactionManager();
    }

    @Override
    public Metadata getMetadata()
    {
        return coordinator.getMetadata();
    }

    @Override
    public TypeManager getTypeManager()
    {
        return coordinator.getTypeManager();
    }

    @Override
    public QueryExplainer getQueryExplainer()
    {
        return coordinator.getQueryExplainer();
    }

    @Override
    public SessionPropertyManager getSessionPropertyManager()
    {
        return coordinator.getSessionPropertyManager();
    }

    @Override
    public SplitManager getSplitManager()
    {
        return coordinator.getSplitManager();
    }

    @Override
    public PageSourceManager getPageSourceManager()
    {
        return coordinator.getPageSourceManager();
    }

    @Override
    public NodePartitioningManager getNodePartitioningManager()
    {
        return coordinator.getNodePartitioningManager();
    }

    @Override
    public StatsCalculator getStatsCalculator()
    {
        return coordinator.getStatsCalculator();
    }

    @Override
    public TestingAccessControlManager getAccessControl()
    {
        return coordinator.getAccessControl();
    }

    @Override
    public TestingGroupProvider getGroupProvider()
    {
        return coordinator.getGroupProvider();
    }

    public SessionPropertyDefaults getSessionPropertyDefaults()
    {
        return coordinator.getSessionPropertyDefaults();
    }

    public TestingTrinoServer getCoordinator()
    {
        return coordinator;
    }

    public Optional<TestingTrinoServer> getBackupCoordinator()
    {
        return backupCoordinator;
    }

    public List<TestingTrinoServer> getServers()
    {
        return ImmutableList.copyOf(servers);
    }

    @Override
    public void installPlugin(Plugin plugin)
    {
        long start = System.nanoTime();
        for (TestingTrinoServer server : servers) {
            server.installPlugin(plugin);
        }
        log.info("Installed plugin %s in %s", plugin.getClass().getSimpleName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    @Override
    public void addFunctions(List<? extends SqlFunction> functions)
    {
        servers.forEach(server -> server.getMetadata().addFunctions(functions));
    }

    public void createCatalog(String catalogName, String connectorName)
    {
        createCatalog(catalogName, connectorName, ImmutableMap.of());
    }

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        long start = System.nanoTime();
        Set<CatalogName> catalogNames = new HashSet<>();
        for (TestingTrinoServer server : servers) {
            catalogNames.add(server.createCatalog(catalogName, connectorName, properties));
        }
        CatalogName catalog = getOnlyElement(catalogNames);
        log.info("Created catalog %s (%s) in %s", catalogName, catalog, nanosSince(start));

        // wait for all nodes to announce the new catalog
        start = System.nanoTime();
        while (!isConnectionVisibleToAllNodes(catalog)) {
            Assertions.assertLessThan(nanosSince(start), new Duration(100, SECONDS), "waiting for connector " + catalog + " to be initialized in every node");
            try {
                MILLISECONDS.sleep(10);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        log.info("Announced catalog %s (%s) in %s", catalogName, catalog, nanosSince(start));
    }

    private boolean isConnectionVisibleToAllNodes(CatalogName catalogName)
    {
        for (TestingTrinoServer server : servers) {
            server.refreshNodes();
            Set<InternalNode> activeNodesWithConnector = server.getActiveNodesWithConnector(catalogName);
            if (activeNodesWithConnector.size() != servers.size()) {
                return false;
            }
        }
        return true;
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
        catch (Throwable e) {
            e.addSuppressed(new Exception("SQL: " + sql));
            throw e;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    public ResultWithQueryId<MaterializedResult> executeWithQueryId(Session session, @Language("SQL") String sql)
    {
        lock.readLock().lock();
        try {
            return trinoClient.execute(session, sql);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public MaterializedResultWithPlan executeWithPlan(Session session, String sql, WarningCollector warningCollector)
    {
        ResultWithQueryId<MaterializedResult> resultWithQueryId = executeWithQueryId(session, sql);
        return new MaterializedResultWithPlan(resultWithQueryId.getResult().toTestTypes(), getQueryPlan(resultWithQueryId.getQueryId()));
    }

    @Override
    public Plan createPlan(Session session, String sql, WarningCollector warningCollector)
    {
        QueryId queryId = executeWithQueryId(session, sql).getQueryId();
        Plan queryPlan = getQueryPlan(queryId);
        coordinator.getQueryManager().cancelQuery(queryId);
        return queryPlan;
    }

    public Plan getQueryPlan(QueryId queryId)
    {
        return coordinator.getQueryPlan(queryId);
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
        for (TestingTrinoServer server : servers) {
            server.injectTaskFailure(
                    traceToken,
                    stageId,
                    partitionId,
                    attemptId,
                    injectionType,
                    errorType);
        }
    }

    @Override
    public void loadExchangeManager(String name, Map<String, String> properties)
    {
        for (TestingTrinoServer server : servers) {
            server.loadExchangeManager(name, properties);
        }
    }

    @Override
    public final void close()
    {
        cancelAllQueries();
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void cancelAllQueries()
    {
        QueryManager queryManager = coordinator.getQueryManager();
        for (BasicQueryInfo queryInfo : queryManager.getQueries()) {
            if (!queryInfo.getState().isDone()) {
                try {
                    queryManager.cancelQuery(queryInfo.getQueryId());
                }
                catch (RuntimeException e) {
                    // TODO (https://github.com/trinodb/trino/issues/6723) query cancellation can sometimes fail
                    log.warn(e, "Failed to cancel query");
                }
            }
        }
    }

    private static void closeUnchecked(AutoCloseable closeable)
    {
        try {
            closeable.close();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public static class Builder<SELF extends Builder<?>>
    {
        private Session defaultSession;
        private int nodeCount = 3;
        private Map<String, String> extraProperties = new HashMap<>();
        private Map<String, String> coordinatorProperties = ImmutableMap.of();
        private Optional<Map<String, String>> backupCoordinatorProperties = Optional.empty();
        private Consumer<QueryRunner> additionalSetup = querRunner -> {};
        private String environment = ENVIRONMENT;
        private Module additionalModule = EMPTY_MODULE;
        private Optional<Path> baseDataDir = Optional.empty();
        private List<SystemAccessControl> systemAccessControls = ImmutableList.of();
        private List<EventListener> eventListeners = ImmutableList.of();

        protected Builder(Session defaultSession)
        {
            this.defaultSession = requireNonNull(defaultSession, "defaultSession is null");
        }

        public SELF amendSession(Function<SessionBuilder, SessionBuilder> amendSession)
        {
            SessionBuilder builder = Session.builder(defaultSession);
            this.defaultSession = amendSession.apply(builder).build();
            return self();
        }

        public SELF setNodeCount(int nodeCount)
        {
            this.nodeCount = nodeCount;
            return self();
        }

        public SELF setExtraProperties(Map<String, String> extraProperties)
        {
            this.extraProperties = new HashMap<>(extraProperties);
            return self();
        }

        public SELF addExtraProperty(String key, String value)
        {
            this.extraProperties.put(key, value);
            return self();
        }

        public SELF setCoordinatorProperties(Map<String, String> coordinatorProperties)
        {
            this.coordinatorProperties = coordinatorProperties;
            return self();
        }

        public SELF setBackupCoordinatorProperties(Map<String, String> backupCoordinatorProperties)
        {
            this.backupCoordinatorProperties = Optional.of(backupCoordinatorProperties);
            return self();
        }

        /**
         * Additional configuration to be applied on {@link QueryRunner} being built.
         * Invoked after engine configuration is applied, but before connector-specific configurations
         * (if any) are applied.
         */
        public SELF setAdditionalSetup(Consumer<QueryRunner> additionalSetup)
        {
            this.additionalSetup = requireNonNull(additionalSetup, "additionalSetup is null");
            return self();
        }

        /**
         * Sets coordinator properties being equal to a map containing given key and value.
         * Note, that calling this method OVERWRITES previously set property values.
         * As a result, it should only be used when only one coordinator property needs to be set.
         */
        public SELF setSingleCoordinatorProperty(String key, String value)
        {
            return setCoordinatorProperties(ImmutableMap.of(key, value));
        }

        public SELF setEnvironment(String environment)
        {
            this.environment = environment;
            return self();
        }

        public SELF setAdditionalModule(Module additionalModule)
        {
            this.additionalModule = requireNonNull(additionalModule, "additionalModules is null");
            return self();
        }

        public SELF setBaseDataDir(Optional<Path> baseDataDir)
        {
            this.baseDataDir = requireNonNull(baseDataDir, "baseDataDir is null");
            return self();
        }

        @SuppressWarnings("unused")
        public SELF setSystemAccessControl(SystemAccessControl systemAccessControl)
        {
            return setSystemAccessControls(ImmutableList.of(requireNonNull(systemAccessControl, "systemAccessControl is null")));
        }

        @SuppressWarnings("unused")
        public SELF setSystemAccessControls(List<SystemAccessControl> systemAccessControls)
        {
            this.systemAccessControls = ImmutableList.copyOf(requireNonNull(systemAccessControls, "systemAccessControls is null"));
            return self();
        }

        @SuppressWarnings("unused")
        public SELF setEventListener(EventListener eventListener)
        {
            return setEventListeners(ImmutableList.of(requireNonNull(eventListener, "eventListener is null")));
        }

        @SuppressWarnings("unused")
        public SELF setEventListeners(List<EventListener> eventListeners)
        {
            this.eventListeners = ImmutableList.copyOf(requireNonNull(eventListeners, "eventListeners is null"));
            return self();
        }

        public SELF enableBackupCoordinator()
        {
            if (backupCoordinatorProperties.isEmpty()) {
                setBackupCoordinatorProperties(ImmutableMap.of());
            }
            return self();
        }

        @SuppressWarnings("unchecked")
        protected SELF self()
        {
            return (SELF) this;
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = new DistributedQueryRunner(
                    defaultSession,
                    nodeCount,
                    extraProperties,
                    coordinatorProperties,
                    backupCoordinatorProperties,
                    environment,
                    additionalModule,
                    baseDataDir,
                    systemAccessControls,
                    eventListeners);

            try {
                additionalSetup.accept(queryRunner);
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }

            return queryRunner;
        }
    }
}
