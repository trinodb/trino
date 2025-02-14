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
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.discovery.server.testing.TestingDiscoveryServer;
import io.airlift.http.server.HttpServer;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.trino.Session;
import io.trino.Session.SessionBuilder;
import io.trino.client.ClientSession;
import io.trino.client.StatementClient;
import io.trino.connector.CoordinatorDynamicCatalogManager;
import io.trino.cost.StatsCalculator;
import io.trino.execution.FailureInjector.InjectedFailureType;
import io.trino.execution.QueryManager;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AllNodes;
import io.trino.metadata.FunctionBundle;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SessionPropertyManager;
import io.trino.server.BasicQueryInfo;
import io.trino.server.PluginManager;
import io.trino.server.SessionPropertyDefaults;
import io.trino.server.testing.FactoryConfiguration;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.ErrorType;
import io.trino.spi.Plugin;
import io.trino.spi.QueryId;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.security.SystemAccessControl;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.Plan;
import io.trino.testing.LocalSpoolingManager.LocalSpoolingPlugin;
import io.trino.testing.containers.OpenTracingCollector;
import io.trino.transaction.TransactionManager;
import io.trino.util.Ciphers;
import okhttp3.OkHttpClient;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.intellij.lang.annotations.Language;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.airlift.log.Level.DEBUG;
import static io.airlift.log.Level.ERROR;
import static io.airlift.log.Level.WARN;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getenv;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class DistributedQueryRunner
        implements QueryRunner
{
    private static final Logger log = Logger.get(DistributedQueryRunner.class);
    private static final String ENVIRONMENT = "testing";
    private static final AtomicInteger unclosedInstances = new AtomicInteger();

    private TestingDiscoveryServer discoveryServer;
    private TestingTrinoServer coordinator;
    private Optional<TestingTrinoServer> backupCoordinator;
    private Consumer<Map<String, String>> registerNewWorker;
    private final InMemorySpanExporter spanExporter = InMemorySpanExporter.create();
    private final List<TestingTrinoServer> servers = new CopyOnWriteArrayList<>();
    private final List<FunctionBundle> functionBundles = new CopyOnWriteArrayList<>(ImmutableList.of(CustomFunctionBundle.CUSTOM_FUNCTIONS));
    private final List<Plugin> plugins = new CopyOnWriteArrayList<>();

    private final Closer closer = Closer.create();

    private TestingTrinoClient trinoClient;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicInteger concurrentQueries = new AtomicInteger();
    private volatile boolean spansValid = true;

    private boolean closed;

    public static Builder<?> builder(Session defaultSession)
    {
        return new Builder<>(defaultSession);
    }

    private DistributedQueryRunner(
            Session defaultSession,
            int workerCount,
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            Optional<Map<String, String>> backupCoordinatorProperties,
            String environment,
            Module additionalModule,
            Optional<Path> baseDataDir,
            Optional<FactoryConfiguration> systemAccessControlConfiguration,
            Optional<List<SystemAccessControl>> systemAccessControls,
            List<EventListener> eventListeners,
            List<AutoCloseable> extraCloseables,
            TestingTrinoClientFactory testingTrinoClientFactory)
            throws Exception
    {
        requireNonNull(defaultSession, "defaultSession is null");

        long start = System.nanoTime();
        setupLogging();

        try {
            long discoveryStart = System.nanoTime();
            discoveryServer = new TestingDiscoveryServer(environment);
            closer.register(() -> closeUnchecked(discoveryServer));
            extraCloseables.forEach(closeable -> closer.register(() -> closeUnchecked(closeable)));
            log.debug("Created TestingDiscoveryServer in %s", nanosSince(discoveryStart));

            registerNewWorker = additionalWorkerProperties -> {
                @SuppressWarnings("resource")
                TestingTrinoServer ignored = createServer(
                        false,
                        ImmutableMap.<String, String>builder()
                                .putAll(extraProperties)
                                .putAll(additionalWorkerProperties)
                                .buildOrThrow(),
                        environment,
                        additionalModule,
                        baseDataDir,
                        Optional.empty(),
                        Optional.of(ImmutableList.of()),
                        ImmutableList.of());
            };

            for (int i = 0; i < workerCount; i++) {
                registerNewWorker.accept(Map.of());
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

            coordinator = createServer(
                    true,
                    extraCoordinatorProperties,
                    environment,
                    additionalModule,
                    baseDataDir,
                    systemAccessControlConfiguration,
                    systemAccessControls,
                    eventListeners);

            backupCoordinator = backupCoordinatorProperties.map(properties -> createServer(
                    true,
                    ImmutableMap.<String, String>builder()
                            .putAll(extraProperties)
                            .putAll(properties)
                            .buildOrThrow(),
                    environment,
                    additionalModule,
                    baseDataDir,
                    systemAccessControlConfiguration,
                    systemAccessControls,
                    eventListeners));
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
        defaultSession = defaultSession.toSessionRepresentation().toSession(
                coordinator.getSessionPropertyManager(),
                defaultSession.getIdentity().getExtraCredentials(),
                defaultSession.getExchangeEncryptionKey());

        this.trinoClient = closer.register(testingTrinoClientFactory.create(coordinator, defaultSession));

        ensureNodesGloballyVisible();
        log.info("Created DistributedQueryRunner in %s (unclosed instances = %s)", nanosSince(start), unclosedInstances.incrementAndGet());
    }

    public void registerResource(AutoCloseable resource)
    {
        requireNonNull(resource, "resource is null");
        checkState(!closed, "already closed");
        closer.register(() -> {
            try {
                resource.close();
            }
            catch (Exception e) {
                throwIfUnchecked(e);
                throwIfInstanceOf(e, IOException.class);
                throw new RuntimeException(e);
            }
        });
    }

    private TestingTrinoServer createServer(
            boolean coordinator,
            Map<String, String> extraCoordinatorProperties,
            String environment,
            Module additionalModule,
            Optional<Path> baseDataDir,
            Optional<FactoryConfiguration> systemAccessControlConfiguration,
            Optional<List<SystemAccessControl>> systemAccessControls,
            List<EventListener> eventListeners)
    {
        TestingTrinoServer server = closer.register(createTestingTrinoServer(
                discoveryServer.getBaseUrl(),
                coordinator,
                extraCoordinatorProperties,
                environment,
                additionalModule,
                baseDataDir,
                SimpleSpanProcessor.create(spanExporter),
                systemAccessControlConfiguration,
                systemAccessControls,
                eventListeners,
                newServer -> {
                    functionBundles.forEach(newServer::addFunctions);
                    plugins.forEach(newServer::installPlugin);
                }));
        servers.add(server);
        return server;
    }

    private static void setupLogging()
    {
        Logging logging = Logging.initialize();
        logging.setLevel("Bootstrap", WARN);
        logging.setLevel("org.glassfish", ERROR);
        logging.setLevel("org.eclipse.jetty.server", WARN);
        logging.setLevel("org.hibernate.validator.internal.util.Version", WARN);
        logging.setLevel(PluginManager.class.getName(), WARN);
        logging.setLevel(CoordinatorDynamicCatalogManager.class.getName(), WARN);
        logging.setLevel("io.trino.execution.scheduler.faulttolerant", DEBUG);
    }

    private static TestingTrinoServer createTestingTrinoServer(
            URI discoveryUri,
            boolean coordinator,
            Map<String, String> extraProperties,
            String environment,
            Module additionalModule,
            Optional<Path> baseDataDir,
            SpanProcessor spanProcessor,
            Optional<FactoryConfiguration> systemAccessControlConfiguration,
            Optional<List<SystemAccessControl>> systemAccessControls,
            List<EventListener> eventListeners,
            Consumer<TestingTrinoServer> additionalConfiguration)
    {
        long start = System.nanoTime();
        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder()
                .put("query.client.timeout", "10m")
                // Use few threads in tests to preserve resources on CI
                .put("discovery.http-client.min-threads", "1") // default 8
                .put("exchange.http-client.min-threads", "1") // default 8
                .put("node-manager.http-client.min-threads", "1") // default 8
                .put("exchange.page-buffer-client.max-callback-threads", "5") // default 25
                .put("exchange.http-client.idle-timeout", "1h")
                .put("task.max-index-memory", "16kB"); // causes index joins to fault load
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
                .setSpanProcessor(spanProcessor)
                .setSystemAccessControlConfiguration(systemAccessControlConfiguration)
                .setSystemAccessControls(systemAccessControls)
                .setEventListeners(eventListeners)
                .setAdditionalConfiguration(additionalConfiguration)
                .build();

        String nodeRole = coordinator ? "coordinator" : "worker";
        log.debug("Created TestingTrinoServer %s in %s: %s", nodeRole, nanosSince(start).convertToMostSuccinctTimeUnit(), server.getBaseUrl());

        return server;
    }

    public void addServers(int nodeCount)
    {
        for (int i = 0; i < nodeCount; i++) {
            registerNewWorker.accept(Map.of());
        }
        ensureNodesGloballyVisible();
    }

    /**
     * Simulate worker restart as e.g. in Kubernetes after a pod is killed.
     */
    public void restartWorker(TestingTrinoServer server)
            throws Exception
    {
        URI baseUrl = server.getBaseUrl();
        checkState(servers.remove(server), "Server not found: %s", server);
        HttpServer workerHttpServer = server.getInstance(Key.get(HttpServer.class));
        // Prevent any HTTP communication with the worker, as if the worker process was killed.
        Field serverField = HttpServer.class.getDeclaredField("server");
        serverField.setAccessible(true);
        Connector httpConnector = getOnlyElement(asList(((Server) serverField.get(workerHttpServer)).getConnectors()));
        httpConnector.stop();
        server.close();

        Map<String, String> reusePort = Map.of("http-server.http.port", Integer.toString(baseUrl.getPort()));
        registerNewWorker.accept(reusePort);
        // Verify the address was reused.
        assertThat(servers.stream()
                .map(TestingTrinoServer::getBaseUrl)
                .filter(baseUrl::equals))
                .hasSize(1);
        // Do not wait for new server to be fully registered with other servers
    }

    private void ensureNodesGloballyVisible()
    {
        for (TestingTrinoServer server : servers) {
            AllNodes nodes = server.refreshNodes();
            verify(nodes.getInactiveNodes().isEmpty(), "Node manager has inactive nodes");
            verify(nodes.getActiveNodes().size() == servers.size(), "Node manager has wrong active node count");
        }
    }

    public TestingTrinoClient getClient()
    {
        return trinoClient;
    }

    @Override
    public List<SpanData> getSpans()
    {
        checkState(spansValid, "No valid spans, queries were executing concurrently");
        return spanExporter.getFinishedSpanItems();
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
    public PlannerContext getPlannerContext()
    {
        return coordinator.getPlannerContext();
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
    public TestingGroupProviderManager getGroupProvider()
    {
        return coordinator.getGroupProvider();
    }

    public SessionPropertyDefaults getSessionPropertyDefaults()
    {
        return coordinator.getSessionPropertyDefaults();
    }

    @Override
    public TestingTrinoServer getCoordinator()
    {
        return coordinator;
    }

    public List<TestingTrinoServer> getServers()
    {
        return ImmutableList.copyOf(servers);
    }

    @Override
    public void installPlugin(Plugin plugin)
    {
        plugins.add(plugin);
        servers.forEach(server -> server.installPlugin(plugin));
    }

    @Override
    public void addFunctions(FunctionBundle functionBundle)
    {
        functionBundles.add(functionBundle);
        servers.forEach(server -> server.addFunctions(functionBundle));
    }

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        long start = System.nanoTime();
        coordinator.createCatalog(catalogName, connectorName, properties);
        backupCoordinator.ifPresent(backup -> backup.createCatalog(catalogName, connectorName, properties));
        log.debug("Created catalog %s in %s", catalogName, nanosSince(start));
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
    public MaterializedResult execute(Session session, @Language("SQL") String sql)
    {
        return executeInternal(session, sql).getResult();
    }

    @Override
    public MaterializedResultWithPlan executeWithPlan(Session session, String sql)
    {
        ResultWithQueryId<MaterializedResult> result = executeInternal(session, sql);
        return new MaterializedResultWithPlan(result.getQueryId(), coordinator.getQueryPlan(result.getQueryId()), result.getResult());
    }

    private ResultWithQueryId<MaterializedResult> executeInternal(Session session, @Language("SQL") String sql)
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
            return coordinator.getQueryExplainer().getLogicalPlan(
                    session,
                    coordinator.getInstance(Key.get(SqlParser.class)).createStatement(sql),
                    ImmutableList.of(),
                    WarningCollector.NOOP,
                    createPlanOptimizersStatsCollector());
        }
        finally {
            concurrentQueries.decrementAndGet();
        }
    }

    public Plan getQueryPlan(QueryId queryId)
    {
        return coordinator.getQueryPlan(queryId).orElseThrow();
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
    public void loadSpoolingManager(String name, Map<String, String> properties)
    {
        for (TestingTrinoServer server : servers) {
            server.loadSpoolingManager(name, properties);
        }
    }

    @Override
    public final void close()
    {
        if (closed) {
            return;
        }
        cancelAllQueries();
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        discoveryServer = null;
        coordinator = null;
        backupCoordinator = Optional.empty();
        registerNewWorker = _ -> {
            throw new IllegalStateException("Already closed");
        };
        servers.clear();
        functionBundles.clear();
        plugins.clear();
        unclosedInstances.decrementAndGet();
        trinoClient = null;
        closed = true;
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
        private boolean withTracing;
        private int workerCount = 2;
        private Map<String, String> extraProperties = ImmutableMap.of();
        private Map<String, String> coordinatorProperties = ImmutableMap.of();
        private Optional<Map<String, String>> backupCoordinatorProperties = Optional.empty();
        private Consumer<QueryRunner> additionalSetup = queryRunner -> {};
        private String environment = ENVIRONMENT;
        private Module additionalModule = EMPTY_MODULE;
        private Optional<Path> baseDataDir = Optional.empty();
        private Optional<FactoryConfiguration> systemAccessControlConfiguration = Optional.empty();
        private Optional<List<SystemAccessControl>> systemAccessControls = Optional.empty();
        private List<EventListener> eventListeners = ImmutableList.of();
        private ImmutableList.Builder<AutoCloseable> extraCloseables = ImmutableList.builder();
        private TestingTrinoClientFactory testingTrinoClientFactory = TestingTrinoClient::new;
        private Optional<String> encoding = Optional.empty();

        protected Builder(Session defaultSession)
        {
            this.defaultSession = requireNonNull(defaultSession, "defaultSession is null");
            String tracingEnabled = firstNonNull(getenv("TESTS_TRACING_ENABLED"), "false");
            this.withTracing = parseBoolean(tracingEnabled) || tracingEnabled.equals("1");
        }

        @CanIgnoreReturnValue
        public SELF amendSession(Function<SessionBuilder, SessionBuilder> amendSession)
        {
            SessionBuilder builder = Session.builder(defaultSession);
            this.defaultSession = amendSession.apply(builder).build();
            return self();
        }

        @CanIgnoreReturnValue
        public SELF setWorkerCount(int workerCount)
        {
            this.workerCount = workerCount;
            return self();
        }

        @CanIgnoreReturnValue
        public SELF setExtraProperties(Map<String, String> extraProperties)
        {
            this.extraProperties = ImmutableMap.copyOf(extraProperties);
            return self();
        }

        @CanIgnoreReturnValue
        public SELF addExtraProperties(Map<String, String> extraProperties)
        {
            this.extraProperties = addProperties(this.extraProperties, extraProperties);
            return self();
        }

        @CanIgnoreReturnValue
        public SELF addExtraProperty(String key, String value)
        {
            this.extraProperties = addProperty(this.extraProperties, key, value);
            return self();
        }

        @CanIgnoreReturnValue
        public SELF setCoordinatorProperties(Map<String, String> coordinatorProperties)
        {
            this.coordinatorProperties = ImmutableMap.copyOf(coordinatorProperties);
            return self();
        }

        @CanIgnoreReturnValue
        public SELF addCoordinatorProperty(String key, String value)
        {
            this.coordinatorProperties = addProperty(this.coordinatorProperties, key, value);
            return self();
        }

        @CanIgnoreReturnValue
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
        @CanIgnoreReturnValue
        public SELF setAdditionalSetup(Consumer<QueryRunner> additionalSetup)
        {
            this.additionalSetup = requireNonNull(additionalSetup, "additionalSetup is null");
            return self();
        }

        @CanIgnoreReturnValue
        public SELF setEnvironment(String environment)
        {
            this.environment = environment;
            return self();
        }

        @CanIgnoreReturnValue
        public SELF setAdditionalModule(Module additionalModule)
        {
            this.additionalModule = requireNonNull(additionalModule, "additionalModules is null");
            return self();
        }

        @CanIgnoreReturnValue
        public SELF setBaseDataDir(Optional<Path> baseDataDir)
        {
            this.baseDataDir = requireNonNull(baseDataDir, "baseDataDir is null");
            return self();
        }

        @CanIgnoreReturnValue
        public SELF setSystemAccessControl(String name, Map<String, String> configuration)
        {
            this.systemAccessControlConfiguration = Optional.of(new FactoryConfiguration(name, configuration));
            return self();
        }

        @SuppressWarnings("unused")
        @CanIgnoreReturnValue
        public SELF setSystemAccessControl(SystemAccessControl systemAccessControl)
        {
            return setSystemAccessControls(ImmutableList.of(requireNonNull(systemAccessControl, "systemAccessControl is null")));
        }

        @SuppressWarnings("unused")
        @CanIgnoreReturnValue
        public SELF setSystemAccessControls(List<SystemAccessControl> systemAccessControls)
        {
            this.systemAccessControls = Optional.of(ImmutableList.copyOf(requireNonNull(systemAccessControls, "systemAccessControls is null")));
            return self();
        }

        @SuppressWarnings("unused")
        @CanIgnoreReturnValue
        public SELF setEventListener(EventListener eventListener)
        {
            return setEventListeners(ImmutableList.of(requireNonNull(eventListener, "eventListener is null")));
        }

        @SuppressWarnings("unused")
        @CanIgnoreReturnValue
        public SELF setEventListeners(List<EventListener> eventListeners)
        {
            this.eventListeners = ImmutableList.copyOf(requireNonNull(eventListeners, "eventListeners is null"));
            return self();
        }

        @SuppressWarnings("unused")
        @CanIgnoreReturnValue
        public SELF setTestingTrinoClientFactory(TestingTrinoClientFactory testingTrinoClientFactory)
        {
            this.testingTrinoClientFactory = requireNonNull(testingTrinoClientFactory, "testingTrinoClientFactory is null");
            return self();
        }

        @CanIgnoreReturnValue
        public SELF enableBackupCoordinator()
        {
            if (backupCoordinatorProperties.isEmpty()) {
                setBackupCoordinatorProperties(ImmutableMap.of());
            }
            return self();
        }

        @CanIgnoreReturnValue
        public SELF registerResource(AutoCloseable closeable)
        {
            checkState(extraCloseables != null, "query runner is already built");
            extraCloseables.add(requireNonNull(closeable, "closeable is null"));
            return self();
        }

        @CanIgnoreReturnValue
        public SELF registerResources(Iterable<? extends AutoCloseable> closeables)
        {
            closeables.forEach(this::registerResource);
            return self();
        }

        @CanIgnoreReturnValue
        public SELF withTracing()
        {
            this.withTracing = true;
            return self();
        }

        public SELF withProtocolSpooling(String encoding)
        {
            this.encoding = Optional.of(encoding);
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
            if (encoding.isPresent()) {
                setTestingTrinoClientFactory((server, session) -> createClient(server, session, encoding.get()));
                addExtraProperty("protocol.spooling.enabled", "true");
                // create smaller number of segments
                addExtraProperty("protocol.spooling.initial-segment-size", "16MB");
                addExtraProperty("protocol.spooling.maximum-segment-size", "32MB");
                addExtraProperty("protocol.spooling.shared-secret-key", randomAESKey());
                // LocalSpoolingManager doesn't support direct storage access
                addExtraProperty("protocol.spooling.retrieval-mode", "coordinator_proxy");
                setAdditionalSetup(queryRunner -> {
                    queryRunner.installPlugin(new LocalSpoolingPlugin());
                    queryRunner.loadSpoolingManager("test-local", Map.of());
                });
            }
            if (withTracing) {
                OpenTracingCollector collector = new OpenTracingCollector();
                collector.start();
                extraCloseables.add(collector);
                addExtraProperties(Map.of("tracing.enabled", "true", "tracing.exporter.endpoint", collector.getExporterEndpoint().toString()));
                checkState(eventListeners.isEmpty(), "eventListeners already set");
                setEventListener(new EventListener()
                {
                    @Override
                    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
                    {
                        String queryId = queryCompletedEvent.getMetadata().getQueryId();
                        log.info("TRACING: %s :: %s", queryId, collector.searchForQueryId(queryId));
                    }
                });
            }

            Optional<FactoryConfiguration> systemAccessControlConfiguration = this.systemAccessControlConfiguration;
            Optional<List<SystemAccessControl>> systemAccessControls = this.systemAccessControls;
            if (systemAccessControlConfiguration.isEmpty() && systemAccessControls.isEmpty()) {
                systemAccessControls = Optional.of(ImmutableList.of());
            }

            DistributedQueryRunner queryRunner = new DistributedQueryRunner(
                    defaultSession,
                    workerCount,
                    extraProperties,
                    coordinatorProperties,
                    backupCoordinatorProperties,
                    environment,
                    additionalModule,
                    baseDataDir,
                    systemAccessControlConfiguration,
                    systemAccessControls,
                    eventListeners,
                    extraCloseables.build(),
                    testingTrinoClientFactory);
            extraCloseables = null;

            try {
                additionalSetup.accept(queryRunner);
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }

            return queryRunner;
        }

        protected static Map<String, String> addProperties(Map<String, String> properties, Map<String, String> update)
        {
            return ImmutableMap.<String, String>builder()
                    .putAll(requireNonNull(properties, "properties is null"))
                    .putAll(requireNonNull(update, "update is null"))
                    .buildOrThrow();
        }

        protected static ImmutableMap<String, String> addProperty(Map<String, String> extraProperties, String key, String value)
        {
            return ImmutableMap.<String, String>builder()
                    .putAll(requireNonNull(extraProperties, "properties is null"))
                    .put(requireNonNull(key, "key is null"), requireNonNull(value, "value is null"))
                    .buildOrThrow();
        }
    }

    public interface TestingTrinoClientFactory
    {
        TestingTrinoClient create(TestingTrinoServer server, Session session);
    }

    private static TestingTrinoClient createClient(TestingTrinoServer testingTrinoServer, Session session, String encoding)
    {
        return new TestingTrinoClient(testingTrinoServer, new TestingStatementClientFactory() {
            @Override
            public StatementClient create(OkHttpClient httpClient, Session session, ClientSession clientSession, String query)
            {
                ClientSession clientSessionSpooled = ClientSession
                        .builder(clientSession)
                        .encoding(Optional.ofNullable(encoding))
                        .build();
                return newStatementClient(httpClient, clientSessionSpooled, query, Optional.empty());
            }
        }, session, new OkHttpClient());
    }

    private static String randomAESKey()
    {
        return Base64.getEncoder()
                .encodeToString(Ciphers.createRandomAesEncryptionKey().getEncoded());
    }
}
