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
package io.trino.server.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.net.HostAndPort;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ServiceSelectorManager;
import io.airlift.discovery.client.testing.TestingDiscoveryModule;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.http.server.tracing.TracingServletFilter;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.Level;
import io.airlift.log.Logging;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.openmetrics.JmxOpenMetricsModule;
import io.airlift.tracing.TracingModule;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.trino.Session;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.connector.CatalogManagerConfig.CatalogMangerKind;
import io.trino.connector.CatalogManagerModule;
import io.trino.connector.CatalogStoreManager;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.cost.StatsCalculator;
import io.trino.dispatcher.DispatchManager;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.FailureInjector;
import io.trino.execution.FailureInjector.InjectedFailureType;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.execution.SqlQueryManager;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.resourcegroups.InternalResourceGroupManager;
import io.trino.memory.ClusterMemoryManager;
import io.trino.memory.LocalMemoryManager;
import io.trino.metadata.AllNodes;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.FunctionBundle;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.security.GroupProviderManager;
import io.trino.server.NodeStateManager;
import io.trino.server.PluginInstaller;
import io.trino.server.PrefixObjectNameGeneratorModule;
import io.trino.server.QuerySessionSupplier;
import io.trino.server.ServerMainModule;
import io.trino.server.SessionContext;
import io.trino.server.SessionPropertyDefaults;
import io.trino.server.SessionSupplier;
import io.trino.server.ShutdownAction;
import io.trino.server.StartupStatus;
import io.trino.server.protocol.spooling.SpoolingManagerRegistry;
import io.trino.server.security.CertificateAuthenticatorManager;
import io.trino.server.security.ServerSecurityModule;
import io.trino.spi.ErrorType;
import io.trino.spi.Plugin;
import io.trino.spi.QueryId;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorName;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.session.PropertyMetadata;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.analyzer.QueryExplainer;
import io.trino.sql.analyzer.QueryExplainerFactory;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.Plan;
import io.trino.testing.ProcedureTester;
import io.trino.testing.TestingAccessControlManager;
import io.trino.testing.TestingEventListenerManager;
import io.trino.testing.TestingGroupProvider;
import io.trino.testing.TestingGroupProviderManager;
import io.trino.testing.TestingWarningCollectorModule;
import io.trino.tracing.ForTracing;
import io.trino.tracing.TracingAccessControl;
import io.trino.transaction.TransactionManager;
import io.trino.transaction.TransactionManagerModule;
import jakarta.servlet.Filter;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Integer.parseInt;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.isDirectory;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TestingTrinoServer
        implements Closeable
{
    static {
        Logging logging = Logging.initialize();
        logging.setLevel("io.trino.event.QueryMonitor", Level.ERROR);
        logging.setLevel("org.eclipse.jetty", Level.ERROR);
        logging.setLevel("io.airlift.concurrent.BoundedExecutor", Level.OFF);

        // Trino server behavior does not depend on locale settings.
        // Use en_US as this is what Trino is tested with.
        Locale.setDefault(Locale.US);
    }

    public static final String SESSION_START_TIME_PROPERTY = "session_start_time";
    private static final String VERSION = "testversion";

    public static TestingTrinoServer create()
    {
        return builder().build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private final Injector injector;
    private final Path baseDataDir;
    private final boolean preserveData;
    private final LifeCycleManager lifeCycleManager;
    private final PluginInstaller pluginInstaller;
    private final Optional<CatalogManager> catalogManager;
    private final TestingHttpServer server;
    private final TransactionManager transactionManager;
    private final TablePropertyManager tablePropertyManager;
    private final PlannerContext plannerContext;
    private final QueryExplainer queryExplainer;
    private final SessionPropertyManager sessionPropertyManager;
    private final GlobalFunctionCatalog globalFunctionCatalog;
    private final StatsCalculator statsCalculator;
    private final TestingAccessControlManager accessControl;
    private final TestingGroupProviderManager groupProvider;
    private final ProcedureTester procedureTester;
    private final Optional<InternalResourceGroupManager<?>> resourceGroupManager;
    private final SessionPropertyDefaults sessionPropertyDefaults;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final ClusterMemoryManager clusterMemoryManager;
    private final LocalMemoryManager localMemoryManager;
    private final InternalNodeManager nodeManager;
    private final ServiceSelectorManager serviceSelectorManager;
    private final DispatchManager dispatchManager;
    private final SqlQueryManager queryManager;
    private final SqlTaskManager taskManager;
    private final NodeStateManager nodeStateManager;
    private final ShutdownAction shutdownAction;
    private final MBeanServer mBeanServer;
    private final boolean coordinator;
    private final FailureInjector failureInjector;
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private final SpoolingManagerRegistry spoolingManagerRegistry;

    public static class TestShutdownAction
            implements ShutdownAction
    {
        private final CountDownLatch shutdownCalled = new CountDownLatch(1);

        @GuardedBy("this")
        private boolean isWorkerShutdown;

        @Override
        public synchronized void onShutdown()
        {
            isWorkerShutdown = true;
            shutdownCalled.countDown();
        }

        public void waitForShutdownComplete(long millis)
                throws InterruptedException
        {
            shutdownCalled.await(millis, MILLISECONDS);
        }

        public synchronized boolean isWorkerShutdown()
        {
            return isWorkerShutdown;
        }
    }

    private TestingTrinoServer(
            boolean coordinator,
            Map<String, String> properties,
            Optional<String> environment,
            Optional<URI> discoveryUri,
            Module additionalModule,
            Optional<Path> baseDataDir,
            Optional<SpanProcessor> spanProcessor,
            Optional<FactoryConfiguration> systemAccessControlConfiguration,
            Optional<FactoryConfiguration> spoolingConfiguration,
            Optional<List<SystemAccessControl>> systemAccessControls,
            List<EventListener> eventListeners,
            Consumer<TestingTrinoServer> additionalConfiguration,
            CatalogMangerKind catalogMangerKind)
    {
        this.coordinator = coordinator;

        this.baseDataDir = baseDataDir.orElseGet(TestingTrinoServer::tempDirectory);
        this.preserveData = baseDataDir.isPresent();

        properties = new HashMap<>(properties);
        int httpPort = parseInt(firstNonNull(properties.remove("http-server.http.port"), "0"));

        ImmutableMap.Builder<String, String> serverProperties = ImmutableMap.<String, String>builder()
                .putAll(properties)
                .put("coordinator", String.valueOf(coordinator))
                .put("catalog.management", catalogMangerKind.name())
                .put("task.concurrency", "4")
                .put("task.max-worker-threads", "4")
                // Use task.min-writer-count > 1, as this allows to expose writer-concurrency related bugs.
                .put("task.min-writer-count", "2")
                .put("exchange.client-threads", "4")
                // Reduce memory footprint in tests
                .put("exchange.max-buffer-size", "4MB")
                .put("internal-communication.shared-secret", "internal-shared-secret");

        if (coordinator) {
            if (catalogMangerKind == CatalogMangerKind.DYNAMIC) {
                Optional<String> catalogStore = Optional.ofNullable(properties.get("catalog.store"));
                if (catalogStore.isEmpty()) {
                    serverProperties.put("catalog.store", "memory");
                }
            }
            serverProperties.put("failure-detector.enabled", "false");

            // Reduce memory footprint in tests
            serverProperties.put("query.min-expire-age", "5s");
        }

        serverProperties.put("optimizer.ignore-stats-calculator-failures", "false");

        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new TestingNodeModule(environment))
                .add(new TestingHttpServerModule(httpPort))
                .add(new JsonModule())
                .add(new JaxrsModule())
                .add(new MBeanModule())
                .add(new PrefixObjectNameGeneratorModule("io.trino"))
                .add(new TestingJmxModule())
                .add(new JmxOpenMetricsModule())
                .add(new TracingModule("trino", VERSION))
                .add(new ServerSecurityModule())
                .add(new CatalogManagerModule())
                .add(new TransactionManagerModule())
                .add(new ServerMainModule(VERSION))
                .add(new TestingWarningCollectorModule())
                .add(binder -> {
                    newSetBinder(binder, Filter.class)
                            .addBinding()
                            .to(TracingServletFilter.class);
                    binder.bind(EventListenerConfig.class).in(Scopes.SINGLETON);
                    binder.bind(AccessControlConfig.class).in(Scopes.SINGLETON);
                    binder.bind(TestingAccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(TestingGroupProvider.class).in(Scopes.SINGLETON);
                    binder.bind(TestingEventListenerManager.class).in(Scopes.SINGLETON);
                    binder.bind(AccessControlManager.class).to(TestingAccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(EventListenerManager.class).to(TestingEventListenerManager.class).in(Scopes.SINGLETON);
                    binder.bind(TestingGroupProviderManager.class).in(Scopes.SINGLETON);
                    binder.bind(GroupProvider.class).to(TestingGroupProviderManager.class).in(Scopes.SINGLETON);
                    binder.bind(GroupProviderManager.class).to(TestingGroupProviderManager.class).in(Scopes.SINGLETON);
                    binder.bind(AccessControl.class).annotatedWith(ForTracing.class).to(AccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(AccessControl.class).to(TracingAccessControl.class).in(Scopes.SINGLETON);
                    binder.bind(ShutdownAction.class).to(TestShutdownAction.class).in(Scopes.SINGLETON);
                    binder.bind(NodeStateManager.class).in(Scopes.SINGLETON);
                    binder.bind(ProcedureTester.class).in(Scopes.SINGLETON);
                    binder.bind(ExchangeManagerRegistry.class).in(Scopes.SINGLETON);
                    spanProcessor.ifPresent(processor -> newSetBinder(binder, SpanProcessor.class).addBinding().toInstance(processor));

                    newSetBinder(binder, SystemSessionPropertiesProvider.class)
                            .addBinding().toInstance(() -> List.of(new PropertyMetadata<>(
                                    SESSION_START_TIME_PROPERTY,
                                    "Override session start time",
                                    VARCHAR,
                                    Instant.class,
                                    null,
                                    true,
                                    millis -> Instant.parse((String) millis),
                                    Instant::toString)));
                    if (coordinator) {
                        binder.bind(QuerySessionSupplier.class).in(Scopes.SINGLETON);
                        newOptionalBinder(binder, SessionSupplier.class).setBinding().to(TestingSessionSupplier.class).in(Scopes.SINGLETON);
                    }
                });

        if (coordinator) {
            modules.add(new TestingSessionTimeModule());
        }

        if (discoveryUri.isPresent()) {
            requireNonNull(environment, "environment required when discoveryUri is present");
            serverProperties.put("discovery.uri", discoveryUri.get().toString());
            modules.add(new DiscoveryModule());
        }
        else {
            modules.add(new TestingDiscoveryModule());
        }

        modules.add(additionalModule);

        Bootstrap app = new Bootstrap(modules.build());

        Map<String, String> optionalProperties = new HashMap<>();
        environment.ifPresent(env -> optionalProperties.put("node.environment", env));

        injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(serverProperties.buildOrThrow())
                .setOptionalConfigurationProperties(optionalProperties)
                .quiet()
                .initialize();

        injector.getInstance(Announcer.class).start();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        pluginInstaller = injector.getInstance(PluginInstaller.class);

        var catalogStoreManager = injector.getInstance(Key.get(new TypeLiteral<Optional<CatalogStoreManager>>() {}));
        catalogStoreManager.ifPresent(CatalogStoreManager::loadConfiguredCatalogStore);

        Optional<CatalogManager> catalogManager = Optional.empty();
        if (injector.getExistingBinding(Key.get(CatalogManager.class)) != null) {
            catalogManager = Optional.of(injector.getInstance(CatalogManager.class));
        }
        this.catalogManager = catalogManager;

        server = injector.getInstance(TestingHttpServer.class);
        transactionManager = injector.getInstance(TransactionManager.class);
        tablePropertyManager = injector.getInstance(TablePropertyManager.class);
        globalFunctionCatalog = injector.getInstance(GlobalFunctionCatalog.class);
        plannerContext = injector.getInstance(PlannerContext.class);
        accessControl = injector.getInstance(TestingAccessControlManager.class);
        groupProvider = injector.getInstance(TestingGroupProviderManager.class);
        procedureTester = injector.getInstance(ProcedureTester.class);
        splitManager = injector.getInstance(SplitManager.class);
        pageSourceManager = injector.getInstance(PageSourceManager.class);
        sessionPropertyManager = injector.getInstance(SessionPropertyManager.class);
        if (coordinator) {
            dispatchManager = injector.getInstance(DispatchManager.class);
            queryManager = (SqlQueryManager) injector.getInstance(QueryManager.class);
            queryExplainer = injector.getInstance(QueryExplainerFactory.class)
                    .createQueryExplainer(injector.getInstance(AnalyzerFactory.class));
            resourceGroupManager = Optional.of((InternalResourceGroupManager<?>) injector.getInstance(InternalResourceGroupManager.class));
            sessionPropertyDefaults = injector.getInstance(SessionPropertyDefaults.class);
            nodePartitioningManager = injector.getInstance(NodePartitioningManager.class);
            clusterMemoryManager = injector.getInstance(ClusterMemoryManager.class);
            statsCalculator = injector.getInstance(StatsCalculator.class);
            injector.getInstance(CertificateAuthenticatorManager.class).useDefaultAuthenticator();
        }
        else {
            dispatchManager = null;
            queryManager = null;
            queryExplainer = null;
            resourceGroupManager = Optional.empty();
            sessionPropertyDefaults = null;
            nodePartitioningManager = null;
            clusterMemoryManager = null;
            statsCalculator = null;
        }
        localMemoryManager = injector.getInstance(LocalMemoryManager.class);
        nodeManager = injector.getInstance(InternalNodeManager.class);
        serviceSelectorManager = injector.getInstance(ServiceSelectorManager.class);
        nodeStateManager = injector.getInstance(NodeStateManager.class);
        taskManager = injector.getInstance(SqlTaskManager.class);
        shutdownAction = injector.getInstance(ShutdownAction.class);
        mBeanServer = injector.getInstance(MBeanServer.class);
        failureInjector = injector.getInstance(FailureInjector.class);
        exchangeManagerRegistry = injector.getInstance(ExchangeManagerRegistry.class);
        spoolingManagerRegistry = injector.getInstance(SpoolingManagerRegistry.class);

        systemAccessControlConfiguration.ifPresentOrElse(
                configuration -> {
                    checkArgument(systemAccessControls.isEmpty(), "systemAccessControlConfiguration and systemAccessControls cannot be both present");
                    accessControl.loadSystemAccessControl(configuration.factoryName(), configuration.configuration());
                },
                () -> accessControl.setSystemAccessControls(systemAccessControls.orElseThrow()));

        spoolingConfiguration.ifPresent(config ->
                spoolingManagerRegistry.loadSpoolingManager(config.factoryName(), config.configuration()));

        EventListenerManager eventListenerManager = injector.getInstance(EventListenerManager.class);
        eventListeners.forEach(eventListenerManager::addEventListener);

        getFutureValue(injector.getInstance(Announcer.class).forceAnnounce());
        // Must be run before startup is considered complete and node will therefore accept tasks.
        // Technically `this` reference might escape here. However, the object is fully constructed.
        additionalConfiguration.accept(this);
        injector.getInstance(StartupStatus.class).startupComplete();

        refreshNodes();
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(() -> {
                if (isDirectory(baseDataDir) && !preserveData) {
                    deleteRecursively(baseDataDir, ALLOW_INSECURE);
                }
            });

            closer.register(() -> {
                if (lifeCycleManager != null) {
                    lifeCycleManager.stop();
                }
            });
        }
    }

    public void installPlugin(Plugin plugin)
    {
        pluginInstaller.installPlugin(plugin);
    }

    public DispatchManager getDispatchManager()
    {
        return dispatchManager;
    }

    public QueryManager getQueryManager()
    {
        return queryManager;
    }

    public Optional<Plan> getQueryPlan(QueryId queryId)
    {
        return queryManager.getQueryPlan(queryId);
    }

    public QueryInfo getFullQueryInfo(QueryId queryId)
    {
        return queryManager.getFullQueryInfo(queryId);
    }

    public void addFinalQueryInfoListener(QueryId queryId, StateChangeListener<QueryInfo> stateChangeListener)
    {
        queryManager.addFinalQueryInfoListener(queryId, stateChangeListener);
    }

    public void createCatalog(String catalogName, String connectorName)
    {
        createCatalog(catalogName, connectorName, ImmutableMap.of());
    }

    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        if (catalogManager.isEmpty()) {
            // this is a worker so catalogs are dynamically registered
            return;
        }
        catalogManager.get().createCatalog(new CatalogName(catalogName), new ConnectorName(connectorName), properties, false);
    }

    public void loadExchangeManager(String name, Map<String, String> properties)
    {
        exchangeManagerRegistry.loadExchangeManager(name, properties);
    }

    public void loadSpoolingManager(String name, Map<String, String> properties)
    {
        spoolingManagerRegistry.loadSpoolingManager(name, properties);
    }

    public Path getBaseDataDir()
    {
        return baseDataDir;
    }

    public URI getBaseUrl()
    {
        return server.getBaseUrl();
    }

    public URI getHttpsBaseUrl()
    {
        return server.getHttpServerInfo().getHttpsUri();
    }

    public URI resolve(String path)
    {
        return server.getBaseUrl().resolve(path);
    }

    public HostAndPort getAddress()
    {
        return HostAndPort.fromParts(getBaseUrl().getHost(), getBaseUrl().getPort());
    }

    public HostAndPort getHttpsAddress()
    {
        URI httpsUri = server.getHttpServerInfo().getHttpsUri();
        return HostAndPort.fromParts(httpsUri.getHost(), httpsUri.getPort());
    }

    public TransactionManager getTransactionManager()
    {
        return transactionManager;
    }

    public TablePropertyManager getTablePropertyManager()
    {
        return tablePropertyManager;
    }

    public PlannerContext getPlannerContext()
    {
        return plannerContext;
    }

    public QueryExplainer getQueryExplainer()
    {
        return queryExplainer;
    }

    public SessionPropertyManager getSessionPropertyManager()
    {
        return sessionPropertyManager;
    }

    public void addFunctions(FunctionBundle functionBundle)
    {
        globalFunctionCatalog.addFunctions(functionBundle);
    }

    public StatsCalculator getStatsCalculator()
    {
        checkState(coordinator, "not a coordinator");
        return statsCalculator;
    }

    public TestingAccessControlManager getAccessControl()
    {
        return accessControl;
    }

    public TestingGroupProviderManager getGroupProvider()
    {
        return groupProvider;
    }

    public ProcedureTester getProcedureTester()
    {
        return procedureTester;
    }

    public SplitManager getSplitManager()
    {
        return splitManager;
    }

    public PageSourceManager getPageSourceManager()
    {
        return pageSourceManager;
    }

    public Optional<InternalResourceGroupManager<?>> getResourceGroupManager()
    {
        return resourceGroupManager;
    }

    public SessionPropertyDefaults getSessionPropertyDefaults()
    {
        return sessionPropertyDefaults;
    }

    public NodePartitioningManager getNodePartitioningManager()
    {
        return nodePartitioningManager;
    }

    public LocalMemoryManager getLocalMemoryManager()
    {
        return localMemoryManager;
    }

    public ClusterMemoryManager getClusterMemoryManager()
    {
        checkState(coordinator, "not a coordinator");
        return clusterMemoryManager;
    }

    public MBeanServer getMbeanServer()
    {
        return mBeanServer;
    }

    public NodeStateManager getNodeStateManager()
    {
        return nodeStateManager;
    }

    public SqlTaskManager getTaskManager()
    {
        return taskManager;
    }

    public ShutdownAction getShutdownAction()
    {
        return shutdownAction;
    }

    public Connector getConnector(String catalogName)
    {
        checkState(coordinator, "not a coordinator");
        CatalogHandle catalogHandle = catalogManager.orElseThrow().getCatalog(new CatalogName(catalogName))
                .orElseThrow(() -> new IllegalArgumentException("Catalog '%s' not found".formatted(catalogName)))
                .getCatalogHandle();
        return injector.getInstance(ConnectorServicesProvider.class)
                .getConnectorServices(catalogHandle)
                .getConnector();
    }

    public boolean isCoordinator()
    {
        return coordinator;
    }

    public final AllNodes refreshNodes()
    {
        serviceSelectorManager.forceRefresh();
        nodeManager.refreshNodes();
        return nodeManager.getAllNodes();
    }

    public <T> T getInstance(Key<T> key)
    {
        return injector.getInstance(key);
    }

    public void injectTaskFailure(
            String traceToken,
            int stageId,
            int partitionId,
            int attemptId,
            InjectedFailureType injectionType,
            Optional<ErrorType> errorType)
    {
        failureInjector.injectTaskFailure(
                traceToken,
                stageId,
                partitionId,
                attemptId,
                injectionType,
                errorType);
    }

    private static Path tempDirectory()
    {
        try {
            return createTempDirectory("TrinoTest");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class Builder
    {
        private boolean coordinator = true;
        private Map<String, String> properties = ImmutableMap.of();
        private Optional<String> environment = Optional.empty();
        private Optional<URI> discoveryUri = Optional.empty();
        private Module additionalModule = EMPTY_MODULE;
        private Optional<Path> baseDataDir = Optional.empty();
        private Optional<SpanProcessor> spanProcessor = Optional.empty();
        private Optional<FactoryConfiguration> systemAccessControlConfiguration = Optional.empty();
        private Optional<FactoryConfiguration> spoolingConfiguration = Optional.empty();
        private Optional<List<SystemAccessControl>> systemAccessControls = Optional.of(ImmutableList.of());
        private List<EventListener> eventListeners = ImmutableList.of();
        private Consumer<TestingTrinoServer> additionalConfiguration = _ -> {};
        private CatalogMangerKind catalogMangerKind = CatalogMangerKind.DYNAMIC;

        public Builder setCoordinator(boolean coordinator)
        {
            this.coordinator = coordinator;
            return this;
        }

        public Builder addProperty(String name, String value)
        {
            this.properties = ImmutableMap.<String, String>builder()
                    .putAll(properties)
                    .put(name, value)
                    .buildOrThrow();
            return this;
        }

        public Builder setProperties(Map<String, String> properties)
        {
            this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
            return this;
        }

        public Builder setEnvironment(String environment)
        {
            this.environment = Optional.of(environment);
            return this;
        }

        public Builder setDiscoveryUri(URI discoveryUri)
        {
            this.discoveryUri = Optional.of(discoveryUri);
            return this;
        }

        public Builder setAdditionalModule(Module additionalModule)
        {
            this.additionalModule = requireNonNull(additionalModule, "additionalModule is null");
            return this;
        }

        public Builder setBaseDataDir(Optional<Path> baseDataDir)
        {
            this.baseDataDir = requireNonNull(baseDataDir, "baseDataDir is null");
            return this;
        }

        public Builder setSpanProcessor(SpanProcessor spanProcessor)
        {
            this.spanProcessor = Optional.of(spanProcessor);
            return this;
        }

        public Builder setSystemAccessControlConfiguration(Optional<FactoryConfiguration> systemAccessControlConfiguration)
        {
            this.systemAccessControlConfiguration = requireNonNull(systemAccessControlConfiguration, "systemAccessControlConfiguration is null");
            return this;
        }

        public Builder setSystemAccessControl(SystemAccessControl systemAccessControl)
        {
            return setSystemAccessControls(Optional.of(ImmutableList.of(requireNonNull(systemAccessControl, "systemAccessControl is null"))));
        }

        public Builder setSystemAccessControls(Optional<List<SystemAccessControl>> systemAccessControls)
        {
            this.systemAccessControls = systemAccessControls.map(ImmutableList::copyOf);
            return this;
        }

        public Builder setEventListeners(List<EventListener> eventListeners)
        {
            this.eventListeners = ImmutableList.copyOf(requireNonNull(eventListeners, "eventListeners is null"));
            return this;
        }

        public Builder setAdditionalConfiguration(Consumer<TestingTrinoServer> additionalConfiguration)
        {
            this.additionalConfiguration = additionalConfiguration;
            return this;
        }

        public Builder setCatalogMangerKind(CatalogMangerKind catalogMangerKind)
        {
            this.catalogMangerKind = requireNonNull(catalogMangerKind, "catalogMangerKind is null");
            return this;
        }

        public TestingTrinoServer build()
        {
            return new TestingTrinoServer(
                    coordinator,
                    properties,
                    environment,
                    discoveryUri,
                    additionalModule,
                    baseDataDir,
                    spanProcessor,
                    systemAccessControlConfiguration,
                    spoolingConfiguration,
                    systemAccessControls,
                    eventListeners,
                    additionalConfiguration,
                    catalogMangerKind);
        }
    }

    private static class TestingSessionSupplier
            implements SessionSupplier
    {
        private final QuerySessionSupplier querySessionSupplier;

        @Inject
        public TestingSessionSupplier(QuerySessionSupplier querySessionSupplier)
        {
            this.querySessionSupplier = querySessionSupplier;
        }

        @Override
        public Session createSession(QueryId queryId, Span querySpan, SessionContext context)
        {
            Session session = querySessionSupplier.createSession(queryId, querySpan, context);
            Instant sessionStart = session.getSystemProperty(SESSION_START_TIME_PROPERTY, Instant.class);
            if (sessionStart != null) {
                session = Session.builder(session)
                        .setStart(sessionStart)
                        .build();
            }
            return session;
        }
    }
}
