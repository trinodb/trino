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
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ServiceSelectorManager;
import io.airlift.discovery.client.testing.TestingDiscoveryModule;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.openmetrics.JmxOpenMetricsModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.trino.connector.CatalogManagerModule;
import io.trino.connector.ConnectorName;
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
import io.trino.metadata.FunctionManager;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.ProcedureRegistry;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.security.GroupProviderManager;
import io.trino.server.GracefulShutdownHandler;
import io.trino.server.PluginInstaller;
import io.trino.server.Server;
import io.trino.server.ServerMainModule;
import io.trino.server.SessionPropertyDefaults;
import io.trino.server.ShutdownAction;
import io.trino.server.security.CertificateAuthenticatorManager;
import io.trino.server.security.ServerSecurityModule;
import io.trino.spi.ErrorType;
import io.trino.spi.Plugin;
import io.trino.spi.QueryId;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.type.TypeManager;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
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
import io.trino.transaction.TransactionManager;
import io.trino.transaction.TransactionManagerModule;
import org.weakref.jmx.guice.MBeanModule;

import javax.annotation.concurrent.GuardedBy;
import javax.management.MBeanServer;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static java.lang.Integer.parseInt;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.isDirectory;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TestingTrinoServer
        implements Closeable
{
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
    private final Metadata metadata;
    private final TypeManager typeManager;
    private final QueryExplainer queryExplainer;
    private final SessionPropertyManager sessionPropertyManager;
    private final FunctionManager functionManager;
    private final GlobalFunctionCatalog globalFunctionCatalog;
    private final StatsCalculator statsCalculator;
    private final ProcedureRegistry procedureRegistry;
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
    private final GracefulShutdownHandler gracefulShutdownHandler;
    private final ShutdownAction shutdownAction;
    private final MBeanServer mBeanServer;
    private final boolean coordinator;
    private final FailureInjector failureInjector;
    private final ExchangeManagerRegistry exchangeManagerRegistry;

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
            List<SystemAccessControl> systemAccessControls,
            List<EventListener> eventListeners)
    {
        this.coordinator = coordinator;

        this.baseDataDir = baseDataDir.orElseGet(TestingTrinoServer::tempDirectory);
        this.preserveData = baseDataDir.isPresent();

        properties = new HashMap<>(properties);
        String coordinatorPort = properties.remove("http-server.http.port");
        if (coordinatorPort == null) {
            coordinatorPort = "0";
        }

        ImmutableMap.Builder<String, String> serverProperties = ImmutableMap.<String, String>builder()
                .putAll(properties)
                .put("coordinator", String.valueOf(coordinator))
                .put("catalog.management", "dynamic")
                .put("task.concurrency", "4")
                .put("task.max-worker-threads", "4")
                // Use task.writer-count > 1, as this allows to expose writer-concurrency related bugs.
                .put("task.writer-count", "2")
                .put("exchange.client-threads", "4")
                // Reduce memory footprint in tests
                .put("exchange.max-buffer-size", "4MB")
                .put("internal-communication.shared-secret", "internal-shared-secret");

        if (coordinator) {
            // TODO: enable failure detector
            serverProperties.put("failure-detector.enabled", "false");
            serverProperties.put("catalog.store", "memory");

            // Reduce memory footprint in tests
            serverProperties.put("query.min-expire-age", "5s");
        }

        serverProperties.put("optimizer.ignore-stats-calculator-failures", "false");

        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new TestingNodeModule(environment))
                .add(new TestingHttpServerModule(parseInt(coordinator ? coordinatorPort : "0")))
                .add(new JsonModule())
                .add(new JaxrsModule())
                .add(new MBeanModule())
                .add(new TestingJmxModule())
                .add(new JmxOpenMetricsModule())
                .add(new EventModule())
                .add(new TraceTokenModule())
                .add(new ServerSecurityModule())
                .add(new CatalogManagerModule())
                .add(new TransactionManagerModule())
                .add(new ServerMainModule("testversion"))
                .add(new TestingWarningCollectorModule())
                .add(binder -> {
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
                    binder.bind(AccessControl.class).to(AccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(ShutdownAction.class).to(TestShutdownAction.class).in(Scopes.SINGLETON);
                    binder.bind(GracefulShutdownHandler.class).in(Scopes.SINGLETON);
                    binder.bind(ProcedureTester.class).in(Scopes.SINGLETON);
                    binder.bind(ExchangeManagerRegistry.class).in(Scopes.SINGLETON);
                });

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

        Optional<CatalogManager> catalogManager = Optional.empty();
        if (injector.getExistingBinding(Key.get(CatalogManager.class)) != null) {
            catalogManager = Optional.of(injector.getInstance(CatalogManager.class));
        }
        this.catalogManager = catalogManager;

        server = injector.getInstance(TestingHttpServer.class);
        transactionManager = injector.getInstance(TransactionManager.class);
        globalFunctionCatalog = injector.getInstance(GlobalFunctionCatalog.class);
        metadata = injector.getInstance(Metadata.class);
        typeManager = injector.getInstance(TypeManager.class);
        functionManager = injector.getInstance(FunctionManager.class);
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
            procedureRegistry = injector.getInstance(ProcedureRegistry.class);
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
            procedureRegistry = null;
        }
        localMemoryManager = injector.getInstance(LocalMemoryManager.class);
        nodeManager = injector.getInstance(InternalNodeManager.class);
        serviceSelectorManager = injector.getInstance(ServiceSelectorManager.class);
        gracefulShutdownHandler = injector.getInstance(GracefulShutdownHandler.class);
        taskManager = injector.getInstance(SqlTaskManager.class);
        shutdownAction = injector.getInstance(ShutdownAction.class);
        mBeanServer = injector.getInstance(MBeanServer.class);
        failureInjector = injector.getInstance(FailureInjector.class);
        exchangeManagerRegistry = injector.getInstance(ExchangeManagerRegistry.class);

        accessControl.setSystemAccessControls(systemAccessControls);

        EventListenerManager eventListenerManager = injector.getInstance(EventListenerManager.class);
        eventListeners.forEach(eventListenerManager::addEventListener);

        injector.getInstance(Announcer.class).forceAnnounce();

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
        pluginInstaller.installPlugin(plugin, ignored -> plugin.getClass().getClassLoader());
    }

    public DispatchManager getDispatchManager()
    {
        return dispatchManager;
    }

    public QueryManager getQueryManager()
    {
        return queryManager;
    }

    public Plan getQueryPlan(QueryId queryId)
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
        catalogManager.get().createCatalog(catalogName, new ConnectorName(connectorName), properties, false);
    }

    public void loadExchangeManager(String name, Map<String, String> properties)
    {
        exchangeManagerRegistry.loadExchangeManager(name, properties);
    }

    /**
     * Add the event listeners from connectors.  Connector event listeners are
     * only supported for statically loaded catalogs, and this doesn't match up
     * with the model of the testing Trino server.  This method should only be
     * called once after all catalogs are added.
     */
    public void addConnectorEventListeners()
    {
        Server.addConnectorEventListeners(
                injector.getInstance(CatalogManager.class),
                injector.getInstance(ConnectorServicesProvider.class),
                injector.getInstance(EventListenerManager.class));
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

    public Metadata getMetadata()
    {
        return metadata;
    }

    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    public QueryExplainer getQueryExplainer()
    {
        return queryExplainer;
    }

    public SessionPropertyManager getSessionPropertyManager()
    {
        return sessionPropertyManager;
    }

    public FunctionManager getFunctionManager()
    {
        return functionManager;
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

    public ProcedureRegistry getProcedureRegistry()
    {
        return procedureRegistry;
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

    public ExchangeManager getExchangeManager()
    {
        return exchangeManagerRegistry.getExchangeManager();
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

    public GracefulShutdownHandler getGracefulShutdownHandler()
    {
        return gracefulShutdownHandler;
    }

    public SqlTaskManager getTaskManager()
    {
        return taskManager;
    }

    public ShutdownAction getShutdownAction()
    {
        return shutdownAction;
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

    public void waitForNodeRefresh(Duration timeout)
            throws InterruptedException, TimeoutException
    {
        Instant start = Instant.now();
        while (refreshNodes().getActiveNodes().size() < 1) {
            if (Duration.between(start, Instant.now()).compareTo(timeout) > 0) {
                throw new TimeoutException("Timed out while waiting for the node to refresh");
            }
            MILLISECONDS.sleep(10);
        }
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
        private List<SystemAccessControl> systemAccessControls = ImmutableList.of();
        private List<EventListener> eventListeners = ImmutableList.of();

        public Builder setCoordinator(boolean coordinator)
        {
            this.coordinator = coordinator;
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

        public Builder setSystemAccessControls(List<SystemAccessControl> systemAccessControls)
        {
            this.systemAccessControls = ImmutableList.copyOf(requireNonNull(systemAccessControls, "systemAccessControls is null"));
            return this;
        }

        public Builder setEventListeners(List<EventListener> eventListeners)
        {
            this.eventListeners = ImmutableList.copyOf(requireNonNull(eventListeners, "eventListeners is null"));
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
                    systemAccessControls,
                    eventListeners);
        }
    }
}
