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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.slice.Slice;
import io.airlift.stats.GcMonitor;
import io.airlift.stats.JmxGcMonitor;
import io.airlift.stats.PauseMeter;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.FeaturesConfig;
import io.trino.SystemSessionProperties;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.block.BlockJsonSerde;
import io.trino.client.NodeVersion;
import io.trino.connector.system.SystemConnectorModule;
import io.trino.dispatcher.DispatchManager;
import io.trino.event.SplitMonitor;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.ExplainAnalyzeContext;
import io.trino.execution.FailureInjectionConfig;
import io.trino.execution.FailureInjector;
import io.trino.execution.LocationFactory;
import io.trino.execution.MemoryRevokingScheduler;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.TableExecuteContextManager;
import io.trino.execution.TaskManagementExecutor;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.executor.MultilevelSplitQueue;
import io.trino.execution.executor.TaskExecutor;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.TopologyAwareNodeSelectorModule;
import io.trino.execution.scheduler.UniformNodeSelectorModule;
import io.trino.index.IndexManager;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.LocalMemoryManagerExporter;
import io.trino.memory.MemoryInfo;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.MemoryResource;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.DisabledSystemSecurityMetadata;
import io.trino.metadata.DiscoveryNodeManager;
import io.trino.metadata.ForNodeManager;
import io.trino.metadata.FunctionBundle;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.HandleJsonModule;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.LiteralFunction;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.ProcedureRegistry;
import io.trino.metadata.SystemFunctionBundle;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.metadata.TableFunctionRegistry;
import io.trino.metadata.TableProceduresRegistry;
import io.trino.metadata.TypeRegistry;
import io.trino.operator.DirectExchangeClientConfig;
import io.trino.operator.DirectExchangeClientFactory;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.operator.ForExchange;
import io.trino.operator.GroupByHashPageIndexerFactory;
import io.trino.operator.OperatorFactories;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.operator.RetryPolicy;
import io.trino.operator.TrinoOperatorFactories;
import io.trino.operator.index.IndexJoinLookupStats;
import io.trino.operator.scalar.json.JsonExistsFunction;
import io.trino.operator.scalar.json.JsonQueryFunction;
import io.trino.operator.scalar.json.JsonValueFunction;
import io.trino.server.ExpressionSerialization.ExpressionDeserializer;
import io.trino.server.ExpressionSerialization.ExpressionSerializer;
import io.trino.server.PluginManager.PluginsProvider;
import io.trino.server.SliceSerialization.SliceDeserializer;
import io.trino.server.SliceSerialization.SliceSerializer;
import io.trino.server.protocol.PreparedStatementEncoder;
import io.trino.server.remotetask.HttpLocationFactory;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;
import io.trino.spiller.FileSingleStreamSpillerFactory;
import io.trino.spiller.GenericPartitioningSpillerFactory;
import io.trino.spiller.GenericSpillerFactory;
import io.trino.spiller.LocalSpillManager;
import io.trino.spiller.NodeSpillConfig;
import io.trino.spiller.PartitioningSpillerFactory;
import io.trino.spiller.SingleStreamSpillerFactory;
import io.trino.spiller.SpillerFactory;
import io.trino.spiller.SpillerStats;
import io.trino.split.PageSinkManager;
import io.trino.split.PageSinkProvider;
import io.trino.split.PageSourceManager;
import io.trino.split.PageSourceProvider;
import io.trino.split.SplitManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.SqlEnvironmentConfig;
import io.trino.sql.analyzer.SessionTimeProvider;
import io.trino.sql.analyzer.StatementAnalyzerFactory;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.CompilerConfig;
import io.trino.sql.planner.LocalExecutionPlanner;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.tree.Expression;
import io.trino.type.BlockTypeOperators;
import io.trino.type.InternalTypeManager;
import io.trino.type.JsonPath2016Type;
import io.trino.type.TypeDeserializer;
import io.trino.type.TypeOperatorsCache;
import io.trino.type.TypeSignatureDeserializer;
import io.trino.type.TypeSignatureKeyDeserializer;
import io.trino.util.FinalizerService;
import io.trino.version.EmbedVersion;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.execution.scheduler.NodeSchedulerConfig.NodeSchedulerPolicy.TOPOLOGY;
import static io.trino.execution.scheduler.NodeSchedulerConfig.NodeSchedulerPolicy.UNIFORM;
import static io.trino.operator.RetryPolicy.TASK;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ServerMainModule
        extends AbstractConfigurationAwareModule
{
    private final String nodeVersion;

    public ServerMainModule(String nodeVersion)
    {
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        ServerConfig serverConfig = buildConfigObject(ServerConfig.class);

        if (serverConfig.isCoordinator()) {
            install(new CoordinatorModule());
        }
        else {
            install(new WorkerModule());
        }

        binder.bind(StartupStatus.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfigDefaults(HttpServerConfig.class, httpServerConfig -> {
            httpServerConfig.setAdminEnabled(false);
        });

        binder.bind(PreparedStatementEncoder.class).in(Scopes.SINGLETON);
        binder.bind(HttpRequestSessionContextFactory.class).in(Scopes.SINGLETON);
        install(new InternalCommunicationModule());

        QueryManagerConfig queryManagerConfig = buildConfigObject(QueryManagerConfig.class);
        RetryPolicy retryPolicy = queryManagerConfig.getRetryPolicy();
        if (retryPolicy == TASK) {
            configBinder(binder).bindConfigDefaults(QueryManagerConfig.class, QueryManagerConfig::applyFaultTolerantExecutionDefaults);
        }

        configBinder(binder).bindConfig(FeaturesConfig.class);
        if (retryPolicy == TASK) {
            configBinder(binder).bindConfigDefaults(FeaturesConfig.class, FeaturesConfig::applyFaultTolerantExecutionDefaults);
        }

        configBinder(binder).bindConfig(OptimizerConfig.class);
        configBinder(binder).bindConfig(ProtocolConfig.class);

        binder.bind(SqlParser.class).in(Scopes.SINGLETON);

        jaxrsBinder(binder).bind(ThrowableMapper.class);

        configBinder(binder).bindConfig(SqlEnvironmentConfig.class);

        newOptionalBinder(binder, ExplainAnalyzeContext.class);
        binder.bind(StatementAnalyzerFactory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, SessionTimeProvider.class)
                .setDefault().toInstance(SessionTimeProvider.DEFAULT);

        // GC Monitor
        binder.bind(GcMonitor.class).to(JmxGcMonitor.class).in(Scopes.SINGLETON);
        newExporter(binder).export(GcMonitor.class).withGeneratedName();

        // session properties
        newSetBinder(binder, SystemSessionPropertiesProvider.class).addBinding().to(SystemSessionProperties.class);
        binder.bind(SystemSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(SessionPropertyDefaults.class).in(Scopes.SINGLETON);

        // node manager
        discoveryBinder(binder).bindSelector("trino");
        binder.bind(DiscoveryNodeManager.class).in(Scopes.SINGLETON);
        binder.bind(InternalNodeManager.class).to(DiscoveryNodeManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(DiscoveryNodeManager.class).withGeneratedName();
        httpClientBinder(binder).bindHttpClient("node-manager", ForNodeManager.class)
                .withTracing()
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                });

        // node scheduler
        // TODO: remove from NodePartitioningManager and move to CoordinatorModule
        configBinder(binder).bindConfig(NodeSchedulerConfig.class);
        binder.bind(NodeScheduler.class).in(Scopes.SINGLETON);
        binder.bind(NodeTaskMap.class).in(Scopes.SINGLETON);
        newExporter(binder).export(NodeScheduler.class).withGeneratedName();

        // network topology
        // TODO: move to CoordinatorModule when NodeScheduler is moved
        install(conditionalModule(
                NodeSchedulerConfig.class,
                config -> UNIFORM == config.getNodeSchedulerPolicy(),
                new UniformNodeSelectorModule()));
        install(conditionalModule(
                NodeSchedulerConfig.class,
                config -> TOPOLOGY == config.getNodeSchedulerPolicy(),
                new TopologyAwareNodeSelectorModule()));

        // task execution
        configBinder(binder).bindConfig(FailureInjectionConfig.class);
        binder.bind(FailureInjector.class).in(Scopes.SINGLETON);
        jaxrsBinder(binder).bind(TaskResource.class);
        newExporter(binder).export(TaskResource.class).withGeneratedName();
        jaxrsBinder(binder).bind(TaskExecutorResource.class);
        newExporter(binder).export(TaskExecutorResource.class).withGeneratedName();
        binder.bind(TaskManagementExecutor.class).in(Scopes.SINGLETON);
        binder.bind(SqlTaskManager.class).in(Scopes.SINGLETON);
        binder.bind(TableExecuteContextManager.class).in(Scopes.SINGLETON);

        // memory revoking scheduler
        binder.bind(MemoryRevokingScheduler.class).in(Scopes.SINGLETON);

        // Add monitoring for JVM pauses
        binder.bind(PauseMeter.class).in(Scopes.SINGLETON);
        newExporter(binder).export(PauseMeter.class).withGeneratedName();

        configBinder(binder).bindConfig(MemoryManagerConfig.class);
        if (retryPolicy == TASK) {
            configBinder(binder).bindConfigDefaults(MemoryManagerConfig.class, MemoryManagerConfig::applyFaultTolerantExecutionDefaults);
        }

        configBinder(binder).bindConfig(NodeMemoryConfig.class);
        binder.bind(LocalMemoryManager.class).in(Scopes.SINGLETON);
        binder.bind(LocalMemoryManagerExporter.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, VersionEmbedder.class).setDefault().to(EmbedVersion.class).in(Scopes.SINGLETON);
        newExporter(binder).export(SqlTaskManager.class).withGeneratedName();
        binder.bind(TaskExecutor.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TaskExecutor.class).withGeneratedName();
        binder.bind(MultilevelSplitQueue.class).in(Scopes.SINGLETON);
        newExporter(binder).export(MultilevelSplitQueue.class).withGeneratedName();
        binder.bind(LocalExecutionPlanner.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(CompilerConfig.class);
        binder.bind(ExpressionCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ExpressionCompiler.class).withGeneratedName();
        binder.bind(PageFunctionCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(PageFunctionCompiler.class).withGeneratedName();
        configBinder(binder).bindConfig(TaskManagerConfig.class);
        if (retryPolicy == TASK) {
            configBinder(binder).bindConfigDefaults(TaskManagerConfig.class, TaskManagerConfig::applyFaultTolerantExecutionDefaults);
        }
        binder.bind(IndexJoinLookupStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(IndexJoinLookupStats.class).withGeneratedName();
        binder.bind(AsyncHttpExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(AsyncHttpExecutionMBean.class).withGeneratedName();
        binder.bind(JoinFilterFunctionCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(JoinFilterFunctionCompiler.class).withGeneratedName();
        binder.bind(JoinCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(JoinCompiler.class).withGeneratedName();
        binder.bind(OrderingCompiler.class).in(Scopes.SINGLETON);
        newExporter(binder).export(OrderingCompiler.class).withGeneratedName();
        binder.bind(PagesIndex.Factory.class).to(PagesIndex.DefaultFactory.class);
        newOptionalBinder(binder, OperatorFactories.class).setDefault().to(TrinoOperatorFactories.class).in(Scopes.SINGLETON);

        jaxrsBinder(binder).bind(PagesResponseWriter.class);

        // exchange client
        binder.bind(DirectExchangeClientSupplier.class).to(DirectExchangeClientFactory.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("exchange", ForExchange.class)
                .withTracing()
                .withFilter(GenerateTraceTokenRequestFilter.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                    config.setMaxConnectionsPerServer(250);
                    config.setMaxContentLength(DataSize.of(32, MEGABYTE));
                });

        configBinder(binder).bindConfig(DirectExchangeClientConfig.class);
        binder.bind(ExchangeExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ExchangeExecutionMBean.class).withGeneratedName();

        // execution
        binder.bind(LocationFactory.class).to(HttpLocationFactory.class).in(Scopes.SINGLETON);

        // memory manager
        jaxrsBinder(binder).bind(MemoryResource.class);
        jsonCodecBinder(binder).bindJsonCodec(MemoryInfo.class);

        // data stream provider
        binder.bind(PageSourceManager.class).in(Scopes.SINGLETON);
        binder.bind(PageSourceProvider.class).to(PageSourceManager.class).in(Scopes.SINGLETON);

        // page sink provider
        binder.bind(PageSinkManager.class).in(Scopes.SINGLETON);
        binder.bind(PageSinkProvider.class).to(PageSinkManager.class).in(Scopes.SINGLETON);

        // metadata
        binder.bind(MetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(Metadata.class).to(MetadataManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, SystemSecurityMetadata.class)
                .setDefault()
                .to(DisabledSystemSecurityMetadata.class)
                .in(Scopes.SINGLETON);
        binder.bind(GlobalFunctionCatalog.class).in(Scopes.SINGLETON);
        binder.bind(TypeOperatorsCache.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TypeOperatorsCache.class).as(factory -> factory.generatedNameOf(TypeOperators.class));
        binder.bind(BlockTypeOperators.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TypeOperatorsCache.class).withGeneratedName();
        binder.bind(ProcedureRegistry.class).in(Scopes.SINGLETON);
        binder.bind(TableProceduresRegistry.class).in(Scopes.SINGLETON);
        binder.bind(TableFunctionRegistry.class).in(Scopes.SINGLETON);
        binder.bind(PlannerContext.class).in(Scopes.SINGLETON);

        // function
        binder.bind(FunctionManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, FunctionBundle.class);
        binder.bind(RegisterFunctionBundles.class).asEagerSingleton();

        // type
        binder.bind(TypeAnalyzer.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonBinder(binder).addDeserializerBinding(TypeSignature.class).to(TypeSignatureDeserializer.class);
        jsonBinder(binder).addKeyDeserializerBinding(TypeSignature.class).to(TypeSignatureKeyDeserializer.class);
        binder.bind(TypeRegistry.class).in(Scopes.SINGLETON);
        binder.bind(TypeManager.class).to(InternalTypeManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, Type.class);
        binder.bind(RegisterJsonPath2016Type.class).asEagerSingleton();

        // split manager
        binder.bind(SplitManager.class).in(Scopes.SINGLETON);

        // node partitioning manager
        binder.bind(NodePartitioningManager.class).in(Scopes.SINGLETON);

        // index manager
        binder.bind(IndexManager.class).in(Scopes.SINGLETON);

        // handle resolver
        binder.install(new HandleJsonModule());

        // system connector
        binder.install(new SystemConnectorModule());

        // slice
        jsonBinder(binder).addSerializerBinding(Slice.class).to(SliceSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Slice.class).to(SliceDeserializer.class);

        // expression
        jsonBinder(binder).addSerializerBinding(Expression.class).to(ExpressionSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Expression.class).to(ExpressionDeserializer.class);

        // split monitor
        binder.bind(SplitMonitor.class).in(Scopes.SINGLETON);

        // version and announcement
        binder.bind(NodeVersion.class).toInstance(new NodeVersion(nodeVersion));
        discoveryBinder(binder).bindHttpAnnouncement("trino")
                .addProperty("node_version", nodeVersion)
                .addProperty("coordinator", String.valueOf(serverConfig.isCoordinator()));

        // server info resource
        jaxrsBinder(binder).bind(ServerInfoResource.class);

        // node status resource
        jaxrsBinder(binder).bind(StatusResource.class);

        // plugin manager
        newOptionalBinder(binder, PluginInstaller.class).setDefault()
                .to(PluginManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, PluginsProvider.class).setDefault()
                .to(ServerPluginsProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ServerPluginsProviderConfig.class);

        // block encodings
        binder.bind(BlockEncodingManager.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
        jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
        binder.bind(BlockEncodingSerde.class).to(InternalBlockEncodingSerde.class).in(Scopes.SINGLETON);

        // thread visualizer
        jaxrsBinder(binder).bind(ThreadResource.class);

        // PageSorter
        binder.bind(PageSorter.class).to(PagesIndexPageSorter.class).in(Scopes.SINGLETON);

        // PageIndexer
        binder.bind(PageIndexerFactory.class).to(GroupByHashPageIndexerFactory.class).in(Scopes.SINGLETON);

        // Finalizer
        binder.bind(FinalizerService.class).in(Scopes.SINGLETON);

        // Spiller
        binder.bind(SpillerFactory.class).to(GenericSpillerFactory.class).in(Scopes.SINGLETON);
        binder.bind(SingleStreamSpillerFactory.class).to(FileSingleStreamSpillerFactory.class).in(Scopes.SINGLETON);
        binder.bind(PartitioningSpillerFactory.class).to(GenericPartitioningSpillerFactory.class).in(Scopes.SINGLETON);
        binder.bind(SpillerStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(SpillerStats.class).withGeneratedName();
        binder.bind(LocalSpillManager.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(NodeSpillConfig.class);

        // Dynamic Filtering
        configBinder(binder).bindConfig(DynamicFilterConfig.class);

        // dispatcher
        // TODO remove dispatcher fromm ServerMainModule, and bind dependent components only on coordinators
        newOptionalBinder(binder, DispatchManager.class);

        // Added for RuleStatsSystemTable
        // TODO: remove this when system tables are bound separately for coordinator and worker
        newOptionalBinder(binder, RuleStatsRecorder.class);

        // cleanup
        binder.bind(ExecutorCleanup.class).in(Scopes.SINGLETON);
    }

    private static class RegisterFunctionBundles
    {
        @Inject
        public RegisterFunctionBundles(GlobalFunctionCatalog globalFunctionCatalog, Set<FunctionBundle> functionBundles)
        {
            for (FunctionBundle functionBundle : functionBundles) {
                globalFunctionCatalog.addFunctions(functionBundle);
            }
        }
    }

    @ProvidesIntoSet
    @Singleton
    public static FunctionBundle systemFunctionBundle(FeaturesConfig featuresConfig, TypeOperators typeOperators, BlockTypeOperators blockTypeOperators, NodeVersion nodeVersion)
    {
        return SystemFunctionBundle.create(featuresConfig, typeOperators, blockTypeOperators, nodeVersion);
    }

    @ProvidesIntoSet
    @Singleton
    // literal function must be registered lazily to break circular dependency
    public static FunctionBundle literalFunctionBundle(BlockEncodingSerde blockEncodingSerde)
    {
        return new InternalFunctionBundle(new LiteralFunction(blockEncodingSerde));
    }

    @ProvidesIntoSet
    @Singleton
    // not adding to system function bundle to avoid mutual dependency FunctionManager <-> MetadataManager in testing instance constructors
    public static FunctionBundle jsonFunctionBundle(FunctionManager functionManager, Metadata metadata, TypeManager typeManager)
    {
        return new InternalFunctionBundle(
                new JsonExistsFunction(functionManager, metadata, typeManager),
                new JsonValueFunction(functionManager, metadata, typeManager),
                new JsonQueryFunction(functionManager, metadata, typeManager));
    }

    // working around circular dependency Type <-> TypeManager
    private static class RegisterJsonPath2016Type
    {
        @Inject
        public RegisterJsonPath2016Type(BlockEncodingSerde blockEncodingSerde, TypeManager typeManager, TypeRegistry typeRegistry)
        {
            typeRegistry.addType(new JsonPath2016Type(new TypeDeserializer(typeManager), blockEncodingSerde));
        }
    }

    @Provides
    @Singleton
    public static TypeOperators createTypeOperators(TypeOperatorsCache typeOperatorsCache)
    {
        return new TypeOperators(typeOperatorsCache);
    }

    @Provides
    @Singleton
    @ForStartup
    public static Executor createStartupExecutor(ServerConfig config)
    {
        if (!config.isConcurrentStartup()) {
            return directExecutor();
        }
        return new BoundedExecutor(
                newCachedThreadPool(daemonThreadsNamed("startup-%s")),
                Runtime.getRuntime().availableProcessors());
    }

    @Provides
    @Singleton
    @ForExchange
    public static ScheduledExecutorService createExchangeExecutor(DirectExchangeClientConfig config)
    {
        return newScheduledThreadPool(config.getClientThreads(), daemonThreadsNamed("exchange-client-%s"));
    }

    @Provides
    @Singleton
    @ForAsyncHttp
    public static ExecutorService createAsyncHttpResponseCoreExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("async-http-response-%s"));
    }

    @Provides
    @Singleton
    @ForAsyncHttp
    public static BoundedExecutor createAsyncHttpResponseExecutor(@ForAsyncHttp ExecutorService coreExecutor, TaskManagerConfig config)
    {
        return new BoundedExecutor(coreExecutor, config.getHttpResponseThreads());
    }

    @Provides
    @Singleton
    @ForAsyncHttp
    public static ScheduledExecutorService createAsyncHttpTimeoutExecutor(TaskManagerConfig config)
    {
        return newScheduledThreadPool(config.getHttpTimeoutThreads(), daemonThreadsNamed("async-http-timeout-%s"));
    }

    public static class ExecutorCleanup
    {
        private final List<ExecutorService> executors;

        @Inject
        public ExecutorCleanup(
                @ForExchange ScheduledExecutorService exchangeExecutor,
                @ForAsyncHttp ExecutorService httpResponseExecutor,
                @ForAsyncHttp ScheduledExecutorService httpTimeoutExecutor)
        {
            executors = ImmutableList.of(
                    exchangeExecutor,
                    httpResponseExecutor,
                    httpTimeoutExecutor);
        }

        @PreDestroy
        public void shutdown()
        {
            executors.forEach(ExecutorService::shutdownNow);
        }
    }
}
