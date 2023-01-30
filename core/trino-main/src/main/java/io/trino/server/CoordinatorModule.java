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
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.cost.CostCalculator;
import io.trino.cost.CostCalculator.EstimatedExchanges;
import io.trino.cost.CostCalculatorUsingExchanges;
import io.trino.cost.CostCalculatorWithEstimatedExchanges;
import io.trino.cost.CostComparator;
import io.trino.cost.StatsAndCosts;
import io.trino.cost.StatsCalculatorModule;
import io.trino.cost.TaskCountEstimator;
import io.trino.dispatcher.DispatchExecutor;
import io.trino.dispatcher.DispatchManager;
import io.trino.dispatcher.DispatchQueryFactory;
import io.trino.dispatcher.FailedDispatchQueryFactory;
import io.trino.dispatcher.LocalDispatchQueryFactory;
import io.trino.dispatcher.QueuedStatementResource;
import io.trino.event.QueryMonitor;
import io.trino.event.QueryMonitorConfig;
import io.trino.execution.ClusterSizeMonitor;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.ExplainAnalyzeContext;
import io.trino.execution.ForQueryExecution;
import io.trino.execution.QueryExecution;
import io.trino.execution.QueryExecutionMBean;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.QueryManager;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.QueryPerformanceFetcher;
import io.trino.execution.QueryPreparer;
import io.trino.execution.RemoteTaskFactory;
import io.trino.execution.SqlQueryManager;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.TaskStatus;
import io.trino.execution.resourcegroups.InternalResourceGroupManager;
import io.trino.execution.resourcegroups.LegacyResourceGroupConfigurationManager;
import io.trino.execution.resourcegroups.ResourceGroupManager;
import io.trino.execution.scheduler.BinPackingNodeAllocatorService;
import io.trino.execution.scheduler.ConstantPartitionMemoryEstimator;
import io.trino.execution.scheduler.EventDrivenTaskSourceFactory;
import io.trino.execution.scheduler.FixedCountNodeAllocatorService;
import io.trino.execution.scheduler.NodeAllocatorService;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.PartitionMemoryEstimatorFactory;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.execution.scheduler.TaskDescriptorStorage;
import io.trino.execution.scheduler.TaskExecutionStats;
import io.trino.execution.scheduler.policy.AllAtOnceExecutionPolicy;
import io.trino.execution.scheduler.policy.ExecutionPolicy;
import io.trino.execution.scheduler.policy.PhasedExecutionPolicy;
import io.trino.failuredetector.FailureDetectorModule;
import io.trino.memory.ClusterMemoryManager;
import io.trino.memory.ForMemoryManager;
import io.trino.memory.LeastWastedEffortTaskLowMemoryKiller;
import io.trino.memory.LowMemoryKiller;
import io.trino.memory.LowMemoryKiller.ForQueryLowMemoryKiller;
import io.trino.memory.LowMemoryKiller.ForTaskLowMemoryKiller;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.MemoryManagerConfig.LowMemoryQueryKillerPolicy;
import io.trino.memory.MemoryManagerConfig.LowMemoryTaskKillerPolicy;
import io.trino.memory.NoneLowMemoryKiller;
import io.trino.memory.TotalReservationLowMemoryKiller;
import io.trino.memory.TotalReservationOnBlockedNodesQueryLowMemoryKiller;
import io.trino.memory.TotalReservationOnBlockedNodesTaskLowMemoryKiller;
import io.trino.operator.ForScheduler;
import io.trino.operator.OperatorStats;
import io.trino.server.protocol.ExecutingStatementResource;
import io.trino.server.protocol.QueryInfoUrlFactory;
import io.trino.server.remotetask.RemoteTaskStats;
import io.trino.server.ui.WebUiModule;
import io.trino.server.ui.WorkerResource;
import io.trino.spi.memory.ClusterMemoryPoolManager;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.analyzer.QueryExplainerFactory;
import io.trino.sql.planner.OptimizerStatsMBeanExporter;
import io.trino.sql.planner.PlanFragmenter;
import io.trino.sql.planner.PlanOptimizers;
import io.trino.sql.planner.PlanOptimizersFactory;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.SplitSourceFactory;
import io.trino.sql.rewrite.DescribeInputRewrite;
import io.trino.sql.rewrite.DescribeOutputRewrite;
import io.trino.sql.rewrite.ExplainRewrite;
import io.trino.sql.rewrite.ShowQueriesRewrite;
import io.trino.sql.rewrite.ShowStatsRewrite;
import io.trino.sql.rewrite.StatementRewrite;
import io.trino.sql.rewrite.StatementRewrite.Rewrite;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.execution.scheduler.NodeSchedulerConfig.NodeAllocatorType.BIN_PACKING;
import static io.trino.execution.scheduler.NodeSchedulerConfig.NodeAllocatorType.FIXED_COUNT;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CoordinatorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new WebUiModule());

        // coordinator announcement
        discoveryBinder(binder).bindHttpAnnouncement("trino-coordinator");

        // statement resource
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        jaxrsBinder(binder).bind(QueuedStatementResource.class);
        jaxrsBinder(binder).bind(ExecutingStatementResource.class);
        binder.bind(StatementHttpExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(StatementHttpExecutionMBean.class).withGeneratedName();
        binder.bind(QueryInfoUrlFactory.class).in(Scopes.SINGLETON);

        // allow large prepared statements in headers
        configBinder(binder).bindConfigDefaults(HttpServerConfig.class, config -> {
            config.setMaxRequestHeaderSize(DataSize.of(2, MEGABYTE));
            config.setMaxResponseHeaderSize(DataSize.of(2, MEGABYTE));
        });

        // failure detector
        binder.install(new FailureDetectorModule());
        jaxrsBinder(binder).bind(NodeResource.class);
        jaxrsBinder(binder).bind(WorkerResource.class);
        httpClientBinder(binder).bindHttpClient("workerInfo", ForWorkerInfo.class);

        // query monitor
        jsonCodecBinder(binder).bindJsonCodec(ExecutionFailureInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(OperatorStats.class);
        jsonCodecBinder(binder).bindJsonCodec(StageInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(StatsAndCosts.class);
        configBinder(binder).bindConfig(QueryMonitorConfig.class);
        binder.bind(QueryMonitor.class).in(Scopes.SINGLETON);

        // query manager
        jaxrsBinder(binder).bind(QueryResource.class);
        jaxrsBinder(binder).bind(QueryStateInfoResource.class);
        jaxrsBinder(binder).bind(ResourceGroupStateInfoResource.class);
        binder.bind(QueryIdGenerator.class).in(Scopes.SINGLETON);
        binder.bind(QueryManager.class).to(SqlQueryManager.class).in(Scopes.SINGLETON);
        binder.bind(QueryPreparer.class).in(Scopes.SINGLETON);
        binder.bind(SessionSupplier.class).to(QuerySessionSupplier.class).in(Scopes.SINGLETON);
        binder.bind(InternalResourceGroupManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(InternalResourceGroupManager.class).withGeneratedName();
        binder.bind(ResourceGroupManager.class).to(InternalResourceGroupManager.class);
        binder.bind(LegacyResourceGroupConfigurationManager.class).in(Scopes.SINGLETON);

        // dispatcher
        binder.bind(DispatchManager.class).in(Scopes.SINGLETON);
        // export under the old name, for backwards compatibility
        newExporter(binder).export(DispatchManager.class).as(generator -> generator.generatedNameOf(QueryManager.class));
        binder.bind(FailedDispatchQueryFactory.class).in(Scopes.SINGLETON);
        binder.bind(DispatchExecutor.class).in(Scopes.SINGLETON);

        // local dispatcher
        binder.bind(DispatchQueryFactory.class).to(LocalDispatchQueryFactory.class);

        // cluster memory manager
        binder.bind(ClusterMemoryManager.class).in(Scopes.SINGLETON);
        binder.bind(ClusterMemoryPoolManager.class).to(ClusterMemoryManager.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("memoryManager", ForMemoryManager.class)
                .withTracing()
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                });

        bindLowMemoryTaskKiller(LowMemoryTaskKillerPolicy.NONE, NoneLowMemoryKiller.class);
        bindLowMemoryTaskKiller(LowMemoryTaskKillerPolicy.TOTAL_RESERVATION_ON_BLOCKED_NODES, TotalReservationOnBlockedNodesTaskLowMemoryKiller.class);
        bindLowMemoryTaskKiller(LowMemoryTaskKillerPolicy.LEAST_WASTE, LeastWastedEffortTaskLowMemoryKiller.class);
        bindLowMemoryQueryKiller(LowMemoryQueryKillerPolicy.NONE, NoneLowMemoryKiller.class);
        bindLowMemoryQueryKiller(LowMemoryQueryKillerPolicy.TOTAL_RESERVATION, TotalReservationLowMemoryKiller.class);
        bindLowMemoryQueryKiller(LowMemoryQueryKillerPolicy.TOTAL_RESERVATION_ON_BLOCKED_NODES, TotalReservationOnBlockedNodesQueryLowMemoryKiller.class);

        newExporter(binder).export(ClusterMemoryManager.class).withGeneratedName();

        // node allocator
        install(conditionalModule(
                NodeSchedulerConfig.class,
                config -> FIXED_COUNT == config.getNodeAllocatorType(),
                innerBinder -> {
                    innerBinder.bind(NodeAllocatorService.class).to(FixedCountNodeAllocatorService.class).in(Scopes.SINGLETON);
                    innerBinder.bind(PartitionMemoryEstimatorFactory.class).toInstance(ConstantPartitionMemoryEstimator::new);
                }));
        install(conditionalModule(
                NodeSchedulerConfig.class,
                config -> BIN_PACKING == config.getNodeAllocatorType(),
                innerBinder -> {
                    innerBinder.bind(BinPackingNodeAllocatorService.class).in(Scopes.SINGLETON);
                    innerBinder.bind(NodeAllocatorService.class).to(BinPackingNodeAllocatorService.class);
                    innerBinder.bind(PartitionMemoryEstimatorFactory.class).to(BinPackingNodeAllocatorService.class);
                }));

        // node monitor
        binder.bind(ClusterSizeMonitor.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ClusterSizeMonitor.class).withGeneratedName();

        // statistics calculator
        binder.install(new StatsCalculatorModule());

        // cost calculator
        binder.bind(TaskCountEstimator.class).in(Scopes.SINGLETON);
        binder.bind(CostCalculator.class).to(CostCalculatorUsingExchanges.class).in(Scopes.SINGLETON);
        binder.bind(CostCalculator.class).annotatedWith(EstimatedExchanges.class).to(CostCalculatorWithEstimatedExchanges.class).in(Scopes.SINGLETON);
        binder.bind(CostComparator.class).in(Scopes.SINGLETON);

        // dynamic filtering service
        binder.bind(DynamicFilterService.class).in(Scopes.SINGLETON);

        // analyzer
        binder.bind(AnalyzerFactory.class).in(Scopes.SINGLETON);

        // statement rewriter
        binder.bind(StatementRewrite.class).in(Scopes.SINGLETON);
        Multibinder<Rewrite> rewriteBinder = newSetBinder(binder, Rewrite.class);
        rewriteBinder.addBinding().to(DescribeInputRewrite.class).in(Scopes.SINGLETON);
        rewriteBinder.addBinding().to(DescribeOutputRewrite.class).in(Scopes.SINGLETON);
        rewriteBinder.addBinding().to(ShowQueriesRewrite.class).in(Scopes.SINGLETON);
        rewriteBinder.addBinding().to(ShowStatsRewrite.class).in(Scopes.SINGLETON);
        rewriteBinder.addBinding().to(ExplainRewrite.class).in(Scopes.SINGLETON);

        // planner
        binder.bind(PlanFragmenter.class).in(Scopes.SINGLETON);
        binder.bind(PlanOptimizersFactory.class).to(PlanOptimizers.class).in(Scopes.SINGLETON);

        // Optimizer/Rule Stats exporter
        binder.bind(RuleStatsRecorder.class).in(Scopes.SINGLETON);
        binder.bind(OptimizerStatsMBeanExporter.class).in(Scopes.SINGLETON);

        // query explainer
        binder.bind(QueryExplainerFactory.class).in(Scopes.SINGLETON);

        // explain analyze
        binder.bind(ExplainAnalyzeContext.class).in(Scopes.SINGLETON);

        // execution scheduler
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskStatus.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(FailTaskRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(VersionedDynamicFilterDomains.class);
        binder.bind(RemoteTaskFactory.class).to(HttpRemoteTaskFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RemoteTaskFactory.class).withGeneratedName();

        binder.bind(RemoteTaskStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RemoteTaskStats.class).withGeneratedName();

        httpClientBinder(binder).bindHttpClient("scheduler", ForScheduler.class)
                .withTracing()
                .withFilter(GenerateTraceTokenRequestFilter.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                    config.setMaxConnectionsPerServer(250);
                });

        binder.bind(ScheduledExecutorService.class).annotatedWith(ForScheduler.class)
                .toInstance(newSingleThreadScheduledExecutor(threadsNamed("stage-scheduler")));

        // query execution
        QueryManagerConfig queryManagerConfig = buildConfigObject(QueryManagerConfig.class);
        ThreadPoolExecutor queryExecutor = new ThreadPoolExecutor(
                queryManagerConfig.getQueryExecutorPoolSize(),
                queryManagerConfig.getQueryExecutorPoolSize(),
                60, SECONDS,
                new LinkedBlockingQueue<>(1000),
                threadsNamed("query-execution-%s"));
        queryExecutor.allowCoreThreadTimeOut(true);
        binder.bind(ExecutorService.class).annotatedWith(ForQueryExecution.class).toInstance(queryExecutor);
        binder.bind(QueryExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryExecutionMBean.class)
                .as(generator -> generator.generatedNameOf(QueryExecution.class));

        binder.bind(SplitSourceFactory.class).in(Scopes.SINGLETON);
        binder.bind(SplitSchedulerStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(SplitSchedulerStats.class).withGeneratedName();

        binder.bind(EventDrivenTaskSourceFactory.class).in(Scopes.SINGLETON);
        binder.bind(TaskDescriptorStorage.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TaskDescriptorStorage.class).withGeneratedName();

        binder.bind(TaskExecutionStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TaskExecutionStats.class).withGeneratedName();

        MapBinder<String, ExecutionPolicy> executionPolicyBinder = newMapBinder(binder, String.class, ExecutionPolicy.class);
        executionPolicyBinder.addBinding("all-at-once").to(AllAtOnceExecutionPolicy.class);
        executionPolicyBinder.addBinding("phased").to(PhasedExecutionPolicy.class);

        install(new QueryExecutionFactoryModule());

        // cleanup
        binder.bind(ExecutorCleanup.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    public static ResourceGroupManager<?> getResourceGroupManager(@SuppressWarnings("rawtypes") ResourceGroupManager manager)
    {
        return manager;
    }

    @Provides
    @Singleton
    public static QueryPerformanceFetcher createQueryPerformanceFetcher(QueryManager queryManager)
    {
        return queryManager::getFullQueryInfo;
    }

    @Provides
    @Singleton
    @ForStatementResource
    public static ExecutorService createStatementResponseCoreExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("statement-response-%s"));
    }

    @Provides
    @Singleton
    @ForStatementResource
    public static BoundedExecutor createStatementResponseExecutor(@ForStatementResource ExecutorService coreExecutor, TaskManagerConfig config)
    {
        return new BoundedExecutor(coreExecutor, config.getHttpResponseThreads());
    }

    @Provides
    @Singleton
    @ForStatementResource
    public static ScheduledExecutorService createStatementTimeoutExecutor(TaskManagerConfig config)
    {
        return newScheduledThreadPool(config.getHttpTimeoutThreads(), daemonThreadsNamed("statement-timeout-%s"));
    }

    private void bindLowMemoryQueryKiller(LowMemoryQueryKillerPolicy policy, Class<? extends LowMemoryKiller> clazz)
    {
        install(conditionalModule(
                MemoryManagerConfig.class,
                config -> policy == config.getLowMemoryQueryKillerPolicy(),
                binder -> binder
                        .bind(LowMemoryKiller.class)
                        .annotatedWith(ForQueryLowMemoryKiller.class)
                        .to(clazz)
                        .in(Scopes.SINGLETON)));
    }

    private void bindLowMemoryTaskKiller(LowMemoryTaskKillerPolicy policy, Class<? extends LowMemoryKiller> clazz)
    {
        install(conditionalModule(
                MemoryManagerConfig.class,
                config -> policy == config.getLowMemoryTaskKillerPolicy(),
                binder -> binder
                        .bind(LowMemoryKiller.class)
                        .annotatedWith(ForTaskLowMemoryKiller.class)
                        .to(clazz)
                        .in(Scopes.SINGLETON)));
    }

    public static class ExecutorCleanup
    {
        private final List<ExecutorService> executors;

        @Inject
        public ExecutorCleanup(
                @ForStatementResource ExecutorService statementResponseExecutor,
                @ForStatementResource ScheduledExecutorService statementTimeoutExecutor,
                @ForQueryExecution ExecutorService queryExecutionExecutor,
                @ForScheduler ScheduledExecutorService schedulerExecutor)
        {
            executors = ImmutableList.<ExecutorService>builder()
                    .add(statementResponseExecutor)
                    .add(statementTimeoutExecutor)
                    .add(queryExecutionExecutor)
                    .add(schedulerExecutor)
                    .build();
        }

        @PreDestroy
        public void shutdown()
        {
            executors.forEach(ExecutorService::shutdownNow);
        }
    }
}
