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
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.ExplainAnalyzeContext;
import io.trino.execution.ForQueryExecution;
import io.trino.execution.QueryExecution;
import io.trino.execution.QueryExecutionMBean;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.QueryManager;
import io.trino.execution.QueryPerformanceFetcher;
import io.trino.execution.QueryPreparer;
import io.trino.execution.RemoteTaskFactory;
import io.trino.execution.SqlQueryManager;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.TaskStatus;
import io.trino.execution.VersionedSummaryInfoCollector.VersionedSummaryInfo;
import io.trino.execution.resourcegroups.InternalResourceGroupManager;
import io.trino.execution.resourcegroups.LegacyResourceGroupConfigurationManager;
import io.trino.execution.resourcegroups.ResourceGroupManager;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.execution.scheduler.StageTaskSourceFactory;
import io.trino.execution.scheduler.TaskSourceFactory;
import io.trino.execution.scheduler.policy.AllAtOnceExecutionPolicy;
import io.trino.execution.scheduler.policy.ExecutionPolicy;
import io.trino.execution.scheduler.policy.LegacyPhasedExecutionPolicy;
import io.trino.execution.scheduler.policy.PhasedExecutionPolicy;
import io.trino.failuredetector.FailureDetectorModule;
import io.trino.memory.ClusterMemoryManager;
import io.trino.memory.ForMemoryManager;
import io.trino.memory.LowMemoryKiller;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.MemoryManagerConfig.LowMemoryKillerPolicy;
import io.trino.memory.NoneLowMemoryKiller;
import io.trino.memory.TotalReservationLowMemoryKiller;
import io.trino.memory.TotalReservationOnBlockedNodesLowMemoryKiller;
import io.trino.metadata.CatalogManager;
import io.trino.operator.ForScheduler;
import io.trino.operator.OperatorStats;
import io.trino.server.protocol.ExecutingStatementResource;
import io.trino.server.protocol.QueryInfoUrlFactory;
import io.trino.server.remotetask.RemoteTaskStats;
import io.trino.server.ui.WebUiModule;
import io.trino.server.ui.WorkerResource;
import io.trino.spi.VersionEmbedder;
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
import io.trino.transaction.ForTransactionManager;
import io.trino.transaction.InMemoryTransactionManager;
import io.trino.transaction.TransactionManager;
import io.trino.transaction.TransactionManagerConfig;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
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
        bindLowMemoryKiller(LowMemoryKillerPolicy.NONE, NoneLowMemoryKiller.class);
        bindLowMemoryKiller(LowMemoryKillerPolicy.TOTAL_RESERVATION, TotalReservationLowMemoryKiller.class);
        bindLowMemoryKiller(LowMemoryKillerPolicy.TOTAL_RESERVATION_ON_BLOCKED_NODES, TotalReservationOnBlockedNodesLowMemoryKiller.class);
        newExporter(binder).export(ClusterMemoryManager.class).withGeneratedName();

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
        newOptionalBinder(binder, PlanOptimizersFactory.class)
                .setDefault().to(PlanOptimizers.class).in(Scopes.SINGLETON);

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
        jsonCodecBinder(binder).bindJsonCodec(VersionedSummaryInfo.class);
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
        binder.bind(ExecutorService.class).annotatedWith(ForQueryExecution.class)
                .toInstance(newCachedThreadPool(threadsNamed("query-execution-%s")));
        binder.bind(QueryExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryExecutionMBean.class)
                .as(generator -> generator.generatedNameOf(QueryExecution.class));

        binder.bind(SplitSourceFactory.class).in(Scopes.SINGLETON);
        binder.bind(SplitSchedulerStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(SplitSchedulerStats.class).withGeneratedName();

        binder.bind(TaskSourceFactory.class).to(StageTaskSourceFactory.class).in(Scopes.SINGLETON);

        MapBinder<String, ExecutionPolicy> executionPolicyBinder = newMapBinder(binder, String.class, ExecutionPolicy.class);
        executionPolicyBinder.addBinding("all-at-once").to(AllAtOnceExecutionPolicy.class);
        executionPolicyBinder.addBinding("legacy-phased").to(LegacyPhasedExecutionPolicy.class);
        executionPolicyBinder.addBinding("phased").to(PhasedExecutionPolicy.class);

        install(new QueryExecutionFactoryModule());

        // cleanup
        binder.bind(ExecutorCleanup.class).in(Scopes.SINGLETON);
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

    @Provides
    @Singleton
    @ForTransactionManager
    public static ScheduledExecutorService createTransactionIdleCheckExecutor()
    {
        return newSingleThreadScheduledExecutor(daemonThreadsNamed("transaction-idle-check"));
    }

    @Provides
    @Singleton
    @ForTransactionManager
    public static ExecutorService createTransactionFinishingExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("transaction-finishing-%s"));
    }

    @Provides
    @Singleton
    public static TransactionManager createTransactionManager(
            TransactionManagerConfig config,
            CatalogManager catalogManager,
            VersionEmbedder versionEmbedder,
            @ForTransactionManager ScheduledExecutorService idleCheckExecutor,
            @ForTransactionManager ExecutorService finishingExecutor)
    {
        return InMemoryTransactionManager.create(config, idleCheckExecutor, catalogManager, versionEmbedder.embedVersion(finishingExecutor));
    }

    private void bindLowMemoryKiller(LowMemoryKillerPolicy policy, Class<? extends LowMemoryKiller> clazz)
    {
        install(conditionalModule(
                MemoryManagerConfig.class,
                config -> policy == config.getLowMemoryKillerPolicy(),
                binder -> binder.bind(LowMemoryKiller.class).to(clazz).in(Scopes.SINGLETON)));
    }

    public static class ExecutorCleanup
    {
        private final List<ExecutorService> executors;

        @Inject
        public ExecutorCleanup(
                @ForStatementResource ExecutorService statementResponseExecutor,
                @ForStatementResource ScheduledExecutorService statementTimeoutExecutor,
                @ForQueryExecution ExecutorService queryExecutionExecutor,
                @ForScheduler ScheduledExecutorService schedulerExecutor,
                @ForTransactionManager ExecutorService transactionFinishingExecutor,
                @ForTransactionManager ScheduledExecutorService transactionIdleExecutor)
        {
            executors = ImmutableList.<ExecutorService>builder()
                    .add(statementResponseExecutor)
                    .add(statementTimeoutExecutor)
                    .add(queryExecutionExecutor)
                    .add(schedulerExecutor)
                    .add(transactionFinishingExecutor)
                    .add(transactionIdleExecutor)
                    .build();
        }

        @PreDestroy
        public void shutdown()
        {
            executors.forEach(ExecutorService::shutdownNow);
        }
    }
}
