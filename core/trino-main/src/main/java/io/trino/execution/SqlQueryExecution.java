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
package io.trino.execution;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.concurrent.SetThreadName;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.cost.CostCalculator;
import io.trino.cost.StatsCalculator;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.scheduler.EventDrivenFaultTolerantQueryScheduler;
import io.trino.execution.scheduler.EventDrivenTaskSourceFactory;
import io.trino.execution.scheduler.NodeAllocatorService;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.PartitionMemoryEstimatorFactory;
import io.trino.execution.scheduler.PipelinedQueryScheduler;
import io.trino.execution.scheduler.QueryScheduler;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.execution.scheduler.TaskDescriptorStorage;
import io.trino.execution.scheduler.TaskExecutionStats;
import io.trino.execution.scheduler.policy.ExecutionPolicy;
import io.trino.execution.warnings.WarningCollector;
import io.trino.failuredetector.FailureDetector;
import io.trino.metadata.TableHandle;
import io.trino.operator.ForScheduler;
import io.trino.operator.RetryPolicy;
import io.trino.server.BasicQueryInfo;
import io.trino.server.DynamicFilterService;
import io.trino.server.protocol.Slug;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analyzer;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.planner.InputExtractor;
import io.trino.sql.planner.LogicalPlanner;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.PlanFragmenter;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.PlanOptimizersFactory;
import io.trino.sql.planner.SplitSourceFactory;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.isEnableDynamicFiltering;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.PLANNING;
import static io.trino.server.DynamicFilterService.DynamicFiltersStats;
import static io.trino.spi.StandardErrorCode.STACK_OVERFLOW;
import static io.trino.tracing.ScopedSpan.scopedSpan;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class SqlQueryExecution
        implements QueryExecution
{
    private final QueryStateMachine stateMachine;
    private final Slug slug;
    private final Tracer tracer;
    private final PlannerContext plannerContext;
    private final SplitSourceFactory splitSourceFactory;
    private final NodePartitioningManager nodePartitioningManager;
    private final NodeScheduler nodeScheduler;
    private final NodeAllocatorService nodeAllocatorService;
    private final PartitionMemoryEstimatorFactory partitionMemoryEstimatorFactory;
    private final TaskExecutionStats taskExecutionStats;
    private final List<PlanOptimizer> planOptimizers;
    private final PlanFragmenter planFragmenter;
    private final RemoteTaskFactory remoteTaskFactory;
    private final int scheduleSplitBatchSize;
    private final ExecutorService queryExecutor;
    private final ScheduledExecutorService schedulerExecutor;
    private final FailureDetector failureDetector;

    private final AtomicReference<QueryScheduler> queryScheduler = new AtomicReference<>();
    private final AtomicReference<Plan> queryPlan = new AtomicReference<>();
    private final NodeTaskMap nodeTaskMap;
    private final ExecutionPolicy executionPolicy;
    private final SplitSchedulerStats schedulerStats;
    private final Analysis analysis;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final DynamicFilterService dynamicFilterService;
    private final TableExecuteContextManager tableExecuteContextManager;
    private final TypeAnalyzer typeAnalyzer;
    private final SqlTaskManager coordinatorTaskManager;
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private final EventDrivenTaskSourceFactory eventDrivenTaskSourceFactory;
    private final TaskDescriptorStorage taskDescriptorStorage;
    private final PlanOptimizersStatsCollector planOptimizersStatsCollector;

    private SqlQueryExecution(
            PreparedQuery preparedQuery,
            QueryStateMachine stateMachine,
            Slug slug,
            Tracer tracer,
            PlannerContext plannerContext,
            AnalyzerFactory analyzerFactory,
            SplitSourceFactory splitSourceFactory,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            NodeAllocatorService nodeAllocatorService,
            PartitionMemoryEstimatorFactory partitionMemoryEstimatorFactory,
            TaskExecutionStats taskExecutionStats,
            List<PlanOptimizer> planOptimizers,
            PlanFragmenter planFragmenter,
            RemoteTaskFactory remoteTaskFactory,
            int scheduleSplitBatchSize,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap,
            ExecutionPolicy executionPolicy,
            SplitSchedulerStats schedulerStats,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            DynamicFilterService dynamicFilterService,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector,
            TableExecuteContextManager tableExecuteContextManager,
            TypeAnalyzer typeAnalyzer,
            SqlTaskManager coordinatorTaskManager,
            ExchangeManagerRegistry exchangeManagerRegistry,
            EventDrivenTaskSourceFactory eventDrivenTaskSourceFactory,
            TaskDescriptorStorage taskDescriptorStorage)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            this.slug = requireNonNull(slug, "slug is null");
            this.tracer = requireNonNull(tracer, "tracer is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            this.nodeAllocatorService = requireNonNull(nodeAllocatorService, "nodeAllocatorService is null");
            this.partitionMemoryEstimatorFactory = requireNonNull(partitionMemoryEstimatorFactory, "partitionMemoryEstimatorFactory is null");
            this.taskExecutionStats = requireNonNull(taskExecutionStats, "taskExecutionStats is null");
            this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
            this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
            this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
            this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
            this.executionPolicy = requireNonNull(executionPolicy, "executionPolicy is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
            this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");

            checkArgument(scheduleSplitBatchSize > 0, "scheduleSplitBatchSize must be greater than 0");
            this.scheduleSplitBatchSize = scheduleSplitBatchSize;

            this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");

            // analyze query
            this.analysis = analyze(preparedQuery, stateMachine, warningCollector, planOptimizersStatsCollector, analyzerFactory);

            stateMachine.addStateChangeListener(state -> {
                if (!state.isDone()) {
                    return;
                }
                unregisterDynamicFilteringQuery(
                        dynamicFilterService.getDynamicFilteringStats(stateMachine.getQueryId(), stateMachine.getSession()));

                tableExecuteContextManager.unregisterTableExecuteContextForQuery(stateMachine.getQueryId());
            });

            this.remoteTaskFactory = new MemoryTrackingRemoteTaskFactory(requireNonNull(remoteTaskFactory, "remoteTaskFactory is null"), stateMachine);
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.coordinatorTaskManager = requireNonNull(coordinatorTaskManager, "coordinatorTaskManager is null");
            this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
            this.eventDrivenTaskSourceFactory = requireNonNull(eventDrivenTaskSourceFactory, "taskSourceFactory is null");
            this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
            this.planOptimizersStatsCollector = requireNonNull(planOptimizersStatsCollector, "planOptimizersStatsCollector is null");
        }
    }

    private synchronized void registerDynamicFilteringQuery(PlanRoot plan)
    {
        if (!isEnableDynamicFiltering(stateMachine.getSession())) {
            return;
        }

        if (isDone()) {
            // query has finished or was cancelled asynchronously
            return;
        }

        dynamicFilterService.registerQuery(this, plan.getRoot());
        stateMachine.setDynamicFiltersStatsSupplier(
                () -> dynamicFilterService.getDynamicFilteringStats(
                        stateMachine.getQueryId(),
                        stateMachine.getSession()));
    }

    private synchronized void unregisterDynamicFilteringQuery(DynamicFiltersStats finalDynamicFiltersStats)
    {
        checkState(isDone(), "Expected query to be in done state");
        stateMachine.setDynamicFiltersStatsSupplier(() -> finalDynamicFiltersStats);
        dynamicFilterService.removeQuery(stateMachine.getQueryId());
    }

    private static Analysis analyze(
            PreparedQuery preparedQuery,
            QueryStateMachine stateMachine,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector,
            AnalyzerFactory analyzerFactory)
    {
        stateMachine.beginAnalysis();

        requireNonNull(preparedQuery, "preparedQuery is null");
        Analyzer analyzer = analyzerFactory.createAnalyzer(
                stateMachine.getSession(),
                preparedQuery.getParameters(),
                bindParameters(preparedQuery.getStatement(), preparedQuery.getParameters()),
                warningCollector,
                planOptimizersStatsCollector);
        Analysis analysis;
        try {
            analysis = analyzer.analyze(preparedQuery.getStatement());
        }
        catch (StackOverflowError e) {
            throw new TrinoException(STACK_OVERFLOW, "statement is too large (stack overflow during analysis)", e);
        }

        stateMachine.setUpdateType(analysis.getUpdateType());
        stateMachine.setReferencedTables(analysis.getReferencedTables());
        stateMachine.setRoutines(analysis.getRoutines());

        stateMachine.endAnalysis();

        return analysis;
    }

    @Override
    public Slug getSlug()
    {
        return slug;
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        // acquire reference to scheduler before checking finalQueryInfo, because
        // state change listener sets finalQueryInfo and then clears scheduler when
        // the query finishes.
        QueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getUserMemoryReservation();
        }
        if (scheduler == null) {
            return DataSize.ofBytes(0);
        }
        return succinctBytes(scheduler.getUserMemoryReservation());
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        // acquire reference to scheduler before checking finalQueryInfo, because
        // state change listener sets finalQueryInfo and then clears scheduler when
        // the query finishes.
        QueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalMemoryReservation();
        }
        if (scheduler == null) {
            return DataSize.ofBytes(0);
        }
        return succinctBytes(scheduler.getTotalMemoryReservation());
    }

    @Override
    public DateTime getCreateTime()
    {
        return stateMachine.getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return stateMachine.getExecutionStartTime();
    }

    @Override
    public Optional<Duration> getPlanningTime()
    {
        return stateMachine.getPlanningTime();
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        return stateMachine.getLastHeartbeat();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return stateMachine.getEndTime();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        QueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalCpuTime();
        }
        if (scheduler == null) {
            return new Duration(0, SECONDS);
        }
        return scheduler.getTotalCpuTime();
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return stateMachine.getFinalQueryInfo()
                .map(BasicQueryInfo::new)
                .orElseGet(() -> stateMachine.getBasicQueryInfo(Optional.ofNullable(queryScheduler.get()).map(QueryScheduler::getBasicStageStats)));
    }

    @Override
    public void start()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            try {
                if (!stateMachine.transitionToPlanning()) {
                    // query already started or finished
                    return;
                }

                AtomicReference<Thread> planningThread = new AtomicReference<>(currentThread());
                stateMachine.getStateChange(PLANNING).addListener(() -> {
                    if (stateMachine.getQueryState() == FAILED) {
                        synchronized (this) {
                            Thread thread = planningThread.get();
                            if (thread != null) {
                                thread.interrupt();
                            }
                        }
                    }
                }, directExecutor());

                try {
                    PlanRoot plan = planQuery();
                    // DynamicFilterService needs plan for query to be registered.
                    // Query should be registered before dynamic filter suppliers are requested in distribution planning.
                    registerDynamicFilteringQuery(plan);
                    planDistribution(plan);
                }
                finally {
                    synchronized (this) {
                        planningThread.set(null);
                        // Clear the interrupted flag in case there was a race condition where
                        // the planning thread was interrupted right after planning completes above
                        Thread.interrupted();
                    }
                }

                tableExecuteContextManager.registerTableExecuteContextForQuery(getQueryId());

                if (!stateMachine.transitionToStarting()) {
                    // query already started or finished
                    return;
                }

                // if query is not finished, start the scheduler, otherwise cancel it
                QueryScheduler scheduler = queryScheduler.get();

                if (!stateMachine.isDone()) {
                    scheduler.start();
                }
            }
            catch (Throwable e) {
                fail(e);
                throwIfInstanceOf(e, Error.class);
            }
        }
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            stateMachine.addStateChangeListener(stateChangeListener);
        }
    }

    @Override
    public Session getSession()
    {
        return stateMachine.getSession();
    }

    @Override
    public void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        stateMachine.addQueryInfoStateChangeListener(stateChangeListener);
    }

    private PlanRoot planQuery()
    {
        Span span = tracer.spanBuilder("planner")
                .setParent(Context.current().with(getSession().getQuerySpan()))
                .startSpan();
        try (var ignored = scopedSpan(span)) {
            return doPlanQuery();
        }
        catch (StackOverflowError e) {
            throw new TrinoException(STACK_OVERFLOW, "statement is too large (stack overflow during analysis)", e);
        }
    }

    private PlanRoot doPlanQuery()
    {
        // plan query
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        LogicalPlanner logicalPlanner = new LogicalPlanner(stateMachine.getSession(),
                planOptimizers,
                idAllocator,
                plannerContext,
                typeAnalyzer,
                statsCalculator,
                costCalculator,
                stateMachine.getWarningCollector(),
                planOptimizersStatsCollector);
        Plan plan = logicalPlanner.plan(analysis);
        queryPlan.set(plan);

        // fragment the plan
        SubPlan fragmentedPlan;
        try (var ignored = scopedSpan(tracer, "fragment-plan")) {
            fragmentedPlan = planFragmenter.createSubPlans(stateMachine.getSession(), plan, false, stateMachine.getWarningCollector());
        }

        // extract inputs
        try (var ignored = scopedSpan(tracer, "extract-inputs")) {
            stateMachine.setInputs(new InputExtractor(plannerContext.getMetadata(), stateMachine.getSession()).extractInputs(fragmentedPlan));
        }

        stateMachine.setOutput(analysis.getTarget());

        boolean explainAnalyze = analysis.getStatement() instanceof ExplainAnalyze;
        return new PlanRoot(fragmentedPlan, !explainAnalyze);
    }

    private void planDistribution(PlanRoot plan)
    {
        // if query was canceled, skip creating scheduler
        if (stateMachine.isDone()) {
            return;
        }

        // record output field
        PlanFragment rootFragment = plan.getRoot().getFragment();
        stateMachine.setColumns(
                ((OutputNode) rootFragment.getRoot()).getColumnNames(),
                rootFragment.getTypes());

        RetryPolicy retryPolicy = getRetryPolicy(getSession());
        QueryScheduler scheduler;
        switch (retryPolicy) {
            case QUERY:
            case NONE:
                scheduler = new PipelinedQueryScheduler(
                        stateMachine,
                        plan.getRoot(),
                        nodePartitioningManager,
                        nodeScheduler,
                        remoteTaskFactory,
                        plan.isSummarizeTaskInfos(),
                        scheduleSplitBatchSize,
                        queryExecutor,
                        schedulerExecutor,
                        failureDetector,
                        nodeTaskMap,
                        executionPolicy,
                        tracer,
                        schedulerStats,
                        dynamicFilterService,
                        tableExecuteContextManager,
                        plannerContext.getMetadata(),
                        splitSourceFactory,
                        coordinatorTaskManager);
                break;
            case TASK:
                scheduler = new EventDrivenFaultTolerantQueryScheduler(
                        stateMachine,
                        plannerContext.getMetadata(),
                        remoteTaskFactory,
                        taskDescriptorStorage,
                        eventDrivenTaskSourceFactory,
                        plan.isSummarizeTaskInfos(),
                        nodeTaskMap,
                        queryExecutor,
                        schedulerExecutor,
                        tracer,
                        schedulerStats,
                        partitionMemoryEstimatorFactory,
                        nodePartitioningManager,
                        exchangeManagerRegistry.getExchangeManager(),
                        nodeAllocatorService,
                        failureDetector,
                        dynamicFilterService,
                        taskExecutionStats,
                        plan.getRoot());
                break;
            default:
                throw new IllegalArgumentException("Unexpected retry policy: " + retryPolicy);
        }

        queryScheduler.set(scheduler);
    }

    @Override
    public void cancelQuery()
    {
        stateMachine.transitionToCanceled();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        requireNonNull(stageId, "stageId is null");

        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            QueryScheduler scheduler = queryScheduler.get();
            if (scheduler != null) {
                scheduler.cancelStage(stageId);
            }
        }
    }

    @Override
    public void failTask(TaskId taskId, Exception reason)
    {
        requireNonNull(taskId, "stageId is null");

        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            QueryScheduler scheduler = queryScheduler.get();
            if (scheduler != null) {
                scheduler.failTask(taskId, reason);
            }
        }
    }

    @Override
    public void fail(Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        stateMachine.transitionToFailed(cause);
    }

    @Override
    public boolean isDone()
    {
        return getState().isDone();
    }

    @Override
    public void setOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        stateMachine.setOutputInfoListener(listener);
    }

    @Override
    public void outputTaskFailed(TaskId taskId, Throwable failure)
    {
        stateMachine.outputTaskFailed(taskId, failure);
    }

    @Override
    public void resultsConsumed()
    {
        stateMachine.resultsConsumed();
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return stateMachine.getStateChange(currentState);
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public void pruneInfo()
    {
        stateMachine.pruneQueryInfo();
    }

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            // acquire reference to scheduler before checking finalQueryInfo, because
            // state change listener sets finalQueryInfo and then clears scheduler when
            // the query finishes.
            QueryScheduler scheduler = queryScheduler.get();

            return stateMachine.getFinalQueryInfo().orElseGet(() -> buildQueryInfo(scheduler));
        }
    }

    @Override
    public QueryState getState()
    {
        return stateMachine.getQueryState();
    }

    @Override
    public Plan getQueryPlan()
    {
        return queryPlan.get();
    }

    private QueryInfo buildQueryInfo(QueryScheduler scheduler)
    {
        Optional<StageInfo> stageInfo = Optional.empty();
        if (scheduler != null) {
            stageInfo = Optional.ofNullable(scheduler.getStageInfo());
        }

        QueryInfo queryInfo = stateMachine.updateQueryInfo(stageInfo);
        if (queryInfo.isFinalQueryInfo()) {
            // capture the final query state and drop reference to the scheduler
            queryScheduler.set(null);
        }

        return queryInfo;
    }

    @Override
    public boolean shouldWaitForMinWorkers()
    {
        return shouldWaitForMinWorkers(analysis.getStatement());
    }

    private boolean shouldWaitForMinWorkers(Statement statement)
    {
        if (statement instanceof Query) {
            // Allow set session statements and queries on internal system connectors to run without waiting
            Collection<TableHandle> tables = analysis.getTables();
            return !tables.stream()
                    .map(TableHandle::getCatalogHandle)
                    .allMatch(catalogName -> catalogName.getType().isInternal());
        }
        return true;
    }

    private static class PlanRoot
    {
        private final SubPlan root;
        private final boolean summarizeTaskInfos;

        public PlanRoot(SubPlan root, boolean summarizeTaskInfos)
        {
            this.root = requireNonNull(root, "root is null");
            this.summarizeTaskInfos = summarizeTaskInfos;
        }

        public SubPlan getRoot()
        {
            return root;
        }

        public boolean isSummarizeTaskInfos()
        {
            return summarizeTaskInfos;
        }
    }

    public static class SqlQueryExecutionFactory
            implements QueryExecutionFactory<QueryExecution>
    {
        private final Tracer tracer;
        private final SplitSchedulerStats schedulerStats;
        private final int scheduleSplitBatchSize;
        private final PlannerContext plannerContext;
        private final AnalyzerFactory analyzerFactory;
        private final SplitSourceFactory splitSourceFactory;
        private final NodePartitioningManager nodePartitioningManager;
        private final NodeScheduler nodeScheduler;
        private final NodeAllocatorService nodeAllocatorService;
        private final PartitionMemoryEstimatorFactory partitionMemoryEstimatorFactory;
        private final TaskExecutionStats taskExecutionStats;
        private final List<PlanOptimizer> planOptimizers;
        private final PlanFragmenter planFragmenter;
        private final RemoteTaskFactory remoteTaskFactory;
        private final ExecutorService queryExecutor;
        private final ScheduledExecutorService schedulerExecutor;
        private final FailureDetector failureDetector;
        private final NodeTaskMap nodeTaskMap;
        private final Map<String, ExecutionPolicy> executionPolicies;
        private final StatsCalculator statsCalculator;
        private final CostCalculator costCalculator;
        private final DynamicFilterService dynamicFilterService;
        private final TableExecuteContextManager tableExecuteContextManager;
        private final TypeAnalyzer typeAnalyzer;
        private final SqlTaskManager coordinatorTaskManager;
        private final ExchangeManagerRegistry exchangeManagerRegistry;
        private final EventDrivenTaskSourceFactory eventDrivenTaskSourceFactory;
        private final TaskDescriptorStorage taskDescriptorStorage;

        @Inject
        SqlQueryExecutionFactory(
                Tracer tracer,
                QueryManagerConfig config,
                PlannerContext plannerContext,
                AnalyzerFactory analyzerFactory,
                SplitSourceFactory splitSourceFactory,
                NodePartitioningManager nodePartitioningManager,
                NodeScheduler nodeScheduler,
                NodeAllocatorService nodeAllocatorService,
                PartitionMemoryEstimatorFactory partitionMemoryEstimatorFactory,
                TaskExecutionStats taskExecutionStats,
                PlanOptimizersFactory planOptimizersFactory,
                PlanFragmenter planFragmenter,
                RemoteTaskFactory remoteTaskFactory,
                @ForQueryExecution ExecutorService queryExecutor,
                @ForScheduler ScheduledExecutorService schedulerExecutor,
                FailureDetector failureDetector,
                NodeTaskMap nodeTaskMap,
                Map<String, ExecutionPolicy> executionPolicies,
                SplitSchedulerStats schedulerStats,
                StatsCalculator statsCalculator,
                CostCalculator costCalculator,
                DynamicFilterService dynamicFilterService,
                TableExecuteContextManager tableExecuteContextManager,
                TypeAnalyzer typeAnalyzer,
                SqlTaskManager coordinatorTaskManager,
                ExchangeManagerRegistry exchangeManagerRegistry,
                EventDrivenTaskSourceFactory eventDrivenTaskSourceFactory,
                TaskDescriptorStorage taskDescriptorStorage)
        {
            this.tracer = requireNonNull(tracer, "tracer is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.scheduleSplitBatchSize = config.getScheduleSplitBatchSize();
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
            this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            this.nodeAllocatorService = requireNonNull(nodeAllocatorService, "nodeAllocatorService is null");
            this.partitionMemoryEstimatorFactory = requireNonNull(partitionMemoryEstimatorFactory, "partitionMemoryEstimatorFactory is null");
            this.taskExecutionStats = requireNonNull(taskExecutionStats, "taskExecutionStats is null");
            this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
            this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
            this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
            this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
            this.executionPolicies = requireNonNull(executionPolicies, "executionPolicies is null");
            this.planOptimizers = planOptimizersFactory.get();
            this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
            this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.coordinatorTaskManager = requireNonNull(coordinatorTaskManager, "coordinatorTaskManager is null");
            this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
            this.eventDrivenTaskSourceFactory = requireNonNull(eventDrivenTaskSourceFactory, "eventDrivenTaskSourceFactory is null");
            this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
        }

        @Override
        public QueryExecution createQueryExecution(
                PreparedQuery preparedQuery,
                QueryStateMachine stateMachine,
                Slug slug,
                WarningCollector warningCollector,
                PlanOptimizersStatsCollector planOptimizersStatsCollector)
        {
            String executionPolicyName = SystemSessionProperties.getExecutionPolicy(stateMachine.getSession());
            ExecutionPolicy executionPolicy = executionPolicies.get(executionPolicyName);
            checkArgument(executionPolicy != null, "No execution policy %s", executionPolicyName);

            return new SqlQueryExecution(
                    preparedQuery,
                    stateMachine,
                    slug,
                    tracer,
                    plannerContext,
                    analyzerFactory,
                    splitSourceFactory,
                    nodePartitioningManager,
                    nodeScheduler,
                    nodeAllocatorService,
                    partitionMemoryEstimatorFactory,
                    taskExecutionStats,
                    planOptimizers,
                    planFragmenter,
                    remoteTaskFactory,
                    scheduleSplitBatchSize,
                    queryExecutor,
                    schedulerExecutor,
                    failureDetector,
                    nodeTaskMap,
                    executionPolicy,
                    schedulerStats,
                    statsCalculator,
                    costCalculator,
                    dynamicFilterService,
                    warningCollector,
                    planOptimizersStatsCollector,
                    tableExecuteContextManager,
                    typeAnalyzer,
                    coordinatorTaskManager,
                    exchangeManagerRegistry,
                    eventDrivenTaskSourceFactory,
                    taskDescriptorStorage);
        }
    }
}
