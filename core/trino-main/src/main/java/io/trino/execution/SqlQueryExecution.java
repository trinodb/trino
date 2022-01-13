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
import io.airlift.concurrent.SetThreadName;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.connector.CatalogName;
import io.trino.cost.CostCalculator;
import io.trino.cost.StatsCalculator;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.scheduler.NodeAllocatorService;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.PartitionMemoryEstimator;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.execution.scheduler.SqlQueryScheduler;
import io.trino.execution.scheduler.TaskDescriptorStorage;
import io.trino.execution.scheduler.TaskSourceFactory;
import io.trino.execution.scheduler.policy.ExecutionPolicy;
import io.trino.execution.warnings.WarningCollector;
import io.trino.failuredetector.FailureDetector;
import io.trino.metadata.TableHandle;
import io.trino.operator.ForScheduler;
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
import javax.inject.Inject;

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
import static io.trino.SystemSessionProperties.isEnableDynamicFiltering;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.PLANNING;
import static io.trino.server.DynamicFilterService.DynamicFiltersStats;
import static io.trino.spi.StandardErrorCode.STACK_OVERFLOW;
import static io.trino.sql.ParameterUtils.parameterExtractor;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class SqlQueryExecution
        implements QueryExecution
{
    private final QueryStateMachine stateMachine;
    private final Slug slug;
    private final PlannerContext plannerContext;
    private final SplitSourceFactory splitSourceFactory;
    private final NodePartitioningManager nodePartitioningManager;
    private final NodeScheduler nodeScheduler;
    private final NodeAllocatorService nodeAllocatorService;
    private final PartitionMemoryEstimator partitionMemoryEstimator;
    private final List<PlanOptimizer> planOptimizers;
    private final PlanFragmenter planFragmenter;
    private final RemoteTaskFactory remoteTaskFactory;
    private final int scheduleSplitBatchSize;
    private final ExecutorService queryExecutor;
    private final ScheduledExecutorService schedulerExecutor;
    private final FailureDetector failureDetector;

    private final AtomicReference<SqlQueryScheduler> queryScheduler = new AtomicReference<>();
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
    private final TaskSourceFactory taskSourceFactory;
    private final TaskDescriptorStorage taskDescriptorStorage;

    private SqlQueryExecution(
            PreparedQuery preparedQuery,
            QueryStateMachine stateMachine,
            Slug slug,
            PlannerContext plannerContext,
            AnalyzerFactory analyzerFactory,
            SplitSourceFactory splitSourceFactory,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            NodeAllocatorService nodeAllocatorService,
            PartitionMemoryEstimator partitionMemoryEstimator,
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
            TableExecuteContextManager tableExecuteContextManager,
            TypeAnalyzer typeAnalyzer,
            SqlTaskManager coordinatorTaskManager,
            ExchangeManagerRegistry exchangeManagerRegistry,
            TaskSourceFactory taskSourceFactory,
            TaskDescriptorStorage taskDescriptorStorage)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            this.slug = requireNonNull(slug, "slug is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            this.nodeAllocatorService = requireNonNull(nodeAllocatorService, "nodeAllocatorService is null");
            this.partitionMemoryEstimator = requireNonNull(partitionMemoryEstimator, "partitionMemoryEstimator is null");
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
            this.analysis = analyze(preparedQuery, stateMachine, warningCollector, analyzerFactory);

            stateMachine.addStateChangeListener(state -> {
                if (!state.isDone()) {
                    return;
                }
                unregisterDynamicFilteringQuery(
                        dynamicFilterService.getDynamicFilteringStats(stateMachine.getQueryId(), stateMachine.getSession()));

                tableExecuteContextManager.unregisterTableExecuteContextForQuery(stateMachine.getQueryId());
            });

            // when the query finishes cache the final query info, and clear the reference to the output stage
            AtomicReference<SqlQueryScheduler> queryScheduler = this.queryScheduler;
            stateMachine.addStateChangeListener(state -> {
                if (!state.isDone()) {
                    return;
                }

                // query is now done, so abort any work that is still running
                SqlQueryScheduler scheduler = queryScheduler.get();
                if (scheduler != null) {
                    scheduler.abort();
                }
            });

            this.remoteTaskFactory = new MemoryTrackingRemoteTaskFactory(requireNonNull(remoteTaskFactory, "remoteTaskFactory is null"), stateMachine);
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.coordinatorTaskManager = requireNonNull(coordinatorTaskManager, "coordinatorTaskManager is null");
            this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
            this.taskSourceFactory = requireNonNull(taskSourceFactory, "taskSourceFactory is null");
            this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
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
            AnalyzerFactory analyzerFactory)
    {
        stateMachine.beginAnalysis();

        requireNonNull(preparedQuery, "preparedQuery is null");
        Analyzer analyzer = analyzerFactory.createAnalyzer(
                stateMachine.getSession(),
                preparedQuery.getParameters(),
                parameterExtractor(preparedQuery.getStatement(), preparedQuery.getParameters()),
                warningCollector);
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
        SqlQueryScheduler scheduler = queryScheduler.get();
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
        SqlQueryScheduler scheduler = queryScheduler.get();
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
        SqlQueryScheduler scheduler = queryScheduler.get();
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
                .orElseGet(() -> stateMachine.getBasicQueryInfo(Optional.ofNullable(queryScheduler.get()).map(SqlQueryScheduler::getBasicStageStats)));
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
                SqlQueryScheduler scheduler = queryScheduler.get();

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
        try {
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
                stateMachine.getWarningCollector());
        Plan plan = logicalPlanner.plan(analysis);
        queryPlan.set(plan);

        // fragment the plan
        SubPlan fragmentedPlan = planFragmenter.createSubPlans(stateMachine.getSession(), plan, false, stateMachine.getWarningCollector());

        // extract inputs
        List<Input> inputs = new InputExtractor(plannerContext.getMetadata(), stateMachine.getSession()).extractInputs(fragmentedPlan);
        stateMachine.setInputs(inputs);

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

        // build the stage execution objects (this doesn't schedule execution)
        SqlQueryScheduler scheduler = new SqlQueryScheduler(
                stateMachine,
                plan.getRoot(),
                nodePartitioningManager,
                nodeScheduler,
                nodeAllocatorService,
                partitionMemoryEstimator,
                remoteTaskFactory,
                plan.isSummarizeTaskInfos(),
                scheduleSplitBatchSize,
                queryExecutor,
                schedulerExecutor,
                failureDetector,
                nodeTaskMap,
                executionPolicy,
                schedulerStats,
                dynamicFilterService,
                tableExecuteContextManager,
                plannerContext.getMetadata(),
                splitSourceFactory,
                coordinatorTaskManager,
                exchangeManagerRegistry,
                taskSourceFactory,
                taskDescriptorStorage);

        queryScheduler.set(scheduler);

        // if query was canceled during scheduler creation, abort the scheduler
        // directly since the callback may have already fired
        if (stateMachine.isDone()) {
            scheduler.abort();
            queryScheduler.set(null);
        }
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
            SqlQueryScheduler scheduler = queryScheduler.get();
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
            SqlQueryScheduler scheduler = queryScheduler.get();
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
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        stateMachine.addOutputInfoListener(listener);
    }

    @Override
    public void outputTaskFailed(TaskId taskId, Throwable failure)
    {
        stateMachine.outputTaskFailed(taskId, failure);
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
            SqlQueryScheduler scheduler = queryScheduler.get();

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

    private QueryInfo buildQueryInfo(SqlQueryScheduler scheduler)
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
                    .map(TableHandle::getCatalogName)
                    .allMatch(CatalogName::isInternalSystemConnector);
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
        private final SplitSchedulerStats schedulerStats;
        private final int scheduleSplitBatchSize;
        private final PlannerContext plannerContext;
        private final AnalyzerFactory analyzerFactory;
        private final SplitSourceFactory splitSourceFactory;
        private final NodePartitioningManager nodePartitioningManager;
        private final NodeScheduler nodeScheduler;
        private final NodeAllocatorService nodeAllocatorService;
        private final PartitionMemoryEstimator partitionMemoryEstimator;
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
        private final TaskSourceFactory taskSourceFactory;
        private final TaskDescriptorStorage taskDescriptorStorage;

        @Inject
        SqlQueryExecutionFactory(
                QueryManagerConfig config,
                PlannerContext plannerContext,
                AnalyzerFactory analyzerFactory,
                SplitSourceFactory splitSourceFactory,
                NodePartitioningManager nodePartitioningManager,
                NodeScheduler nodeScheduler,
                NodeAllocatorService nodeAllocatorService,
                PartitionMemoryEstimator partitionMemoryEstimator,
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
                TaskSourceFactory taskSourceFactory,
                TaskDescriptorStorage taskDescriptorStorage)
        {
            requireNonNull(config, "config is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.scheduleSplitBatchSize = config.getScheduleSplitBatchSize();
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
            this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            this.nodeAllocatorService = requireNonNull(nodeAllocatorService, "nodeAllocatorService is null");
            this.partitionMemoryEstimator = requireNonNull(partitionMemoryEstimator, "partitionMemoryEstimator is null");
            this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
            this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
            this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
            this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
            this.executionPolicies = requireNonNull(executionPolicies, "executionPolicies is null");
            this.planOptimizers = requireNonNull(planOptimizersFactory, "planOptimizersFactory is null").get();
            this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
            this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.coordinatorTaskManager = requireNonNull(coordinatorTaskManager, "coordinatorTaskManager is null");
            this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
            this.taskSourceFactory = requireNonNull(taskSourceFactory, "taskSourceFactory is null");
            this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
        }

        @Override
        public QueryExecution createQueryExecution(
                PreparedQuery preparedQuery,
                QueryStateMachine stateMachine,
                Slug slug,
                WarningCollector warningCollector)
        {
            String executionPolicyName = SystemSessionProperties.getExecutionPolicy(stateMachine.getSession());
            ExecutionPolicy executionPolicy = executionPolicies.get(executionPolicyName);
            checkArgument(executionPolicy != null, "No execution policy %s", executionPolicyName);

            return new SqlQueryExecution(
                    preparedQuery,
                    stateMachine,
                    slug,
                    plannerContext,
                    analyzerFactory,
                    splitSourceFactory,
                    nodePartitioningManager,
                    nodeScheduler,
                    nodeAllocatorService,
                    partitionMemoryEstimator,
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
                    tableExecuteContextManager,
                    typeAnalyzer,
                    coordinatorTaskManager,
                    exchangeManagerRegistry,
                    taskSourceFactory,
                    taskDescriptorStorage);
        }
    }
}
