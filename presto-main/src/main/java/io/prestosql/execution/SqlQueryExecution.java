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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.connector.CatalogName;
import io.prestosql.cost.CostCalculator;
import io.prestosql.cost.StatsCalculator;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.execution.scheduler.ExecutionPolicy;
import io.prestosql.execution.scheduler.NodeScheduler;
import io.prestosql.execution.scheduler.SplitSchedulerStats;
import io.prestosql.execution.scheduler.SqlQueryScheduler;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.failuredetector.FailureDetector;
import io.prestosql.memory.VersionedMemoryPoolId;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.operator.ForScheduler;
import io.prestosql.security.AccessControl;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.DynamicFilterService;
import io.prestosql.server.DynamicFilterService.StageDynamicFilters;
import io.prestosql.server.protocol.Slug;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.split.SplitManager;
import io.prestosql.split.SplitSource;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.analyzer.Analyzer;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.DistributedExecutionPlanner;
import io.prestosql.sql.planner.InputExtractor;
import io.prestosql.sql.planner.LogicalPlanner;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.sql.planner.PartitioningHandle;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.PlanFragmenter;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.PlanOptimizers;
import io.prestosql.sql.planner.StageExecutionPlan;
import io.prestosql.sql.planner.SubPlan;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.tree.Explain;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.Statement;
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
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.SystemSessionProperties.isEnableDynamicFiltering;
import static io.prestosql.execution.buffer.OutputBuffers.BROADCAST_PARTITION_ID;
import static io.prestosql.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.prestosql.execution.scheduler.SqlQueryScheduler.createSqlQueryScheduler;
import static io.prestosql.server.DynamicFilterService.DynamicFiltersStats;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.ParameterUtils.parameterExtractor;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class SqlQueryExecution
        implements QueryExecution
{
    private static final Logger log = Logger.get(SqlQueryExecution.class);

    private static final OutputBufferId OUTPUT_BUFFER_ID = new OutputBufferId(0);

    private final QueryStateMachine stateMachine;
    private final Slug slug;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final SplitManager splitManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final NodeScheduler nodeScheduler;
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

    private SqlQueryExecution(
            PreparedQuery preparedQuery,
            QueryStateMachine stateMachine,
            Slug slug,
            Metadata metadata,
            AccessControl accessControl,
            SqlParser sqlParser,
            SplitManager splitManager,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            List<PlanOptimizer> planOptimizers,
            PlanFragmenter planFragmenter,
            RemoteTaskFactory remoteTaskFactory,
            int scheduleSplitBatchSize,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap,
            QueryExplainer queryExplainer,
            ExecutionPolicy executionPolicy,
            SplitSchedulerStats schedulerStats,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            DynamicFilterService dynamicFilterService,
            WarningCollector warningCollector)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            this.slug = requireNonNull(slug, "slug is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
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

            checkArgument(scheduleSplitBatchSize > 0, "scheduleSplitBatchSize must be greater than 0");
            this.scheduleSplitBatchSize = scheduleSplitBatchSize;

            this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");

            // analyze query
            this.analysis = analyze(preparedQuery, stateMachine, metadata, accessControl, sqlParser, queryExplainer, warningCollector);

            stateMachine.addStateChangeListener(state -> {
                if (!state.isDone()) {
                    return;
                }
                unregisterDynamicFilteringQuery(
                        dynamicFilterService.getDynamicFilteringStats(stateMachine.getQueryId(), stateMachine.getSession()));
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

    private Analysis analyze(
            PreparedQuery preparedQuery,
            QueryStateMachine stateMachine,
            Metadata metadata,
            AccessControl accessControl,
            SqlParser sqlParser,
            QueryExplainer queryExplainer,
            WarningCollector warningCollector)
    {
        stateMachine.beginAnalysis();

        requireNonNull(preparedQuery, "preparedQuery is null");
        Analyzer analyzer = new Analyzer(
                stateMachine.getSession(),
                metadata,
                sqlParser,
                accessControl,
                Optional.of(queryExplainer),
                preparedQuery.getParameters(),
                parameterExtractor(preparedQuery.getStatement(), preparedQuery.getParameters()),
                warningCollector);
        Analysis analysis = analyzer.analyze(preparedQuery.getStatement());

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
    public VersionedMemoryPoolId getMemoryPool()
    {
        return stateMachine.getMemoryPool();
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        stateMachine.setMemoryPool(poolId);
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

                PlanRoot plan = planQuery();
                // DynamicFilterService needs plan for query to be registered.
                // Query should be registered before dynamic filter suppliers are requested in distribution planning.
                registerDynamicFilteringQuery(plan);
                planDistribution(plan);

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
            throw new PrestoException(NOT_SUPPORTED, "statement is too large (stack overflow during analysis)", e);
        }
    }

    private PlanRoot doPlanQuery()
    {
        // plan query
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        LogicalPlanner logicalPlanner = new LogicalPlanner(stateMachine.getSession(), planOptimizers, idAllocator, metadata, new TypeAnalyzer(sqlParser, metadata), statsCalculator, costCalculator, stateMachine.getWarningCollector());
        Plan plan = logicalPlanner.plan(analysis);
        queryPlan.set(plan);

        // extract inputs
        List<Input> inputs = new InputExtractor(metadata, stateMachine.getSession()).extractInputs(plan.getRoot());
        stateMachine.setInputs(inputs);

        stateMachine.setOutput(analysis.getTarget());

        // fragment the plan
        SubPlan fragmentedPlan = planFragmenter.createSubPlans(stateMachine.getSession(), plan, false, stateMachine.getWarningCollector());

        boolean explainAnalyze = analysis.getStatement() instanceof Explain && ((Explain) analysis.getStatement()).isAnalyze();
        return new PlanRoot(fragmentedPlan, !explainAnalyze);
    }

    private void planDistribution(PlanRoot plan)
    {
        // plan the execution on the active nodes
        DistributedExecutionPlanner distributedPlanner = new DistributedExecutionPlanner(splitManager, metadata, dynamicFilterService);
        StageExecutionPlan outputStageExecutionPlan = distributedPlanner.plan(plan.getRoot(), stateMachine.getSession());

        // ensure split sources are closed
        stateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                closeSplitSources(outputStageExecutionPlan);
            }
        });

        // if query was canceled, skip creating scheduler
        if (stateMachine.isDone()) {
            return;
        }

        // record output field
        stateMachine.setColumns(outputStageExecutionPlan.getFieldNames(), outputStageExecutionPlan.getFragment().getTypes());

        PartitioningHandle partitioningHandle = plan.getRoot().getFragment().getPartitioningScheme().getPartitioning().getHandle();
        OutputBuffers rootOutputBuffers = createInitialEmptyOutputBuffers(partitioningHandle)
                .withBuffer(OUTPUT_BUFFER_ID, BROADCAST_PARTITION_ID)
                .withNoMoreBufferIds();

        // build the stage execution objects (this doesn't schedule execution)
        SqlQueryScheduler scheduler = createSqlQueryScheduler(
                stateMachine,
                outputStageExecutionPlan,
                nodePartitioningManager,
                nodeScheduler,
                remoteTaskFactory,
                stateMachine.getSession(),
                plan.isSummarizeTaskInfos(),
                scheduleSplitBatchSize,
                queryExecutor,
                schedulerExecutor,
                failureDetector,
                rootOutputBuffers,
                nodeTaskMap,
                executionPolicy,
                schedulerStats,
                dynamicFilterService);

        queryScheduler.set(scheduler);

        // if query was canceled during scheduler creation, abort the scheduler
        // directly since the callback may have already fired
        if (stateMachine.isDone()) {
            scheduler.abort();
            queryScheduler.set(null);
        }
    }

    private static void closeSplitSources(StageExecutionPlan plan)
    {
        for (SplitSource source : plan.getSplitSources().values()) {
            try {
                source.close();
            }
            catch (Throwable t) {
                log.warn(t, "Error closing split source");
            }
        }

        for (StageExecutionPlan stage : plan.getSubStages()) {
            closeSplitSources(stage);
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

    public List<StageDynamicFilters> getStageDynamicFilters()
    {
        SqlQueryScheduler scheduler = queryScheduler.get();
        if (scheduler == null) {
            return ImmutableList.of();
        }

        return queryScheduler.get().getStageDynamicFilters();
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
        private final Metadata metadata;
        private final AccessControl accessControl;
        private final SqlParser sqlParser;
        private final SplitManager splitManager;
        private final NodePartitioningManager nodePartitioningManager;
        private final NodeScheduler nodeScheduler;
        private final List<PlanOptimizer> planOptimizers;
        private final PlanFragmenter planFragmenter;
        private final RemoteTaskFactory remoteTaskFactory;
        private final QueryExplainer queryExplainer;
        private final ExecutorService queryExecutor;
        private final ScheduledExecutorService schedulerExecutor;
        private final FailureDetector failureDetector;
        private final NodeTaskMap nodeTaskMap;
        private final Map<String, ExecutionPolicy> executionPolicies;
        private final StatsCalculator statsCalculator;
        private final CostCalculator costCalculator;
        private final DynamicFilterService dynamicFilterService;

        @Inject
        SqlQueryExecutionFactory(
                QueryManagerConfig config,
                Metadata metadata,
                AccessControl accessControl,
                SqlParser sqlParser,
                SplitManager splitManager,
                NodePartitioningManager nodePartitioningManager,
                NodeScheduler nodeScheduler,
                PlanOptimizers planOptimizers,
                PlanFragmenter planFragmenter,
                RemoteTaskFactory remoteTaskFactory,
                @ForQueryExecution ExecutorService queryExecutor,
                @ForScheduler ScheduledExecutorService schedulerExecutor,
                FailureDetector failureDetector,
                NodeTaskMap nodeTaskMap,
                QueryExplainer queryExplainer,
                Map<String, ExecutionPolicy> executionPolicies,
                SplitSchedulerStats schedulerStats,
                StatsCalculator statsCalculator,
                CostCalculator costCalculator,
                DynamicFilterService dynamicFilterService)
        {
            requireNonNull(config, "config is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.scheduleSplitBatchSize = config.getScheduleSplitBatchSize();
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
            this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
            this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
            this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
            this.executionPolicies = requireNonNull(executionPolicies, "schedulerPolicies is null");
            this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null").get();
            this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
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
                    metadata,
                    accessControl,
                    sqlParser,
                    splitManager,
                    nodePartitioningManager,
                    nodeScheduler,
                    planOptimizers,
                    planFragmenter,
                    remoteTaskFactory,
                    scheduleSplitBatchSize,
                    queryExecutor,
                    schedulerExecutor,
                    failureDetector,
                    nodeTaskMap,
                    queryExplainer,
                    executionPolicy,
                    schedulerStats,
                    statsCalculator,
                    costCalculator,
                    dynamicFilterService,
                    warningCollector);
        }
    }
}
