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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.exchange.ExchangeInput;
import io.trino.execution.QueryExecution.QueryOutputInfo;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.operator.BlockedReason;
import io.trino.operator.OperatorStats;
import io.trino.security.AccessControl;
import io.trino.server.BasicQueryInfo;
import io.trino.server.BasicQueryStats;
import io.trino.server.ResultQueryInfo;
import io.trino.spi.ErrorCode;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.eventlistener.RoutineInfo;
import io.trino.spi.eventlistener.StageGcStatistics;
import io.trino.spi.eventlistener.TableInfo;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.Output;
import io.trino.sql.planner.PlanFragment;
import io.trino.tracing.TrinoAttributes;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionInfo;
import io.trino.transaction.TransactionManager;
import jakarta.annotation.Nullable;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.isSpoolingEnabled;
import static io.trino.execution.BasicStageStats.EMPTY_STAGE_STATS;
import static io.trino.execution.QueryState.DISPATCHING;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.execution.QueryState.FINISHING;
import static io.trino.execution.QueryState.PLANNING;
import static io.trino.execution.QueryState.QUEUED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.execution.QueryState.STARTING;
import static io.trino.execution.QueryState.TERMINAL_QUERY_STATES;
import static io.trino.execution.QueryState.WAITING_FOR_RESOURCES;
import static io.trino.execution.StageInfo.getAllStages;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.server.DynamicFilterService.DynamicFiltersStats;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.USER_CANCELED;
import static io.trino.spi.resourcegroups.QueryType.SELECT;
import static io.trino.util.Ciphers.createRandomAesEncryptionKey;
import static io.trino.util.Ciphers.serializeAesEncryptionKey;
import static io.trino.util.Failures.toFailure;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class QueryStateMachine
{
    private static final Logger QUERY_STATE_LOG = Logger.get(QueryStateMachine.class);

    private final QueryId queryId;
    private final String query;
    private final Optional<String> preparedQuery;
    private final Session session;
    private final URI self;
    private final ResourceGroupId resourceGroup;
    private final TransactionManager transactionManager;
    private final Metadata metadata;
    private final QueryOutputManager outputManager;
    private final Executor stateMachineExecutor;

    private final AtomicLong currentUserMemory = new AtomicLong();
    private final AtomicLong peakUserMemory = new AtomicLong();

    private final AtomicLong currentRevocableMemory = new AtomicLong();
    private final AtomicLong peakRevocableMemory = new AtomicLong();

    // peak of the user + system + revocable memory reservation
    private final AtomicLong currentTotalMemory = new AtomicLong();
    private final AtomicLong peakTotalMemory = new AtomicLong();

    private final AtomicLong peakTaskUserMemory = new AtomicLong();
    private final AtomicLong peakTaskRevocableMemory = new AtomicLong();
    private final AtomicLong peakTaskTotalMemory = new AtomicLong();

    private final QueryStateTimer queryStateTimer;

    private final StateMachine<QueryState> queryState;
    private final AtomicBoolean queryCleanedUp = new AtomicBoolean();

    private final AtomicReference<String> setCatalog = new AtomicReference<>();
    private final AtomicReference<String> setSchema = new AtomicReference<>();
    private final AtomicReference<String> setPath = new AtomicReference<>();

    private final AtomicReference<String> setAuthorizationUser = new AtomicReference<>();
    private final AtomicBoolean resetAuthorizationUser = new AtomicBoolean();

    private final Map<String, String> setSessionProperties = new ConcurrentHashMap<>();
    private final Set<String> resetSessionProperties = Sets.newConcurrentHashSet();

    private final Map<String, SelectedRole> setRoles = new ConcurrentHashMap<>();

    private final Map<String, String> addedPreparedStatements = new ConcurrentHashMap<>();
    private final Set<String> deallocatedPreparedStatements = Sets.newConcurrentHashSet();

    private final AtomicReference<TransactionId> startedTransactionId = new AtomicReference<>();
    private final AtomicBoolean clearTransactionId = new AtomicBoolean();

    private final AtomicReference<String> updateType = new AtomicReference<>();

    private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();

    private final AtomicReference<Set<Input>> inputs = new AtomicReference<>(ImmutableSet.of());
    private final AtomicReference<Optional<Output>> output = new AtomicReference<>(Optional.empty());
    private final AtomicReference<List<TableInfo>> referencedTables = new AtomicReference<>(ImmutableList.of());
    private final AtomicReference<List<RoutineInfo>> routines = new AtomicReference<>(ImmutableList.of());
    private final StateMachine<Optional<QueryInfo>> finalQueryInfo;

    private final WarningCollector warningCollector;
    private final PlanOptimizersStatsCollector planOptimizersStatsCollector;

    private final Optional<QueryType> queryType;

    @GuardedBy("dynamicFiltersStatsSupplierLock")
    private Supplier<DynamicFiltersStats> dynamicFiltersStatsSupplier = () -> DynamicFiltersStats.EMPTY;
    private final Object dynamicFiltersStatsSupplierLock = new Object();

    private final AtomicBoolean committed = new AtomicBoolean();
    private final AtomicBoolean consumed = new AtomicBoolean();

    private final NodeVersion version;

    private QueryStateMachine(
            String query,
            Optional<String> preparedQuery,
            Session session,
            URI self,
            ResourceGroupId resourceGroup,
            TransactionManager transactionManager,
            Executor stateMachineExecutor,
            Ticker ticker,
            Metadata metadata,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector queryStatsCollector,
            Optional<QueryType> queryType,
            NodeVersion version)
    {
        this.query = requireNonNull(query, "query is null");
        this.preparedQuery = requireNonNull(preparedQuery, "preparedQuery is null");
        this.session = requireNonNull(session, "session is null");
        this.queryId = session.getQueryId();
        this.self = requireNonNull(self, "self is null");
        this.resourceGroup = requireNonNull(resourceGroup, "resourceGroup is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.queryStateTimer = new QueryStateTimer(ticker);
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.stateMachineExecutor = requireNonNull(stateMachineExecutor, "stateMachineExecutor is null");
        this.planOptimizersStatsCollector = requireNonNull(queryStatsCollector, "queryStatsCollector is null");

        this.queryState = new StateMachine<>("query " + query, stateMachineExecutor, QUEUED, TERMINAL_QUERY_STATES);
        this.finalQueryInfo = new StateMachine<>("finalQueryInfo-" + queryId, stateMachineExecutor, Optional.empty());
        this.outputManager = new QueryOutputManager(stateMachineExecutor);
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.version = requireNonNull(version, "version is null");
    }

    /**
     * Created QueryStateMachines must be transitioned to terminal states to clean up resources.
     */
    public static QueryStateMachine begin(
            Optional<TransactionId> existingTransactionId,
            String query,
            Optional<String> preparedQuery,
            Session session,
            URI self,
            ResourceGroupId resourceGroup,
            boolean transactionControl,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Executor executor,
            Metadata metadata,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector queryStatsCollector,
            Optional<QueryType> queryType,
            boolean faultTolerantExecutionExchangeEncryptionEnabled,
            NodeVersion version)
    {
        return beginWithTicker(
                existingTransactionId,
                query,
                preparedQuery,
                session,
                self,
                resourceGroup,
                transactionControl,
                transactionManager,
                accessControl,
                executor,
                Ticker.systemTicker(),
                metadata,
                warningCollector,
                queryStatsCollector,
                queryType,
                faultTolerantExecutionExchangeEncryptionEnabled,
                version);
    }

    static QueryStateMachine beginWithTicker(
            Optional<TransactionId> existingTransactionId,
            String query,
            Optional<String> preparedQuery,
            Session session,
            URI self,
            ResourceGroupId resourceGroup,
            boolean transactionControl,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Executor executor,
            Ticker ticker,
            Metadata metadata,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector queryStatsCollector,
            Optional<QueryType> queryType,
            boolean faultTolerantExecutionExchangeEncryptionEnabled,
            NodeVersion version)
    {
        // if there is an existing transaction, activate it
        existingTransactionId.ifPresent(transactionId -> {
            if (transactionControl) {
                transactionManager.trySetActive(transactionId);
            }
            else {
                transactionManager.checkAndSetActive(transactionId);
            }
        });

        // add the session to the existing transaction, or create a new auto commit transaction for the session
        // NOTE: for the start transaction command, no transaction is created
        if (existingTransactionId.isPresent() || !transactionControl) {
            // TODO: make autocommit isolation level a session parameter
            TransactionId transactionId = existingTransactionId
                    .orElseGet(() -> transactionManager.beginTransaction(true));
            session = session.beginTransactionId(transactionId, transactionManager, accessControl);
        }

        if (getRetryPolicy(session) == TASK && faultTolerantExecutionExchangeEncryptionEnabled) {
            // encryption is mandatory for fault tolerant execution as it relies on an external storage to store intermediate data generated during an exchange
            session = session.withExchangeEncryption(serializeAesEncryptionKey(createRandomAesEncryptionKey()));
        }

        if (!queryType.map(SELECT::equals).orElse(false) || !isSpoolingEnabled(session)) {
            session = session.withoutSpooling();
        }

        Span querySpan = session.getQuerySpan();

        querySpan.setAttribute(TrinoAttributes.QUERY_TYPE, queryType.map(Enum::name).orElse("UNKNOWN"));

        QueryStateMachine queryStateMachine = new QueryStateMachine(
                query,
                preparedQuery,
                session,
                self,
                resourceGroup,
                transactionManager,
                executor,
                ticker,
                metadata,
                warningCollector,
                queryStatsCollector,
                queryType,
                version);

        queryStateMachine.addStateChangeListener(newState -> {
            QUERY_STATE_LOG.debug("Query %s is %s", queryStateMachine.getQueryId(), newState);
            if (newState.isDone()) {
                queryStateMachine.getSession().getTransactionId().ifPresent(transactionManager::trySetInactive);
                queryStateMachine.getOutputManager().setQueryCompleted();
            }
        });

        queryStateMachine.addStateChangeListener(newState -> {
            querySpan.addEvent("query_state", Attributes.of(
                    TrinoAttributes.EVENT_STATE, newState.toString()));
            if (newState.isDone()) {
                queryStateMachine.getFailureInfo().ifPresentOrElse(
                        failure -> {
                            ErrorCode errorCode = requireNonNull(failure.getErrorCode());
                            querySpan.setStatus(StatusCode.ERROR, nullToEmpty(failure.getMessage()))
                                    .recordException(failure.toException())
                                    .setAttribute(TrinoAttributes.ERROR_CODE, errorCode.getCode())
                                    .setAttribute(TrinoAttributes.ERROR_NAME, errorCode.getName())
                                    .setAttribute(TrinoAttributes.ERROR_TYPE, errorCode.getType().toString());
                        },
                        () -> querySpan.setStatus(StatusCode.OK));
                querySpan.end();
            }
        });

        metadata.beginQuery(session);

        return queryStateMachine;
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public Session getSession()
    {
        return session;
    }

    public Executor getStateMachineExecutor()
    {
        return stateMachineExecutor;
    }

    public long getPeakUserMemoryInBytes()
    {
        return peakUserMemory.get();
    }

    public long getPeakRevocableMemoryInBytes()
    {
        return peakRevocableMemory.get();
    }

    public long getPeakTotalMemoryInBytes()
    {
        return peakTotalMemory.get();
    }

    public long getPeakTaskUserMemory()
    {
        return peakTaskUserMemory.get();
    }

    public long getPeakTaskRevocableMemory()
    {
        return peakTaskRevocableMemory.get();
    }

    public long getPeakTaskTotalMemory()
    {
        return peakTaskTotalMemory.get();
    }

    public WarningCollector getWarningCollector()
    {
        return warningCollector;
    }

    public PlanOptimizersStatsCollector getPlanOptimizersStatsCollector()
    {
        return planOptimizersStatsCollector;
    }

    public void updateMemoryUsage(
            long deltaUserMemoryInBytes,
            long deltaRevocableMemoryInBytes,
            long deltaTotalMemoryInBytes,
            long taskUserMemoryInBytes,
            long taskRevocableMemoryInBytes,
            long taskTotalMemoryInBytes)
    {
        currentUserMemory.addAndGet(deltaUserMemoryInBytes);
        currentRevocableMemory.addAndGet(deltaRevocableMemoryInBytes);
        currentTotalMemory.addAndGet(deltaTotalMemoryInBytes);
        peakUserMemory.updateAndGet(currentPeakValue -> Math.max(currentUserMemory.get(), currentPeakValue));
        peakRevocableMemory.updateAndGet(currentPeakValue -> Math.max(currentRevocableMemory.get(), currentPeakValue));
        peakTotalMemory.updateAndGet(currentPeakValue -> Math.max(currentTotalMemory.get(), currentPeakValue));
        peakTaskUserMemory.accumulateAndGet(taskUserMemoryInBytes, Math::max);
        peakTaskRevocableMemory.accumulateAndGet(taskRevocableMemoryInBytes, Math::max);
        peakTaskTotalMemory.accumulateAndGet(taskTotalMemoryInBytes, Math::max);
    }

    public BasicQueryInfo getBasicQueryInfo(Optional<BasicStageStats> rootStage)
    {
        // Query state must be captured first in order to provide a
        // correct view of the query.  For example, building this
        // information, the query could finish, and the task states would
        // never be visible.
        QueryState state = queryState.get();

        ErrorCode errorCode = null;
        if (state == FAILED) {
            ExecutionFailureInfo failureCause = this.failureCause.get();
            if (failureCause != null) {
                errorCode = failureCause.getErrorCode();
            }
        }

        BasicStageStats stageStats = rootStage.orElse(EMPTY_STAGE_STATS);
        BasicQueryStats queryStats = createBasicQueryStats(stageStats);

        return new BasicQueryInfo(
                queryId,
                session.toSessionRepresentation(),
                Optional.of(resourceGroup),
                state,
                stageStats.isScheduled(),
                self,
                query,
                Optional.ofNullable(updateType.get()),
                preparedQuery,
                queryStats,
                errorCode == null ? null : errorCode.getType(),
                errorCode,
                queryType,
                getRetryPolicy(session));
    }

    @VisibleForTesting
    ResultQueryInfo getResultQueryInfo(Optional<BasicStageInfo> stageInfo)
    {
        QueryState state = queryState.get();

        ErrorCode errorCode = null;
        if (state == FAILED) {
            ExecutionFailureInfo failureCause = this.failureCause.get();
            if (failureCause != null) {
                errorCode = failureCause.getErrorCode();
            }
        }

        List<BasicStageInfo> allStages = BasicStageInfo.getAllStages(stageInfo);
        boolean finalInfo = state.isDone() && allStages.stream().allMatch(BasicStageInfo::isFinalStageInfo);

        BasicStageStats stageStats = stageInfo
                .map(stage -> allStages.stream().map(BasicStageInfo::getStageStats).toList())
                .map(BasicStageStats::aggregateBasicStageStats)
                .orElse(EMPTY_STAGE_STATS);

        BasicQueryStats queryStats = createBasicQueryStats(stageStats);

        boolean scheduled = getRetryPolicy(session) == TASK
                ? stageInfo.isPresent() && allStages.stream().map(BasicStageInfo::getState).anyMatch(StageState::isScheduled)
                : stageInfo.isPresent() && allStages.stream().map(BasicStageInfo::getState).allMatch(StageState::isScheduled);

        return new ResultQueryInfo(
                queryId,
                state,
                scheduled,
                updateType.get(),
                queryStats,
                errorCode,
                stageInfo,
                finalInfo,
                failureCause.get(),
                Optional.ofNullable(setCatalog.get()),
                Optional.ofNullable(setSchema.get()),
                Optional.ofNullable(setPath.get()),
                Optional.ofNullable(setAuthorizationUser.get()),
                resetAuthorizationUser.get(),
                setSessionProperties,
                resetSessionProperties,
                setRoles,
                addedPreparedStatements,
                deallocatedPreparedStatements,
                Optional.ofNullable(startedTransactionId.get()),
                clearTransactionId.get(),
                warningCollector.getWarnings());
    }

    private BasicQueryStats createBasicQueryStats(BasicStageStats stageStats)
    {
        BasicQueryStats queryStats = new BasicQueryStats(
                queryStateTimer.getCreateTime(),
                getEndTime().orElse(null),
                queryStateTimer.getQueuedTime(),
                queryStateTimer.getElapsedTime(),
                queryStateTimer.getExecutionTime(),

                stageStats.getFailedTasks(),

                stageStats.getTotalDrivers(),
                stageStats.getQueuedDrivers(),
                stageStats.getRunningDrivers(),
                stageStats.getCompletedDrivers(),
                stageStats.getBlockedDrivers(),

                stageStats.getRawInputDataSize(),
                stageStats.getRawInputPositions(),
                stageStats.getSpilledDataSize(),
                stageStats.getPhysicalInputDataSize(),
                stageStats.getPhysicalWrittenDataSize(),
                stageStats.getInternalNetworkInputDataSize(),

                stageStats.getCumulativeUserMemory(),
                stageStats.getFailedCumulativeUserMemory(),
                stageStats.getUserMemoryReservation(),
                stageStats.getTotalMemoryReservation(),
                succinctBytes(getPeakUserMemoryInBytes()),
                succinctBytes(getPeakTotalMemoryInBytes()),

                queryStateTimer.getPlanningTime(),
                queryStateTimer.getAnalysisTime(),
                stageStats.getTotalCpuTime(),
                stageStats.getFailedCpuTime(),
                stageStats.getTotalScheduledTime(),
                stageStats.getFailedScheduledTime(),
                queryStateTimer.getFinishingTime(),
                stageStats.getPhysicalInputReadTime(),

                stageStats.isFullyBlocked(),
                stageStats.getBlockedReasons(),
                stageStats.getProgressPercentage(),
                stageStats.getRunningPercentage());
        return queryStats;
    }

    @VisibleForTesting
    QueryInfo getQueryInfo(Optional<StageInfo> rootStage)
    {
        // Query state must be captured first in order to provide a
        // correct view of the query.  For example, building this
        // information, the query could finish, and the task states would
        // never be visible.
        QueryState state = queryState.get();

        ExecutionFailureInfo failureCause = null;
        ErrorCode errorCode = null;
        if (state == FAILED) {
            failureCause = this.failureCause.get();
            if (failureCause != null) {
                errorCode = failureCause.getErrorCode();
            }
        }

        List<StageInfo> allStages = getAllStages(rootStage);
        QueryStats queryStats = getQueryStats(rootStage, allStages);
        boolean finalInfo = state.isDone() && allStages.stream().allMatch(StageInfo::isFinalStageInfo);

        return new QueryInfo(
                queryId,
                session.toSessionRepresentation(),
                state,
                self,
                outputManager.getQueryOutputInfo().map(QueryOutputInfo::getColumnNames).orElse(ImmutableList.of()),
                query,
                preparedQuery,
                queryStats,
                Optional.ofNullable(setCatalog.get()),
                Optional.ofNullable(setSchema.get()),
                Optional.ofNullable(setPath.get()),
                Optional.ofNullable(setAuthorizationUser.get()),
                resetAuthorizationUser.get(),
                setSessionProperties,
                resetSessionProperties,
                setRoles,
                addedPreparedStatements,
                deallocatedPreparedStatements,
                Optional.ofNullable(startedTransactionId.get()),
                clearTransactionId.get(),
                updateType.get(),
                rootStage,
                failureCause,
                errorCode,
                warningCollector.getWarnings(),
                inputs.get(),
                output.get(),
                referencedTables.get(),
                routines.get(),
                finalInfo,
                Optional.of(resourceGroup),
                queryType,
                getRetryPolicy(session),
                false,
                version);
    }

    private QueryStats getQueryStats(Optional<StageInfo> rootStage, List<StageInfo> allStages)
    {
        int totalTasks = 0;
        int runningTasks = 0;
        int completedTasks = 0;
        int failedTasks = 0;

        int totalDrivers = 0;
        int queuedDrivers = 0;
        int runningDrivers = 0;
        int blockedDrivers = 0;
        int completedDrivers = 0;

        double cumulativeUserMemory = 0;
        double failedCumulativeUserMemory = 0;
        long userMemoryReservation = 0;
        long revocableMemoryReservation = 0;
        long totalMemoryReservation = 0;

        long totalScheduledTime = 0;
        long failedScheduledTime = 0;
        long totalCpuTime = 0;
        long failedCpuTime = 0;
        long totalBlockedTime = 0;

        long physicalInputDataSize = 0;
        long failedPhysicalInputDataSize = 0;
        long physicalInputPositions = 0;
        long failedPhysicalInputPositions = 0;
        long physicalInputReadTime = 0;
        long failedPhysicalInputReadTime = 0;

        long internalNetworkInputDataSize = 0;
        long failedInternalNetworkInputDataSize = 0;
        long internalNetworkInputPositions = 0;
        long failedInternalNetworkInputPositions = 0;

        long rawInputDataSize = 0;
        long failedRawInputDataSize = 0;
        long rawInputPositions = 0;
        long failedRawInputPositions = 0;

        long processedInputDataSize = 0;
        long failedProcessedInputDataSize = 0;
        long processedInputPositions = 0;
        long failedProcessedInputPositions = 0;

        long inputBlockedTime = 0;
        long failedInputBlockedTime = 0;

        long outputDataSize = 0;
        long failedOutputDataSize = 0;
        long outputPositions = 0;
        long failedOutputPositions = 0;

        long outputBlockedTime = 0;
        long failedOutputBlockedTime = 0;

        long physicalWrittenDataSize = 0;
        long failedPhysicalWrittenDataSize = 0;

        ImmutableList.Builder<StageGcStatistics> stageGcStatistics = ImmutableList.builderWithExpectedSize(allStages.size());

        boolean fullyBlocked = rootStage.isPresent();
        Set<BlockedReason> blockedReasons = new HashSet<>();

        ImmutableList.Builder<OperatorStats> operatorStatsSummary = ImmutableList.builder();
        for (StageInfo stageInfo : allStages) {
            StageStats stageStats = stageInfo.getStageStats();
            totalTasks += stageStats.getTotalTasks();
            runningTasks += stageStats.getRunningTasks();
            completedTasks += stageStats.getCompletedTasks();
            failedTasks += stageStats.getFailedTasks();

            totalDrivers += stageStats.getTotalDrivers();
            queuedDrivers += stageStats.getQueuedDrivers();
            runningDrivers += stageStats.getRunningDrivers();
            blockedDrivers += stageStats.getBlockedDrivers();
            completedDrivers += stageStats.getCompletedDrivers();

            cumulativeUserMemory += stageStats.getCumulativeUserMemory();
            failedCumulativeUserMemory += stageStats.getFailedCumulativeUserMemory();
            userMemoryReservation += stageStats.getUserMemoryReservation().toBytes();
            revocableMemoryReservation += stageStats.getRevocableMemoryReservation().toBytes();
            totalMemoryReservation += stageStats.getTotalMemoryReservation().toBytes();
            totalScheduledTime += stageStats.getTotalScheduledTime().roundTo(MILLISECONDS);
            failedScheduledTime += stageStats.getFailedScheduledTime().roundTo(MILLISECONDS);
            totalCpuTime += stageStats.getTotalCpuTime().roundTo(MILLISECONDS);
            failedCpuTime += stageStats.getFailedCpuTime().roundTo(MILLISECONDS);
            totalBlockedTime += stageStats.getTotalBlockedTime().roundTo(MILLISECONDS);
            if (!stageInfo.getState().isDone()) {
                fullyBlocked &= stageStats.isFullyBlocked();
                blockedReasons.addAll(stageStats.getBlockedReasons());
            }

            physicalInputDataSize += stageStats.getPhysicalInputDataSize().toBytes();
            failedPhysicalInputDataSize += stageStats.getFailedPhysicalInputDataSize().toBytes();
            physicalInputPositions += stageStats.getPhysicalInputPositions();
            failedPhysicalInputPositions += stageStats.getFailedPhysicalInputPositions();
            physicalInputReadTime += stageStats.getPhysicalInputReadTime().roundTo(MILLISECONDS);
            failedPhysicalInputReadTime += stageStats.getFailedPhysicalInputReadTime().roundTo(MILLISECONDS);

            internalNetworkInputDataSize += stageStats.getInternalNetworkInputDataSize().toBytes();
            failedInternalNetworkInputDataSize += stageStats.getFailedInternalNetworkInputDataSize().toBytes();
            internalNetworkInputPositions += stageStats.getInternalNetworkInputPositions();
            failedInternalNetworkInputPositions += stageStats.getFailedInternalNetworkInputPositions();

            PlanFragment plan = stageInfo.getPlan();
            if (plan != null && plan.containsTableScanNode()) {
                rawInputDataSize += stageStats.getRawInputDataSize().toBytes();
                failedRawInputDataSize += stageStats.getFailedRawInputDataSize().toBytes();
                rawInputPositions += stageStats.getRawInputPositions();
                failedRawInputPositions += stageStats.getFailedRawInputPositions();

                processedInputDataSize += stageStats.getProcessedInputDataSize().toBytes();
                failedProcessedInputDataSize += stageStats.getFailedProcessedInputDataSize().toBytes();
                processedInputPositions += stageStats.getProcessedInputPositions();
                failedProcessedInputPositions += stageStats.getFailedProcessedInputPositions();
            }

            inputBlockedTime += stageStats.getInputBlockedTime().roundTo(NANOSECONDS);
            failedInputBlockedTime += stageStats.getFailedInputBlockedTime().roundTo(NANOSECONDS);

            outputBlockedTime += stageStats.getOutputBlockedTime().roundTo(NANOSECONDS);
            failedOutputBlockedTime += stageStats.getFailedOutputBlockedTime().roundTo(NANOSECONDS);

            physicalWrittenDataSize += stageStats.getPhysicalWrittenDataSize().toBytes();
            failedPhysicalWrittenDataSize += stageStats.getFailedPhysicalWrittenDataSize().toBytes();

            stageGcStatistics.add(stageStats.getGcInfo());

            operatorStatsSummary.addAll(stageInfo.getStageStats().getOperatorSummaries());
        }

        if (rootStage.isPresent()) {
            StageStats outputStageStats = rootStage.get().getStageStats();
            outputDataSize += outputStageStats.getOutputDataSize().toBytes();
            failedOutputDataSize += outputStageStats.getFailedOutputDataSize().toBytes();
            outputPositions += outputStageStats.getOutputPositions();
            failedOutputPositions += outputStageStats.getFailedOutputPositions();
        }

        boolean scheduled;
        OptionalDouble progressPercentage;
        OptionalDouble runningPercentage;
        if (getRetryPolicy(session).equals(TASK)) {
            // Unlike pipelined execution, fault tolerant execution doesn't execute stages all at
            // once and some stages will be in PLANNED state in the middle of execution.
            scheduled = rootStage.isPresent() && allStages.stream()
                    .map(StageInfo::getState)
                    .anyMatch(StageState::isScheduled);
            if (!scheduled || totalDrivers == 0) {
                progressPercentage = OptionalDouble.empty();
                runningPercentage = OptionalDouble.empty();
            }
            else {
                double completedPercentageSum = 0.0;
                double runningPercentageSum = 0.0;
                int totalStages = 0;
                Queue<StageInfo> queue = new ArrayDeque<>();
                queue.add(rootStage.get());
                while (!queue.isEmpty()) {
                    StageInfo stage = queue.poll();
                    StageStats stageStats = stage.getStageStats();
                    totalStages++;
                    if (stage.getState().isScheduled()) {
                        completedPercentageSum += 100.0 * stageStats.getCompletedDrivers() / stageStats.getTotalDrivers();
                        runningPercentageSum += 100.0 * stageStats.getRunningDrivers() / stageStats.getTotalDrivers();
                    }
                    queue.addAll(stage.getSubStages());
                }
                progressPercentage = OptionalDouble.of(min(100, completedPercentageSum / totalStages));
                runningPercentage = OptionalDouble.of(min(100, runningPercentageSum / totalStages));
            }
        }
        else {
            scheduled = rootStage.isPresent() && allStages.stream()
                    .map(StageInfo::getState)
                    .allMatch(StageState::isScheduled);
            if (!scheduled || totalDrivers == 0) {
                progressPercentage = OptionalDouble.empty();
                runningPercentage = OptionalDouble.empty();
            }
            else {
                progressPercentage = OptionalDouble.of(min(100, (completedDrivers * 100.0) / totalDrivers));
                runningPercentage = OptionalDouble.of(min(100, (runningDrivers * 100.0) / totalDrivers));
            }
        }

        return new QueryStats(
                queryStateTimer.getCreateTime(),
                getExecutionStartTime().orElse(null),
                getLastHeartbeat(),
                getEndTime().orElse(null),

                queryStateTimer.getElapsedTime(),
                queryStateTimer.getQueuedTime(),
                queryStateTimer.getResourceWaitingTime(),
                queryStateTimer.getDispatchingTime(),
                queryStateTimer.getExecutionTime(),
                queryStateTimer.getAnalysisTime(),
                queryStateTimer.getPlanningTime(),
                queryStateTimer.getPlanningCpuTime(),
                queryStateTimer.getStartingTime(),
                queryStateTimer.getFinishingTime(),

                totalTasks,
                runningTasks,
                completedTasks,
                failedTasks,

                totalDrivers,
                queuedDrivers,
                runningDrivers,
                blockedDrivers,
                completedDrivers,

                cumulativeUserMemory,
                failedCumulativeUserMemory,
                succinctBytes(userMemoryReservation),
                succinctBytes(revocableMemoryReservation),
                succinctBytes(totalMemoryReservation),
                succinctBytes(getPeakUserMemoryInBytes()),
                succinctBytes(getPeakRevocableMemoryInBytes()),
                succinctBytes(getPeakTotalMemoryInBytes()),
                succinctBytes(getPeakTaskUserMemory()),
                succinctBytes(getPeakTaskRevocableMemory()),
                succinctBytes(getPeakTaskTotalMemory()),

                scheduled,
                progressPercentage,
                runningPercentage,

                new Duration(totalScheduledTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(failedScheduledTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalCpuTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(failedCpuTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalBlockedTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                fullyBlocked,
                blockedReasons,

                succinctBytes(physicalInputDataSize),
                succinctBytes(failedPhysicalInputDataSize),
                physicalInputPositions,
                failedPhysicalInputPositions,
                new Duration(physicalInputReadTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(failedPhysicalInputReadTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                succinctBytes(internalNetworkInputDataSize),
                succinctBytes(failedInternalNetworkInputDataSize),
                internalNetworkInputPositions,
                failedInternalNetworkInputPositions,
                succinctBytes(rawInputDataSize),
                succinctBytes(failedRawInputDataSize),
                rawInputPositions,
                failedRawInputPositions,
                succinctBytes(processedInputDataSize),
                succinctBytes(failedProcessedInputDataSize),
                processedInputPositions,
                failedProcessedInputPositions,
                new Duration(inputBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(failedInputBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),

                succinctBytes(outputDataSize),
                succinctBytes(failedOutputDataSize),
                outputPositions,
                failedOutputPositions,

                new Duration(outputBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(failedOutputBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),

                succinctBytes(physicalWrittenDataSize),
                succinctBytes(failedPhysicalWrittenDataSize),

                stageGcStatistics.build(),

                getDynamicFiltersStats(),

                operatorStatsSummary.build(),
                planOptimizersStatsCollector.getTopRuleStats());
    }

    public void setOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        outputManager.setOutputInfoListener(listener);
    }

    public void addOutputTaskFailureListener(TaskFailureListener listener)
    {
        outputManager.addOutputTaskFailureListener(listener);
    }

    public void outputTaskFailed(TaskId taskId, Throwable failure)
    {
        outputManager.outputTaskFailed(taskId, failure);
    }

    public void setColumns(List<String> columnNames, List<Type> columnTypes)
    {
        outputManager.setColumns(columnNames, columnTypes);
    }

    public void updateInputsForQueryResults(List<ExchangeInput> inputs, boolean noMoreInputs)
    {
        outputManager.updateInputsForQueryResults(inputs, noMoreInputs);
    }

    public void setInputs(List<Input> inputs)
    {
        requireNonNull(inputs, "inputs is null");
        this.inputs.set(ImmutableSet.copyOf(inputs));
    }

    public void setOutput(Optional<Output> output)
    {
        requireNonNull(output, "output is null");
        this.output.set(output);
    }

    public void setReferencedTables(List<TableInfo> tables)
    {
        requireNonNull(tables, "tables is null");
        referencedTables.set(ImmutableList.copyOf(tables));
    }

    public void setRoutines(List<RoutineInfo> routines)
    {
        requireNonNull(routines, "routines is null");
        this.routines.set(ImmutableList.copyOf(routines));
    }

    private DynamicFiltersStats getDynamicFiltersStats()
    {
        synchronized (dynamicFiltersStatsSupplierLock) {
            return dynamicFiltersStatsSupplier.get();
        }
    }

    public void setDynamicFiltersStatsSupplier(Supplier<DynamicFiltersStats> dynamicFiltersStatsSupplier)
    {
        synchronized (dynamicFiltersStatsSupplierLock) {
            this.dynamicFiltersStatsSupplier = requireNonNull(dynamicFiltersStatsSupplier, "dynamicFiltersStatsSupplier is null");
        }
    }

    public Map<String, String> getSetSessionProperties()
    {
        return setSessionProperties;
    }

    public void setSetCatalog(String catalog)
    {
        setCatalog.set(requireNonNull(catalog, "catalog is null"));
    }

    public void setSetSchema(String schema)
    {
        setSchema.set(requireNonNull(schema, "schema is null"));
    }

    public void setSetPath(String path)
    {
        requireNonNull(path, "path is null");
        setPath.set(path);
    }

    public String getSetPath()
    {
        return setPath.get();
    }

    public void setSetAuthorizationUser(String authorizationUser)
    {
        checkState(authorizationUser != null && !authorizationUser.isEmpty(), "Authorization user cannot be null or empty");
        setAuthorizationUser.set(authorizationUser);
    }

    public void resetAuthorizationUser()
    {
        checkArgument(setAuthorizationUser.get() == null, "Cannot set and reset the authorization user in the same request");
        resetAuthorizationUser.set(true);
    }

    public void addSetSessionProperties(String key, String value)
    {
        setSessionProperties.put(requireNonNull(key, "key is null"), requireNonNull(value, "value is null"));
    }

    public void addSetRole(String catalog, SelectedRole role)
    {
        setRoles.put(requireNonNull(catalog, "catalog is null"), requireNonNull(role, "role is null"));
    }

    public Set<String> getResetSessionProperties()
    {
        return resetSessionProperties;
    }

    public void addResetSessionProperties(String name)
    {
        resetSessionProperties.add(requireNonNull(name, "name is null"));
    }

    public Map<String, String> getAddedPreparedStatements()
    {
        return addedPreparedStatements;
    }

    public Set<String> getDeallocatedPreparedStatements()
    {
        return deallocatedPreparedStatements;
    }

    public void addPreparedStatement(String key, String value)
    {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");

        addedPreparedStatements.put(key, value);
    }

    public void removePreparedStatement(String key)
    {
        requireNonNull(key, "key is null");

        if (!session.getPreparedStatements().containsKey(key)) {
            throw new TrinoException(NOT_FOUND, "Prepared statement not found: " + key);
        }
        deallocatedPreparedStatements.add(key);
    }

    public void setStartedTransactionId(TransactionId startedTransactionId)
    {
        checkArgument(!clearTransactionId.get(), "Cannot start and clear transaction ID in the same request");
        this.startedTransactionId.set(startedTransactionId);
    }

    public void clearTransactionId()
    {
        checkArgument(startedTransactionId.get() == null, "Cannot start and clear transaction ID in the same request");
        clearTransactionId.set(true);
    }

    public void setUpdateType(String updateType)
    {
        this.updateType.set(updateType);
    }

    public QueryState getQueryState()
    {
        return queryState.get();
    }

    public boolean isDone()
    {
        return queryState.get().isDone();
    }

    public boolean transitionToWaitingForResources()
    {
        queryStateTimer.beginWaitingForResources();
        return queryState.setIf(WAITING_FOR_RESOURCES, currentState -> currentState.ordinal() < WAITING_FOR_RESOURCES.ordinal());
    }

    public boolean transitionToDispatching()
    {
        queryStateTimer.beginDispatching();
        return queryState.setIf(DISPATCHING, currentState -> currentState.ordinal() < DISPATCHING.ordinal());
    }

    public boolean transitionToPlanning()
    {
        queryStateTimer.beginPlanning();
        return queryState.setIf(PLANNING, currentState -> currentState.ordinal() < PLANNING.ordinal());
    }

    public boolean transitionToStarting()
    {
        queryStateTimer.beginStarting();
        return queryState.setIf(STARTING, currentState -> currentState.ordinal() < STARTING.ordinal());
    }

    public boolean transitionToRunning()
    {
        queryStateTimer.beginRunning();
        return queryState.setIf(RUNNING, currentState -> currentState.ordinal() < RUNNING.ordinal());
    }

    public boolean transitionToFinishing()
    {
        queryStateTimer.beginFinishing();

        if (!queryState.setIf(FINISHING, currentState -> currentState != FINISHING && !currentState.isDone())) {
            return false;
        }

        try {
            cleanupQuery();
        }
        catch (Exception e) {
            transitionToFailed(e);
            return true;
        }

        Optional<TransactionInfo> transaction = session.getTransactionId().flatMap(transactionManager::getTransactionInfoIfExist);
        if (transaction.isPresent() && transaction.get().isAutoCommitContext()) {
            ListenableFuture<Void> commitFuture = transactionManager.asyncCommit(transaction.get().getTransactionId());
            Futures.addCallback(commitFuture, new FutureCallback<>()
            {
                @Override
                public void onSuccess(@Nullable Void result)
                {
                    committed.set(true);
                    transitionToFinishedIfReady();
                }

                @Override
                public void onFailure(Throwable throwable)
                {
                    transitionToFailed(throwable);
                }
            }, directExecutor());
        }
        else {
            committed.set(true);
            transitionToFinishedIfReady();
        }
        return true;
    }

    public void resultsConsumed()
    {
        consumed.set(true);
        transitionToFinishedIfReady();
    }

    private void transitionToFinishedIfReady()
    {
        if (queryState.get().isDone()) {
            return;
        }

        if (!committed.get() || !consumed.get()) {
            return;
        }

        queryStateTimer.endQuery();

        queryState.setIf(FINISHED, currentState -> !currentState.isDone());
    }

    public boolean transitionToCanceled()
    {
        return transitionToFailed(new TrinoException(USER_CANCELED, "Query was canceled"), false);
    }

    public boolean transitionToFailed(Throwable throwable)
    {
        return transitionToFailed(throwable, true);
    }

    private boolean transitionToFailed(Throwable throwable, boolean log)
    {
        queryStateTimer.endQuery();

        // NOTE: The failure cause must be set before triggering the state change, so
        // listeners can observe the exception. This is safe because the failure cause
        // can only be observed if the transition to FAILED is successful.
        requireNonNull(throwable, "throwable is null");
        failureCause.compareAndSet(null, toFailure(throwable));

        cleanupQueryQuietly();

        QueryState oldState = queryState.trySet(FAILED);
        if (oldState.isDone()) {
            if (log) {
                QUERY_STATE_LOG.debug(throwable, "Failure after query %s finished", queryId);
            }
            return false;
        }

        try {
            if (log) {
                QUERY_STATE_LOG.debug(throwable, "Query %s failed", queryId);
            }
            session.getTransactionId().flatMap(transactionManager::getTransactionInfoIfExist).ifPresent(transaction -> {
                try {
                    if (transaction.isAutoCommitContext()) {
                        transactionManager.asyncAbort(transaction.getTransactionId());
                    }
                    else {
                        transactionManager.fail(transaction.getTransactionId());
                    }
                }
                catch (RuntimeException e) {
                    // This shouldn't happen but be safe and just fail the transaction directly
                    if (log) {
                        QUERY_STATE_LOG.error(e, "Error aborting transaction for failed query. Transaction will be failed directly");
                    }
                }
            });
        }
        finally {
            // if the query has not started, then there is no final query info to wait for
            if (oldState.ordinal() <= PLANNING.ordinal()) {
                finalQueryInfo.compareAndSet(Optional.empty(), Optional.of(getQueryInfo(Optional.empty())));
            }
        }

        return true;
    }

    private void cleanupQuery()
    {
        // only execute cleanup once
        if (queryCleanedUp.compareAndSet(false, true)) {
            metadata.cleanupQuery(session);
        }
    }

    private void cleanupQueryQuietly()
    {
        try {
            cleanupQuery();
        }
        catch (Throwable t) {
            QUERY_STATE_LOG.error("Error cleaning up query: %s", t);
        }
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        queryState.addStateChangeListener(stateChangeListener);
    }

    /**
     * Add a listener for the final query info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor.
     */
    public void addQueryInfoStateChangeListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        AtomicBoolean done = new AtomicBoolean();
        StateChangeListener<Optional<QueryInfo>> fireOnceStateChangeListener = finalQueryInfo -> {
            // QueryInfo.isPresent() does not mean this is a terminal state for finalQueryInfo state machine
            if (finalQueryInfo.isPresent() && done.compareAndSet(false, true)) {
                stateChangeListener.stateChanged(finalQueryInfo.get());
            }
        };
        finalQueryInfo.addStateChangeListener(fireOnceStateChangeListener);
    }

    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return queryState.getStateChange(currentState);
    }

    public void recordHeartbeat()
    {
        queryStateTimer.recordHeartbeat();
    }

    public void beginAnalysis()
    {
        queryStateTimer.beginAnalysis();
    }

    public void endAnalysis()
    {
        queryStateTimer.endAnalysis();
    }

    public DateTime getCreateTime()
    {
        return queryStateTimer.getCreateTime();
    }

    public Optional<DateTime> getExecutionStartTime()
    {
        return queryStateTimer.getExecutionStartTime();
    }

    public Optional<Duration> getPlanningTime()
    {
        // Execution start time is empty if planning has not started
        return queryStateTimer.getExecutionStartTime()
                .map(_ -> queryStateTimer.getPlanningTime());
    }

    public DateTime getLastHeartbeat()
    {
        return queryStateTimer.getLastHeartbeat();
    }

    public Optional<DateTime> getEndTime()
    {
        return queryStateTimer.getEndTime();
    }

    public Optional<ExecutionFailureInfo> getFailureInfo()
    {
        if (queryState.get() != FAILED) {
            return Optional.empty();
        }
        return Optional.ofNullable(this.failureCause.get());
    }

    public Optional<QueryInfo> getFinalQueryInfo()
    {
        return finalQueryInfo.get();
    }

    public QueryInfo updateQueryInfo(Optional<StageInfo> stageInfo)
    {
        QueryInfo queryInfo = getQueryInfo(stageInfo);
        if (queryInfo.isFinalQueryInfo()) {
            finalQueryInfo.compareAndSet(Optional.empty(), Optional.of(queryInfo));
        }
        return queryInfo;
    }

    public ResultQueryInfo updateResultQueryInfo(Optional<BasicStageInfo> stageInfo, Supplier<Optional<StageInfo>> stageInfoProvider)
    {
        ResultQueryInfo queryInfo = getResultQueryInfo(stageInfo);
        if (queryInfo.finalQueryInfo()) {
            QueryInfo fullQueryInfo = getQueryInfo(stageInfoProvider.get());
            finalQueryInfo.compareAndSet(Optional.empty(), Optional.of(fullQueryInfo));
            return new ResultQueryInfo(fullQueryInfo);
        }
        return queryInfo;
    }

    public void pruneQueryInfo()
    {
        Optional<QueryInfo> finalInfo = finalQueryInfo.get();
        if (finalInfo.isEmpty() || finalInfo.get().getOutputStage().isEmpty() || finalInfo.get().isPruned()) {
            return;
        }

        QueryInfo prunedQueryInfo = QueryStateMachine.pruneQueryInfo(finalInfo.get(), version);
        finalQueryInfo.compareAndSet(finalInfo, Optional.of(prunedQueryInfo));
    }

    public static QueryInfo pruneQueryInfo(QueryInfo queryInfo, NodeVersion version)
    {
        Optional<StageInfo> prunedOutputStage = queryInfo.getOutputStage().map(outputStage -> new StageInfo(
                outputStage.getStageId(),
                outputStage.getState(),
                null, // Remove the plan
                outputStage.isCoordinatorOnly(),
                outputStage.getTypes(),
                outputStage.getStageStats(),
                ImmutableList.of(), // Remove the tasks
                ImmutableList.of(), // Remove the substages
                ImmutableMap.of(), // Remove tables
                outputStage.getFailureCause()));

        return new QueryInfo(
                queryInfo.getQueryId(),
                queryInfo.getSession(),
                queryInfo.getState(),
                queryInfo.getSelf(),
                queryInfo.getFieldNames(),
                queryInfo.getQuery(),
                queryInfo.getPreparedQuery(),
                pruneQueryStats(queryInfo.getQueryStats()),
                queryInfo.getSetCatalog(),
                queryInfo.getSetSchema(),
                queryInfo.getSetPath(),
                queryInfo.getSetAuthorizationUser(),
                queryInfo.isResetAuthorizationUser(),
                queryInfo.getSetSessionProperties(),
                queryInfo.getResetSessionProperties(),
                queryInfo.getSetRoles(),
                queryInfo.getAddedPreparedStatements(),
                queryInfo.getDeallocatedPreparedStatements(),
                queryInfo.getStartedTransactionId(),
                queryInfo.isClearTransactionId(),
                queryInfo.getUpdateType(),
                prunedOutputStage,
                queryInfo.getFailureInfo(),
                queryInfo.getErrorCode(),
                queryInfo.getWarnings(),
                queryInfo.getInputs(),
                queryInfo.getOutput(),
                queryInfo.getReferencedTables(),
                queryInfo.getRoutines(),
                queryInfo.isFinalQueryInfo(),
                queryInfo.getResourceGroupId(),
                queryInfo.getQueryType(),
                queryInfo.getRetryPolicy(),
                true,
                version);
    }

    private static QueryStats pruneQueryStats(QueryStats queryStats)
    {
        return new QueryStats(
                queryStats.getCreateTime(),
                queryStats.getExecutionStartTime(),
                queryStats.getLastHeartbeat(),
                queryStats.getEndTime(),
                queryStats.getElapsedTime(),
                queryStats.getQueuedTime(),
                queryStats.getResourceWaitingTime(),
                queryStats.getDispatchingTime(),
                queryStats.getExecutionTime(),
                queryStats.getAnalysisTime(),
                queryStats.getPlanningTime(),
                queryStats.getPlanningCpuTime(),
                queryStats.getStartingTime(),
                queryStats.getFinishingTime(),
                queryStats.getTotalTasks(),
                queryStats.getRunningTasks(),
                queryStats.getCompletedTasks(),
                queryStats.getFailedTasks(),
                queryStats.getTotalDrivers(),
                queryStats.getQueuedDrivers(),
                queryStats.getRunningDrivers(),
                queryStats.getBlockedDrivers(),
                queryStats.getCompletedDrivers(),
                queryStats.getCumulativeUserMemory(),
                queryStats.getFailedCumulativeUserMemory(),
                queryStats.getUserMemoryReservation(),
                queryStats.getRevocableMemoryReservation(),
                queryStats.getTotalMemoryReservation(),
                queryStats.getPeakUserMemoryReservation(),
                queryStats.getPeakRevocableMemoryReservation(),
                queryStats.getPeakTotalMemoryReservation(),
                queryStats.getPeakTaskUserMemory(),
                queryStats.getPeakTaskRevocableMemory(),
                queryStats.getPeakTaskTotalMemory(),
                queryStats.isScheduled(),
                queryStats.getProgressPercentage(),
                queryStats.getRunningPercentage(),
                queryStats.getTotalScheduledTime(),
                queryStats.getFailedScheduledTime(),
                queryStats.getTotalCpuTime(),
                queryStats.getFailedCpuTime(),
                queryStats.getTotalBlockedTime(),
                queryStats.isFullyBlocked(),
                queryStats.getBlockedReasons(),
                queryStats.getPhysicalInputDataSize(),
                queryStats.getFailedPhysicalInputDataSize(),
                queryStats.getPhysicalInputPositions(),
                queryStats.getFailedPhysicalInputPositions(),
                queryStats.getPhysicalInputReadTime(),
                queryStats.getFailedPhysicalInputReadTime(),
                queryStats.getInternalNetworkInputDataSize(),
                queryStats.getFailedInternalNetworkInputDataSize(),
                queryStats.getInternalNetworkInputPositions(),
                queryStats.getFailedInternalNetworkInputPositions(),
                queryStats.getRawInputDataSize(),
                queryStats.getFailedRawInputDataSize(),
                queryStats.getRawInputPositions(),
                queryStats.getFailedRawInputPositions(),
                queryStats.getProcessedInputDataSize(),
                queryStats.getFailedProcessedInputDataSize(),
                queryStats.getProcessedInputPositions(),
                queryStats.getFailedProcessedInputPositions(),
                queryStats.getInputBlockedTime(),
                queryStats.getFailedInputBlockedTime(),
                queryStats.getOutputDataSize(),
                queryStats.getFailedOutputDataSize(),
                queryStats.getOutputPositions(),
                queryStats.getFailedOutputPositions(),
                queryStats.getOutputBlockedTime(),
                queryStats.getFailedOutputBlockedTime(),
                queryStats.getPhysicalWrittenDataSize(),
                queryStats.getFailedPhysicalWrittenDataSize(),
                queryStats.getStageGcStatistics(),
                queryStats.getDynamicFiltersStats(),
                ImmutableList.of(), // Remove the operator summaries as OperatorInfo (especially DirectExchangeClientStatus) can hold onto a large amount of memory
                ImmutableList.of());
    }

    public boolean isQueryInfoPruned()
    {
        return finalQueryInfo.get()
                .map(QueryInfo::isPruned)
                .orElse(false);
    }

    private QueryOutputManager getOutputManager()
    {
        return outputManager;
    }

    public static class QueryOutputManager
    {
        private final Executor executor;

        @GuardedBy("this")
        private Optional<Consumer<QueryOutputInfo>> listener = Optional.empty();

        @GuardedBy("this")
        private List<String> columnNames;
        @GuardedBy("this")
        private List<Type> columnTypes;
        @GuardedBy("this")
        private boolean noMoreInputs;
        @GuardedBy("this")
        private boolean queryCompleted;

        private final Queue<ExchangeInput> inputsQueue = new ConcurrentLinkedQueue<>();

        @GuardedBy("this")
        private final Map<TaskId, Throwable> outputTaskFailures = new HashMap<>();
        @GuardedBy("this")
        private final List<TaskFailureListener> outputTaskFailureListeners = new ArrayList<>();

        public QueryOutputManager(Executor executor)
        {
            this.executor = requireNonNull(executor, "executor is null");
        }

        public void setOutputInfoListener(Consumer<QueryOutputInfo> listener)
        {
            requireNonNull(listener, "listener is null");

            Optional<QueryOutputInfo> queryOutputInfo;
            synchronized (this) {
                checkState(this.listener.isEmpty(), "listener is already set");
                this.listener = Optional.of(listener);
                queryOutputInfo = getQueryOutputInfo();
            }
            fireStateChangedIfReady(queryOutputInfo, Optional.of(listener));
        }

        public void setColumns(List<String> columnNames, List<Type> columnTypes)
        {
            requireNonNull(columnNames, "columnNames is null");
            requireNonNull(columnTypes, "columnTypes is null");
            checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes must be the same size");

            Optional<QueryOutputInfo> queryOutputInfo;
            Optional<Consumer<QueryOutputInfo>> listener;
            synchronized (this) {
                checkState(this.columnNames == null && this.columnTypes == null, "output fields already set");
                this.columnNames = ImmutableList.copyOf(columnNames);
                this.columnTypes = ImmutableList.copyOf(columnTypes);

                queryOutputInfo = getQueryOutputInfo();
                listener = this.listener;
            }
            fireStateChangedIfReady(queryOutputInfo, listener);
        }

        public void updateInputsForQueryResults(List<ExchangeInput> newInputs, boolean noMoreInputs)
        {
            requireNonNull(newInputs, "newInputs is null");

            Optional<QueryOutputInfo> queryOutputInfo;
            Optional<Consumer<QueryOutputInfo>> listener;
            synchronized (this) {
                if (!queryCompleted) {
                    // noMoreInputs can be set more than once
                    checkState(newInputs.isEmpty() || !this.noMoreInputs, "new inputs added after no more inputs set");
                    inputsQueue.addAll(newInputs);
                    this.noMoreInputs = noMoreInputs;
                }
                queryOutputInfo = getQueryOutputInfo();
                listener = this.listener;
            }
            fireStateChangedIfReady(queryOutputInfo, listener);
        }

        public synchronized void setQueryCompleted()
        {
            if (queryCompleted) {
                return;
            }
            queryCompleted = true;
            inputsQueue.clear();
            outputTaskFailureListeners.clear();
            noMoreInputs = true;
        }

        public void addOutputTaskFailureListener(TaskFailureListener listener)
        {
            Map<TaskId, Throwable> failures;
            synchronized (this) {
                if (!queryCompleted) {
                    outputTaskFailureListeners.add(listener);
                }
                failures = ImmutableMap.copyOf(outputTaskFailures);
            }
            if (!failures.isEmpty()) {
                executor.execute(() -> failures.forEach(listener::onTaskFailed));
            }
        }

        public void outputTaskFailed(TaskId taskId, Throwable failure)
        {
            List<TaskFailureListener> listeners;
            synchronized (this) {
                if (queryCompleted) {
                    // ignore late task failed events after query is completed
                    return;
                }
                outputTaskFailures.putIfAbsent(taskId, failure);
                listeners = ImmutableList.copyOf(outputTaskFailureListeners);
            }
            if (!listeners.isEmpty()) {
                executor.execute(() -> {
                    for (TaskFailureListener listener : listeners) {
                        listener.onTaskFailed(taskId, failure);
                    }
                });
            }
        }

        private synchronized Optional<QueryOutputInfo> getQueryOutputInfo()
        {
            if (columnNames == null || columnTypes == null) {
                return Optional.empty();
            }
            return Optional.of(new QueryOutputInfo(columnNames, columnTypes, inputsQueue, noMoreInputs));
        }

        private void fireStateChangedIfReady(Optional<QueryOutputInfo> info, Optional<Consumer<QueryOutputInfo>> listener)
        {
            if (info.isEmpty() || listener.isEmpty()) {
                return;
            }
            executor.execute(() -> listener.get().accept(info.get()));
        }
    }
}
