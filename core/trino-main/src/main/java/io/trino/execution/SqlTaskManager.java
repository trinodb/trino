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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.stats.CounterStat;
import io.airlift.stats.GcMonitor;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.event.SplitMonitor;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.OutputBuffers.OutputBufferId;
import io.trino.execution.executor.TaskExecutor;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.MemoryPool;
import io.trino.memory.MemoryPoolAssignment;
import io.trino.memory.MemoryPoolAssignmentsRequest;
import io.trino.memory.NodeMemoryConfig;
import io.trino.memory.QueryContext;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.predicate.Domain;
import io.trino.spiller.LocalSpillManager;
import io.trino.spiller.NodeSpillConfig;
import io.trino.sql.planner.LocalExecutionPlanner;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;
import org.joda.time.DateTime;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.SystemSessionProperties.getQueryMaxMemoryPerNode;
import static io.trino.SystemSessionProperties.getQueryMaxTotalMemoryPerNode;
import static io.trino.SystemSessionProperties.getQueryMaxTotalMemoryPerTask;
import static io.trino.SystemSessionProperties.resourceOvercommit;
import static io.trino.execution.SqlTask.createSqlTask;
import static io.trino.memory.LocalMemoryManager.GENERAL_POOL;
import static io.trino.memory.LocalMemoryManager.RESERVED_POOL;
import static io.trino.spi.StandardErrorCode.ABANDONED_TASK;
import static io.trino.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class SqlTaskManager
        implements TaskManager, Closeable
{
    private static final Logger log = Logger.get(SqlTaskManager.class);

    private final VersionEmbedder versionEmbedder;
    private final ExecutorService taskNotificationExecutor;
    private final ThreadPoolExecutorMBean taskNotificationExecutorMBean;

    private final ScheduledExecutorService taskManagementExecutor;
    private final ScheduledExecutorService driverYieldExecutor;

    private final Duration infoCacheTime;
    private final Duration clientTimeout;

    private final LocalMemoryManager localMemoryManager;
    private final LoadingCache<QueryId, QueryContext> queryContexts;
    private final LoadingCache<TaskId, SqlTask> tasks;

    private final SqlTaskIoStats cachedStats = new SqlTaskIoStats();
    private final SqlTaskIoStats finishedTaskStats = new SqlTaskIoStats();

    private final long queryMaxMemoryPerNode;
    private final long queryMaxTotalMemoryPerNode;
    private final Optional<DataSize> queryMaxMemoryPerTask;

    @GuardedBy("this")
    private long currentMemoryPoolAssignmentVersion;
    @GuardedBy("this")
    private String coordinatorId;

    private final CounterStat failedTasks = new CounterStat();

    @Inject
    public SqlTaskManager(
            VersionEmbedder versionEmbedder,
            LocalExecutionPlanner planner,
            LocationFactory locationFactory,
            TaskExecutor taskExecutor,
            SplitMonitor splitMonitor,
            NodeInfo nodeInfo,
            LocalMemoryManager localMemoryManager,
            TaskManagementExecutor taskManagementExecutor,
            TaskManagerConfig config,
            NodeMemoryConfig nodeMemoryConfig,
            LocalSpillManager localSpillManager,
            NodeSpillConfig nodeSpillConfig,
            GcMonitor gcMonitor)
    {
        requireNonNull(nodeInfo, "nodeInfo is null");
        requireNonNull(config, "config is null");
        infoCacheTime = config.getInfoMaxAge();
        clientTimeout = config.getClientTimeout();

        DataSize maxBufferSize = config.getSinkMaxBufferSize();
        DataSize maxBroadcastBufferSize = config.getSinkMaxBroadcastBufferSize();

        this.versionEmbedder = requireNonNull(versionEmbedder, "versionEmbedder is null");
        taskNotificationExecutor = newFixedThreadPool(config.getTaskNotificationThreads(), threadsNamed("task-notification-%s"));
        taskNotificationExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) taskNotificationExecutor);

        this.taskManagementExecutor = requireNonNull(taskManagementExecutor, "taskManagementExecutor cannot be null").getExecutor();
        this.driverYieldExecutor = newScheduledThreadPool(config.getTaskYieldThreads(), threadsNamed("task-yield-%s"));

        SqlTaskExecutionFactory sqlTaskExecutionFactory = new SqlTaskExecutionFactory(taskNotificationExecutor, taskExecutor, planner, splitMonitor, config);

        this.localMemoryManager = requireNonNull(localMemoryManager, "localMemoryManager is null");
        DataSize maxQueryMemoryPerNode = nodeMemoryConfig.getMaxQueryMemoryPerNode();
        DataSize maxQueryTotalMemoryPerNode = nodeMemoryConfig.getMaxQueryTotalMemoryPerNode();
        queryMaxMemoryPerTask = nodeMemoryConfig.getMaxQueryTotalMemoryPerTask();
        DataSize maxQuerySpillPerNode = nodeSpillConfig.getQueryMaxSpillPerNode();

        queryMaxMemoryPerNode = maxQueryMemoryPerNode.toBytes();
        queryMaxTotalMemoryPerNode = maxQueryTotalMemoryPerNode.toBytes();

        queryContexts = CacheBuilder.newBuilder().weakValues().build(CacheLoader.from(
                queryId -> createQueryContext(queryId, localMemoryManager, localSpillManager, gcMonitor, maxQueryMemoryPerNode, maxQueryTotalMemoryPerNode, queryMaxMemoryPerTask, maxQuerySpillPerNode)));

        tasks = CacheBuilder.newBuilder().build(CacheLoader.from(
                taskId -> createSqlTask(
                        taskId,
                        locationFactory.createLocalTaskLocation(taskId),
                        nodeInfo.getNodeId(),
                        queryContexts.getUnchecked(taskId.getQueryId()),
                        sqlTaskExecutionFactory,
                        taskNotificationExecutor,
                        sqlTask -> finishedTaskStats.merge(sqlTask.getIoStats()),
                        maxBufferSize,
                        maxBroadcastBufferSize,
                        failedTasks)));
    }

    private QueryContext createQueryContext(
            QueryId queryId,
            LocalMemoryManager localMemoryManager,
            LocalSpillManager localSpillManager,
            GcMonitor gcMonitor,
            DataSize maxQueryUserMemoryPerNode,
            DataSize maxQueryTotalMemoryPerNode,
            Optional<DataSize> maxQueryMemoryPerTask,
            DataSize maxQuerySpillPerNode)
    {
        return new QueryContext(
                queryId,
                maxQueryUserMemoryPerNode,
                maxQueryTotalMemoryPerNode,
                maxQueryMemoryPerTask,
                localMemoryManager.getGeneralPool(),
                gcMonitor,
                taskNotificationExecutor,
                driverYieldExecutor,
                maxQuerySpillPerNode,
                localSpillManager.getSpillSpaceTracker());
    }

    @Override
    public synchronized void updateMemoryPoolAssignments(MemoryPoolAssignmentsRequest assignments)
    {
        if (coordinatorId != null && coordinatorId.equals(assignments.getCoordinatorId()) && assignments.getVersion() <= currentMemoryPoolAssignmentVersion) {
            return;
        }
        currentMemoryPoolAssignmentVersion = assignments.getVersion();
        if (coordinatorId != null && !coordinatorId.equals(assignments.getCoordinatorId())) {
            log.warn("Switching coordinator affinity from %s to %s", coordinatorId, assignments.getCoordinatorId());
        }
        coordinatorId = assignments.getCoordinatorId();

        for (MemoryPoolAssignment assignment : assignments.getAssignments()) {
            if (assignment.getPoolId().equals(GENERAL_POOL)) {
                queryContexts.getUnchecked(assignment.getQueryId()).setMemoryPool(localMemoryManager.getGeneralPool());
            }
            else if (assignment.getPoolId().equals(RESERVED_POOL)) {
                MemoryPool reservedPool = localMemoryManager.getReservedPool()
                        .orElseThrow(() -> new IllegalArgumentException(format("Cannot move %s to the reserved pool as the reserved pool is not enabled", assignment.getQueryId())));
                queryContexts.getUnchecked(assignment.getQueryId()).setMemoryPool(reservedPool);
            }
            else {
                throw new IllegalArgumentException(format("Cannot move %s to %s as the target memory pool id is invalid", assignment.getQueryId(), assignment.getPoolId()));
            }
        }
    }

    @PostConstruct
    public void start()
    {
        taskManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                removeOldTasks();
            }
            catch (Throwable e) {
                log.warn(e, "Error removing old tasks");
            }
            try {
                failAbandonedTasks();
            }
            catch (Throwable e) {
                log.warn(e, "Error canceling abandoned tasks");
            }
        }, 200, 200, TimeUnit.MILLISECONDS);

        taskManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                updateStats();
            }
            catch (Throwable e) {
                log.warn(e, "Error updating stats");
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    @Override
    @PreDestroy
    public void close()
    {
        boolean taskCanceled = false;
        for (SqlTask task : tasks.asMap().values()) {
            if (task.getTaskState().isDone()) {
                continue;
            }
            task.failed(new TrinoException(SERVER_SHUTTING_DOWN, format("Server is shutting down. Task %s has been canceled", task.getTaskId())));
            taskCanceled = true;
        }
        if (taskCanceled) {
            try {
                TimeUnit.SECONDS.sleep(5);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        taskNotificationExecutor.shutdownNow();
    }

    @Managed
    @Flatten
    public SqlTaskIoStats getIoStats()
    {
        return cachedStats;
    }

    @Managed(description = "Task notification executor")
    @Nested
    public ThreadPoolExecutorMBean getTaskNotificationExecutor()
    {
        return taskNotificationExecutorMBean;
    }

    @Managed(description = "Failed tasks counter")
    @Nested
    public CounterStat getFailedTasks()
    {
        return failedTasks;
    }

    public List<SqlTask> getAllTasks()
    {
        return ImmutableList.copyOf(tasks.asMap().values());
    }

    @Override
    public List<TaskInfo> getAllTaskInfo()
    {
        return tasks.asMap().values().stream()
                .map(SqlTask::getTaskInfo)
                .collect(toImmutableList());
    }

    @Override
    public TaskInfo getTaskInfo(TaskId taskId)
    {
        requireNonNull(taskId, "taskId is null");

        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskInfo();
    }

    @Override
    public TaskStatus getTaskStatus(TaskId taskId)
    {
        requireNonNull(taskId, "taskId is null");

        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskStatus();
    }

    @Override
    public ListenableFuture<TaskInfo> getTaskInfo(TaskId taskId, long currentVersion)
    {
        requireNonNull(taskId, "taskId is null");

        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskInfo(currentVersion);
    }

    @Override
    public String getTaskInstanceId(TaskId taskId)
    {
        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskInstanceId();
    }

    @Override
    public ListenableFuture<TaskStatus> getTaskStatus(TaskId taskId, long currentVersion)
    {
        requireNonNull(taskId, "taskId is null");

        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskStatus(currentVersion);
    }

    @Override
    public VersionedDynamicFilterDomains acknowledgeAndGetNewDynamicFilterDomains(TaskId taskId, long currentDynamicFiltersVersion)
    {
        requireNonNull(taskId, "taskId is null");

        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        return sqlTask.acknowledgeAndGetNewDynamicFilterDomains(currentDynamicFiltersVersion);
    }

    @Override
    public TaskInfo updateTask(
            Session session,
            TaskId taskId,
            Optional<PlanFragment> fragment,
            List<TaskSource> sources,
            OutputBuffers outputBuffers,
            Map<DynamicFilterId, Domain> dynamicFilterDomains)
    {
        try {
            return versionEmbedder.embedVersion(() -> doUpdateTask(session, taskId, fragment, sources, outputBuffers, dynamicFilterDomains)).call();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            // impossible, doUpdateTask does not throw checked exceptions
            throw new RuntimeException(e);
        }
    }

    private TaskInfo doUpdateTask(
            Session session,
            TaskId taskId,
            Optional<PlanFragment> fragment,
            List<TaskSource> sources,
            OutputBuffers outputBuffers,
            Map<DynamicFilterId, Domain> dynamicFilterDomains)
    {
        requireNonNull(session, "session is null");
        requireNonNull(taskId, "taskId is null");
        requireNonNull(fragment, "fragment is null");
        requireNonNull(sources, "sources is null");
        requireNonNull(outputBuffers, "outputBuffers is null");

        SqlTask sqlTask = tasks.getUnchecked(taskId);
        QueryContext queryContext = sqlTask.getQueryContext();
        if (!queryContext.isMemoryLimitsInitialized()) {
            long sessionQueryMaxMemoryPerNode = getQueryMaxMemoryPerNode(session).toBytes();
            long sessionQueryTotalMaxMemoryPerNode = getQueryMaxTotalMemoryPerNode(session).toBytes();

            Optional<DataSize> effectiveQueryMaxMemoryPerTask = getQueryMaxTotalMemoryPerTask(session);
            if (queryMaxMemoryPerTask.isPresent() &&
                    (effectiveQueryMaxMemoryPerTask.isEmpty() || effectiveQueryMaxMemoryPerTask.get().toBytes() > queryMaxMemoryPerTask.get().toBytes())) {
                effectiveQueryMaxMemoryPerTask = queryMaxMemoryPerTask;
            }

            // Session properties are only allowed to decrease memory limits, not increase them
            queryContext.initializeMemoryLimits(
                    resourceOvercommit(session),
                    min(sessionQueryMaxMemoryPerNode, queryMaxMemoryPerNode),
                    min(sessionQueryTotalMaxMemoryPerNode, queryMaxTotalMemoryPerNode),
                    effectiveQueryMaxMemoryPerTask);
        }

        sqlTask.recordHeartbeat();
        return sqlTask.updateTask(session, fragment, sources, outputBuffers, dynamicFilterDomains);
    }

    @Override
    public ListenableFuture<BufferResult> getTaskResults(TaskId taskId, OutputBufferId bufferId, long startingSequenceId, DataSize maxSize)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");
        checkArgument(startingSequenceId >= 0, "startingSequenceId is negative");
        requireNonNull(maxSize, "maxSize is null");

        return tasks.getUnchecked(taskId).getTaskResults(bufferId, startingSequenceId, maxSize);
    }

    @Override
    public void acknowledgeTaskResults(TaskId taskId, OutputBufferId bufferId, long sequenceId)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");
        checkArgument(sequenceId >= 0, "sequenceId is negative");

        tasks.getUnchecked(taskId).acknowledgeTaskResults(bufferId, sequenceId);
    }

    @Override
    public TaskInfo abortTaskResults(TaskId taskId, OutputBufferId bufferId)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        return tasks.getUnchecked(taskId).abortTaskResults(bufferId);
    }

    @Override
    public TaskInfo cancelTask(TaskId taskId)
    {
        requireNonNull(taskId, "taskId is null");

        return tasks.getUnchecked(taskId).cancel();
    }

    @Override
    public TaskInfo abortTask(TaskId taskId)
    {
        requireNonNull(taskId, "taskId is null");

        return tasks.getUnchecked(taskId).abort();
    }

    @Override
    public TaskInfo failTask(TaskId taskId, Throwable failure)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(failure, "failure is null");

        return tasks.getUnchecked(taskId).failed(failure);
    }

    public void removeOldTasks()
    {
        DateTime oldestAllowedTask = DateTime.now().minus(infoCacheTime.toMillis());
        tasks.asMap().values().stream()
                .map(SqlTask::getTaskInfo)
                .filter(Objects::nonNull)
                .forEach(taskInfo -> {
                    TaskId taskId = taskInfo.getTaskStatus().getTaskId();
                    try {
                        DateTime endTime = taskInfo.getStats().getEndTime();
                        if (endTime != null && endTime.isBefore(oldestAllowedTask)) {
                            tasks.asMap().remove(taskId);
                        }
                    }
                    catch (RuntimeException e) {
                        log.warn(e, "Error while inspecting age of complete task %s", taskId);
                    }
                });
    }

    public void failAbandonedTasks()
    {
        DateTime now = DateTime.now();
        DateTime oldestAllowedHeartbeat = now.minus(clientTimeout.toMillis());
        for (SqlTask sqlTask : tasks.asMap().values()) {
            try {
                TaskInfo taskInfo = sqlTask.getTaskInfo();
                TaskStatus taskStatus = taskInfo.getTaskStatus();
                if (taskStatus.getState().isDone()) {
                    continue;
                }
                DateTime lastHeartbeat = taskInfo.getLastHeartbeat();
                if (lastHeartbeat != null && lastHeartbeat.isBefore(oldestAllowedHeartbeat)) {
                    log.info("Failing abandoned task %s", taskStatus.getTaskId());
                    sqlTask.failed(new TrinoException(ABANDONED_TASK, format("Task %s has not been accessed since %s: currentTime %s", taskStatus.getTaskId(), lastHeartbeat, now)));
                }
            }
            catch (RuntimeException e) {
                log.warn(e, "Error while inspecting age of task %s", sqlTask.getTaskId());
            }
        }
    }

    //
    // Jmxutils only calls nested getters once, so we are forced to maintain a single
    // instance and periodically recalculate the stats.
    //
    private void updateStats()
    {
        SqlTaskIoStats tempIoStats = new SqlTaskIoStats();
        tempIoStats.merge(finishedTaskStats);

        // there is a race here between task completion, which merges stats into
        // finishedTaskStats, and getting the stats from the task.  Since we have
        // already merged the final stats, we could miss the stats from this task
        // which would result in an under-count, but we will not get an over-count.
        tasks.asMap().values().stream()
                .filter(task -> !task.getTaskState().isDone())
                .forEach(task -> tempIoStats.merge(task.getIoStats()));

        cachedStats.resetTo(tempIoStats);
    }

    @Override
    public void addStateChangeListener(TaskId taskId, StateChangeListener<TaskState> stateChangeListener)
    {
        requireNonNull(taskId, "taskId is null");
        tasks.getUnchecked(taskId).addStateChangeListener(stateChangeListener);
    }

    @Override
    public void addSourceTaskFailureListener(TaskId taskId, TaskFailureListener listener)
    {
        tasks.getUnchecked(taskId).addSourceTaskFailureListener(listener);
    }

    @Override
    public Optional<String> getTraceToken(TaskId taskId)
    {
        return tasks.getUnchecked(taskId).getTraceToken();
    }

    @VisibleForTesting
    public QueryContext getQueryContext(QueryId queryId)

    {
        return queryContexts.getUnchecked(queryId);
    }
}
