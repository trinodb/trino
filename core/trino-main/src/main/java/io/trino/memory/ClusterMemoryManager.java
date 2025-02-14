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
package io.trino.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.common.io.Closer;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.execution.LocationFactory;
import io.trino.execution.QueryExecution;
import io.trino.execution.QueryInfo;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.memory.LowMemoryKiller.ForQueryLowMemoryKiller;
import io.trino.memory.LowMemoryKiller.ForTaskLowMemoryKiller;
import io.trino.memory.LowMemoryKiller.RunningQueryInfo;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.operator.RetryPolicy;
import io.trino.server.BasicQueryInfo;
import io.trino.server.ServerConfig;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.memory.MemoryPoolInfo;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.collect.Sets.difference;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.ExceededMemoryLimitException.exceededGlobalTotalLimit;
import static io.trino.ExceededMemoryLimitException.exceededGlobalUserLimit;
import static io.trino.SystemSessionProperties.RESOURCE_OVERCOMMIT;
import static io.trino.SystemSessionProperties.getQueryMaxMemory;
import static io.trino.SystemSessionProperties.getQueryMaxTotalMemory;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.resourceOvercommit;
import static io.trino.metadata.NodeState.ACTIVE;
import static io.trino.metadata.NodeState.DRAINED;
import static io.trino.metadata.NodeState.DRAINING;
import static io.trino.metadata.NodeState.SHUTTING_DOWN;
import static io.trino.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class ClusterMemoryManager
{
    private static final Logger log = Logger.get(ClusterMemoryManager.class);
    private static final String EXPORTED_POOL_NAME = "general";

    private final ExecutorService listenerExecutor = newSingleThreadExecutor(daemonThreadsNamed("cluster-memory-manager-listener-%s"));
    private final ClusterMemoryLeakDetector memoryLeakDetector = new ClusterMemoryLeakDetector();
    private final InternalNodeManager nodeManager;
    private final LocationFactory locationFactory;
    private final HttpClient httpClient;
    private final MBeanExporter exporter;
    private final JsonCodec<MemoryInfo> memoryInfoCodec;
    private final DataSize maxQueryMemory;
    private final DataSize maxQueryTotalMemory;
    private final boolean includeCoordinator;
    private final List<LowMemoryKiller> lowMemoryKillers;
    private final AtomicLong totalAvailableProcessors = new AtomicLong();
    private final AtomicLong clusterUserMemoryReservation = new AtomicLong();
    private final AtomicLong clusterTotalMemoryReservation = new AtomicLong();
    private final AtomicLong clusterMemoryBytes = new AtomicLong();
    private final AtomicLong queriesKilledDueToOutOfMemory = new AtomicLong();
    private final AtomicLong tasksKilledDueToOutOfMemory = new AtomicLong();

    @GuardedBy("this")
    private final Map<String, RemoteNodeMemory> nodes = new HashMap<>();

    @GuardedBy("this")
    private final List<Consumer<MemoryPoolInfo>> changeListeners = new ArrayList<>();

    private final ClusterMemoryPool pool;

    @GuardedBy("this")
    private Optional<KillTarget> lastKillTarget = Optional.empty();

    @Inject
    public ClusterMemoryManager(
            @ForMemoryManager HttpClient httpClient,
            InternalNodeManager nodeManager,
            LocationFactory locationFactory,
            MBeanExporter exporter,
            JsonCodec<MemoryInfo> memoryInfoCodec,
            @ForTaskLowMemoryKiller LowMemoryKiller taskLowMemoryKiller,
            @ForQueryLowMemoryKiller LowMemoryKiller queryLowMemoryKiller,
            ServerConfig serverConfig,
            MemoryManagerConfig config,
            NodeSchedulerConfig nodeSchedulerConfig)
    {
        checkState(serverConfig.isCoordinator(), "ClusterMemoryManager must not be bound on worker");

        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.exporter = requireNonNull(exporter, "exporter is null");
        this.memoryInfoCodec = requireNonNull(memoryInfoCodec, "memoryInfoCodec is null");
        requireNonNull(taskLowMemoryKiller, "taskLowMemoryKiller is null");
        requireNonNull(queryLowMemoryKiller, "queryLowMemoryKiller is null");
        this.lowMemoryKillers = ImmutableList.of(
                taskLowMemoryKiller, // try to kill tasks first
                queryLowMemoryKiller);
        this.maxQueryMemory = config.getMaxQueryMemory();
        this.maxQueryTotalMemory = config.getMaxQueryTotalMemory();
        this.includeCoordinator = nodeSchedulerConfig.isIncludeCoordinator();

        verify(maxQueryMemory.toBytes() <= maxQueryTotalMemory.toBytes(),
                "maxQueryMemory cannot be greater than maxQueryTotalMemory");

        this.pool = new ClusterMemoryPool();
        exportMemoryPool();
    }

    private void exportMemoryPool()
    {
        try {
            exporter.exportWithGeneratedName(pool, ClusterMemoryPool.class, EXPORTED_POOL_NAME);
        }
        catch (JmxException e) {
            log.error(e, "Error exporting memory pool");
        }
    }

    public synchronized void addChangeListener(Consumer<MemoryPoolInfo> listener)
    {
        changeListeners.add(listener);
    }

    public synchronized void process(Iterable<QueryExecution> runningQueries, Supplier<List<BasicQueryInfo>> allQueryInfoSupplier)
    {
        // TODO revocable memory reservations can also leak and may need to be detected in the future
        // We are only concerned about the leaks in the memory pool.
        memoryLeakDetector.checkForMemoryLeaks(allQueryInfoSupplier, pool.getQueryMemoryReservations());

        boolean outOfMemory = isClusterOutOfMemory();

        boolean queryKilled = false;
        long totalUserMemoryBytes = 0L;
        long totalMemoryBytes = 0L;
        for (QueryExecution query : runningQueries) {
            boolean resourceOvercommit = resourceOvercommit(query.getSession());
            long userMemoryReservation = query.getUserMemoryReservation().toBytes();
            long totalMemoryReservation = query.getTotalMemoryReservation().toBytes();
            totalUserMemoryBytes += userMemoryReservation;
            totalMemoryBytes += totalMemoryReservation;

            if (getRetryPolicy(query.getSession()) == RetryPolicy.TASK) {
                // Memory limit for fault tolerant queries should only be enforced by the MemoryPool.
                // LowMemoryKiller is responsible for freeing up the MemoryPool if necessary.
                continue;
            }

            if (resourceOvercommit && outOfMemory) {
                // If a query has requested resource overcommit, only kill it if the cluster has run out of memory
                DataSize memory = succinctBytes(getQueryMemoryReservation(query));
                query.fail(new TrinoException(CLUSTER_OUT_OF_MEMORY,
                        format("The cluster is out of memory and %s=true, so this query was killed. It was using %s of memory", RESOURCE_OVERCOMMIT, memory)));
                queryKilled = true;
            }

            if (!resourceOvercommit) {
                long userMemoryLimit = min(maxQueryMemory.toBytes(), getQueryMaxMemory(query.getSession()).toBytes());
                if (userMemoryReservation > userMemoryLimit) {
                    query.fail(exceededGlobalUserLimit(succinctBytes(userMemoryLimit)));
                    queryKilled = true;
                }

                long totalMemoryLimit = min(maxQueryTotalMemory.toBytes(), getQueryMaxTotalMemory(query.getSession()).toBytes());
                if (totalMemoryReservation > totalMemoryLimit) {
                    query.fail(exceededGlobalTotalLimit(succinctBytes(totalMemoryLimit)));
                    queryKilled = true;
                }
            }
        }

        clusterUserMemoryReservation.set(totalUserMemoryBytes);
        clusterTotalMemoryReservation.set(totalMemoryBytes);

        if (!lowMemoryKillers.isEmpty() && outOfMemory && !queryKilled) {
            if (isLastKillTargetGone()) {
                callOomKiller(runningQueries);
            }
            else {
                log.debug("Last killed target is still not gone: %s", lastKillTarget);
            }
        }

        updateMemoryPool(Iterables.size(runningQueries));
        updateNodes();
    }

    private synchronized void callOomKiller(Iterable<QueryExecution> runningQueries)
    {
        List<RunningQueryInfo> runningQueryInfos = Streams.stream(runningQueries)
                .map(this::createQueryMemoryInfo)
                .collect(toImmutableList());

        Map<String, MemoryInfo> nodeMemoryInfosByNode = nodes.entrySet().stream()
                .filter(entry -> entry.getValue().getInfo().isPresent())
                .collect(toImmutableMap(
                        Entry::getKey,
                        entry -> entry.getValue().getInfo().get()));

        for (LowMemoryKiller lowMemoryKiller : lowMemoryKillers) {
            List<MemoryInfo> nodeMemoryInfos = ImmutableList.copyOf(nodeMemoryInfosByNode.values());
            Optional<KillTarget> killTarget = lowMemoryKiller.chooseTargetToKill(runningQueryInfos, nodeMemoryInfos);

            if (killTarget.isPresent()) {
                if (killTarget.get().isWholeQuery()) {
                    QueryId queryId = killTarget.get().getQuery();
                    log.debug("Low memory killer chose %s", queryId);
                    Optional<QueryExecution> chosenQuery = findRunningQuery(runningQueries, killTarget.get().getQuery());
                    if (chosenQuery.isPresent()) {
                        // See comments in  isQueryGone for why chosenQuery might be absent.
                        chosenQuery.get().fail(new TrinoException(CLUSTER_OUT_OF_MEMORY, "Query killed because the cluster is out of memory. Please try again in a few minutes."));
                        queriesKilledDueToOutOfMemory.incrementAndGet();
                        lastKillTarget = killTarget;
                        logQueryKill(queryId, nodeMemoryInfosByNode);
                    }
                }
                else {
                    Set<TaskId> tasks = killTarget.get().getTasks();
                    log.debug("Low memory killer chose %s", tasks);
                    ImmutableSet.Builder<TaskId> killedTasksBuilder = ImmutableSet.builder();
                    for (TaskId task : tasks) {
                        Optional<QueryExecution> runningQuery = findRunningQuery(runningQueries, task.getQueryId());
                        if (runningQuery.isPresent()) {
                            runningQuery.get().failTask(task, new TrinoException(CLUSTER_OUT_OF_MEMORY, "Task killed because the cluster is out of memory."));
                            tasksKilledDueToOutOfMemory.incrementAndGet();
                            killedTasksBuilder.add(task);
                        }
                    }
                    // only record tasks actually killed
                    ImmutableSet<TaskId> killedTasks = killedTasksBuilder.build();
                    if (!killedTasks.isEmpty()) {
                        lastKillTarget = Optional.of(KillTarget.selectedTasks(killedTasks));
                        logTasksKill(killedTasks, nodeMemoryInfosByNode);
                    }
                }
                break; // skip other killers
            }
        }
    }

    @GuardedBy("this")
    private boolean isLastKillTargetGone()
    {
        if (lastKillTarget.isEmpty()) {
            return true;
        }

        if (lastKillTarget.get().isWholeQuery()) {
            return isQueryGone(lastKillTarget.get().getQuery());
        }

        return areTasksGone(lastKillTarget.get().getTasks());
    }

    private boolean isQueryGone(QueryId killedQuery)
    {
        // If the lastKilledQuery is marked as leaked by the ClusterMemoryLeakDetector we consider the lastKilledQuery as gone,
        // so that the ClusterMemoryManager can continue to make progress even if there are leaks.
        // Even if the weak references to the leaked queries are GCed in the ClusterMemoryLeakDetector, it will mark the same queries
        // as leaked in its next run, and eventually the ClusterMemoryManager will make progress.
        if (memoryLeakDetector.wasQueryPossiblyLeaked(killedQuery)) {
            lastKillTarget = Optional.empty();
            return true;
        }

        // pool fields is updated based on nodes field.
        // Therefore, if the query is gone from pool field, it should also be gone from nodes field.
        // However, since nodes can updated asynchronously, it has the potential of coming back after being gone.
        // Therefore, even if the query appears to be gone here, it might be back when one inspects nodes later.
        return !pool
                .getQueryMemoryReservations()
                .containsKey(killedQuery);
    }

    private boolean areTasksGone(Set<TaskId> tasks)
    {
        // We build list of tasks based on MemoryPoolInfo objects, so it is consistent with memory usage reported for nodes.
        // This will only contain tasks for queries with task retries enabled - but this is what we want.
        Set<TaskId> runningTasks = getRunningTasks();
        return tasks.stream().noneMatch(runningTasks::contains);
    }

    private ImmutableSet<TaskId> getRunningTasks()
    {
        return nodes.values().stream()
                .map(RemoteNodeMemory::getInfo)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .flatMap(memoryInfo -> memoryInfo.getPool().getTaskMemoryReservations().keySet().stream())
                .map(TaskId::valueOf)
                .collect(toImmutableSet());
    }

    private Optional<QueryExecution> findRunningQuery(Iterable<QueryExecution> runningQueries, QueryId queryId)
    {
        return Streams.stream(runningQueries).filter(query -> queryId.equals(query.getQueryId())).collect(toOptional());
    }

    private void logQueryKill(QueryId killedQueryId, Map<String, MemoryInfo> nodeMemoryInfosByNode)
    {
        if (!log.isInfoEnabled()) {
            return;
        }
        StringBuilder nodeDescription = new StringBuilder();
        nodeDescription.append("Query Kill Decision: Killed ").append(killedQueryId).append("\n");
        nodeDescription.append(formatKillScenario(nodeMemoryInfosByNode));
        log.info("%s", nodeDescription);
    }

    private void logTasksKill(Set<TaskId> tasks, Map<String, MemoryInfo> nodeMemoryInfosByNode)
    {
        if (!log.isInfoEnabled()) {
            return;
        }
        StringBuilder nodeDescription = new StringBuilder();
        nodeDescription.append("Query Kill Decision: Tasks Killed ")
                .append(tasks)
                .append("\n");
        nodeDescription.append(formatKillScenario(nodeMemoryInfosByNode));
        log.info("%s", nodeDescription);
    }

    private String formatKillScenario(Map<String, MemoryInfo> nodes)
    {
        StringBuilder stringBuilder = new StringBuilder();
        for (Entry<String, MemoryInfo> entry : nodes.entrySet()) {
            String nodeId = entry.getKey();
            MemoryInfo nodeMemoryInfo = entry.getValue();
            MemoryPoolInfo memoryPoolInfo = nodeMemoryInfo.getPool();
            stringBuilder.append("Node[").append(nodeId).append("]: ");
            stringBuilder.append("MaxBytes ").append(memoryPoolInfo.getMaxBytes()).append(' ');
            stringBuilder.append("FreeBytes ").append(memoryPoolInfo.getFreeBytes() + memoryPoolInfo.getReservedRevocableBytes()).append(' ');
            stringBuilder.append("Queries ");
            Joiner.on(",").withKeyValueSeparator("=").appendTo(stringBuilder, memoryPoolInfo.getQueryMemoryReservations()).append(' ');
            stringBuilder.append("Tasks ");
            Joiner.on(",").withKeyValueSeparator("=").appendTo(stringBuilder, memoryPoolInfo.getTaskMemoryReservations());
            stringBuilder.append('\n');
        }
        return stringBuilder.toString();
    }

    @VisibleForTesting
    ClusterMemoryPool getPool()
    {
        return pool;
    }

    private boolean isClusterOutOfMemory()
    {
        return pool.getBlockedNodes() > 0;
    }

    private RunningQueryInfo createQueryMemoryInfo(QueryExecution query)
    {
        QueryInfo queryInfo = query.getQueryInfo();
        ImmutableMap.Builder<TaskId, TaskInfo> taskInfosBuilder = ImmutableMap.builder();
        queryInfo.getOutputStage().ifPresent(stage -> getTaskInfos(stage, taskInfosBuilder));
        return new RunningQueryInfo(
                query.getQueryId(),
                query.getTotalMemoryReservation().toBytes(),
                taskInfosBuilder.buildOrThrow(),
                getRetryPolicy(query.getSession()));
    }

    private void getTaskInfos(StageInfo stageInfo, ImmutableMap.Builder<TaskId, TaskInfo> taskInfosBuilder)
    {
        for (TaskInfo taskInfo : stageInfo.getTasks()) {
            taskInfosBuilder.put(taskInfo.taskStatus().getTaskId(), taskInfo);
        }
        for (StageInfo subStage : stageInfo.getSubStages()) {
            getTaskInfos(subStage, taskInfosBuilder);
        }
    }

    private long getQueryMemoryReservation(QueryExecution query)
    {
        return query.getTotalMemoryReservation().toBytes();
    }

    private synchronized void updateNodes()
    {
        ImmutableSet.Builder<InternalNode> builder = ImmutableSet.builder();
        Set<InternalNode> aliveNodes = builder
                .addAll(nodeManager.getNodes(ACTIVE))
                .addAll(nodeManager.getNodes(SHUTTING_DOWN))
                .addAll(nodeManager.getNodes(DRAINING))
                .addAll(nodeManager.getNodes(DRAINED))
                .build();

        ImmutableSet<String> aliveNodeIds = aliveNodes.stream()
                .map(InternalNode::getNodeIdentifier)
                .collect(toImmutableSet());

        // Remove nodes that don't exist anymore
        // Make a copy to materialize the set difference
        Set<String> deadNodes = ImmutableSet.copyOf(difference(nodes.keySet(), aliveNodeIds));
        nodes.keySet().removeAll(deadNodes);

        // Add new nodes
        for (InternalNode node : aliveNodes) {
            if (!nodes.containsKey(node.getNodeIdentifier())) {
                nodes.put(node.getNodeIdentifier(), new RemoteNodeMemory(node, httpClient, memoryInfoCodec, locationFactory.createMemoryInfoLocation(node)));
            }
        }

        // Schedule refresh
        for (RemoteNodeMemory node : nodes.values()) {
            node.asyncRefresh();
        }
    }

    private synchronized void updateMemoryPool(int queryCount)
    {
        // Update view of cluster memory and pools
        List<MemoryInfo> nodeMemoryInfos = nodes.values().stream()
                .map(RemoteNodeMemory::getInfo)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());

        long totalProcessors = nodeMemoryInfos.stream()
                .mapToLong(MemoryInfo::getAvailableProcessors)
                .sum();
        totalAvailableProcessors.set(totalProcessors);

        long totalClusterMemory = nodeMemoryInfos.stream()
                .mapToLong(memoryInfo -> memoryInfo.getPool().getMaxBytes())
                .sum();
        clusterMemoryBytes.set(totalClusterMemory);

        pool.update(nodeMemoryInfos, queryCount);
        if (!changeListeners.isEmpty()) {
            MemoryPoolInfo info = pool.getInfo();
            for (Consumer<MemoryPoolInfo> listener : changeListeners) {
                listenerExecutor.execute(() -> listener.accept(info));
            }
        }
    }

    public synchronized Map<String, Optional<MemoryInfo>> getWorkersMemoryInfo()
    {
        Map<String, Optional<MemoryInfo>> memoryInfo = new HashMap<>();
        for (Entry<String, RemoteNodeMemory> entry : nodes.entrySet()) {
            if (!includeCoordinator && entry.getValue().getNode().isCoordinator()) {
                continue;
            }
            String workerId = entry.getKey();
            memoryInfo.put(workerId, entry.getValue().getInfo());
        }
        return memoryInfo;
    }

    public synchronized Map<String, Optional<MemoryInfo>> getAllNodesMemoryInfo()
    {
        Map<String, Optional<MemoryInfo>> memoryInfo = new HashMap<>();
        for (Entry<String, RemoteNodeMemory> entry : nodes.entrySet()) {
            String workerId = entry.getKey();
            memoryInfo.put(workerId, entry.getValue().getInfo());
        }
        return memoryInfo;
    }

    @PreDestroy
    public synchronized void destroy()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(() -> exporter.unexportWithGeneratedName(ClusterMemoryPool.class, EXPORTED_POOL_NAME));
            closer.register(listenerExecutor::shutdownNow);
        }
    }

    @Managed
    public long getTotalAvailableProcessors()
    {
        return totalAvailableProcessors.get();
    }

    @Managed
    public int getNumberOfLeakedQueries()
    {
        return memoryLeakDetector.getNumberOfLeakedQueries();
    }

    @Managed
    public long getClusterUserMemoryReservation()
    {
        return clusterUserMemoryReservation.get();
    }

    @Managed
    public long getClusterTotalMemoryReservation()
    {
        return clusterTotalMemoryReservation.get();
    }

    @Managed
    public long getClusterMemoryBytes()
    {
        return clusterMemoryBytes.get();
    }

    @Managed
    public long getQueriesKilledDueToOutOfMemory()
    {
        return queriesKilledDueToOutOfMemory.get();
    }

    @Managed
    public long getTasksKilledDueToOutOfMemory()
    {
        return tasksKilledDueToOutOfMemory.get();
    }
}
