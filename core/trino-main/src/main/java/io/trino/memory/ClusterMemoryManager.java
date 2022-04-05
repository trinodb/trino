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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.common.io.Closer;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.LocationFactory;
import io.trino.execution.QueryExecution;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskStatus;
import io.trino.memory.LowMemoryKiller.QueryMemoryInfo;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.operator.RetryPolicy;
import io.trino.server.BasicQueryInfo;
import io.trino.server.ServerConfig;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.memory.ClusterMemoryPoolManager;
import io.trino.spi.memory.MemoryPoolInfo;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.collect.Sets.difference;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.ExceededMemoryLimitException.exceededGlobalTotalLimit;
import static io.trino.ExceededMemoryLimitException.exceededGlobalUserLimit;
import static io.trino.SystemSessionProperties.RESOURCE_OVERCOMMIT;
import static io.trino.SystemSessionProperties.getQueryMaxMemory;
import static io.trino.SystemSessionProperties.getQueryMaxTotalMemory;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.resourceOvercommit;
import static io.trino.metadata.NodeState.ACTIVE;
import static io.trino.metadata.NodeState.SHUTTING_DOWN;
import static io.trino.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ClusterMemoryManager
        implements ClusterMemoryPoolManager
{
    private static final Logger log = Logger.get(ClusterMemoryManager.class);
    private static final String EXPORTED_POOL_NAME = "general";

    private final ExecutorService listenerExecutor = Executors.newSingleThreadExecutor();
    private final ClusterMemoryLeakDetector memoryLeakDetector = new ClusterMemoryLeakDetector();
    private final InternalNodeManager nodeManager;
    private final LocationFactory locationFactory;
    private final HttpClient httpClient;
    private final MBeanExporter exporter;
    private final JsonCodec<MemoryInfo> memoryInfoCodec;
    private final DataSize maxQueryMemory;
    private final DataSize maxQueryTotalMemory;
    private final LowMemoryKiller lowMemoryKiller;
    private final Duration killOnOutOfMemoryDelay;
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
    private long lastTimeNotOutOfMemory = System.nanoTime();

    @GuardedBy("this")
    private KillTarget lastKillTarget;

    @Inject
    public ClusterMemoryManager(
            @ForMemoryManager HttpClient httpClient,
            InternalNodeManager nodeManager,
            LocationFactory locationFactory,
            MBeanExporter exporter,
            JsonCodec<MemoryInfo> memoryInfoCodec,
            QueryIdGenerator queryIdGenerator,
            LowMemoryKiller lowMemoryKiller,
            ServerConfig serverConfig,
            MemoryManagerConfig config,
            NodeMemoryConfig nodeMemoryConfig)
    {
        requireNonNull(config, "config is null");
        requireNonNull(nodeMemoryConfig, "nodeMemoryConfig is null");
        requireNonNull(serverConfig, "serverConfig is null");
        checkState(serverConfig.isCoordinator(), "ClusterMemoryManager must not be bound on worker");

        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.exporter = requireNonNull(exporter, "exporter is null");
        this.memoryInfoCodec = requireNonNull(memoryInfoCodec, "memoryInfoCodec is null");
        this.lowMemoryKiller = requireNonNull(lowMemoryKiller, "lowMemoryKiller is null");
        this.maxQueryMemory = config.getMaxQueryMemory();
        this.maxQueryTotalMemory = config.getMaxQueryTotalMemory();
        this.killOnOutOfMemoryDelay = config.getKillOnOutOfMemoryDelay();

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

    @Override
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
        if (!outOfMemory) {
            lastTimeNotOutOfMemory = System.nanoTime();
        }

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

        if (!(lowMemoryKiller instanceof NoneLowMemoryKiller) &&
                outOfMemory &&
                !queryKilled &&
                nanosSince(lastTimeNotOutOfMemory).compareTo(killOnOutOfMemoryDelay) > 0) {
            if (isLastKillTargetGone(runningQueries)) {
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
        List<QueryMemoryInfo> queryMemoryInfoList = Streams.stream(runningQueries)
                .map(this::createQueryMemoryInfo)
                .collect(toImmutableList());

        List<MemoryInfo> nodeMemoryInfos = nodes.values().stream()
                .map(RemoteNodeMemory::getInfo)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());

        Optional<KillTarget> killTarget = lowMemoryKiller.chooseQueryToKill(queryMemoryInfoList, nodeMemoryInfos);

        if (killTarget.isPresent()) {
            if (killTarget.get().isWholeQuery()) {
                QueryId queryId = killTarget.get().getQuery();
                log.debug("Low memory killer chose %s", queryId);
                Optional<QueryExecution> chosenQuery = findRunningQuery(runningQueries, killTarget.get().getQuery());
                if (chosenQuery.isPresent()) {
                    // See comments in  isQueryGone for why chosenQuery might be absent.
                    chosenQuery.get().fail(new TrinoException(CLUSTER_OUT_OF_MEMORY, "Query killed because the cluster is out of memory. Please try again in a few minutes."));
                    queriesKilledDueToOutOfMemory.incrementAndGet();
                    lastKillTarget = killTarget.get();
                    logQueryKill(queryId, nodeMemoryInfos);
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
                    lastKillTarget = KillTarget.selectedTasks(killedTasks);
                    logTasksKill(killedTasks, nodeMemoryInfos);
                }
            }
        }
    }

    @GuardedBy("this")
    private boolean isLastKillTargetGone(Iterable<QueryExecution> runningQueries)
    {
        if (lastKillTarget == null) {
            return true;
        }

        if (lastKillTarget.isWholeQuery()) {
            return isQueryGone(lastKillTarget.getQuery());
        }

        return areTasksGone(lastKillTarget.getTasks(), runningQueries);
    }

    private boolean isQueryGone(QueryId killedQuery)
    {
        // If the lastKilledQuery is marked as leaked by the ClusterMemoryLeakDetector we consider the lastKilledQuery as gone,
        // so that the ClusterMemoryManager can continue to make progress even if there are leaks.
        // Even if the weak references to the leaked queries are GCed in the ClusterMemoryLeakDetector, it will mark the same queries
        // as leaked in its next run, and eventually the ClusterMemoryManager will make progress.
        if (memoryLeakDetector.wasQueryPossiblyLeaked(killedQuery)) {
            lastKillTarget = null;
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

    private boolean areTasksGone(Set<TaskId> tasks, Iterable<QueryExecution> runningQueries)
    {
        List<QueryExecution> queryExecutions = tasks.stream()
                .map(TaskId::getQueryId)
                .distinct()
                .map(query -> findRunningQuery(runningQueries, query))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());

        if (queryExecutions.isEmpty()) {
            // all queries we care about are gone
            return true;
        }

        Set<TaskId> runningTasks = queryExecutions.stream()
                .flatMap(query -> getRunningTasksForQuery(query).stream())
                .map(TaskStatus::getTaskId)
                .collect(toImmutableSet());

        return tasks.stream().noneMatch(runningTasks::contains);
    }

    private Optional<QueryExecution> findRunningQuery(Iterable<QueryExecution> runningQueries, QueryId queryId)
    {
        return Streams.stream(runningQueries).filter(query -> queryId.equals(query.getQueryId())).collect(toOptional());
    }

    private void logQueryKill(QueryId killedQueryId, List<MemoryInfo> nodes)
    {
        if (!log.isInfoEnabled()) {
            return;
        }
        StringBuilder nodeDescription = new StringBuilder();
        nodeDescription.append("Query Kill Decision: Killed ").append(killedQueryId).append("\n");
        nodeDescription.append(formatKillScenario(nodes));
        log.info("%s", nodeDescription);
    }

    private void logTasksKill(Set<TaskId> tasks, List<MemoryInfo> nodes)
    {
        if (!log.isInfoEnabled()) {
            return;
        }
        StringBuilder nodeDescription = new StringBuilder();
        nodeDescription.append("Query Kill Decision: Tasks Killed ")
                .append(tasks)
                .append("(")
                .append(tasks)
                .append(")")
                .append("\n");
        nodeDescription.append(formatKillScenario(nodes));
        log.info("%s", nodeDescription);
    }

    private String formatKillScenario(List<MemoryInfo> nodes)
    {
        StringBuilder stringBuilder = new StringBuilder();
        for (MemoryInfo nodeMemoryInfo : nodes) {
            MemoryPoolInfo memoryPoolInfo = nodeMemoryInfo.getPool();
            stringBuilder.append("Query Kill Scenario: ");
            stringBuilder.append("MaxBytes ").append(memoryPoolInfo.getMaxBytes()).append(' ');
            stringBuilder.append("FreeBytes ").append(memoryPoolInfo.getFreeBytes() + memoryPoolInfo.getReservedRevocableBytes()).append(' ');
            stringBuilder.append("Queries ");
            Joiner.on(",").withKeyValueSeparator("=").appendTo(stringBuilder, memoryPoolInfo.getQueryMemoryReservations());
            stringBuilder.append("Tasks ");
            Joiner.on(",").withKeyValueSeparator("=").appendTo(stringBuilder, nodeMemoryInfo.getTasksMemoryInfo().asMap());
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

    private QueryMemoryInfo createQueryMemoryInfo(QueryExecution query)
    {
        return new QueryMemoryInfo(query.getQueryId(), query.getTotalMemoryReservation().toBytes());
    }

    private List<TaskStatus> getRunningTasksForQuery(QueryExecution query)
    {
        Optional<StageInfo> outputStage = query.getQueryInfo().getOutputStage();
        if (outputStage.isEmpty()) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<TaskStatus> builder = ImmutableList.builder();
        collectRunningTasksForStage(outputStage.get(), builder);
        return builder.build();
    }

    private void collectRunningTasksForStage(StageInfo stage, ImmutableList.Builder<TaskStatus> builder)
    {
        for (TaskInfo task : stage.getTasks()) {
            TaskStatus taskStatus = task.getTaskStatus();
            if (!taskStatus.getState().isDone()) {
                builder.add(taskStatus);
            }
        }
        for (StageInfo subStage : stage.getSubStages()) {
            collectRunningTasksForStage(subStage, builder);
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

    public synchronized Map<String, Optional<MemoryInfo>> getWorkerMemoryInfo()
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
