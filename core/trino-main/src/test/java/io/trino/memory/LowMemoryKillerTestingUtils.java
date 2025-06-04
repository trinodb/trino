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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.client.NodeVersion;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.metadata.InternalNode;
import io.trino.operator.RetryPolicy;
import io.trino.spi.QueryId;
import io.trino.spi.memory.MemoryPoolInfo;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public final class LowMemoryKillerTestingUtils
{
    private LowMemoryKillerTestingUtils() {}

    static List<MemoryInfo> toNodeMemoryInfoList(long memoryPoolMaxBytes, Map<String, Map<String, Long>> queries)
    {
        return toNodeMemoryInfoList(memoryPoolMaxBytes, queries, ImmutableMap.of());
    }

    static List<MemoryInfo> toNodeMemoryInfoList(long memoryPoolMaxBytes, Map<String, Map<String, Long>> queries, Map<String, Map<String, Map<Integer, Long>>> tasks)
    {
        Map<InternalNode, NodeReservation> nodeReservations = new HashMap<>();

        for (Map.Entry<String, Map<String, Long>> entry : queries.entrySet()) {
            QueryId queryId = new QueryId(entry.getKey());
            Map<String, Long> reservationByNode = entry.getValue();

            for (Map.Entry<String, Long> nodeEntry : reservationByNode.entrySet()) {
                InternalNode node = new InternalNode(nodeEntry.getKey(), URI.create("http://localhost"), new NodeVersion("version"), false);
                long bytes = nodeEntry.getValue();
                if (bytes == 0) {
                    continue;
                }
                nodeReservations.computeIfAbsent(node, _ -> new NodeReservation()).add(queryId, bytes);
            }
        }

        ImmutableList.Builder<MemoryInfo> result = ImmutableList.builder();
        for (Map.Entry<InternalNode, NodeReservation> entry : nodeReservations.entrySet()) {
            NodeReservation nodeReservation = entry.getValue();
            MemoryPoolInfo memoryPoolInfo = new MemoryPoolInfo(
                    memoryPoolMaxBytes,
                    nodeReservation.getTotalReservedBytes(),
                    0,
                    nodeReservation.getReservationByQuery(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    tasksMemoryInfoForNode(entry.getKey().getNodeIdentifier(), tasks),
                    ImmutableMap.of());
            result.add(new MemoryInfo(7, memoryPoolInfo));
        }
        return result.build();
    }

    private static Map<String, Long> tasksMemoryInfoForNode(String nodeIdentifier, Map<String, Map<String, Map<Integer, Long>>> tasks)
    {
        ImmutableMap.Builder<String, Long> result = ImmutableMap.builder();
        for (Map.Entry<String, Map<String, Map<Integer, Long>>> queryNodesEntry : tasks.entrySet()) {
            for (Map.Entry<String, Map<Integer, Long>> nodeTasksEntry : queryNodesEntry.getValue().entrySet()) {
                if (!nodeIdentifier.equals(nodeTasksEntry.getKey())) {
                    continue;
                }

                for (Map.Entry<Integer, Long> partitionReservationEntry : nodeTasksEntry.getValue().entrySet()) {
                    result.put(taskId(queryNodesEntry.getKey(), partitionReservationEntry.getKey()).toString(), partitionReservationEntry.getValue());
                }
            }
        }
        return result.buildOrThrow();
    }

    static TaskId taskId(String query, int partition)
    {
        return new TaskId(new StageId(QueryId.valueOf(query), 0), partition, 0);
    }

    static List<LowMemoryKiller.RunningQueryInfo> toRunningQueryInfoList(Map<String, Map<String, Long>> queries)
    {
        return toRunningQueryInfoList(queries, ImmutableSet.of());
    }

    static List<LowMemoryKiller.RunningQueryInfo> toRunningQueryInfoList(Map<String, Map<String, Long>> queries, Set<String> queriesWithTaskLevelRetries)
    {
        return toRunningQueryInfoList(queries, queriesWithTaskLevelRetries, ImmutableMap.of());
    }

    static List<LowMemoryKiller.RunningQueryInfo> toRunningQueryInfoList(Map<String, Map<String, Long>> queries, Set<String> queriesWithTaskLevelRetries, Map<String, Map<Integer, TaskInfo>> taskInfos)
    {
        ImmutableList.Builder<LowMemoryKiller.RunningQueryInfo> result = ImmutableList.builder();
        for (Map.Entry<String, Map<String, Long>> entry : queries.entrySet()) {
            String queryId = entry.getKey();
            long totalReservation = entry.getValue().values().stream()
                    .mapToLong(x -> x)
                    .sum();

            Map<TaskId, TaskInfo> queryTaskInfos = taskInfos.getOrDefault(queryId, ImmutableMap.of()).entrySet().stream()
                    .collect(toImmutableMap(
                            taskEntry -> taskId(queryId, taskEntry.getKey()),
                            Map.Entry::getValue));

            result.add(new LowMemoryKiller.RunningQueryInfo(
                    new QueryId(queryId),
                    totalReservation,
                    queryTaskInfos,
                    queriesWithTaskLevelRetries.contains(queryId) ? RetryPolicy.TASK : RetryPolicy.NONE));
        }
        return result.build();
    }

    private static class NodeReservation
    {
        private long totalReservedBytes;
        private final Map<QueryId, Long> reservationByQuery = new HashMap<>();

        public void add(QueryId queryId, long bytes)
        {
            totalReservedBytes += bytes;
            reservationByQuery.put(queryId, bytes);
        }

        public long getTotalReservedBytes()
        {
            return totalReservedBytes;
        }

        public Map<QueryId, Long> getReservationByQuery()
        {
            return reservationByQuery;
        }
    }
}
