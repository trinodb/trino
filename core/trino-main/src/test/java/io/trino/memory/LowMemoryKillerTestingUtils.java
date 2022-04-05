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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.trino.TaskMemoryInfo;
import io.trino.client.NodeVersion;
import io.trino.execution.TaskId;
import io.trino.metadata.InternalNode;
import io.trino.operator.RetryPolicy;
import io.trino.spi.QueryId;
import io.trino.spi.memory.MemoryPoolInfo;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class LowMemoryKillerTestingUtils
{
    private LowMemoryKillerTestingUtils() {}

    static List<MemoryInfo> toNodeMemoryInfoList(long memoryPoolMaxBytes, Map<String, Map<String, Long>> queries)
    {
        return toNodeMemoryInfoList(memoryPoolMaxBytes, queries, ImmutableMap.of());
    }

    static List<MemoryInfo> toNodeMemoryInfoList(long memoryPoolMaxBytes, Map<String, Map<String, Long>> queries, Map<String, Map<String, Map<String, Long>>> tasks)
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
                nodeReservations.computeIfAbsent(node, ignored -> new NodeReservation()).add(queryId, bytes);
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
                    ImmutableMap.of());
            result.add(new MemoryInfo(7, memoryPoolInfo, tasksMemoryInfoForNode(entry.getKey().getNodeIdentifier(), tasks)));
        }
        return result.build();
    }

    private static ListMultimap<QueryId, TaskMemoryInfo> tasksMemoryInfoForNode(String nodeIdentifier, Map<String, Map<String, Map<String, Long>>> tasks)
    {
        ImmutableListMultimap.Builder<QueryId, TaskMemoryInfo> result = ImmutableListMultimap.builder();
        for (Map.Entry<String, Map<String, Map<String, Long>>> queryNodesEntry : tasks.entrySet()) {
            QueryId query = QueryId.valueOf(queryNodesEntry.getKey());
            for (Map.Entry<String, Map<String, Long>> nodeTasksEntry : queryNodesEntry.getValue().entrySet()) {
                if (!nodeIdentifier.equals(nodeTasksEntry.getKey())) {
                    continue;
                }

                for (Map.Entry<String, Long> taskReservationEntry : nodeTasksEntry.getValue().entrySet()) {
                    TaskId taskId = TaskId.valueOf(taskReservationEntry.getKey());
                    long taskReservation = taskReservationEntry.getValue();
                    result.put(query, new TaskMemoryInfo(taskId, taskReservation));
                }
            }
        }
        return result.build();
    }

    static List<LowMemoryKiller.QueryMemoryInfo> toQueryMemoryInfoList(Map<String, Map<String, Long>> queries)
    {
        return toQueryMemoryInfoList(queries, ImmutableSet.of());
    }

    static List<LowMemoryKiller.QueryMemoryInfo> toQueryMemoryInfoList(Map<String, Map<String, Long>> queries, Set<String> queriesWithTaskLevelRetries)
    {
        ImmutableList.Builder<LowMemoryKiller.QueryMemoryInfo> result = ImmutableList.builder();
        for (Map.Entry<String, Map<String, Long>> entry : queries.entrySet()) {
            String queryId = entry.getKey();
            long totalReservation = entry.getValue().values().stream()
                    .mapToLong(x -> x)
                    .sum();
            result.add(new LowMemoryKiller.QueryMemoryInfo(
                    new QueryId(queryId),
                    totalReservation,
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
