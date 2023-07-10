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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.spi.QueryId;
import io.trino.spi.memory.MemoryPoolInfo;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.operator.RetryPolicy.TASK;

public class TotalReservationOnBlockedNodesTaskLowMemoryKiller
        implements LowMemoryKiller
{
    @Override
    public Optional<KillTarget> chooseTargetToKill(List<RunningQueryInfo> runningQueries, List<MemoryInfo> nodes)
    {
        Set<QueryId> queriesWithTaskRetryPolicy = runningQueries.stream()
                                                          .filter(query -> query.getRetryPolicy() == TASK)
                                                          .map(RunningQueryInfo::getQueryId)
                                                          .collect(toImmutableSet());

        if (queriesWithTaskRetryPolicy.isEmpty()) {
            return Optional.empty();
        }

        Map<QueryId, RunningQueryInfo> runningQueriesById = Maps.uniqueIndex(runningQueries, RunningQueryInfo::getQueryId);

        ImmutableSet.Builder<TaskId> tasksToKillBuilder = ImmutableSet.builder();
        for (MemoryInfo node : nodes) {
            MemoryPoolInfo memoryPool = node.getPool();
            if (memoryPool == null) {
                continue;
            }
            if (memoryPool.getFreeBytes() + memoryPool.getReservedRevocableBytes() > 0) {
                continue;
            }

            findBiggestTask(runningQueriesById, memoryPool, true) // try just speculative
                    .or(() -> findBiggestTask(runningQueriesById, memoryPool, false)) // fallback to any task
                    .ifPresent(tasksToKillBuilder::add);
        }
        Set<TaskId> tasksToKill = tasksToKillBuilder.build();
        if (tasksToKill.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(KillTarget.selectedTasks(tasksToKill));
    }

    private static Optional<TaskId> findBiggestTask(Map<QueryId, RunningQueryInfo> runningQueries, MemoryPoolInfo memoryPool, boolean onlySpeculative)
    {
        Stream<SimpleEntry<TaskId, Long>> stream = memoryPool.getTaskMemoryReservations().entrySet().stream()
                // consider only tasks from queries with task retries enabled
                .map(entry -> new SimpleEntry<>(TaskId.valueOf(entry.getKey()), entry.getValue()))
                .filter(entry -> runningQueries.containsKey(entry.getKey().getQueryId()))
                .filter(entry -> runningQueries.get(entry.getKey().getQueryId()).getRetryPolicy() == TASK);

        if (onlySpeculative) {
            stream = stream.filter(entry -> {
                TaskInfo taskInfo = runningQueries.get(entry.getKey().getQueryId()).getTaskInfos().get(entry.getKey());
                if (taskInfo == null) {
                    return false;
                }
                return taskInfo.getTaskStatus().isSpeculative();
            });
        }

        return stream
                .max(Map.Entry.comparingByValue())
                .map(SimpleEntry::getKey);
    }
}
