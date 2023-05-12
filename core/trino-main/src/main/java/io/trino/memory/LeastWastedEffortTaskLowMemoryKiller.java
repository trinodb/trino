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
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.operator.RetryPolicy;
import io.trino.operator.TaskStats;
import io.trino.spi.QueryId;
import io.trino.spi.memory.MemoryPoolInfo;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Comparator.comparing;

public class LeastWastedEffortTaskLowMemoryKiller
        implements LowMemoryKiller
{
    private static final long MIN_WALL_TIME = Duration.of(30, ChronoUnit.SECONDS).toMillis();

    @Override
    public Optional<KillTarget> chooseTargetToKill(List<RunningQueryInfo> runningQueries, List<MemoryInfo> nodes)
    {
        Set<QueryId> queriesWithTaskRetryPolicy = runningQueries.stream()
                                                          .filter(query -> query.getRetryPolicy() == RetryPolicy.TASK)
                                                          .map(RunningQueryInfo::getQueryId)
                                                          .collect(toImmutableSet());

        if (queriesWithTaskRetryPolicy.isEmpty()) {
            return Optional.empty();
        }

        ImmutableSet.Builder<TaskId> tasksToKillBuilder = ImmutableSet.builder();

        Map<TaskId, TaskInfo> taskInfos = runningQueries.stream()
                .filter(queryInfo -> queriesWithTaskRetryPolicy.contains(queryInfo.getQueryId()))
                .flatMap(queryInfo -> queryInfo.getTaskInfos().entrySet().stream())
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue));

        for (MemoryInfo node : nodes) {
            MemoryPoolInfo memoryPool = node.getPool();
            if (memoryPool == null) {
                continue;
            }
            if (memoryPool.getFreeBytes() + memoryPool.getReservedRevocableBytes() > 0) {
                continue;
            }

            findBiggestTask(queriesWithTaskRetryPolicy, taskInfos, memoryPool, true) // try just speculative
                    .or(() -> findBiggestTask(queriesWithTaskRetryPolicy, taskInfos, memoryPool, false)) // fallback to any task
                    .ifPresent(tasksToKillBuilder::add);
        }
        Set<TaskId> tasksToKill = tasksToKillBuilder.build();
        if (tasksToKill.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(KillTarget.selectedTasks(tasksToKill));
    }

    private static Optional<TaskId> findBiggestTask(Set<QueryId> queriesWithTaskRetryPolicy, Map<TaskId, TaskInfo> taskInfos, MemoryPoolInfo memoryPool, boolean onlySpeculative)
    {
        Stream<SimpleEntry<TaskId, Long>> stream = memoryPool.getTaskMemoryReservations().entrySet().stream()
                .map(entry -> new SimpleEntry<>(TaskId.valueOf(entry.getKey()), entry.getValue()))
                .filter(entry -> queriesWithTaskRetryPolicy.contains(entry.getKey().getQueryId()));

        if (onlySpeculative) {
            stream = stream.filter(entry -> {
                TaskInfo taskInfo = taskInfos.get(entry.getKey());
                if (taskInfo == null) {
                    return false;
                }
                return taskInfo.getTaskStatus().isSpeculative();
            });
        }

        return stream
                .max(comparing(entry -> {
                    TaskId taskId = entry.getKey();
                    Long memoryUsed = entry.getValue();
                    long wallTime = 0;
                    if (taskInfos.containsKey(taskId)) {
                        TaskStats stats = taskInfos.get(taskId).getStats();
                        wallTime = stats.getTotalScheduledTime().toMillis() + stats.getTotalBlockedTime().toMillis();
                    }
                    wallTime = Math.max(wallTime, MIN_WALL_TIME); // only look at memory consumption for fairly short-lived tasks
                    return (double) memoryUsed / wallTime;
                }))
                .map(SimpleEntry::getKey);
    }
}
