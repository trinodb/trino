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
import io.trino.operator.RetryPolicy;
import io.trino.spi.QueryId;
import io.trino.spi.memory.MemoryPoolInfo;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class TotalReservationOnBlockedNodesTaskLowMemoryKiller
        implements LowMemoryKiller
{
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
        for (MemoryInfo node : nodes) {
            MemoryPoolInfo memoryPool = node.getPool();
            if (memoryPool == null) {
                continue;
            }
            if (memoryPool.getFreeBytes() + memoryPool.getReservedRevocableBytes() > 0) {
                continue;
            }

            memoryPool.getTaskMemoryReservations().entrySet().stream()
                    // consider only tasks from queries with task retries enabled
                    .map(entry -> new SimpleEntry<>(TaskId.valueOf(entry.getKey()), entry.getValue()))
                    .filter(entry -> queriesWithTaskRetryPolicy.contains(entry.getKey().getQueryId()))
                    .max(Map.Entry.comparingByValue())
                    .map(SimpleEntry::getKey)
                    .ifPresent(tasksToKillBuilder::add);
        }
        Set<TaskId> tasksToKill = tasksToKillBuilder.build();
        if (tasksToKill.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(KillTarget.selectedTasks(tasksToKill));
    }
}
