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
package io.trino.sql.planner.planprinter;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import io.airlift.units.Duration;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskInfo;
import io.trino.operator.OperatorStats;
import io.trino.operator.PipelineStats;
import io.trino.operator.TaskStats;
import io.trino.operator.WindowInfo;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.getLast;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.util.MoreMaps.mergeMaps;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;

public final class PlanNodeStatsSummarizer
{
    private PlanNodeStatsSummarizer() {}

    public static Map<PlanNodeId, PlanNodeStats> aggregateStageStats(List<StageInfo> stageInfos)
    {
        return aggregateTaskStats(stageInfos.stream()
                .flatMap(s -> s.getTasks().stream())
                .collect(toList()));
    }

    public static Map<PlanNodeId, PlanNodeStats> aggregateTaskStats(List<TaskInfo> taskInfos)
    {
        ListMultimap<PlanNodeId, PlanNodeStats> groupedStats = ArrayListMultimap.create();
        List<PlanNodeStats> planNodeStats = taskInfos.stream()
                .map(TaskInfo::stats)
                .flatMap(taskStats -> getPlanNodeStats(taskStats).stream())
                .collect(toList());
        for (PlanNodeStats stats : planNodeStats) {
            groupedStats.put(stats.getPlanNodeId(), stats);
        }

        ImmutableMap.Builder<PlanNodeId, PlanNodeStats> aggregatedStatsBuilder = ImmutableMap.builder();
        for (PlanNodeId planNodeId : groupedStats.keySet()) {
            List<PlanNodeStats> groupedPlanNodeStats = groupedStats.get(planNodeId);
            aggregatedStatsBuilder.put(planNodeId, groupedPlanNodeStats.get(0).mergeWith(groupedPlanNodeStats.subList(1, groupedPlanNodeStats.size())));
        }
        return aggregatedStatsBuilder.buildOrThrow();
    }

    private static List<PlanNodeStats> getPlanNodeStats(TaskStats taskStats)
    {
        // Best effort to reconstruct the plan nodes from operators.
        // Because stats are collected separately from query execution,
        // it's possible that some or all of them are missing or out of date.
        // For example, a LIMIT clause can cause a query to finish before stats
        // are collected from the leaf stages.
        Set<PlanNodeId> planNodeIds = new HashSet<>();

        Map<PlanNodeId, Long> planNodeInputPositions = new HashMap<>();
        Map<PlanNodeId, Long> planNodeInputBytes = new HashMap<>();
        Map<PlanNodeId, Long> planNodeOutputPositions = new HashMap<>();
        Map<PlanNodeId, Long> planNodeOutputBytes = new HashMap<>();
        Map<PlanNodeId, Long> planNodeSpilledDataSize = new HashMap<>();
        Map<PlanNodeId, Long> planNodeScheduledMillis = new HashMap<>();
        Map<PlanNodeId, Long> planNodeCpuMillis = new HashMap<>();
        Map<PlanNodeId, Long> planNodePhysicalInputDataSize = new HashMap<>();
        Map<PlanNodeId, Double> planNodePhysicalInputReadNanos = new HashMap<>();
        Map<PlanNodeId, Long> planNodeBlockedMillis = new HashMap<>();

        Map<PlanNodeId, Map<String, BasicOperatorStats>> basicOperatorStats = new HashMap<>();
        Map<PlanNodeId, WindowOperatorStats> windowNodeStats = new HashMap<>();

        for (PipelineStats pipelineStats : taskStats.getPipelines()) {
            // Due to eventual consistently collected stats, these could be empty
            if (pipelineStats.getOperatorSummaries().isEmpty()) {
                continue;
            }

            Set<PlanNodeId> processedNodes = new HashSet<>();
            PlanNodeId inputPlanNode = pipelineStats.getOperatorSummaries().iterator().next().getPlanNodeId();
            PlanNodeId outputPlanNode = getLast(pipelineStats.getOperatorSummaries()).getPlanNodeId();

            // Gather input statistics
            for (OperatorStats operatorStats : pipelineStats.getOperatorSummaries()) {
                PlanNodeId planNodeId = operatorStats.getPlanNodeId();
                planNodeIds.add(planNodeId);

                long scheduledMillis = operatorStats.getAddInputWall().toMillis() + operatorStats.getGetOutputWall().toMillis() + operatorStats.getFinishWall().toMillis();
                planNodeScheduledMillis.merge(planNodeId, scheduledMillis, Long::sum);

                long cpuMillis = operatorStats.getAddInputCpu().toMillis() + operatorStats.getGetOutputCpu().toMillis() + operatorStats.getFinishCpu().toMillis();
                planNodeCpuMillis.merge(planNodeId, cpuMillis, Long::sum);

                planNodeBlockedMillis.merge(planNodeId, operatorStats.getBlockedWall().toMillis(), Long::sum);
                planNodeSpilledDataSize.merge(planNodeId, operatorStats.getSpilledDataSize().toBytes(), Long::sum);
                planNodePhysicalInputDataSize.merge(planNodeId, operatorStats.getPhysicalInputDataSize().toBytes(), Long::sum);
                planNodePhysicalInputReadNanos.merge(planNodeId, operatorStats.getPhysicalInputReadTime().getValue(NANOSECONDS), Double::sum);
                // A plan node like LocalExchange consists of LocalExchangeSource which links to another pipeline containing LocalExchangeSink
                if (operatorStats.getPlanNodeId().equals(inputPlanNode) && !pipelineStats.isInputPipeline()) {
                    continue;
                }
                // Skip DynamicFilterSourceOperator as input operator as for join build side HashBuilderOperator metrics
                // should be reported
                if (operatorStats.getOperatorType().equals("DynamicFilterSourceOperator")) {
                    continue;
                }
                if (processedNodes.contains(planNodeId)) {
                    continue;
                }

                basicOperatorStats.merge(planNodeId,
                        ImmutableMap.of(
                                operatorStats.getOperatorType(),
                                new BasicOperatorStats(
                                        operatorStats.getTotalDrivers(),
                                        operatorStats.getInputPositions(),
                                        operatorStats.getSumSquaredInputPositions(),
                                        operatorStats.getMetrics(),
                                        operatorStats.getConnectorMetrics())),
                        (map1, map2) -> mergeMaps(map1, map2, BasicOperatorStats::merge));

                planNodeInputPositions.merge(planNodeId, operatorStats.getInputPositions(), Long::sum);
                planNodeInputBytes.merge(planNodeId, operatorStats.getInputDataSize().toBytes(), Long::sum);
                processedNodes.add(planNodeId);
            }

            // Gather output statistics
            processedNodes.clear();
            for (OperatorStats operatorStats : pipelineStats.getOperatorSummaries().reversed()) {
                PlanNodeId planNodeId = operatorStats.getPlanNodeId();

                // An "internal" pipeline like a hash build, links to another pipeline which is the actual output for this plan node
                if (operatorStats.getPlanNodeId().equals(outputPlanNode) && !pipelineStats.isOutputPipeline()) {
                    continue;
                }
                if (processedNodes.contains(planNodeId)) {
                    continue;
                }

                planNodeOutputPositions.merge(planNodeId, operatorStats.getOutputPositions(), Long::sum);
                planNodeOutputBytes.merge(planNodeId, operatorStats.getOutputDataSize().toBytes(), Long::sum);
                processedNodes.add(planNodeId);
            }

            // Gather auxiliary statistics
            for (OperatorStats operatorStats : pipelineStats.getOperatorSummaries()) {
                PlanNodeId planNodeId = operatorStats.getPlanNodeId();

                // The only statistics we have for Window Functions are very low level, thus displayed only in VERBOSE mode
                if (operatorStats.getInfo() instanceof WindowInfo windowInfo) {
                    windowNodeStats.merge(planNodeId, WindowOperatorStats.create(windowInfo), WindowOperatorStats::mergeWith);
                }
            }
        }

        List<PlanNodeStats> stats = new ArrayList<>();
        for (PlanNodeId planNodeId : planNodeIds) {
            if (!planNodeInputPositions.containsKey(planNodeId)) {
                continue;
            }

            PlanNodeStats nodeStats;

            // It's possible there will be no output stats because all the pipelines that we observed were non-output.
            // For example in a query like SELECT * FROM a JOIN b ON c = d LIMIT 1
            // It's possible to observe stats after the build starts, but before the probe does
            // and therefore only have scheduled time, but no output stats
            long outputPositions = planNodeOutputPositions.getOrDefault(planNodeId, 0L);

            if (windowNodeStats.containsKey(planNodeId)) {
                nodeStats = new WindowPlanNodeStats(
                        planNodeId,
                        new Duration(planNodeScheduledMillis.get(planNodeId), MILLISECONDS),
                        new Duration(planNodeCpuMillis.get(planNodeId), MILLISECONDS),
                        new Duration(planNodeBlockedMillis.get(planNodeId), MILLISECONDS),
                        planNodeInputPositions.get(planNodeId),
                        succinctBytes(planNodeInputBytes.get(planNodeId)),
                        outputPositions,
                        succinctBytes(planNodeOutputBytes.getOrDefault(planNodeId, 0L)),
                        succinctBytes(planNodeSpilledDataSize.get(planNodeId)),
                        basicOperatorStats.get(planNodeId),
                        windowNodeStats.get(planNodeId));
            }
            else {
                nodeStats = new PlanNodeStats(
                        planNodeId,
                        new Duration(planNodeScheduledMillis.get(planNodeId), MILLISECONDS),
                        new Duration(planNodeCpuMillis.get(planNodeId), MILLISECONDS),
                        new Duration(planNodeBlockedMillis.get(planNodeId), MILLISECONDS),
                        planNodeInputPositions.get(planNodeId),
                        succinctBytes(planNodeInputBytes.get(planNodeId)),
                        succinctBytes(planNodePhysicalInputDataSize.getOrDefault(planNodeId, 0L)),
                        new Duration(planNodePhysicalInputReadNanos.getOrDefault(planNodeId, 0.0), NANOSECONDS),
                        outputPositions,
                        succinctBytes(planNodeOutputBytes.getOrDefault(planNodeId, 0L)),
                        succinctBytes(planNodeSpilledDataSize.get(planNodeId)),
                        basicOperatorStats.get(planNodeId));
            }

            stats.add(nodeStats);
        }
        return stats;
    }
}
