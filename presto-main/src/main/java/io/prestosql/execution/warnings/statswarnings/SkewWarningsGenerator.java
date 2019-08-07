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

package io.prestosql.execution.warnings.statswarnings;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.StageInfo;
import io.prestosql.execution.TaskInfo;
import io.prestosql.spi.PrestoWarning;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.prestosql.spi.connector.StandardWarningCode.STAGE_SKEW;

public class SkewWarningsGenerator
        implements ExecutionStatisticsWarningsGenerator
{
    @Override
    public List<PrestoWarning> generateStatsWarnings(QueryInfo queryInfo, Session session)
    {
        List<String> skewMessages = new ArrayList<>();
        visitStage(queryInfo, skewMessages);

        ImmutableList.Builder<PrestoWarning> allWarnings = new ImmutableList.Builder<>();

        if (!skewMessages.isEmpty()) {
            allWarnings.add(new PrestoWarning(STAGE_SKEW, String.join(",", skewMessages)));
        }
        return allWarnings.build();
    }

    private void visitStage(QueryInfo queryInfo, List<String> skewMessages)
    {
        if (queryInfo.getOutputStage().isPresent()) {
            visitStageHelper(queryInfo.getOutputStage().get(), skewMessages);
        }
    }

    private void visitStageHelper(StageInfo stage, List<String> skewMessages)
    {
        String stageId = Integer.toString(stage.getStageId().getId());
        List<String> skewWarnings = generateSkewWarning(stage.getTasks());
        if (!skewWarnings.isEmpty()) {
            skewMessages.add(String.format("StageId: %s has these skews: %s", stageId, skewWarnings));
        }

        for (StageInfo stageInfo : stage.getSubStages()) {
            visitStageHelper(stageInfo, skewMessages);
        }
    }

    private List<String> generateSkewWarning(List<TaskInfo> taskInfos)
    {
        List<Double> cpuValues = taskInfos
                .stream()
                .map(taskInfo -> taskInfo.getStats().getTotalCpuTime().getValue())
                .collect(Collectors.toList());

        double squaredSum = cpuValues
                .stream().map(val -> Math.pow(val, 2))
                .mapToDouble(Double::doubleValue)
                .sum();

        double cpuMean = (cpuValues
                .stream()
                .mapToDouble(Double::doubleValue)
                .sum() / cpuValues.size());

        double cpuSTD = Math.sqrt(squaredSum / taskInfos.size() - Math.pow(cpuMean, 2));

        return taskInfos
            .stream()
            .filter(taskInfo -> (taskInfo.getStats().getTotalCpuTime().getValue() - cpuMean) / cpuSTD >= 2)
            .map(taskInfo -> String.format("Task Id: %s CPU skew: %s", taskInfo.getTaskStatus().getTaskId(),
                    taskInfo.getStats().getTotalCpuTime().getValue()))
            .collect(ImmutableList.toImmutableList());
    }
}
