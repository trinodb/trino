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
import com.google.common.math.Quantiles;
import com.google.common.math.Stats;
import io.prestosql.Session;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.StageInfo;
import io.prestosql.execution.TaskInfo;
import io.prestosql.spi.PrestoWarning;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.spi.connector.StandardWarningCode.STAGE_SKEW;
import static java.lang.String.format;

public class SkewWarningsGenerator
        implements ExecutionStatisticsWarningsGenerator
{
    @Override
    public List<PrestoWarning> generateExecutionStatisticsWarnings(QueryInfo queryInfo, Session session)
    {
        ImmutableList.Builder<PrestoWarning> allWarnings = new ImmutableList.Builder<>();
        processStage(queryInfo, allWarnings);
        return allWarnings.build();
    }

    private void processStage(QueryInfo queryInfo, ImmutableList.Builder<PrestoWarning> allWarnings)
    {
        if (queryInfo.getOutputStage().isPresent()) {
            processStageHelper(queryInfo.getOutputStage().get(), allWarnings);
        }
    }

    private void processStageHelper(StageInfo stage, ImmutableList.Builder<PrestoWarning> allWarnings)
    {
        String stageId = Integer.toString(stage.getStageId().getId());
        Optional<PrestoWarning> skewWarning = generateSkewWarning(stage.getTasks(), stageId);
        if (skewWarning.isPresent()) {
            allWarnings.add(skewWarning.get());
        }

        for (StageInfo stageInfo : stage.getSubStages()) {
            processStageHelper(stageInfo, allWarnings);
        }
    }

    private Optional<PrestoWarning> generateSkewWarning(List<TaskInfo> taskInfos, String stageId)
    {
        List<Double> cpuValues = taskInfos
                .stream()
                .map(taskInfo -> taskInfo.getStats().getTotalCpuTime().getValue())
                .collect(Collectors.toList());

        double cpuMean = Stats.meanOf(cpuValues);
        double cpuMedian = Quantiles.median().compute(cpuValues);
        double cpuSTD = Stats.of(cpuValues).populationStandardDeviation();

        if (cpuSTD == 0) {
            return Optional.empty();
        }

        List<String> skewMessages = taskInfos
                .stream()
                .filter(taskInfo -> (taskInfo.getStats().getTotalCpuTime().getValue() - cpuMean) / cpuSTD >= 2)
                .map(taskInfo -> String.format("Task Id %s.%s", taskInfo.getTaskStatus().getTaskId().getStageId().getId(),
                        taskInfo.getTaskStatus().getTaskId().getId()))
                .collect(ImmutableList.toImmutableList());

        if (!skewMessages.isEmpty()) {
            return Optional.of(
                    new PrestoWarning(STAGE_SKEW,
                            format("StageId %s has these tasks' CPU times take long than others %s. Mean %.3f, Median %.3f",
                            stageId,
                            skewMessages,
                            cpuMean,
                            cpuMedian)));
        }

        return Optional.empty();
    }
}
