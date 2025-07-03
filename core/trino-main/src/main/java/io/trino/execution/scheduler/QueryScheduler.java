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
package io.trino.execution.scheduler;

import io.airlift.units.Duration;
import io.trino.execution.BasicStageStats;
import io.trino.execution.BasicStagesInfo;
import io.trino.execution.StageId;
import io.trino.execution.StagesInfo;
import io.trino.execution.TaskId;

public interface QueryScheduler
{
    void start();

    void cancelStage(StageId stageId);

    void failTask(TaskId taskId, Throwable failureCause);

    BasicStageStats getBasicStageStats();

    BasicStagesInfo getBasicStagesInfo();

    StagesInfo getStagesInfo();

    long getUserMemoryReservation();

    long getTotalMemoryReservation();

    Duration getTotalCpuTime();
}
