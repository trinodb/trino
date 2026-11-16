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
package io.trino.execution.scheduler.faulttolerant;

import com.google.common.collect.ImmutableMap;
import io.trino.execution.StageId;
import io.trino.execution.scheduler.faulttolerant.EventDrivenFaultTolerantQueryScheduler.PrioritizedScheduledTask;
import io.trino.execution.scheduler.faulttolerant.EventDrivenFaultTolerantQueryScheduler.SchedulingQueue;
import io.trino.spi.QueryId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

class TestFaultTolerantSchedulingQueue
{
    private static final StageId BUILD_SOURCE_STAGE = new StageId(new QueryId("query"), 0);
    private static final StageId BUILD_STAGE = new StageId(new QueryId("query"), 1);
    private static final StageId PROBE_STAGE = new StageId(new QueryId("query"), 2);

    @ParameterizedTest
    @EnumSource(TaskExecutionClass.class)
    void testPrioritiesFollowReorderedPlan(TaskExecutionClass executionClass)
    {
        SchedulingQueue queue = new SchedulingQueue();
        queue.addOrUpdate(task(BUILD_SOURCE_STAGE, 0, executionClass, 0));
        queue.addOrUpdate(task(BUILD_STAGE, 1, executionClass, 1));
        queue.addOrUpdate(task(PROBE_STAGE, 0, executionClass, 2));
        queue.addOrUpdate(task(BUILD_STAGE, 0, executionClass, 1));

        queue.updatePriorities(ImmutableMap.of(PROBE_STAGE, 0, BUILD_STAGE, 1));
        assertThat(queue.pollOrThrow(executionClass)).isEqualTo(task(PROBE_STAGE, 0, executionClass, 0));
        assertThat(queue.pollOrThrow(executionClass)).isEqualTo(task(BUILD_STAGE, 1, executionClass, 1));
        assertThat(queue.pollOrThrow(executionClass)).isEqualTo(task(BUILD_STAGE, 0, executionClass, 1));
        assertThat(queue.isEmpty(executionClass)).isTrue();
    }

    private static PrioritizedScheduledTask task(StageId stageId, int partitionId, TaskExecutionClass executionClass, int priority)
    {
        return switch (executionClass) {
            case STANDARD -> PrioritizedScheduledTask.create(stageId, partitionId, priority);
            case SPECULATIVE -> PrioritizedScheduledTask.createSpeculative(stageId, partitionId, priority, false);
            case EAGER_SPECULATIVE -> PrioritizedScheduledTask.createSpeculative(stageId, partitionId, priority, true);
        };
    }
}
