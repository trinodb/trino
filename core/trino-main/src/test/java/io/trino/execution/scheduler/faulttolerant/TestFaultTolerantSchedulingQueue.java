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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static io.trino.execution.scheduler.faulttolerant.TaskExecutionClass.EAGER_SPECULATIVE;
import static io.trino.execution.scheduler.faulttolerant.TaskExecutionClass.SPECULATIVE;
import static io.trino.execution.scheduler.faulttolerant.TaskExecutionClass.STANDARD;
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

    @Test
    void testLoadDescriptorsBelowQueueLimit()
    {
        SchedulingQueue queue = new SchedulingQueue();
        assertThat(queue.shouldLoadMoreTaskDescriptors(2, 2)).isTrue();
        queue.addOrUpdate(PrioritizedScheduledTask.create(BUILD_STAGE, 0, 1));
        assertThat(queue.shouldLoadMoreTaskDescriptors(2, 2)).isTrue();
        queue.addOrUpdate(PrioritizedScheduledTask.create(BUILD_STAGE, 1, 1));
        assertThat(queue.shouldLoadMoreTaskDescriptors(2, 2)).isFalse();
    }

    @Test
    void testBuildDescriptorsAreNotBlockedByFullProbeQueue()
    {
        SchedulingQueue queue = new SchedulingQueue();
        for (int partition = 0; partition < 5; partition++) {
            queue.addOrUpdate(PrioritizedScheduledTask.create(PROBE_STAGE, partition, 2));
        }
        assertThat(queue.shouldLoadMoreTaskDescriptors(2, 2)).isFalse();

        assertThat(queue.shouldLoadMoreTaskDescriptors(1, 2)).isTrue();

        PrioritizedScheduledTask buildTask = PrioritizedScheduledTask.create(BUILD_STAGE, 0, 1);
        queue.addOrUpdate(buildTask);
        assertThat(queue.shouldLoadMoreTaskDescriptors(1, 2)).isFalse();
        assertThat(queue.shouldLoadMoreTaskDescriptors(2, 2)).isFalse();
        assertThat(queue.pollOrThrow(STANDARD)).isEqualTo(buildTask);

        assertThat(queue.shouldLoadMoreTaskDescriptors(1, 2)).isTrue();
        assertThat(queue.shouldLoadMoreTaskDescriptors(2, 2)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSpeculativeTasksDoNotFillStandardQueue(boolean eager)
    {
        SchedulingQueue queue = new SchedulingQueue();
        PrioritizedScheduledTask speculativeTask = PrioritizedScheduledTask.createSpeculative(BUILD_STAGE, 0, 1, eager);
        queue.addOrUpdate(speculativeTask);
        assertThat(queue.shouldLoadMoreTaskDescriptors(2, 1)).isTrue();
        assertThat(queue.pollOrThrow(eager ? EAGER_SPECULATIVE : SPECULATIVE)).isEqualTo(speculativeTask);
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
