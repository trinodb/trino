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
package io.trino.execution.executor.scheduler;

import io.trino.execution.executor.ExecutionPriority;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestLowPrioritySchedulingGroup
{
    @Test
    public void testLowPriorityWeights()
    {
        long weightMultiplier = 10;
        SchedulingGroup<String> lowPriorityGroup = new SchedulingGroup<>(new ExecutionPriority(weightMultiplier));
        assertThat(lowPriorityGroup.weight()).isEqualTo(0);

        lowPriorityGroup.enqueue("1", 0);
        assertThat(lowPriorityGroup.weight()).isEqualTo(0);

        assertThat(lowPriorityGroup.dequeue(100)).isEqualTo("1");
        assertThat(lowPriorityGroup.weight()).isEqualTo(weightMultiplier * 100);
        assertThat(lowPriorityGroup.baselineWeight()).isEqualTo(weightMultiplier * 100);

        lowPriorityGroup.enqueue("1", 50);
        assertThat(lowPriorityGroup.weight()).isEqualTo(weightMultiplier * 50);

        lowPriorityGroup.enqueue("2", 0);
        assertThat(lowPriorityGroup.weight()).isEqualTo(weightMultiplier * 50);

        lowPriorityGroup.dequeue(100);
        assertThat(lowPriorityGroup.weight()).isEqualTo(weightMultiplier * 150);

        lowPriorityGroup.dequeue(100);
        assertThat(lowPriorityGroup.weight()).isEqualTo(weightMultiplier * 250);

        lowPriorityGroup.block("1", 100);
        assertThat(lowPriorityGroup.weight()).isEqualTo(weightMultiplier * 350);

        lowPriorityGroup.addWeight(1000);
        assertThat(lowPriorityGroup.weight()).isEqualTo(weightMultiplier * 1350);
    }
}
