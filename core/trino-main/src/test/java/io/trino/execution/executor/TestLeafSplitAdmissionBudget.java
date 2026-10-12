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
package io.trino.execution.executor;

import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLeafSplitAdmissionBudget
{
    private static final TaskId TASK = new TaskId(new StageId("query", 0), 0, 0);
    private static final TaskId OTHER_TASK = new TaskId(new StageId("query", 0), 1, 0);
    private static final TaskId OTHER_QUERY_TASK = new TaskId(new StageId("other_query", 0), 0, 0);

    @Test
    public void testRefusesQueryNearItsLimit()
    {
        assertThat(new LeafSplitAdmissionBudget().fits(TASK, memory(0, 25, 100, 1000), 1, 0)).isFalse();
        assertThat(new LeafSplitAdmissionBudget().fits(TASK, memory(0, 26, 100, 1000), 1, 0)).isTrue();
    }

    @Test
    public void testQueryShareKeepsQuarterOfLimitFree()
    {
        LeafSplitAdmissionBudget budget = new LeafSplitAdmissionBudget();
        TaskMemory memory = memory(0, 100, 100, 1000);
        assertThat(budget.fits(TASK, memory, 50, 0)).isTrue();
        budget.reserve(TASK, 50);
        assertThat(budget.fits(OTHER_TASK, memory, 30, 0)).isFalse();
        assertThat(budget.fits(OTHER_TASK, memory, 25, 0)).isTrue();
    }

    @Test
    public void testPoolShareIsShared()
    {
        LeafSplitAdmissionBudget budget = new LeafSplitAdmissionBudget();
        TaskMemory memory = memory(0, 1000, 1000, 400);
        assertThat(budget.fits(TASK, memory, 60, 0)).isTrue();
        budget.reserve(TASK, 60);
        assertThat(budget.fits(OTHER_QUERY_TASK, memory, 60, 0)).isFalse();
        assertThat(budget.fits(OTHER_QUERY_TASK, memory, 40, 0)).isTrue();
    }

    @Test
    public void testChargesUnreservedBytesOfRunningSplits()
    {
        assertThat(new LeafSplitAdmissionBudget().fits(TASK, memory(10, 60, 80, 1000), 10, 4)).isTrue();
        assertThat(new LeafSplitAdmissionBudget().fits(TASK, memory(9, 60, 80, 1000), 10, 4)).isFalse();
    }

    @Test
    public void testChargesUnreservedBytesOncePerRound()
    {
        LeafSplitAdmissionBudget budget = new LeafSplitAdmissionBudget();
        TaskMemory memory = memory(10, 75, 100, 1000);
        assertThat(budget.fits(TASK, memory, 10, 4)).isTrue();
        budget.reserve(TASK, 10);
        assertThat(budget.fits(TASK, memory, 10, 5)).isTrue();
        budget.reserve(TASK, 10);
        assertThat(budget.fits(TASK, memory, 10, 6)).isFalse();
    }

    @Test
    public void testNoEstimateWithoutTaskMemory()
    {
        assertThat(new LeafSplitAdmissionBudget().admissibleEstimate(TASK, new LeafSplitMemoryEstimate(), 0, 0)).isEqualTo(0);
    }

    @Test
    public void testNoEstimateBeforeLeafDriverSample()
    {
        assertThat(new LeafSplitAdmissionBudget().admissibleEstimate(TASK, memoryEstimate(0), 0, 0)).isEqualTo(0);
    }

    @Test
    public void testEstimateOnlyWhenSplitFits()
    {
        assertThat(new LeafSplitAdmissionBudget().admissibleEstimate(TASK, memoryEstimate(10), 0, 0)).isEqualTo(10);
        assertThat(new LeafSplitAdmissionBudget().admissibleEstimate(TASK, memoryEstimate(80), 0, 0)).isEqualTo(0);
    }

    private static LeafSplitMemoryEstimate memoryEstimate(long leafDriverMaxBytes)
    {
        LeafSplitMemoryEstimate memoryEstimate = new LeafSplitMemoryEstimate();
        memoryEstimate.setTaskMemory(new TestingTaskMemory(0, leafDriverMaxBytes, 100, 100, 1000));
        return memoryEstimate;
    }

    private static TaskMemory memory(long leafPipelineBytes, long queryFreeBytes, long queryMaxBytes, long poolFreeBytes)
    {
        return new TestingTaskMemory(leafPipelineBytes, 0, queryFreeBytes, queryMaxBytes, poolFreeBytes);
    }
}
