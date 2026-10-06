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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.OptionalDouble;

import static io.trino.execution.BasicStageStats.aggregateBasicStageStats;
import static io.trino.execution.BasicStageStats.aggregateFaultTolerantBasicStageStats;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

public class TestBasicStageStats
{
    @Test
    public void testUnscheduledStageHidesPipelinedProgress()
    {
        BasicStageStats aggregate = aggregateBasicStageStats(ImmutableList.of(stage(true, 10, 10, 0), stage(false, 0, 0, 0)));

        assertThat(aggregate.isScheduled()).isFalse();
        assertThat(aggregate.getProgressPercentage()).isEmpty();
        assertThat(aggregate.getRunningPercentage()).isEmpty();
    }

    /**
     * A fault tolerant stage is PLANNED from when it is created until its first task reports; it must not blank the
     * progress of the query meanwhile.
     */
    @Test
    public void testUnscheduledStageDoesNotHideFaultTolerantProgress()
    {
        BasicStageStats aggregate = aggregateFaultTolerantBasicStageStats(ImmutableList.of(stage(true, 10, 10, 0), stage(false, 0, 0, 0)), 2);

        assertThat(aggregate.isScheduled()).isTrue();
        assertThat(aggregate.getProgressPercentage()).hasValue(50.0);
        assertThat(aggregate.getRunningPercentage()).hasValue(0.0);
    }

    @Test
    public void testFaultTolerantStagesNotCreatedYetCountAsNoProgress()
    {
        BasicStageStats aggregate = aggregateFaultTolerantBasicStageStats(ImmutableList.of(stage(true, 10, 10, 0)), 4);

        assertThat(aggregate.getProgressPercentage()).hasValue(25.0);
    }

    @Test
    public void testFaultTolerantStagesWeighTheSame()
    {
        BasicStageStats aggregate = aggregateFaultTolerantBasicStageStats(ImmutableList.of(stage(true, 100, 50, 10), stage(true, 2, 2, 0)), 2);

        assertThat(aggregate.getCompletedDrivers()).isEqualTo(52);
        assertThat(aggregate.getTotalDrivers()).isEqualTo(102);
        assertThat(aggregate.getProgressPercentage().orElseThrow()).isCloseTo(75.0, within(1e-9));
        assertThat(aggregate.getRunningPercentage().orElseThrow()).isCloseTo(5.0, within(1e-9));
    }

    @Test
    public void testFaultTolerantProgressWithoutScheduledStage()
    {
        BasicStageStats aggregate = aggregateFaultTolerantBasicStageStats(ImmutableList.of(stage(false, 0, 0, 0)), 3);

        assertThat(aggregate.isScheduled()).isFalse();
        assertThat(aggregate.getProgressPercentage()).isEmpty();
        assertThat(aggregate.getRunningPercentage()).isEmpty();
    }

    @Test
    public void testFaultTolerantProgressWithoutStages()
    {
        BasicStageStats aggregate = aggregateFaultTolerantBasicStageStats(ImmutableList.of(), 3);

        assertThat(aggregate.isScheduled()).isFalse();
        assertThat(aggregate.getProgressPercentage()).isEmpty();
    }

    /**
     * The registry may hold stages the current plan no longer references; progress must stay within 100%.
     */
    @Test
    public void testFaultTolerantProgressWithMoreStagesThanPlan()
    {
        BasicStageStats aggregate = aggregateFaultTolerantBasicStageStats(ImmutableList.of(stage(true, 10, 10, 0), stage(true, 10, 10, 0)), 1);

        assertThat(aggregate.getProgressPercentage()).hasValue(100.0);
    }

    private static BasicStageStats stage(boolean scheduled, int totalDrivers, int completedDrivers, int runningDrivers)
    {
        return new BasicStageStats(
                scheduled,

                0,

                totalDrivers,
                totalDrivers - completedDrivers - runningDrivers,
                runningDrivers,
                completedDrivers,
                0,

                DataSize.ofBytes(0),
                0,
                new Duration(0, SECONDS),
                DataSize.ofBytes(0),

                DataSize.ofBytes(0),
                0,

                0,

                DataSize.ofBytes(0),

                0,
                0,
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),

                new Duration(0, SECONDS),
                new Duration(0, SECONDS),
                new Duration(0, SECONDS),
                new Duration(0, SECONDS),

                false,
                ImmutableSet.of(),

                OptionalDouble.empty(),
                OptionalDouble.empty());
    }
}
