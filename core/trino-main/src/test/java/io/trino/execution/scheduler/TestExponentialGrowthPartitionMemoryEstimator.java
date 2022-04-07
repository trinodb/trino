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

import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.scheduler.PartitionMemoryEstimator.MemoryRequirements;
import io.trino.spi.StandardErrorCode;
import io.trino.testing.TestingSession;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.StandardErrorCode.ADMINISTRATIVELY_PREEMPTED;
import static io.trino.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExponentialGrowthPartitionMemoryEstimator
{
    @Test
    public void testEstimator()
            throws Exception
    {
        ExponentialGrowthPartitionMemoryEstimator estimator = new ExponentialGrowthPartitionMemoryEstimator();
        Session session = TestingSession.testSessionBuilder().build();

        assertThat(estimator.getInitialMemoryRequirements(session, DataSize.of(107, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(DataSize.of(107, MEGABYTE), false));

        // peak memory of failed task 10MB
        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(DataSize.of(50, MEGABYTE), false),
                        DataSize.of(10, MEGABYTE),
                        StandardErrorCode.CORRUPT_PAGE.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(50, MEGABYTE), false));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(DataSize.of(50, MEGABYTE), false),
                        DataSize.of(10, MEGABYTE),
                        StandardErrorCode.CLUSTER_OUT_OF_MEMORY.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(150, MEGABYTE), false));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(DataSize.of(50, MEGABYTE), false),
                        DataSize.of(10, MEGABYTE),
                        EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(150, MEGABYTE), false));

        // peak memory of failed task 70MB
        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(DataSize.of(50, MEGABYTE), false),
                        DataSize.of(70, MEGABYTE),
                        StandardErrorCode.CORRUPT_PAGE.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(70, MEGABYTE), false));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(DataSize.of(50, MEGABYTE), false),
                        DataSize.of(70, MEGABYTE),
                        EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(210, MEGABYTE), false));

        // register a couple successful attempts; 90th percentile is at 300MB
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(1000, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(100, MEGABYTE), true, Optional.empty());

        // for initial we should pick estimate if greater than default
        assertThat(estimator.getInitialMemoryRequirements(session, DataSize.of(100, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(DataSize.of(300, MEGABYTE), false));

        // if default memory requirements is greater than estimate it should be picked still
        assertThat(estimator.getInitialMemoryRequirements(session, DataSize.of(500, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(DataSize.of(500, MEGABYTE), false));

        // for next we should still pick current initial if greater
        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(DataSize.of(50, MEGABYTE), false),
                        DataSize.of(70, MEGABYTE),
                        EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(300, MEGABYTE), false));

        // a couple oom errors are registered
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(200, MEGABYTE), false, Optional.of(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(200, MEGABYTE), true, Optional.of(CLUSTER_OUT_OF_MEMORY.toErrorCode()));

        // 90th percentile should be now at 200*3 (600)
        assertThat(estimator.getInitialMemoryRequirements(session, DataSize.of(100, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(DataSize.of(600, MEGABYTE), false));

        // a couple oom errors are registered with requested memory greater than peak
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(300, MEGABYTE), false), DataSize.of(200, MEGABYTE), false, Optional.of(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(300, MEGABYTE), false), DataSize.of(200, MEGABYTE), false, Optional.of(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(300, MEGABYTE), false), DataSize.of(200, MEGABYTE), true, Optional.of(CLUSTER_OUT_OF_MEMORY.toErrorCode()));

        // 90th percentile should be now at 300*3 (900)
        assertThat(estimator.getInitialMemoryRequirements(session, DataSize.of(100, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(DataSize.of(900, MEGABYTE), false));

        // other errors should not change estimate
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE), false), DataSize.of(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));

        assertThat(estimator.getInitialMemoryRequirements(session, DataSize.of(100, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(DataSize.of(900, MEGABYTE), false));
    }
}
