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
import io.trino.spi.StandardErrorCode;
import io.trino.testing.TestingSession;
import org.testng.annotations.Test;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
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
                .isEqualTo(new PartitionMemoryEstimator.MemoryRequirements(DataSize.of(107, MEGABYTE), false));

        // peak memory of failed task 10MB
        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new PartitionMemoryEstimator.MemoryRequirements(DataSize.of(50, MEGABYTE), false),
                        DataSize.of(10, MEGABYTE),
                        StandardErrorCode.CORRUPT_PAGE.toErrorCode()))
                .isEqualTo(new PartitionMemoryEstimator.MemoryRequirements(DataSize.of(50, MEGABYTE), false));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new PartitionMemoryEstimator.MemoryRequirements(DataSize.of(50, MEGABYTE), false),
                        DataSize.of(10, MEGABYTE),
                        StandardErrorCode.CLUSTER_OUT_OF_MEMORY.toErrorCode()))
                .isEqualTo(new PartitionMemoryEstimator.MemoryRequirements(DataSize.of(150, MEGABYTE), false));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new PartitionMemoryEstimator.MemoryRequirements(DataSize.of(50, MEGABYTE), false),
                        DataSize.of(10, MEGABYTE),
                        StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()))
                .isEqualTo(new PartitionMemoryEstimator.MemoryRequirements(DataSize.of(150, MEGABYTE), false));

        // peak memory of failed task 70MB
        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new PartitionMemoryEstimator.MemoryRequirements(DataSize.of(50, MEGABYTE), false),
                        DataSize.of(70, MEGABYTE),
                        StandardErrorCode.CORRUPT_PAGE.toErrorCode()))
                .isEqualTo(new PartitionMemoryEstimator.MemoryRequirements(DataSize.of(70, MEGABYTE), false));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new PartitionMemoryEstimator.MemoryRequirements(DataSize.of(50, MEGABYTE), false),
                        DataSize.of(70, MEGABYTE),
                        StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()))
                .isEqualTo(new PartitionMemoryEstimator.MemoryRequirements(DataSize.of(210, MEGABYTE), false));
    }
}
