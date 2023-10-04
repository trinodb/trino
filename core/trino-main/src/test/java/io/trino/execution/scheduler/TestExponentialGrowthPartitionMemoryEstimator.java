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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.scheduler.PartitionMemoryEstimator.MemoryRequirements;
import io.trino.memory.MemoryInfo;
import io.trino.metadata.InternalNode;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.memory.MemoryPoolInfo;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Optional;
import java.util.function.Function;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_COORDINATOR_TASK_MEMORY;
import static io.trino.SystemSessionProperties.FAULT_TOLERANT_EXECUTION_TASK_MEMORY;
import static io.trino.spi.StandardErrorCode.ADMINISTRATIVELY_PREEMPTED;
import static io.trino.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExponentialGrowthPartitionMemoryEstimator
{
    private static final Function<PlanFragmentId, PlanFragment> THROWING_PLAN_FRAGMENT_LOOKUP = planFragmentId -> {
        throw new RuntimeException("should not be used");
    };

    @Test
    public void testDefaultInitialEstimation()
    {
        ExponentialGrowthPartitionMemoryEstimator.Factory estimatorFactory = new ExponentialGrowthPartitionMemoryEstimator.Factory(
                () -> ImmutableMap.of(new InternalNode("a-node", URI.create("local://blah"), NodeVersion.UNKNOWN, false).getNodeIdentifier(), Optional.of(buildWorkerMemoryInfo(DataSize.ofBytes(0)))),
                true);
        estimatorFactory.refreshNodePoolMemoryInfos();

        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty(FAULT_TOLERANT_EXECUTION_COORDINATOR_TASK_MEMORY, "107MB")
                .setSystemProperty(FAULT_TOLERANT_EXECUTION_TASK_MEMORY, "113MB")
                .build();

        assertThat(estimatorFactory.createPartitionMemoryEstimator(session, getPlanFragment(COORDINATOR_DISTRIBUTION), THROWING_PLAN_FRAGMENT_LOOKUP).getInitialMemoryRequirements())
                .isEqualTo(new MemoryRequirements(DataSize.of(107, MEGABYTE)));

        assertThat(estimatorFactory.createPartitionMemoryEstimator(session, getPlanFragment(SINGLE_DISTRIBUTION), THROWING_PLAN_FRAGMENT_LOOKUP).getInitialMemoryRequirements())
                .isEqualTo(new MemoryRequirements(DataSize.of(113, MEGABYTE)));
    }

    @Test
    public void testEstimator()
    {
        ExponentialGrowthPartitionMemoryEstimator.Factory estimatorFactory = new ExponentialGrowthPartitionMemoryEstimator.Factory(
                () -> ImmutableMap.of(new InternalNode("a-node", URI.create("local://blah"), NodeVersion.UNKNOWN, false).getNodeIdentifier(), Optional.of(buildWorkerMemoryInfo(DataSize.ofBytes(0)))),
                true);
        estimatorFactory.refreshNodePoolMemoryInfos();

        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty(FAULT_TOLERANT_EXECUTION_TASK_MEMORY, "107MB")
                .build();

        PartitionMemoryEstimator estimator = estimatorFactory.createPartitionMemoryEstimator(session, getPlanFragment(SINGLE_DISTRIBUTION), THROWING_PLAN_FRAGMENT_LOOKUP);

        assertThat(estimator.getInitialMemoryRequirements())
                .isEqualTo(new MemoryRequirements(DataSize.of(107, MEGABYTE)));

        // peak memory of failed task 10MB
        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        new MemoryRequirements(DataSize.of(50, MEGABYTE)),
                        DataSize.of(10, MEGABYTE),
                        StandardErrorCode.CORRUPT_PAGE.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(50, MEGABYTE)));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        new MemoryRequirements(DataSize.of(50, MEGABYTE)),
                        DataSize.of(10, MEGABYTE),
                        StandardErrorCode.CLUSTER_OUT_OF_MEMORY.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(150, MEGABYTE)));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        new MemoryRequirements(DataSize.of(50, MEGABYTE)),
                        DataSize.of(10, MEGABYTE),
                        StandardErrorCode.TOO_MANY_REQUESTS_FAILED.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(150, MEGABYTE)));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        new MemoryRequirements(DataSize.of(50, MEGABYTE)),
                        DataSize.of(10, MEGABYTE),
                        EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(150, MEGABYTE)));

        // peak memory of failed task 70MB
        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        new MemoryRequirements(DataSize.of(50, MEGABYTE)),
                        DataSize.of(70, MEGABYTE),
                        StandardErrorCode.CORRUPT_PAGE.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(70, MEGABYTE)));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        new MemoryRequirements(DataSize.of(50, MEGABYTE)),
                        DataSize.of(70, MEGABYTE),
                        EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(210, MEGABYTE)));

        // register a couple successful attempts; 90th percentile is at 300MB
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(1000, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(100, MEGABYTE), true, Optional.empty());

        // for initial we should pick estimate if greater than default
        assertThat(estimator.getInitialMemoryRequirements()) // DataSize.of(100, MEGABYTE)
                .isEqualTo(new MemoryRequirements(DataSize.of(300, MEGABYTE)));

        // for next we should still pick current initial if greater
        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        new MemoryRequirements(DataSize.of(50, MEGABYTE)),
                        DataSize.of(70, MEGABYTE),
                        EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(300, MEGABYTE)));

        // a couple oom errors are registered
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(200, MEGABYTE), false, Optional.of(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()));
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(200, MEGABYTE), true, Optional.of(CLUSTER_OUT_OF_MEMORY.toErrorCode()));

        // 90th percentile should be now at 200*3 (600)
        assertThat(estimator.getInitialMemoryRequirements()) // DataSize.of(100, MEGABYTE)
                .isEqualTo(new MemoryRequirements(DataSize.of(600, MEGABYTE)));

        // a couple oom errors are registered with requested memory greater than peak
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(300, MEGABYTE)), DataSize.of(200, MEGABYTE), false, Optional.of(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()));
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(300, MEGABYTE)), DataSize.of(200, MEGABYTE), false, Optional.of(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()));
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(300, MEGABYTE)), DataSize.of(200, MEGABYTE), true, Optional.of(CLUSTER_OUT_OF_MEMORY.toErrorCode()));

        // 90th percentile should be now at 300*3 (900)
        assertThat(estimator.getInitialMemoryRequirements()) // DataSize.of(100, MEGABYTE)
                .isEqualTo(new MemoryRequirements(DataSize.of(900, MEGABYTE)));

        // other errors should not change estimate
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));
        estimator.registerPartitionFinished(new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));

        assertThat(estimator.getInitialMemoryRequirements()) // DataSize.of(100, MEGABYTE)
                .isEqualTo(new MemoryRequirements(DataSize.of(900, MEGABYTE)));
    }

    @Test
    public void testDefaultInitialEstimationPickedIfLarge()
    {
        ExponentialGrowthPartitionMemoryEstimator.Factory estimatorFactory = new ExponentialGrowthPartitionMemoryEstimator.Factory(
                () -> ImmutableMap.of(new InternalNode("a-node", URI.create("local://blah"), NodeVersion.UNKNOWN, false).getNodeIdentifier(), Optional.of(buildWorkerMemoryInfo(DataSize.ofBytes(0)))),
                true);
        estimatorFactory.refreshNodePoolMemoryInfos();

        testInitialEstimationWithFinishedPartitions(estimatorFactory, DataSize.of(300, MEGABYTE), 10, DataSize.of(500, MEGABYTE), DataSize.of(500, MEGABYTE));
        testInitialEstimationWithFinishedPartitions(estimatorFactory, DataSize.of(300, MEGABYTE), 10, DataSize.of(100, MEGABYTE), DataSize.of(300, MEGABYTE));
    }

    private static void testInitialEstimationWithFinishedPartitions(
            ExponentialGrowthPartitionMemoryEstimator.Factory estimatorFactory,
            DataSize recordedMemoryUsage,
            int recordedPartitionsCount,
            DataSize defaultInitialTaskMemory,
            DataSize expectedEstimation)
    {
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty(FAULT_TOLERANT_EXECUTION_TASK_MEMORY, defaultInitialTaskMemory.toString())
                .build();

        PartitionMemoryEstimator estimator = estimatorFactory.createPartitionMemoryEstimator(session, getPlanFragment(SINGLE_DISTRIBUTION), THROWING_PLAN_FRAGMENT_LOOKUP);

        for (int i = 0; i < recordedPartitionsCount; i++) {
            estimator.registerPartitionFinished(new MemoryRequirements(recordedMemoryUsage), recordedMemoryUsage, true, Optional.empty());
        }
        assertThat(estimator.getInitialMemoryRequirements())
                .isEqualTo(new MemoryRequirements(expectedEstimation));
    }

    private static PlanFragment getPlanFragment(PartitioningHandle partitioningHandle)
    {
        return new PlanFragment(
                new PlanFragmentId("exchange_fragment_id"),
                new ValuesNode(new PlanNodeId("values"), 1),
                ImmutableMap.of(),
                partitioningHandle,
                Optional.empty(),
                ImmutableList.of(),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of()),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                Optional.empty());
    }

    private MemoryInfo buildWorkerMemoryInfo(DataSize usedMemory)
    {
        return new MemoryInfo(
                4,
                new MemoryPoolInfo(
                        DataSize.of(64, GIGABYTE).toBytes(),
                        usedMemory.toBytes(),
                        0,
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        ImmutableMap.of()));
    }
}
