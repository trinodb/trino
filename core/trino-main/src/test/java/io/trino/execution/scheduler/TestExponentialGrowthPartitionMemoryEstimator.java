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

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.execution.scheduler.PartitionMemoryEstimator.MemoryRequirements;
import io.trino.memory.MemoryInfo;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.memory.MemoryPoolInfo;
import io.trino.testing.TestingSession;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.StandardErrorCode.ADMINISTRATIVELY_PREEMPTED;
import static io.trino.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExponentialGrowthPartitionMemoryEstimator
{
    @Test
    public void testEstimator()
            throws Exception
    {
        InternalNodeManager nodeManager = new InMemoryNodeManager(new InternalNode("a-node", URI.create("local://blah"), NodeVersion.UNKNOWN, false));
        BinPackingNodeAllocatorService nodeAllocatorService = new BinPackingNodeAllocatorService(
                nodeManager,
                () -> ImmutableMap.of(new InternalNode("a-node", URI.create("local://blah"), NodeVersion.UNKNOWN, false).getNodeIdentifier(), Optional.of(buildWorkerMemoryInfo(DataSize.ofBytes(0)))),
                false,
                true,
                Duration.of(1, MINUTES),
                DataSize.ofBytes(0),
                Ticker.systemTicker());
        nodeAllocatorService.refreshNodePoolMemoryInfos();
        PartitionMemoryEstimator estimator = nodeAllocatorService.createPartitionMemoryEstimator();

        Session session = TestingSession.testSessionBuilder().build();

        assertThat(estimator.getInitialMemoryRequirements(session, DataSize.of(107, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(DataSize.of(107, MEGABYTE)));

        // peak memory of failed task 10MB
        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(DataSize.of(50, MEGABYTE)),
                        DataSize.of(10, MEGABYTE),
                        StandardErrorCode.CORRUPT_PAGE.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(50, MEGABYTE)));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(DataSize.of(50, MEGABYTE)),
                        DataSize.of(10, MEGABYTE),
                        StandardErrorCode.CLUSTER_OUT_OF_MEMORY.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(150, MEGABYTE)));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(DataSize.of(50, MEGABYTE)),
                        DataSize.of(10, MEGABYTE),
                        StandardErrorCode.TOO_MANY_REQUESTS_FAILED.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(150, MEGABYTE)));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(DataSize.of(50, MEGABYTE)),
                        DataSize.of(10, MEGABYTE),
                        EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(150, MEGABYTE)));

        // peak memory of failed task 70MB
        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(DataSize.of(50, MEGABYTE)),
                        DataSize.of(70, MEGABYTE),
                        StandardErrorCode.CORRUPT_PAGE.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(70, MEGABYTE)));

        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(DataSize.of(50, MEGABYTE)),
                        DataSize.of(70, MEGABYTE),
                        EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(210, MEGABYTE)));

        // register a couple successful attempts; 90th percentile is at 300MB
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(1000, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(300, MEGABYTE), true, Optional.empty());
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(100, MEGABYTE), true, Optional.empty());

        // for initial we should pick estimate if greater than default
        assertThat(estimator.getInitialMemoryRequirements(session, DataSize.of(100, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(DataSize.of(300, MEGABYTE)));

        // if default memory requirements is greater than estimate it should be picked still
        assertThat(estimator.getInitialMemoryRequirements(session, DataSize.of(500, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(DataSize.of(500, MEGABYTE)));

        // for next we should still pick current initial if greater
        assertThat(
                estimator.getNextRetryMemoryRequirements(
                        session,
                        new MemoryRequirements(DataSize.of(50, MEGABYTE)),
                        DataSize.of(70, MEGABYTE),
                        EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()))
                .isEqualTo(new MemoryRequirements(DataSize.of(300, MEGABYTE)));

        // a couple oom errors are registered
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(200, MEGABYTE), false, Optional.of(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(200, MEGABYTE), true, Optional.of(CLUSTER_OUT_OF_MEMORY.toErrorCode()));

        // 90th percentile should be now at 200*3 (600)
        assertThat(estimator.getInitialMemoryRequirements(session, DataSize.of(100, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(DataSize.of(600, MEGABYTE)));

        // a couple oom errors are registered with requested memory greater than peak
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(300, MEGABYTE)), DataSize.of(200, MEGABYTE), false, Optional.of(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(300, MEGABYTE)), DataSize.of(200, MEGABYTE), false, Optional.of(EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(300, MEGABYTE)), DataSize.of(200, MEGABYTE), true, Optional.of(CLUSTER_OUT_OF_MEMORY.toErrorCode()));

        // 90th percentile should be now at 300*3 (900)
        assertThat(estimator.getInitialMemoryRequirements(session, DataSize.of(100, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(DataSize.of(900, MEGABYTE)));

        // other errors should not change estimate
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));
        estimator.registerPartitionFinished(session, new MemoryRequirements(DataSize.of(100, MEGABYTE)), DataSize.of(500, MEGABYTE), false, Optional.of(ADMINISTRATIVELY_PREEMPTED.toErrorCode()));

        assertThat(estimator.getInitialMemoryRequirements(session, DataSize.of(100, MEGABYTE)))
                .isEqualTo(new MemoryRequirements(DataSize.of(900, MEGABYTE)));
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
