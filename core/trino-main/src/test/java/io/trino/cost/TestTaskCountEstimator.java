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
package io.trino.cost;

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.node.InternalNode;
import io.trino.node.TestingInternalNodeManager;
import io.trino.spi.NodeVersion;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

final class TestTaskCountEstimator
{
    private static final InternalNode ETL_NODE_1 = new InternalNode("etl1", URI.create("http://10.0.0.1:21"), NodeVersion.UNKNOWN, false, ImmutableSet.of("etl", "shared"));
    private static final InternalNode ETL_NODE_2 = new InternalNode("etl2", URI.create("http://10.0.0.1:22"), NodeVersion.UNKNOWN, false, ImmutableSet.of("etl", "shared"));
    private static final InternalNode ADHOC_NODE = new InternalNode("adhoc1", URI.create("http://10.0.0.1:23"), NodeVersion.UNKNOWN, false, ImmutableSet.of("adhoc", "shared"));
    private static final InternalNode UNGROUPED_NODE = new InternalNode("plain1", URI.create("http://10.0.0.1:24"), NodeVersion.UNKNOWN, false);

    @Test
    void testTaskCountIsScopedToTheNodeGroup()
    {
        TaskCountEstimator estimator = new TaskCountEstimator(
                new NodeSchedulerConfig().setIncludeCoordinator(false),
                TestingInternalNodeManager.createDefault(ETL_NODE_1, ETL_NODE_2, ADHOC_NODE, UNGROUPED_NODE));

        assertThat(estimator.estimateSourceDistributedTaskCount(sessionWithNodeGroup("etl"))).isEqualTo(2);
        assertThat(estimator.estimateSourceDistributedTaskCount(sessionWithNodeGroup("adhoc"))).isEqualTo(1);
        assertThat(estimator.estimateSourceDistributedTaskCount(sessionWithNodeGroup("shared"))).isEqualTo(3);
        assertThat(estimator.estimateSourceDistributedTaskCount(TestingSession.testSessionBuilder().build())).isEqualTo(4);
    }

    @Test
    void testEmptyNodeGroupStillEstimatesAtLeastOneTask()
    {
        TaskCountEstimator estimator = new TaskCountEstimator(
                new NodeSchedulerConfig().setIncludeCoordinator(false),
                TestingInternalNodeManager.createDefault(ADHOC_NODE));

        // the floor prevents underflow elsewhere in costing; the query fails later on node selection
        assertThat(estimator.estimateSourceDistributedTaskCount(sessionWithNodeGroup("etl"))).isEqualTo(1);
    }

    private static Session sessionWithNodeGroup(String nodeGroup)
    {
        return TestingSession.testSessionBuilder()
                .setNodeGroup(Optional.of(nodeGroup))
                .build();
    }
}
