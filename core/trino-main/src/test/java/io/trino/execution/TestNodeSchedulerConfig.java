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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.execution.scheduler.NodeSchedulerConfig.NodeSchedulerPolicy.UNIFORM;
import static io.trino.execution.scheduler.NodeSchedulerConfig.SplitsBalancingPolicy.NODE;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestNodeSchedulerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(NodeSchedulerConfig.class)
                .setNodeSchedulerPolicy(UNIFORM.name())
                .setMinCandidates(10)
                .setMaxSplitsPerNode(100)
                .setMinPendingSplitsPerTask(10)
                .setMaxAdjustedPendingSplitsWeightPerTask(2000)
                .setMaxUnacknowledgedSplitsPerTask(2000)
                .setIncludeCoordinator(true)
                .setSplitsBalancingPolicy(NodeSchedulerConfig.SplitsBalancingPolicy.STAGE)
                .setOptimizedLocalScheduling(true)
                .setAllowedNoMatchingNodePeriod(new Duration(2, MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("node-scheduler.policy", "topology")
                .put("node-scheduler.min-candidates", "11")
                .put("node-scheduler.include-coordinator", "false")
                .put("node-scheduler.min-pending-splits-per-task", "11")
                .put("node-scheduler.max-adjusted-pending-splits-per-task", "33")
                .put("node-scheduler.max-splits-per-node", "101")
                .put("node-scheduler.max-unacknowledged-splits-per-task", "501")
                .put("node-scheduler.splits-balancing-policy", "node")
                .put("node-scheduler.optimized-local-scheduling", "false")
                .put("node-scheduler.allowed-no-matching-node-period", "1m")
                .buildOrThrow();

        NodeSchedulerConfig expected = new NodeSchedulerConfig()
                .setNodeSchedulerPolicy("topology")
                .setIncludeCoordinator(false)
                .setMaxSplitsPerNode(101)
                .setMinPendingSplitsPerTask(11)
                .setMaxAdjustedPendingSplitsWeightPerTask(33)
                .setMaxUnacknowledgedSplitsPerTask(501)
                .setMinCandidates(11)
                .setSplitsBalancingPolicy(NODE)
                .setOptimizedLocalScheduling(false)
                .setAllowedNoMatchingNodePeriod(new Duration(1, MINUTES));

        assertFullMapping(properties, expected);
    }
}
