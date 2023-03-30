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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import io.trino.execution.DynamicFilterConfig;
import io.trino.testing.AbstractTestJoinQueries;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

import static com.google.common.base.Verify.verify;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;

/**
 * @see TestJoinQueriesWithoutDynamicFiltering for tests with dynamic filtering disabled
 */
public class TestJoinQueries
        extends AbstractTestJoinQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        verify(new DynamicFilterConfig().isEnableDynamicFiltering(), "this class assumes dynamic filtering is enabled by default");
        return TpchQueryRunnerBuilder.builder().build();
    }

    @Test
    public void verifyDynamicFilteringEnabled()
    {
        assertQuery(
                "SHOW SESSION LIKE 'enable_dynamic_filtering'",
                "VALUES ('enable_dynamic_filtering', 'true', 'true', 'boolean', 'Enable dynamic filtering')");
    }

    /**
     * This test verifies if a broadcast deadlock is getting properly resolved.
     * <p>
     * A deadlock can happen when the build side of a join overflows the total capacity of the broadcast output buffer.
     * When the broadcast buffer is overflow some data must be discarded. The data from the broadcast output buffer can
     * only be discarded after it is consumed by all consumers. The scheduler is expected to send the "noMoreOutputBuffers"
     * signal when the probe side scheduling is done. However if the probe side scheduling is blocked on split placement
     * the scheduling might never finish. To handle this case a special handling was introduced. When the scheduler detects
     * that the stage is blocked on the split placement and the output buffers of the source tasks of the stage are full the
     * scheduler schedules as many tasks as there are nodes in the cluster (without waiting for the split placement to finish)
     * and sends a signal to the source tasks that no more tasks (thus output buffers) will be created.
     * <p>
     * Note: The test is expected to take ~25 second. The increase in run time is contributed by the decreased split queue size and the
     * decreased size of the broadcast output buffer.
     */
    @Test(timeOut = 120_000)
    public void testBroadcastJoinDeadlockResolution()
            throws Exception
    {
        try (QueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setCoordinatorProperties(ImmutableMap.of(
                        "join-distribution-type", "BROADCAST",
                        "optimizer.join-reordering-strategy", "NONE",
                        // make sure the probe side will get blocked on a split placement
                        "node-scheduler.min-pending-splits-per-task", "1",
                        "node-scheduler.max-splits-per-node", "1",
                        "node-scheduler.max-unacknowledged-splits-per-task", "1"))
                .setExtraProperties(ImmutableMap.of(
                        // make sure the build side will get blocked on a broadcast buffer
                        "sink.max-broadcast-buffer-size", "1kB"))
                // make sure the connector produces enough splits for the scheduling to block on a split placement
                .withSplitsPerNode(10)
                .build()) {
            String sql = "SELECT * FROM supplier s INNER JOIN lineitem l ON s.suppkey = l.suppkey";
            MaterializedResult actual = queryRunner.execute(sql);
            MaterializedResult expected = getQueryRunner().execute(sql);
            assertEqualsIgnoreOrder(actual, expected, "For query: \n " + sql);
        }
    }
}
