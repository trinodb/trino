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
package io.trino.plugin.hive;

import io.trino.Session;
import io.trino.execution.DynamicFilterConfig;
import io.trino.metadata.QualifiedObjectName;
import io.trino.operator.OperatorStats;
import io.trino.testing.AbstractTestJoinQueries;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.google.common.base.Verify.verify;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static org.testng.Assert.assertEquals;

/**
 * @see TestHiveDistributedJoinQueriesWithoutDynamicFiltering for tests with dynamic filtering disabled
 */
public class TestHiveDistributedJoinQueries
        extends AbstractTestJoinQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        verify(new DynamicFilterConfig().isEnableDynamicFiltering(), "this class assumes dynamic filtering is enabled by default");
        return HiveQueryRunner.builder()
                .addExtraProperty("retry-policy", "NONE") // See TestHiveFaultTolerantExecutionJoinQueries for tests with task retries enabled
                .addHiveProperty("hive.dynamic-filtering.wait-timeout", "1h")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Test
    public void verifyDynamicFilteringEnabled()
    {
        assertQuery(
                "SHOW SESSION LIKE 'enable_dynamic_filtering'",
                "VALUES ('enable_dynamic_filtering', 'true', 'true', 'boolean', 'Enable dynamic filtering')");
    }

    @Test
    public void testJoinWithEmptyBuildSide()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .build();
        MaterializedResultWithQueryId result = getDistributedQueryRunner().executeWithQueryId(
                session,
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.totalprice = 123.4567");
        assertEquals(result.getResult().getRowCount(), 0);

        OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(
                result.getQueryId(),
                new QualifiedObjectName(HIVE_CATALOG, "tpch", "lineitem"));
        // Probe-side is not scanned at all, due to dynamic filtering:
        assertEquals(probeStats.getInputPositions(), 0L);
        assertEquals(probeStats.getDynamicFilterSplitsProcessed(), probeStats.getTotalDrivers());
    }
}
