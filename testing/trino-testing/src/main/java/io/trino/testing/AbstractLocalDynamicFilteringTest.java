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
package io.trino.testing;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.operator.OperatorStats;
import io.trino.spi.QueryId;
import io.trino.sql.planner.OptimizerConfig;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.testing.DataProviders.toDataProvider;
import static org.testng.Assert.assertEquals;

public abstract class AbstractLocalDynamicFilteringTest
        extends AbstractTestQueryFramework
{
    private static final int ORDERS_COUNT = 15000;
    private static final int CUSTOMER_COUNT = 1500;

    protected Map<String, String> getPropertyMap()
    {
        // Adjust DF limits to test edge cases
        return ImmutableMap.of(
                "dynamic-filtering.small-broadcast.max-distinct-values-per-driver", "100",
                "dynamic-filtering.small-broadcast.range-row-limit-per-driver", "100",
                "dynamic-filtering.large-broadcast.max-distinct-values-per-driver", "100",
                "dynamic-filtering.large-broadcast.range-row-limit-per-driver", "100000");
    }

    @Test
    public void testCrossJoinLargeBuildSideDynamicFiltering()
    {
        // Probe-side is fully scanned because the build-side is too large for dynamic filtering:
        assertDynamicFiltering(
                "SELECT * FROM orders o, customer c WHERE o.custkey < c.custkey AND c.name < 'Customer#000001000' AND o.custkey > 1000",
                noJoinReordering(BROADCAST),
                0,
                ORDERS_COUNT, CUSTOMER_COUNT);
    }

    @Test(timeOut = 30_000, dataProvider = "joinDistributionTypes")
    public void testJoinDynamicFilteringMultiJoin(OptimizerConfig.JoinDistributionType joinDistributionType)
    {
        assertUpdate("DROP TABLE IF EXISTS t0");
        assertUpdate("DROP TABLE IF EXISTS t1");
        assertUpdate("DROP TABLE IF EXISTS t2");
        assertUpdate("CREATE TABLE t0 (k0 integer, v0 real)");
        assertUpdate("CREATE TABLE t1 (k1 integer, v1 real)");
        assertUpdate("CREATE TABLE t2 (k2 integer, v2 real)");
        assertUpdate("INSERT INTO t0 VALUES (1, 1.0)", 1);
        assertUpdate("INSERT INTO t1 VALUES (1, 2.0)", 1);
        assertUpdate("INSERT INTO t2 VALUES (1, 3.0)", 1);

        assertQuery(
                noJoinReordering(joinDistributionType),
                "SELECT k0, k1, k2 FROM t0, t1, t2 WHERE (k0 = k1) AND (k0 = k2) AND (v0 + v1 = v2)",
                "SELECT 1, 1, 1");
    }

    @DataProvider
    public Object[][] joinDistributionTypes()
    {
        return Stream.of(OptimizerConfig.JoinDistributionType.values())
                .collect(toDataProvider());
    }

    private void assertDynamicFiltering(@Language("SQL") String selectQuery, Session session, int expectedRowCount, int... expectedOperatorRowsRead)
    {
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(session, selectQuery);

        assertEquals(result.getResult().getRowCount(), expectedRowCount);
        assertEquals(getOperatorRowsRead(getDistributedQueryRunner(), result.getQueryId()), Ints.asList(expectedOperatorRowsRead));
    }

    private static List<Integer> getOperatorRowsRead(DistributedQueryRunner runner, QueryId queryId)
    {
        return getScanOperatorStats(runner, queryId).stream()
                .map(OperatorStats::getInputPositions)
                .map(Math::toIntExact)
                .collect(toImmutableList());
    }

    private static List<OperatorStats> getScanOperatorStats(DistributedQueryRunner runner, QueryId queryId)
    {
        QueryStats stats = runner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats();
        return stats.getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().contains("Scan"))
                .collect(toImmutableList());
    }
}
