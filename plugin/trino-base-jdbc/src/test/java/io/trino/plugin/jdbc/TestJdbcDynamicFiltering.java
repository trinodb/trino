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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.operator.OperatorStats;
import io.trino.spi.QueryId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static io.trino.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestJdbcDynamicFiltering
        extends AbstractTestQueryFramework
{
    private static final int LINEITEM_COUNT = 60175;

    private static List<Integer> getOperatorRowsRead(DistributedQueryRunner runner, QueryId queryId)
    {
        QueryStats stats = runner.getCoordinator().getQueryManager().getFullQueryInfo(queryId).getQueryStats();
        return stats.getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().equals("ScanFilterAndProjectOperator"))
                .map(OperatorStats::getInputPositions)
                .map(Math::toIntExact)
                .collect(toImmutableList());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("connection-url", format("jdbc:h2:mem:test%s;DB_CLOSE_DELAY=-1", System.nanoTime() + ThreadLocalRandom.current().nextLong()))
                .put("dynamic-filtering.enabled", "true")
                .put("dynamic-filtering.wait-timeout", "30s")
                .build();
        return H2QueryRunner.createH2QueryRunner(List.of(TpchTable.LINE_ITEM, TpchTable.ORDERS, TpchTable.PART, TpchTable.CUSTOMER), properties);
    }

    @Test
    public void testJoinDynamicFilteringNone()
    {
        // Probe-side is not scanned at all, due to dynamic filtering:
        assertDynamicFiltering(
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.totalprice < 0",
                withBroadcastJoin(),
                0,
                0, 0);
    }

    @Test
    public void testPartitionedJoinNoDynamicFiltering()
    {
        // Probe-side is fully scanned, because local dynamic filtering does not work for partitioned joins:
        assertDynamicFiltering(
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.totalprice < 0",
                withPartitionedJoin(),
                0,
                LINEITEM_COUNT, 0);
    }

    @Test
    public void testJoinDynamicFilteringWithMixedConstraint()
    {
        // Probe-side is fully scanned, because local dynamic filtering does not work for partitioned joins:
        assertDynamicFiltering(
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment = 'nstructions sleep furiously among ' AND lineitem.linenumber < 0",
                withBroadcastJoin(),
                0,
                0, 1);
    }

    @Test
    public void testJoinDynamicFilteringSingleValue()
    {
        assertQueryResult("SELECT orderkey FROM orders WHERE comment = 'nstructions sleep furiously among '", 1L);
        assertQueryResult("SELECT COUNT() FROM lineitem WHERE orderkey = 1", 6L);

        assertQueryResult("SELECT partkey FROM part WHERE comment = 'onic deposits'", 1552L);
        assertQueryResult("SELECT COUNT() FROM lineitem WHERE partkey = 1552", 39L);

        // Join lineitem with a single row of orders
        assertDynamicFiltering(
                "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment = 'nstructions sleep furiously among '",
                withBroadcastJoin(),
                6,
                6, 1);

        // Join lineitem with a single row of part
        assertDynamicFiltering(
                "SELECT l.comment FROM  lineitem l, part p WHERE p.partkey = l.partkey AND p.comment = 'onic deposits'",
                withBroadcastJoin(),
                39,
                39, 1);
    }

    @Test
    public void testJoinDynamicFilteringBlockProbeSide()
    {
        // Wait for both build sides to finish before starting the scan of 'lineitem' table (should be very selective given the dynamic filters).
        assertDynamicFiltering(
                "SELECT l.comment" +
                        " FROM  lineitem l, part p, orders o" +
                        " WHERE l.orderkey = o.orderkey AND o.comment = 'nstructions sleep furiously among '" +
                        " AND p.partkey = l.partkey AND p.comment = 'onic deposits'",
                withBroadcastJoinNonReordering(),
                1,
                1, 1, 1);
    }

    @Test
    public void testSemiJoinDynamicFilteringNone()
    {
        // Probe-side is not scanned at all, due to dynamic filtering:
        assertDynamicFiltering(
                "SELECT * FROM lineitem WHERE lineitem.orderkey IN (SELECT orders.orderkey FROM orders WHERE orders.totalprice < 0)",
                withBroadcastJoin(),
                0,
                0, 0);
    }

    @Test
    public void testPartitionedSemiJoinNoDynamicFiltering()
    {
        // Probe-side is fully scanned, because local dynamic filtering does not work for partitioned joins:
        assertDynamicFiltering(
                "SELECT * FROM lineitem WHERE lineitem.orderkey IN (SELECT orders.orderkey FROM orders WHERE orders.totalprice < 0)",
                withPartitionedJoin(),
                0,
                LINEITEM_COUNT, 0);
    }

    @Test
    public void testSemiJoinDynamicFilteringSingleValue()
    {
        // Join lineitem with a single row of orders
        assertDynamicFiltering(
                "SELECT * FROM lineitem WHERE lineitem.orderkey IN (SELECT orders.orderkey FROM orders WHERE orders.comment = 'nstructions sleep furiously among ')",
                withBroadcastJoin(),
                6,
                6, 1);

        // Join lineitem with a single row of part
        assertDynamicFiltering(
                "SELECT l.comment FROM lineitem l WHERE l.partkey IN (SELECT p.partkey FROM part p WHERE p.comment = 'onic deposits')",
                withBroadcastJoin(),
                39,
                39, 1);
    }

    @Test
    public void testSemiJoinDynamicFilteringBlockProbeSide()
    {
        // Wait for both build sides to finish before starting the scan of 'lineitem' table (should be very selective given the dynamic filters).
        assertDynamicFiltering(
                "SELECT t.comment FROM " +
                        "(SELECT * FROM lineitem l WHERE l.orderkey IN (SELECT o.orderkey FROM orders o WHERE o.comment = 'nstructions sleep furiously among ')) t " +
                        "WHERE t.partkey IN (SELECT p.partkey FROM part p WHERE p.comment = 'onic deposits')",
                withBroadcastJoinNonReordering(),
                1,
                1, 1, 1);
    }

    private void assertDynamicFiltering(@Language("SQL") String selectQuery, Session session, int expectedRowCount, int... expectedOperatorRowsRead)
    {
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(session, selectQuery);

        assertEquals(result.getResult().getRowCount(), expectedRowCount);
        assertEquals(getOperatorRowsRead(getDistributedQueryRunner(), result.getQueryId()), Ints.asList(expectedOperatorRowsRead));
    }

    private Session withBroadcastJoin()
    {
        return Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .build();
    }

    private Session withBroadcastJoinNonReordering()
    {
        return Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .build();
    }

    private Session withPartitionedJoin()
    {
        return Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name())
                .build();
    }

    private void assertQueryResult(@Language("SQL") String sql, Object... expected)
    {
        MaterializedResult rows = computeActual(sql);
        assertEquals(rows.getRowCount(), expected.length);

        for (int i = 0; i < expected.length; i++) {
            MaterializedRow materializedRow = rows.getMaterializedRows().get(i);
            int fieldCount = materializedRow.getFieldCount();
            assertEquals(fieldCount, 1, format("Expected only one column, but got '%d'", fieldCount));
            Object value = materializedRow.getField(0);
            assertEquals(value, expected[i]);
            assertEquals(materializedRow.getFieldCount(), 1);
        }
    }
}
