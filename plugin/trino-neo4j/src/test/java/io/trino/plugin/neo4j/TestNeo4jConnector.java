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
package io.trino.plugin.neo4j;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.sql.planner.OptimizerConfig;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchTable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.SystemSessionProperties.IGNORE_STATS_CALCULATOR_FAILURES;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.join;
import static java.util.Collections.nCopies;

public class TestNeo4jConnector
        extends BaseNeo4jTest
{
    public TestNeo4jConnector()
    {
        super(Optional.of(ImmutableList.of(ORDERS, NATION, REGION, CUSTOMER)));
    }

    @Test
    public void testColumnsInReverseOrder()
    {
        assertQuery(buildNativeQuery("SELECT shippriority, clerk, totalprice FROM TABLE(%s)", TpchTable.ORDERS), "SELECT shippriority, clerk, totalprice FROM orders");
    }

    @Test
    public void testAggregation()
    {
        assertQuery(buildNativeQuery("SELECT sum(orderkey) FROM TABLE(%s)", ORDERS), "SELECT sum(orderkey) FROM orders");
        assertQuery(buildNativeQuery("SELECT sum(totalprice) FROM TABLE(%s)", TpchTable.ORDERS), "SELECT sum(totalprice) FROM orders");
        assertQuery(buildNativeQuery("SELECT max(comment) FROM TABLE(%s)", TpchTable.NATION), "SELECT max(comment) FROM nation");

        assertQuery(buildNativeQuery("SELECT count(*) FROM TABLE(%s)", TpchTable.ORDERS), "SELECT count(*) FROM orders");
        assertQuery(buildNativeQuery("SELECT count(*) FROM TABLE(%s) WHERE orderkey > 10", TpchTable.ORDERS), "SELECT count(*) FROM orders WHERE orderkey > 10");
        assertQuery(buildNativeQuery("SELECT count(*) FROM (SELECT * FROM TABLE(%s) LIMIT 10)", TpchTable.ORDERS), "SELECT count(*) FROM (SELECT * FROM orders LIMIT 10)");
        assertQuery(buildNativeQuery("SELECT count(*) FROM (SELECT * FROM TABLE(%s) WHERE orderkey > 10 LIMIT 10)", TpchTable.ORDERS), "SELECT count(*) FROM (SELECT * FROM orders WHERE orderkey > 10 LIMIT 10)");

        assertQuery(buildNativeQuery("SELECT DISTINCT regionkey FROM TABLE(%s)", TpchTable.NATION), "SELECT DISTINCT regionkey FROM nation");
        assertQuery(buildNativeQuery("SELECT regionkey FROM TABLE(%s) GROUP BY regionkey", TpchTable.NATION), "SELECT regionkey FROM nation GROUP BY regionkey");

        assertQuery(buildNativeQuery("SELECT count(regionkey) FROM TABLE(%s)", TpchTable.NATION), "SELECT count(regionkey) FROM nation");
        assertQuery(buildNativeQuery("SELECT count(DISTINCT regionkey) FROM TABLE(%s)", TpchTable.NATION), "SELECT count(DISTINCT regionkey) FROM nation");
        assertQuery(buildNativeQuery("SELECT regionkey, count(*) FROM TABLE(%s) GROUP BY regionkey", TpchTable.NATION), "SELECT regionkey, count(*) FROM nation GROUP BY regionkey");

        assertQuery(buildNativeQuery("SELECT min(regionkey), max(regionkey) FROM TABLE(%s)", TpchTable.NATION), "SELECT min(regionkey), max(regionkey) FROM nation");
        assertQuery(buildNativeQuery("SELECT min(DISTINCT regionkey), max(DISTINCT regionkey) FROM TABLE(%s)", TpchTable.NATION), "SELECT min(DISTINCT regionkey), max(DISTINCT regionkey) FROM nation");
        assertQuery(buildNativeQuery("SELECT regionkey, min(regionkey), min(name), max(regionkey), max(name) FROM TABLE(%s) GROUP BY regionkey", (TpchTable.NATION)), "SELECT regionkey, min(regionkey), min(name), max(regionkey), max(name) FROM nation GROUP BY regionkey");

        assertQuery(buildNativeQuery("SELECT sum(regionkey) FROM TABLE(%s)", TpchTable.NATION), "SELECT sum(regionkey) FROM nation");
        assertQuery(buildNativeQuery("SELECT sum(DISTINCT regionkey) FROM TABLE(%s)", TpchTable.NATION), "SELECT sum(DISTINCT regionkey) FROM nation");
        assertQuery(buildNativeQuery("SELECT regionkey, sum(regionkey) FROM TABLE(%s) GROUP BY regionkey", TpchTable.NATION), "SELECT regionkey, sum(regionkey) FROM nation GROUP BY regionkey");

        assertQuery(
                buildNativeQuery("SELECT avg(nationkey) FROM TABLE(%s)", TpchTable.NATION),
                "SELECT avg(CAST(nationkey AS double)) FROM nation");
        assertQuery(
                buildNativeQuery("SELECT avg(DISTINCT nationkey) FROM TABLE(%s)", TpchTable.NATION),
                "SELECT avg(DISTINCT CAST(nationkey AS double)) FROM nation");
        assertQuery(
                buildNativeQuery("SELECT regionkey, avg(nationkey) FROM TABLE(%s) GROUP BY regionkey", TpchTable.NATION),
                "SELECT regionkey, avg(CAST(nationkey AS double)) FROM nation GROUP BY regionkey");
//
//        // pruned away aggregation (simplified regression test for https://github.com/trinodb/trino/issues/12598)
        assertQuery(
                buildNativeQuery("SELECT -13 FROM (SELECT count(*) FROM TABLE(%s) )", TpchTable.NATION),
                "VALUES -13");
//        // regression test for https://github.com/trinodb/trino/issues/12598
        assertQuery(
                buildNativeQuery("SELECT count(*) FROM (SELECT count(*) FROM TABLE(%s) UNION ALL SELECT count(*) FROM TABLE(%s))", TpchTable.NATION, TpchTable.REGION),
                "VALUES 2");
    }

    @Test
    public void testExactPredicate()
    {
        assertQueryReturnsEmptyResult(buildNativeQuery("SELECT * FROM TABLE(%s) WHERE orderkey = 10", ORDERS));

        // filtered column is selected
        assertQuery(buildNativeQuery("SELECT custkey, orderkey FROM TABLE(%s) WHERE orderkey = 32", TpchTable.ORDERS), "VALUES (1301, 32)");

        // filtered column is not selected
        assertQuery(buildNativeQuery("SELECT custkey FROM TABLE(%s) WHERE orderkey = 32", ORDERS), "VALUES (1301)");
    }

    @Test
    public void testInListPredicate()
    {
        assertQueryReturnsEmptyResult(buildNativeQuery("SELECT * FROM TABLE(%s) WHERE orderkey IN (10, 11, 20, 21)", ORDERS));

        // filtered column is selected
        assertQuery(buildNativeQuery("SELECT custkey, orderkey FROM TABLE(%s) WHERE orderkey IN (7, 10, 32, 33)", ORDERS), "VALUES (392, 7), (1301, 32), (670, 33)");

        // filtered column is not selected
        assertQuery(buildNativeQuery("SELECT custkey FROM TABLE(%s) WHERE orderkey IN (7, 10, 32, 33)", ORDERS), "VALUES (392), (1301), (670)");
    }

    @Test
    public void testIsNullPredicate()
    {
        assertQueryReturnsEmptyResult(buildNativeQuery("SELECT * FROM TABLE(%s) WHERE orderkey IS NULL", ORDERS));
        assertQueryReturnsEmptyResult(buildNativeQuery("SELECT * FROM TABLE(%s) WHERE orderkey = 10 OR orderkey IS NULL", ORDERS));

        // filtered column is selected
        assertQuery(buildNativeQuery("SELECT custkey, orderkey FROM TABLE(%s) WHERE orderkey = 32 OR orderkey IS NULL", ORDERS), "VALUES (1301, 32)");

        // filtered column is not selected
        assertQuery(buildNativeQuery("SELECT custkey FROM TABLE(%s) WHERE orderkey = 32 OR orderkey IS NULL", ORDERS), "VALUES (1301)");
    }

    @Test
    public void testLikePredicate()
    {
        // filtered column is not selected
        assertQuery(buildNativeQuery("SELECT orderkey FROM TABLE(%s) WHERE orderpriority LIKE '5-L%%'", ORDERS), "SELECT orderkey FROM orders WHERE orderpriority LIKE '5-L%'");

        // filtered column is selected
        assertQuery(buildNativeQuery("SELECT orderkey, orderpriority FROM TABLE(%s) WHERE orderpriority LIKE '5-L%%'", ORDERS), "SELECT orderkey, orderpriority FROM orders WHERE orderpriority LIKE '5-L%'");

        // filtered column is not selected
        assertQuery(buildNativeQuery("SELECT orderkey FROM TABLE(%s) WHERE orderpriority LIKE '5-L__'", ORDERS), "SELECT orderkey FROM orders WHERE orderpriority LIKE '5-L__'");

        // filtered column is selected
        assertQuery(buildNativeQuery("SELECT orderkey, orderpriority FROM TABLE(%s) WHERE orderpriority LIKE '5-L__'", ORDERS), "SELECT orderkey, orderpriority FROM orders WHERE orderpriority LIKE '5-L__'");
    }

    @Test
    public void testMultipleRangesPredicate()
    {
        // List columns explicitly. Some connectors do not maintain column ordering.
        assertQuery(buildNativeQuery("SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                "FROM TABLE(%s) " +
                "WHERE orderkey BETWEEN 10 AND 50", ORDERS), "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                "FROM orders " +
                "WHERE orderkey BETWEEN 10 AND 50");
    }

    @Test
    public void testDateYearOfEraPredicate()
    {
        // Verify the predicate of '-1996-09-14' doesn't match '1997-09-14'. Both values return same formatted string when we use 'yyyy-MM-dd' in DateTimeFormatter
        assertQuery(buildNativeQuery("SELECT orderdate FROM TABLE(%s) WHERE orderdate = DATE '1997-09-14'", ORDERS), "VALUES DATE '1997-09-14'");
        assertQueryReturnsEmptyResult(buildNativeQuery("SELECT * FROM TABLE(%s) WHERE orderdate = DATE '-1996-09-14'", ORDERS));
    }

    @Test
    public void testConcurrentScans()
    {
        String unionMultipleTimes = join(" UNION ALL ", nCopies(25, buildNativeQuery("SELECT * FROM TABLE(%s)", ORDERS)));
        assertQuery("SELECT sum(if(rand() >= 0, orderkey)) FROM (" + unionMultipleTimes + ")", "VALUES 11246812500");
    }

    @Test
    public void testSelectAll()
    {
        assertQuery(buildNativeQuery("SELECT * FROM TABLE(%s)", ORDERS), "SELECT * FROM orders");
    }

    @Test
    public void testSelectInTransaction()
    {
        inTransaction(session -> {
            assertQuery(session, buildNativeQuery("SELECT nationkey, name, regionkey FROM TABLE(%s)", NATION), "SELECT nationkey, name, regionkey FROM nation");
            assertQuery(session, buildNativeQuery("SELECT regionkey, name FROM TABLE(%s)", REGION), "SELECT regionkey, name FROM region");
            assertQuery(session, buildNativeQuery("SELECT nationkey, name, regionkey FROM TABLE(%s)", NATION), "SELECT nationkey, name, regionkey FROM nation");
        });
    }

    @Test(timeOut = 300_000, dataProvider = "joinDistributionTypes")
    public void testJoinWithEmptySides(OptimizerConfig.JoinDistributionType joinDistributionType)
    {
        Session session = noJoinReordering(joinDistributionType);
        // empty build side
        assertQuery(session, buildNativeQuery("SELECT count(*) FROM TABLE(%s) as nation JOIN TABLE(%s) as region ON nation.regionkey = region.regionkey AND region.name = ''", NATION, REGION), "VALUES 0");
        assertQuery(session, buildNativeQuery("SELECT count(*) FROM TABLE(%s) as nation JOIN TABLE(%s) as region ON nation.regionkey = region.regionkey AND region.regionkey < 0", NATION, REGION), "VALUES 0");
        // empty probe side
        assertQuery(session, buildNativeQuery("SELECT count(*) FROM TABLE(%s) as region JOIN TABLE(%s) as nation ON nation.regionkey = region.regionkey AND region.name = ''", REGION, NATION), "VALUES 0");
        assertQuery(session, buildNativeQuery("SELECT count(*) FROM TABLE(%s) as nation JOIN TABLE(%s) as region ON nation.regionkey = region.regionkey AND region.regionkey < 0", NATION, REGION), "VALUES 0");
    }

    @DataProvider
    public Object[][] joinDistributionTypes()
    {
        return Stream.of(OptimizerConfig.JoinDistributionType.values())
                .collect(toDataProvider());
    }

    @Test
    public void testJoin()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(IGNORE_STATS_CALCULATOR_FAILURES, "false")
                .build();

        // 2 inner joins, eligible for join reodering
        assertQuery(
                session,
                buildNativeQuery("SELECT c.name, n.name, r.name " +
                        "FROM TABLE(%s) n " +
                        "JOIN TABLE(%s) c ON c.nationkey = n.nationkey " +
                        "JOIN TABLE(%s) r ON n.regionkey = r.regionkey", NATION, CUSTOMER, REGION),
                "SELECT c.name, n.name, r.name " +
                        "FROM nation n " +
                        "JOIN customer c ON c.nationkey = n.nationkey " +
                        "JOIN region r ON n.regionkey = r.regionkey");
    }

    protected String buildNativeQuery(String sql, TpchTable... tables)
    {
        return String.format(sql, Arrays.stream(tables).map(table -> getTableCypherQuery(table)).toArray());
    }

    protected String getTableCypherQuery(TpchTable tpchTable)
    {
        List<TpchColumn> columns = tpchTable.getColumns();
        String projection = columns.stream().map(column -> "n." + column.getSimplifiedColumnName() + " as " + column.getSimplifiedColumnName()).collect(Collectors.joining(","));
        return String.format("neo4j.system.query(query => 'MATCH (n:%s) return %s')", tpchTable.getTableName(), projection);
    }
//    public static void main(String[] args)
//    {
//        System.out.println(String.format("SELECT orderkey FROM TABLE(%s) WHERE orderpriority LIKE '5-L%%'", "test"));
//    }
}
