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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.spi.type.TimeZoneKey;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static io.trino.SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestAggregations
        extends AbstractTestQueryFramework
{
    protected static final Set<TpchTable<?>> REQUIRED_TPCH_TABLES = ImmutableSet.<TpchTable<?>>builder()
            .add(LINE_ITEM)
            .add(NATION)
            .add(ORDERS)
            .add(REGION)
            .add(CUSTOMER)
            .build();

    @Test
    public void testCountBoolean()
    {
        assertQuery("SELECT COUNT(true) FROM orders");
    }

    @Test
    public void testCountAllWithComparison()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE tax < discount");
    }

    @Test
    public void testCountWithNotPredicate()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE NOT tax < discount");
    }

    @Test
    public void testCountWithNullPredicate()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE NULL");
    }

    @Test
    public void testCountWithIsNullPredicate()
    {
        assertQuery(
                "SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') IS NULL",
                "SELECT COUNT(*) FROM orders WHERE orderstatus = 'F' ");
    }

    @Test
    public void testCountWithIsNotNullPredicate()
    {
        assertQuery(
                "SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') IS NOT NULL",
                "SELECT COUNT(*) FROM orders WHERE orderstatus <> 'F' ");
    }

    @Test
    public void testCountWithNullIfPredicate()
    {
        assertQuery("SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') = orderstatus ");
    }

    @Test
    public void testAggregationPushdownThroughOuterJoinNotFiringInCorrelatedAggregatesLeftSide()
    {
        assertQuery("SELECT max(x) FROM" +
                        "(SELECT * from (VALUES 1) t(x) LEFT JOIN (VALUES 1) t2(y) ON t.x = t2.y)" +
                        "GROUP BY x",
                "VALUES 1");
    }

    @Test
    public void testAggregationPushdownThroughOuterJoinNotFiringInCorrelatedAggregatesRightSide()
    {
        assertQuery("SELECT max(y) FROM" +
                        "(SELECT * from (VALUES 1) t(x) LEFT JOIN (VALUES 1) t2(y) ON t.x = t2.y)" +
                        "GROUP BY y",
                "VALUES 1");
    }

    /**
     * This case tests that Aggregation isn't incorrectly pushed down into the inner source of JoinNode
     * in the case when it uses symbols from the outer source of JoinNode.
     */
    @Test
    public void testAggregationUsingOuterTableSymbols()
    {
        assertQuery(
                "SELECT max_by(n.nationkey, r.regionkey) FROM (SELECT DISTINCT regionkey FROM region) r LEFT JOIN nation n ON n.regionkey = r.regionkey GROUP BY r.regionkey",
                "VALUES 16, 20, 21, 23, 24");
    }

    /**
     * In this case, Aggregation count(*) can be pushed down into the inner source of JoinNode.
     */
    @Test
    public void testCountAllOverJoin()
    {
        assertQuery(
                "SELECT count(*) " +
                        "FROM (SELECT DISTINCT a, b FROM (VALUES (1, 1), (1, 2)) l(a, b)) l " +
                        "LEFT JOIN (SELECT 1 a) r ON l.a = r.a " +
                        "GROUP BY l.a, l.b",
                "VALUES 1, 1");
    }

    @Test
    public void testCountWithCoalescePredicate()
    {
        assertQuery(
                "SELECT COUNT(*) FROM orders WHERE COALESCE(NULLIF(orderstatus, 'F'), 'bar') = 'bar'",
                "SELECT COUNT(*) FROM orders WHERE orderstatus = 'F'");
    }

    @Test
    public void testCountWithAndPredicate()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE tax < discount AND tax > 0.01 AND discount < 0.05");
    }

    @Test
    public void testCountWithOrPredicate()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE tax < 0.01 OR discount > 0.05");
    }

    @Test
    public void testCountWithInlineView()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT orderkey FROM lineitem) x");
    }

    @Test
    public void testNestedCount()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT orderkey, COUNT(*) FROM lineitem GROUP BY orderkey) x");
    }

    @Test
    public void testGroupByOnSupersetOfPartitioning()
    {
        assertQuery("SELECT orderdate, c, count(*) FROM (SELECT orderdate, count(*) c FROM orders GROUP BY orderdate) GROUP BY orderdate, c");
    }

    @Test
    public void testSumOfNulls()
    {
        assertQuery("SELECT orderstatus, sum(CAST(NULL AS BIGINT)) FROM orders GROUP BY orderstatus");
    }

    @Test
    public void testCountAllWithPredicate()
    {
        assertQuery("SELECT COUNT(*) FROM orders WHERE orderstatus = 'F'");
    }

    @Test
    public void testGroupByArray()
    {
        assertQuery("SELECT col[1], count FROM (SELECT ARRAY[custkey] col, COUNT(*) count FROM orders GROUP BY 1 ORDER BY 1)", "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey ORDER BY custkey");
    }

    @Test
    public void testGroupByMap()
    {
        assertQuery("SELECT col[1], count FROM (SELECT MAP(ARRAY[1], ARRAY[custkey]) col, COUNT(*) count FROM orders GROUP BY 1)", "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testGroupByComplexMap()
    {
        assertQuery("SELECT MAP_KEYS(x)[1] FROM (VALUES MAP(ARRAY['a'], ARRAY[ARRAY[1]]), MAP(ARRAY['b'], ARRAY[ARRAY[2]])) t(x) GROUP BY x", "VALUES 'a', 'b'");
    }

    @Test
    public void testGroupByRow()
    {
        assertQuery("SELECT col.col1, count FROM (SELECT CAST(row(custkey, custkey) AS row(col0 bigint, col1 bigint)) col, COUNT(*) count FROM orders GROUP BY 1)", "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testGroupByWithoutAggregation()
    {
        assertQuery("SELECT orderstatus FROM orders GROUP BY orderstatus");
    }

    @Test
    public void testNestedGroupByWithSameKey()
    {
        assertQuery("SELECT custkey, sum(t) FROM (SELECT custkey, count(*) t FROM orders GROUP BY custkey) GROUP BY custkey");
    }

    @Test
    public void testGroupByWithNulls()
    {
        assertQuery("SELECT key, COUNT(*) FROM (" +
                "SELECT CASE " +
                "  WHEN orderkey % 3 = 0 THEN NULL " +
                "  WHEN orderkey % 5 = 0 THEN 0 " +
                "  ELSE orderkey " +
                "  END AS key " +
                "FROM lineitem) " +
                "GROUP BY key");
    }

    @Test
    public void testHistogram()
    {
        assertQuery("SELECT lines, COUNT(*) FROM (SELECT orderkey, COUNT(*) lines FROM lineitem GROUP BY orderkey) U GROUP BY lines");
    }

    @Test
    public void testCountDistinct()
    {
        assertQuery("SELECT COUNT(DISTINCT custkey + 1) FROM orders", "SELECT COUNT(*) FROM (SELECT DISTINCT custkey + 1 FROM orders) t");
    }

    @Test
    public void testMixedDistinctAndZeroOnEmptyInputAggregations()
    {
        assertQuery("SELECT COUNT(DISTINCT linenumber), COUNT(*), COUNT(linenumber) from lineitem where linenumber < 0");
        assertQuery("SELECT COUNT(DISTINCT linenumber), COUNT_IF(linenumber < 0) from lineitem where linenumber < 0", "VALUES (0, 0)");
        assertQuery("SELECT COUNT(DISTINCT linenumber), approx_distinct(linenumber), approx_distinct(linenumber, 0.5) from lineitem where linenumber < 0", "VALUES (0, 0, 0)");
        assertQuery("SELECT COUNT(DISTINCT linenumber), approx_distinct(orderkey > 10), approx_distinct(orderkey > 10, 0.5) from lineitem where linenumber < 0", "VALUES (0, 0, 0)");
    }

    @Test
    public void testDistinctGroupBy()
    {
        @Language("SQL") String query = "" +
                "SELECT COUNT(DISTINCT clerk) AS count_distinct, COUNT(clerk) AS count, orderdate " +
                "FROM orders " +
                "GROUP BY orderdate " +
                "ORDER BY count_distinct, orderdate";
        assertQuery(query);
        assertQuery(
                Session.builder(getSession())
                        .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, "single_step")
                        .build(),
                query);
    }

    @Test
    public void testSingleDistinctOptimizer()
    {
        assertQuery("SELECT custkey, orderstatus, COUNT(DISTINCT orderkey) FROM orders GROUP BY custkey, orderstatus");
        assertQuery("SELECT custkey, orderstatus, COUNT(DISTINCT orderkey), SUM(DISTINCT orderkey) FROM orders GROUP BY custkey, orderstatus");
        assertQuery("" +
                "SELECT custkey, COUNT(DISTINCT orderstatus) FROM (" +
                "   SELECT orders.custkey AS custkey, orders.orderstatus AS orderstatus " +
                "   FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey " +
                "   GROUP BY orders.custkey, orders.orderstatus" +
                ") " +
                "GROUP BY custkey");
        assertQuery("SELECT custkey, COUNT(DISTINCT orderkey), COUNT(DISTINCT orderstatus) FROM orders GROUP BY custkey");

        assertQuery("SELECT SUM(DISTINCT x) FROM (SELECT custkey, COUNT(DISTINCT orderstatus) x FROM orders GROUP BY custkey) t");
    }

    @Test
    public void testExtractDistinctAggregationOptimizer()
    {
        assertQuery("SELECT max(orderstatus), COUNT(orderkey), sum(DISTINCT orderkey) FROM orders");

        assertQuery("SELECT custkey, orderstatus, avg(shippriority), SUM(DISTINCT orderkey) FROM orders GROUP BY custkey, orderstatus");

        assertQuery("SELECT s, MAX(custkey), SUM(a) FROM (" +
                "    SELECT custkey, avg(shippriority) AS a, SUM(DISTINCT orderkey) AS s FROM orders GROUP BY custkey, orderstatus" +
                ") " +
                "GROUP BY s");

        assertQuery("SELECT max(orderstatus), COUNT(DISTINCT orderkey), sum(DISTINCT orderkey) FROM orders");

        assertQuery("SELECT max(orderstatus), COUNT(DISTINCT shippriority), sum(DISTINCT orderkey) FROM orders");

        assertQuery("SELECT COUNT(tan(shippriority)), sum(DISTINCT orderkey) FROM orders");

        assertQuery("SELECT count(DISTINCT a), max(b) FROM (VALUES (row(1, 2), 3)) t(a, b)", "VALUES (1, 3)");

        // Test overlap between GroupBy columns and aggregation columns
        assertQuery("SELECT shippriority, MAX(orderstatus), SUM(DISTINCT shippriority) FROM orders GROUP BY shippriority");

        assertQuery("SELECT shippriority, COUNT(shippriority), SUM(DISTINCT orderkey) FROM orders GROUP BY shippriority");

        assertQuery("SELECT shippriority, COUNT(shippriority), SUM(DISTINCT shippriority) FROM orders GROUP BY shippriority");

        assertQuery("SELECT clerk, shippriority, MAX(orderstatus), SUM(DISTINCT shippriority) FROM orders GROUP BY clerk, shippriority");

        assertQuery("SELECT clerk, shippriority, COUNT(shippriority), SUM(DISTINCT orderkey) FROM orders GROUP BY clerk, shippriority");

        assertQuery("SELECT clerk, shippriority, COUNT(shippriority), SUM(DISTINCT shippriority) FROM orders GROUP BY clerk, shippriority");
    }

    @Test
    public void testDistinctWhere()
    {
        assertQuery("SELECT COUNT(DISTINCT clerk) FROM orders WHERE LENGTH(clerk) > 5");
    }

    @Test
    public void testMultipleDifferentDistinct()
    {
        assertQuery("SELECT COUNT(DISTINCT orderstatus), SUM(DISTINCT custkey) FROM orders");
    }

    @Test
    public void testMultipleDifferentDistinctOverUnion()
    {
        assertQuery(
                """
                SELECT custkey, COUNT(DISTINCT orderkey), COUNT(DISTINCT orderstatus)
                FROM (SELECT orderkey, orderstatus, custkey FROM orders WHERE orderstatus = 'O'
                UNION ALL SELECT orderkey, orderstatus, custkey FROM orders WHERE orderstatus = 'F')
                GROUP BY custkey
                """);
    }

    @Test
    public void testMultipleDistinct()
    {
        assertQuery(
                "SELECT COUNT(DISTINCT custkey), SUM(DISTINCT custkey) FROM orders",
                "SELECT COUNT(*), SUM(custkey) FROM (SELECT DISTINCT custkey FROM orders) t");
    }

    @Test
    public void testComplexDistinct()
    {
        assertQuery(
                "SELECT COUNT(DISTINCT custkey), " +
                        "SUM(DISTINCT custkey), " +
                        "SUM(DISTINCT custkey + 1.0E0), " +
                        "AVG(DISTINCT custkey), " +
                        "VARIANCE(DISTINCT custkey) FROM orders",
                "SELECT COUNT(*), " +
                        "SUM(custkey), " +
                        "SUM(custkey + 1.0), " +
                        "AVG(custkey), " +
                        "VARIANCE(custkey) FROM (SELECT DISTINCT custkey FROM orders) t");
    }

    @Test
    public void testMultiColumnsCountDistinct()
    {
        assertQuery("SELECT COUNT(DISTINCT orderkey), COUNT(DISTINCT custkey) from orders");
        assertQuery("SELECT COUNT(DISTINCT orderkey), COUNT(DISTINCT custkey) from orders group by orderstatus");
        assertQuery("SELECT orderstatus, COUNT(DISTINCT orderkey), COUNT(DISTINCT custkey) from orders group by orderstatus");
        assertQuery("SELECT orderstatus, COUNT(DISTINCT orderkey), SUM(totalprice) from orders group by orderstatus");
        assertQuery("SELECT orderstatus, COUNT(DISTINCT orderkey), COUNT(DISTINCT custkey), SUM(totalprice) from orders group by orderstatus");
        assertQuery("SELECT orderstatus, COUNT(DISTINCT orderkey), COUNT(DISTINCT custkey), COUNT(DISTINCT totalprice), COUNT(custkey), SUM(totalprice) from orders group by orderstatus");
        assertQuery("SELECT orderstatus, COUNT(DISTINCT orderkey), COUNT(DISTINCT totalprice), SUM(totalprice) from orders group by orderstatus");
        assertQuery("SELECT orderstatus, orderpriority, COUNT(orderstatus), COUNT(DISTINCT orderpriority)," +
                " COUNT(DISTINCT orderkey), COUNT(DISTINCT totalprice), SUM(totalprice), MAX(custkey) from orders group by orderstatus, orderpriority");
    }

    // Make sure redundant NULL values are not passed to the aggregations which potentially could happen in GroupId based mixed distinct and non-distinct aggregation implementation
    @Test
    public void testDistinctAggregationSensitiveToNull()
    {
        assertQuery("select a, array_agg(b), array_agg(distinct c) from (values (1,1,1)) t(a,b,c) group by a");
    }

    @Test
    public void testDistinctAndNonDistinctAggregationOnTheSameColumn()
    {
        assertQuery("SELECT COUNT(custkey), COUNT(DISTINCT custkey) FROM orders");
        assertQuery("SELECT COUNT(custkey), SUM(custkey), COUNT(DISTINCT custkey), SUM(DISTINCT custkey) FROM orders");
        assertQuery("SELECT custkey, COUNT(custkey), SUM(custkey), COUNT(DISTINCT custkey), SUM(DISTINCT custkey) FROM orders GROUP BY custkey");
    }

    @Test
    public void testDistinctAndNonDistinctWithoutArgument()
    {
        assertQuery("SELECT COUNT(*), COUNT(DISTINCT nationkey) FROM customer");
    }

    @Test
    public void testMultiInputsDistinct()
    {
        assertQuery(
                "SELECT corr(DISTINCT x, y) FROM " +
                        "(VALUES " +
                        "   (1, 1)," +
                        "   (2, 2)," +
                        "   (2, 2)," +
                        "   (3, 3)" +
                        ") t(x, y)",
                "VALUES (1.0)");

        assertQuery(
                "SELECT corr(DISTINCT x, y), corr(DISTINCT y, x) FROM " +
                        "(VALUES " +
                        "   (1, 1)," +
                        "   (2, 2)," +
                        "   (2, 2)," +
                        "   (3, 3)" +
                        ") t(x, y)",
                "VALUES (1.0, 1.0)");

        assertQuery(
                "SELECT corr(DISTINCT x, y), corr(DISTINCT y, x), count(*) FROM " +
                        "(VALUES " +
                        "   (1, 1)," +
                        "   (2, 2)," +
                        "   (2, 2)," +
                        "   (3, 3)" +
                        ") t(x, y)",
                "VALUES (1.0, 1.0, 4)");

        assertQuery(
                "SELECT corr(DISTINCT x, y), corr(DISTINCT y, x), count(DISTINCT x) FROM " +
                        "(VALUES " +
                        "   (1, 1)," +
                        "   (2, 2)," +
                        "   (2, 2)," +
                        "   (3, 3)" +
                        ") t(x, y)",
                "VALUES (1.0, 1.0, 3)");
    }

    @Test
    public void testAggregationFilter()
    {
        assertQuery("SELECT sum(x) FILTER (WHERE y > 4) FROM (VALUES (1, 3), (2, 4), (2, 4), (4, 5)) t (x, y)", "SELECT 4");
        assertQuery("SELECT sum(x) FILTER (WHERE x > 1), sum(y) FILTER (WHERE y > 4) FROM (VALUES (1, 3), (2, 4), (2, 4), (4, 5)) t (x, y)", "SELECT 8, 5");
        assertQuery("SELECT sum(x) FILTER (WHERE x > 1), sum(x) FROM (VALUES (1), (2), (2), (4)) t (x)", "SELECT 8, 9");
        assertQuery("SELECT count(*) FILTER (WHERE x > 1), sum(x) FROM (VALUES (1, 3), (2, 4), (2, 4), (4, 5)) t (x, y)", "SELECT 3, 9");
        assertQuery("SELECT count(*) FILTER (WHERE x > 1), count(DISTINCT y) FROM (VALUES (1, 10), (2, 10), (3, 10), (4, 20)) t (x, y)", "SELECT 3, 2");

        assertQuery("" +
                        "SELECT sum(b) FILTER (WHERE true) " +
                        "FROM (SELECT count(*) FILTER (WHERE true) AS b)",
                "SELECT 1");

        assertQuery("SELECT count(1) FILTER (WHERE orderstatus = 'O') FROM orders", "SELECT count(*) FROM orders WHERE orderstatus = 'O'");

        // filter out all rows
        assertQuery("SELECT sum(x) FILTER (WHERE y > 5) FROM (VALUES (1, 3), (2, 4), (2, 4), (4, 5)) t (x, y)", "SELECT null");
        assertQuery("SELECT count(*) FILTER (WHERE x > 4), sum(x) FILTER (WHERE y > 5) FROM (VALUES (1, 3), (2, 4), (2, 4), (4, 5)) t (x, y)", "SELECT 0, null");
    }

    @Test
    public void testAggregationFilterWithSubquery()
    {
        assertQuery("" +
                        "WITH company AS (SELECT * FROM (VALUES (1, 10), (2, 20)) t(dep_id, salary)), " +
                        "department AS (SELECT 1 id) " +
                        "SELECT dep_id, sum(salary), sum(salary) FILTER (WHERE EXISTS (SELECT 1 FROM department WHERE department.id = company.dep_id)) " +
                        "FROM company GROUP BY dep_id",
                "VALUES (1, 10, 10), (2, 20, NULL)");
    }

    @Test
    public void testAggregationWithProjection()
    {
        assertQuery("SELECT sum(totalprice * 2) - sum(totalprice) FROM orders");
        assertQuery("SELECT sum(totalprice * 2) + sum(totalprice * 2) FROM orders");
    }

    @Test
    public void testSameInputToAggregates()
    {
        assertQuery("SELECT max(a), max(b) FROM (SELECT custkey a, custkey b FROM orders) x");
    }

    @Test
    public void testAggregationImplicitCoercion()
    {
        assertQuery("SELECT 1.0 / COUNT(*) FROM orders");
        assertQuery("SELECT custkey, 1.0 / COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testAggregationOverRightJoinOverSingleStreamProbe()
    {
        // this should return one row since value is always 'value'
        // this test verifies that the two streams produced by the right join
        // are handled gathered for the aggregation operator
        assertQueryOrdered("" +
                        "SELECT\n" +
                        "  value\n" +
                        "FROM\n" +
                        "(\n" +
                        "    SELECT\n" +
                        "        key\n" +
                        "    FROM\n" +
                        "        (VALUES 'match') AS a(key)\n" +
                        "        LEFT JOIN (SELECT * FROM (VALUES (0)) LIMIT 0) AS x(ignored)\n" +
                        "        ON TRUE\n" +
                        "    GROUP BY 1\n" +
                        ") a\n" +
                        "RIGHT JOIN\n" +
                        "(\n" +
                        "    VALUES\n" +
                        "    ('match', 'value'),\n" +
                        "    ('no-match', 'value')\n" +
                        ") AS b(key, value)\n" +
                        "ON a.key = b.key\n" +
                        "GROUP BY 1\n",
                "VALUES 'value'");
    }

    @Test
    public void testAggregationPushedBelowOuterJoin()
    {
        assertQuery(
                "SELECT * " +
                        "FROM nation n1 " +
                        "WHERE (n1.nationkey > ( " +
                        "SELECT avg(nationkey) " +
                        "FROM nation n2 " +
                        "WHERE n1.regionkey=n2.regionkey))");
        assertQuery(
                "SELECT max(name), min(name), count(nationkey) + 1, count(nationkey) " +
                        "FROM (SELECT DISTINCT regionkey FROM region) AS r1 " +
                        "LEFT JOIN " +
                        "nation " +
                        "ON r1.regionkey = nation.regionkey " +
                        "GROUP BY r1.regionkey " +
                        "HAVING sum(nationkey) < 20");

        assertQuery(
                "SELECT DISTINCT r1.regionkey " +
                        "FROM (SELECT regionkey FROM region INTERSECT SELECT regionkey FROM region WHERE regionkey < 4) AS r1 " +
                        "LEFT JOIN " +
                        "nation " +
                        "ON r1.regionkey = nation.regionkey");

        assertQuery(
                "SELECT max(nationkey) " +
                        "FROM (SELECT regionkey FROM region EXCEPT SELECT regionkey FROM region WHERE regionkey < 4) AS r1 " +
                        "LEFT JOIN " +
                        "nation " +
                        "ON r1.regionkey = nation.regionkey " +
                        "GROUP BY r1.regionkey");

        assertQuery(
                "SELECT max(nationkey) " +
                        "FROM (VALUES CAST (1 AS BIGINT)) v1(col1) " +
                        "LEFT JOIN " +
                        "nation " +
                        "ON v1.col1 = nation.regionkey " +
                        "GROUP BY v1.col1",
                "VALUES 24");
    }

    @Test
    public void testAggregationWithSomeArgumentCasts()
    {
        assertQuery("SELECT APPROX_PERCENTILE(0.1E0, x), AVG(x), MIN(x) FROM (values 1, 1, 1) t(x)", "SELECT 0.1, 1.0, 1");
    }

    @Test
    public void testAggregationWithHaving()
    {
        assertQuery("SELECT a, count(1) FROM (VALUES 1, 2, 3, 2) t(a) GROUP BY a HAVING count(1) > 1", "SELECT 2, 2");
    }

    @Test
    public void testGroupByRepeatedField()
    {
        assertQuery("SELECT sum(custkey) FROM orders GROUP BY orderstatus, orderstatus");
        assertQuery("SELECT count(*) FROM (SELECT orderstatus a, orderstatus b FROM orders) GROUP BY a, b");
    }

    @Test
    public void testGroupByMultipleFieldsWithPredicateOnAggregationArgument()
    {
        assertQuery("SELECT custkey, orderstatus, MAX(orderkey) FROM orders WHERE orderkey = 1 GROUP BY custkey, orderstatus");
    }

    @Test
    public void testReorderOutputsOfGroupByAggregation()
    {
        assertQuery(
                "SELECT orderstatus, a, custkey, b FROM (SELECT custkey, orderstatus, -COUNT(*) a, MAX(orderkey) b FROM orders WHERE orderkey = 1 GROUP BY custkey, orderstatus) T");
    }

    @Test
    public void testGroupAggregationOverNestedGroupByAggregation()
    {
        assertQuery("SELECT sum(custkey), max(orderstatus), min(c) FROM (SELECT orderstatus, custkey, COUNT(*) c FROM orders GROUP BY orderstatus, custkey) T");
    }

    @Test
    public void testGroupByBetween()
    {
        // whole expression in group by
        assertQuery("SELECT orderkey BETWEEN 1 AND 100 FROM orders GROUP BY orderkey BETWEEN 1 AND 100 ");

        // expression in group by
        assertQuery("SELECT CAST(orderkey BETWEEN 1 AND 100 AS BIGINT) FROM orders GROUP BY orderkey");

        // min in group by
        assertQuery("SELECT CAST(50 BETWEEN orderkey AND 100 AS BIGINT) FROM orders GROUP BY orderkey");

        // max in group by
        assertQuery("SELECT CAST(50 BETWEEN 1 AND orderkey AS BIGINT) FROM orders GROUP BY orderkey");
    }

    @Test
    public void testGroupByOrdinal()
    {
        assertQuery(
                "SELECT orderstatus, sum(totalprice) FROM orders GROUP BY 1",
                "SELECT orderstatus, sum(totalprice) FROM orders GROUP BY orderstatus");
    }

    @Test
    public void testGroupBySearchedCase()
    {
        assertQuery("SELECT CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END");

        assertQuery(
                "SELECT CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY 1",
                "SELECT CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END");
    }

    @Test
    public void testGroupBySearchedCaseNoElse()
    {
        // whole CASE in GROUP BY clause
        assertQuery("SELECT CASE WHEN orderstatus = 'O' THEN 'a' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY CASE WHEN orderstatus = 'O' THEN 'a' END");

        assertQuery(
                "SELECT CASE WHEN orderstatus = 'O' THEN 'a' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY 1",
                "SELECT CASE WHEN orderstatus = 'O' THEN 'a' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY CASE WHEN orderstatus = 'O' THEN 'a' END");

        assertQuery("SELECT CASE WHEN true THEN orderstatus END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");
    }

    @Test
    public void testGroupByIf()
    {
        assertQuery(
                "SELECT IF(orderkey between 1 and 5, 'orders', 'others'), sum(totalprice) FROM orders GROUP BY 1",
                "SELECT CASE WHEN orderkey BETWEEN 1 AND 5 THEN 'orders' ELSE 'others' END, sum(totalprice)\n" +
                        "FROM orders\n" +
                        "GROUP BY CASE WHEN orderkey BETWEEN 1 AND 5 THEN 'orders' ELSE 'others' END");
    }

    @Test
    public void testGroupByCase()
    {
        // whole CASE in GROUP BY clause
        assertQuery("SELECT CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END");

        assertQuery(
                "SELECT CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY 1",
                "SELECT CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END");

        // operand in GROUP BY clause
        assertQuery("SELECT CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // condition in GROUP BY clause
        assertQuery("SELECT CASE 'O' WHEN orderstatus THEN 'a' ELSE 'b' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // 'then' in GROUP BY clause
        assertQuery("SELECT CASE 1 WHEN 1 THEN orderstatus ELSE 'x' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // 'else' in GROUP BY clause
        assertQuery("SELECT CASE 1 WHEN 1 THEN 'x' ELSE orderstatus END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");
    }

    @Test
    public void testGroupByCaseNoElse()
    {
        // whole CASE in GROUP BY clause
        assertQuery("SELECT CASE orderstatus WHEN 'O' THEN 'a' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY CASE orderstatus WHEN 'O' THEN 'a' END");

        // operand in GROUP BY clause
        assertQuery("SELECT CASE orderstatus WHEN 'O' THEN 'a' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // condition in GROUP BY clause
        assertQuery("SELECT CASE 'O' WHEN orderstatus THEN 'a' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // 'then' in GROUP BY clause
        assertQuery("SELECT CASE 1 WHEN 1 THEN orderstatus END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");
    }

    @Test
    public void testGroupByCast()
    {
        // whole CAST in GROUP BY expression
        assertQuery("SELECT CAST(orderkey AS VARCHAR), count(*) FROM orders GROUP BY CAST(orderkey AS VARCHAR)");

        assertQuery(
                "SELECT CAST(orderkey AS VARCHAR), count(*) FROM orders GROUP BY 1",
                "SELECT CAST(orderkey AS VARCHAR), count(*) FROM orders GROUP BY CAST(orderkey AS VARCHAR)");

        // argument in GROUP BY expression
        assertQuery("SELECT CAST(orderkey AS VARCHAR), count(*) FROM orders GROUP BY orderkey");
    }

    @Test
    public void testGroupByCoalesce()
    {
        // whole COALESCE in group by
        assertQuery("SELECT COALESCE(orderkey, custkey), count(*) FROM orders GROUP BY COALESCE(orderkey, custkey)");

        assertQuery(
                "SELECT COALESCE(orderkey, custkey), count(*) FROM orders GROUP BY 1",
                "SELECT COALESCE(orderkey, custkey), count(*) FROM orders GROUP BY COALESCE(orderkey, custkey)");

        // operands in group by
        assertQuery("SELECT COALESCE(orderkey, 1), count(*) FROM orders GROUP BY orderkey");

        // operands in group by
        assertQuery("SELECT COALESCE(1, orderkey), count(*) FROM orders GROUP BY orderkey");
    }

    @Test
    public void testGroupByNullIf()
    {
        // whole NULLIF in group by
        assertQuery("SELECT NULLIF(orderkey, custkey), count(*) FROM orders GROUP BY NULLIF(orderkey, custkey)");

        assertQuery(
                "SELECT NULLIF(orderkey, custkey), count(*) FROM orders GROUP BY 1",
                "SELECT NULLIF(orderkey, custkey), count(*) FROM orders GROUP BY NULLIF(orderkey, custkey)");

        // first operand in group by
        assertQuery("SELECT NULLIF(orderkey, 1), count(*) FROM orders GROUP BY orderkey");

        // second operand in group by
        assertQuery("SELECT NULLIF(1, orderkey), count(*) FROM orders GROUP BY orderkey");
    }

    @Test
    public void testGroupByExtract()
    {
        // whole expression in group by
        assertQuery("SELECT EXTRACT(YEAR FROM orderdate), count(*) FROM orders GROUP BY EXTRACT(YEAR FROM orderdate)");

        assertQuery(
                "SELECT EXTRACT(YEAR FROM orderdate), count(*) FROM orders GROUP BY 1",
                "SELECT EXTRACT(YEAR FROM orderdate), count(*) FROM orders GROUP BY EXTRACT(YEAR FROM orderdate)");

        // argument in group by
        assertQuery("SELECT EXTRACT(YEAR FROM orderdate), count(*) FROM orders GROUP BY orderdate");
    }

    @Test
    public void testGroupByNullConstant()
    {
        assertQuery("" +
                "SELECT count(*)\n" +
                "FROM (\n" +
                "  SELECT CAST(null AS VARCHAR) constant, orderdate\n" +
                "  FROM orders\n" +
                ") a\n" +
                "group by constant, orderdate\n");
    }

    @Test
    public void test15WayGroupBy()
    {
        // Among other things, this test verifies we are not getting for overflow in the distributed HashPagePartitionFunction
        assertQuery("" +
                "SELECT " +
                "    orderkey + 1, orderkey + 2, orderkey + 3, orderkey + 4, orderkey + 5, " +
                "    orderkey + 6, orderkey + 7, orderkey + 8, orderkey + 9, orderkey + 10, " +
                "    count(*) " +
                "FROM orders " +
                "GROUP BY " +
                "    orderkey + 1, orderkey + 2, orderkey + 3, orderkey + 4, orderkey + 5, " +
                "    orderkey + 6, orderkey + 7, orderkey + 8, orderkey + 9, orderkey + 10");
    }

    @Test
    public void testApproximateCountDistinct()
    {
        // test NULL
        assertQuery("SELECT approx_distinct(NULL)", "SELECT 0");
        assertQuery("SELECT approx_distinct(NULL, 0.023)", "SELECT 0");

        // test date
        assertQuery("SELECT approx_distinct(orderdate) FROM orders", "SELECT 2443");
        assertQuery("SELECT approx_distinct(orderdate, 0.023) FROM orders", "SELECT 2443");

        // test timestamp
        assertQuery("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP)) FROM orders", "SELECT 2379");
        assertQuery("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP), 0.023) FROM orders", "SELECT 2379");

        // test timestamp(9) (long representation)
        assertQuery("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP(9))) FROM orders", "SELECT 2393");
        assertQuery("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP(9)), 0.023) FROM orders", "SELECT 2393");

        // test timestamp with time zone
        assertQuery("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP WITH TIME ZONE)) FROM orders", "SELECT 2347");
        assertQuery("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP WITH TIME ZONE), 0.023) FROM orders", "SELECT 2347");

        // test timestamp(9) with time zone (long representation)
        assertQuery("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP(9) WITH TIME ZONE)) FROM orders", "SELECT 2322");
        assertQuery("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP(9) WITH TIME ZONE), 0.023) FROM orders", "SELECT 2322");

        // test time
        assertQuery("SELECT approx_distinct(CAST(from_unixtime(custkey) AS TIME)) FROM orders", "SELECT 1005");
        assertQuery("SELECT approx_distinct(CAST(from_unixtime(custkey) AS TIME), 0.023) FROM orders", "SELECT 1005");

        // test time(9) (long representation)
        assertQuery("SELECT approx_distinct(CAST(from_unixtime(custkey) AS TIME(9))) FROM orders", "SELECT 1005");
        assertQuery("SELECT approx_distinct(CAST(from_unixtime(custkey) AS TIME(9)), 0.023) FROM orders", "SELECT 1005");

        Session session = Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("+08:35"))
                .build();

        // test time with time zone
        assertQuery(session, "SELECT approx_distinct(CAST(from_unixtime(custkey) AS TIME WITH TIME ZONE)) FROM orders", "SELECT 1013");
        assertQuery(session, "SELECT approx_distinct(CAST(from_unixtime(custkey) AS TIME WITH TIME ZONE), 0.023) FROM orders", "SELECT 1013");

        // test time(12) with time zone (long representation)
        assertQuery(session, "SELECT approx_distinct(CAST(from_unixtime(custkey) AS TIME(12) WITH TIME ZONE)) FROM orders", "SELECT 969");
        assertQuery(session, "SELECT approx_distinct(CAST(from_unixtime(custkey) AS TIME(12) WITH TIME ZONE), 0.023) FROM orders", "SELECT 969");

        // test short decimal
        assertQuery("SELECT approx_distinct(CAST(custkey AS DECIMAL(18, 0))) FROM orders", "SELECT 990");
        assertQuery("SELECT approx_distinct(CAST(custkey AS DECIMAL(18, 0)), 0.023) FROM orders", "SELECT 990");

        // test long decimal
        assertQuery("SELECT approx_distinct(CAST(custkey AS DECIMAL(25, 20))) FROM orders", "SELECT 988");
        assertQuery("SELECT approx_distinct(CAST(custkey AS DECIMAL(25, 20)), 0.023) FROM orders", "SELECT 988");

        // test real
        assertQuery("SELECT approx_distinct(CAST(custkey AS REAL)) FROM orders", "SELECT 1006");
        assertQuery("SELECT approx_distinct(CAST(custkey AS REAL), 0.023) FROM orders", "SELECT 1006");

        // test bigint
        assertQuery("SELECT approx_distinct(custkey) FROM orders", "SELECT 990");
        assertQuery("SELECT approx_distinct(custkey, 0.023) FROM orders", "SELECT 990");

        // test integer
        assertQuery("SELECT approx_distinct(CAST(custkey AS INTEGER)) FROM orders", "SELECT 990");
        assertQuery("SELECT approx_distinct(CAST(custkey AS INTEGER), 0.023) FROM orders", "SELECT 990");

        // test smallint
        assertQuery("SELECT approx_distinct(CAST(custkey AS SMALLINT)) FROM orders", "SELECT 990");
        assertQuery("SELECT approx_distinct(CAST(custkey AS SMALLINT), 0.023) FROM orders", "SELECT 990");

        // test tinyint
        assertQuery("SELECT approx_distinct(CAST((custkey % 128) AS TINYINT)) FROM orders", "SELECT 128");
        assertQuery("SELECT approx_distinct(CAST((custkey % 128) AS TINYINT), 0.023) FROM orders", "SELECT 128");

        // test double
        assertQuery("SELECT approx_distinct(CAST(custkey AS DOUBLE)) FROM orders", "SELECT 1014");
        assertQuery("SELECT approx_distinct(CAST(custkey AS DOUBLE), 0.023) FROM orders", "SELECT 1014");

        // test varchar
        assertQuery("SELECT approx_distinct(CAST(custkey AS VARCHAR)) FROM orders", "SELECT 1036");
        assertQuery("SELECT approx_distinct(CAST(custkey AS VARCHAR), 0.023) FROM orders", "SELECT 1036");

        // test char
        assertQuery("SELECT approx_distinct(CAST(CAST(custkey AS VARCHAR) AS CHAR(20))) FROM orders", "SELECT 1036");
        assertQuery("SELECT approx_distinct(CAST(CAST(custkey AS VARCHAR) AS CHAR(20)), 0.023) FROM orders", "SELECT 1036");

        // test varbinary
        assertQuery("SELECT approx_distinct(to_utf8(CAST(custkey AS VARCHAR))) FROM orders", "SELECT 1036");
        assertQuery("SELECT approx_distinct(to_utf8(CAST(custkey AS VARCHAR)), 0.023) FROM orders", "SELECT 1036");
    }

    @Test
    public void testSumDataSizeForStats()
    {
        // varchar
        assertQuery("SELECT \"$internal$sum_data_size_for_stats\"(comment) FROM orders", "SELECT sum(length(comment)) FROM orders");

        // char
        // Trino removes trailing whitespaces when casting to CHAR.
        // Hard code the expected data size since there is no easy to way to compute it in H2.
        assertQuery("SELECT \"$internal$sum_data_size_for_stats\"(CAST(comment AS CHAR(1000))) FROM orders", "SELECT 725468");

        // varbinary
        assertQuery("SELECT \"$internal$sum_data_size_for_stats\"(CAST(comment AS VARBINARY)) FROM orders", "SELECT sum(length(comment)) FROM orders");

        // array
        assertQuery("SELECT \"$internal$sum_data_size_for_stats\"(ARRAY[comment]) FROM orders", "SELECT sum(length(comment)) FROM orders");
        assertQuery("SELECT \"$internal$sum_data_size_for_stats\"(ARRAY[comment, comment]) FROM orders", "SELECT 2 * sum(length(comment)) FROM orders");

        // map
        assertQuery("SELECT \"$internal$sum_data_size_for_stats\"(map(ARRAY[1], ARRAY[comment])) FROM orders", "SELECT 4 * count(*) + sum(length(comment)) FROM orders");
        assertQuery("SELECT \"$internal$sum_data_size_for_stats\"(map(ARRAY[1, 2], ARRAY[comment, comment])) FROM orders", "SELECT 2 * 4 * count(*) + 2 * sum(length(comment)) FROM orders");

        // row
        assertQuery("SELECT \"$internal$sum_data_size_for_stats\"(ROW(comment)) FROM orders", "SELECT sum(length(comment)) FROM orders");
        assertQuery("SELECT \"$internal$sum_data_size_for_stats\"(ROW(comment, comment)) FROM orders", "SELECT 2 * sum(length(comment)) FROM orders");
    }

    @Test
    public void testMaxDataSizeForStats()
    {
        // varchar
        assertQuery("SELECT \"$internal$max_data_size_for_stats\"(comment) FROM orders", "SELECT max(length(comment)) FROM orders");

        // char
        assertQuery("SELECT \"$internal$max_data_size_for_stats\"(CAST(comment AS CHAR(1000))) FROM orders", "SELECT max(length(comment)) FROM orders");

        // varbinary
        assertQuery("SELECT \"$internal$max_data_size_for_stats\"(CAST(comment AS VARBINARY)) FROM orders", "SELECT max(length(comment)) FROM orders");

        // $internal$max_data_size_for_stats is not needed for array, map and row
    }

    @Test
    public void testApproximateCountDistinctGroupBy()
    {
        MaterializedResult actual = computeActual("SELECT orderstatus, approx_distinct(custkey) FROM orders GROUP BY orderstatus");
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 990L)
                .row("F", 990L)
                .row("P", 303L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproximateCountDistinctGroupByWithStandardError()
    {
        MaterializedResult actual = computeActual("SELECT orderstatus, approx_distinct(custkey, 0.023) FROM orders GROUP BY orderstatus");
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 990L)
                .row("F", 990L)
                .row("P", 303L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testDistinctNan()
    {
        MaterializedResult actual = computeActual("SELECT DISTINCT a/a FROM (VALUES (0.0e0), (0.0e0)) x (a)");
        assertThat(Double.isNaN((Double) actual.getOnlyValue())).isTrue();
    }

    @Test
    public void testGroupByNan()
    {
        MaterializedResult actual = computeActual("SELECT * FROM (VALUES nan(), nan(), nan()) GROUP BY 1");
        assertThat(Double.isNaN((Double) actual.getOnlyValue())).isTrue();
    }

    @Test
    public void testGroupByNanRow()
    {
        MaterializedResult actual = computeActual("SELECT a, b, c FROM (VALUES ROW(nan(), 1, 2), ROW(nan(), 1, 2)) t(a, b, c) GROUP BY 1, 2, 3");
        List<MaterializedRow> actualRows = actual.getMaterializedRows();
        assertThat(actualRows).hasSize(1);
        assertThat(Double.isNaN((Double) actualRows.get(0).getField(0))).isTrue();
        assertThat(actualRows.get(0).getField(1)).isEqualTo(1);
        assertThat(actualRows.get(0).getField(2)).isEqualTo(2);
    }

    @Test
    public void testGroupByNanArray()
    {
        MaterializedResult actual = computeActual("SELECT a FROM (VALUES (ARRAY[nan(), 2e0, 3e0]), (ARRAY[nan(), 2e0, 3e0])) t(a) GROUP BY a");
        List<MaterializedRow> actualRows = actual.getMaterializedRows();
        assertThat(actualRows).hasSize(1);
        @SuppressWarnings("unchecked")
        List<Double> value = (List<Double>) actualRows.get(0).getField(0);
        assertThat(Double.isNaN(value.get(0))).isTrue();
        assertThat(value.get(1)).isEqualTo(2.0);
        assertThat(value.get(2)).isEqualTo(3.0);
    }

    @Test
    public void testGroupByNanMap()
    {
        MaterializedResult actual = computeActual("SELECT MAP_KEYS(x)[1] FROM (VALUES MAP(ARRAY[nan()], ARRAY[ARRAY[1]]), MAP(ARRAY[nan()], ARRAY[ARRAY[2]])) t(x) GROUP BY 1");
        assertThat(Double.isNaN((Double) actual.getOnlyValue())).isTrue();
    }

    @Test
    public void testGroupByNoAggregations()
    {
        assertQuery("SELECT custkey FROM orders GROUP BY custkey");
    }

    @Test
    public void testGroupByCount()
    {
        assertQuery(
                "SELECT orderstatus, COUNT(*) FROM orders GROUP BY orderstatus",
                "SELECT orderstatus, CAST(COUNT(*) AS INTEGER) FROM orders GROUP BY orderstatus");
    }

    @Test
    public void testGroupByMultipleFields()
    {
        assertQuery("SELECT custkey, orderstatus, COUNT(*) FROM orders GROUP BY custkey, orderstatus");
    }

    @Test
    public void testGroupByWithAlias()
    {
        assertQuery(
                "SELECT orderdate x, COUNT(*) FROM orders GROUP BY orderdate",
                "SELECT orderdate x, CAST(COUNT(*) AS INTEGER) FROM orders GROUP BY orderdate");
    }

    @Test
    public void testGroupBySum()
    {
        assertQuery("SELECT suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY suppkey");
    }

    @Test
    public void testGroupByRequireIntegerCoercion()
    {
        assertQuery("SELECT partkey, COUNT(DISTINCT shipdate), SUM(linenumber) FROM lineitem GROUP BY partkey");
    }

    @Test
    public void testGroupByEmptyGroupingSet()
    {
        assertQuery("SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY ()",
                "SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupByWithWildcard()
    {
        assertQuery("SELECT * FROM (SELECT orderkey FROM orders) t GROUP BY orderkey");
    }

    @Test
    public void testSingleGroupingSet()
    {
        assertQuery(
                "SELECT linenumber, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "GROUP BY GROUPING SETS (linenumber)",
                "SELECT linenumber, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "GROUP BY linenumber");
    }

    @Test
    public void testGroupingSets()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY suppkey");
    }

    @Test
    public void testGroupingSetsNoInput()
    {
        assertQuery(
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY linenumber, suppkey " +
                        "UNION " +
                        "SELECT NULL, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY suppkey");
    }

    @Test
    public void testGroupingSetsWithGlobalAggregationNoInput()
    {
        assertQuery(
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey), ())",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY linenumber, suppkey " +
                        "UNION " +
                        "SELECT NULL, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY suppkey " +
                        "UNION " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0");
    }

    @Test
    public void testGroupingSetsWithSingleDistinct()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey))",
                "SELECT linenumber, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)) FROM lineitem GROUP BY suppkey");
    }

    @Test
    public void testGroupingSetsWithMultipleDistinct()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)), COUNT(DISTINCT linestatus) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey))",
                "SELECT linenumber, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)), COUNT(DISTINCT linestatus) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)), COUNT(DISTINCT linestatus) FROM lineitem GROUP BY suppkey");
    }

    @Test
    public void testGroupingSetsWithMultipleDistinctNoInput()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)), COUNT(DISTINCT linestatus) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey))",
                "SELECT linenumber, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)), COUNT(DISTINCT linestatus) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY linenumber, suppkey " +
                        "UNION " +
                        "SELECT NULL, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)), COUNT(DISTINCT linestatus) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY suppkey");
    }

    @Test
    public void testGroupingSetsGrandTotalSet()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), ())",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsRepeatedSetsAll()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((), (linenumber, suppkey), (), (linenumber, suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem UNION ALL " +
                        "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsRepeatedSetsAllNoInput()
    {
        assertQuery(
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY GROUPING SETS ((), (linenumber, suppkey), (), (linenumber, suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY linenumber, suppkey " +
                        "UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "UNION ALL " +
                        "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY linenumber, suppkey " +
                        "UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0");
    }

    @Test
    public void testGroupingSetsRepeatedSetsDistinct()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY DISTINCT GROUPING SETS ((), (linenumber, suppkey), (), (linenumber, suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsGrandTotalSetFirst()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((), (linenumber), (linenumber, suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT linenumber, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsOnlyGrandTotalSet()
    {
        assertQuery("SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS (())",
                "SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsMultipleGrandTotalSets()
    {
        assertQuery("SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((), ())",
                "SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem UNION ALL " +
                        "SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsMultipleGrandTotalSetsNoInput()
    {
        assertQuery("SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE quantity < 0 GROUP BY GROUPING SETS ((), ())",
                "SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE quantity < 0 UNION ALL " +
                        "SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE quantity < 0");
    }

    @Test
    public void testGroupingSetsAliasedGroupingColumns()
    {
        assertQuery("SELECT lna, lnb, SUM(quantity) " +
                        "FROM (SELECT linenumber lna, linenumber lnb, CAST(quantity AS BIGINT) quantity FROM lineitem) " +
                        "GROUP BY GROUPING SETS ((lna, lnb), (lna), (lnb), ())",
                "SELECT linenumber, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT linenumber, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetMixedExpressionAndColumn()
    {
        assertQuery("SELECT suppkey, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY month(shipdate), ROLLUP(suppkey)",
                "SELECT suppkey, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY month(shipdate), suppkey UNION ALL " +
                        "SELECT NULL, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY month(shipdate)");
    }

    @Test
    public void testGroupingSetMixedExpressionAndOrdinal()
    {
        assertQuery("SELECT suppkey, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY 2, ROLLUP(suppkey)",
                "SELECT suppkey, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY month(shipdate), suppkey UNION ALL " +
                        "SELECT NULL, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY month(shipdate)");
    }

    @Test
    public void testGroupingSetSubsetAndPartitioning()
    {
        assertQuery("SELECT COUNT_IF(x IS NULL) FROM (" +
                        "SELECT x, y, COUNT(z) FROM (SELECT CAST(lineitem.orderkey AS BIGINT) x, lineitem.linestatus y, SUM(lineitem.quantity) z FROM lineitem " +
                        "JOIN orders ON lineitem.orderkey = orders.orderkey GROUP BY 1, 2) GROUP BY GROUPING SETS ((x, y), ()))",
                "SELECT 1");
    }

    @Test
    public void testGroupingSetPredicatePushdown()
    {
        assertQuery("SELECT * FROM (" +
                        "SELECT COALESCE(orderpriority, 'ALL'), COALESCE(shippriority, -1) sp FROM (" +
                        "SELECT orderpriority, shippriority, COUNT(1) FROM orders GROUP BY GROUPING SETS ((orderpriority), (shippriority)))) WHERE sp=-1",
                "SELECT orderpriority, -1 FROM orders GROUP BY orderpriority");
    }

    @Test
    public void testGroupingSetsAggregateOnGroupedColumn()
    {
        assertQuery("SELECT orderpriority, COUNT(orderpriority) FROM orders GROUP BY ROLLUP (orderpriority)",
                "SELECT orderpriority, COUNT(orderpriority) FROM orders GROUP BY orderpriority UNION " +
                        "SELECT NULL, COUNT(orderpriority) FROM orders");
    }

    @Test
    public void testGroupingSetsMultipleAggregatesOnGroupedColumn()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(suppkey), COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), ())",
                "SELECT linenumber, suppkey, SUM(suppkey), COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, NULL, SUM(suppkey), COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsMultipleAggregatesOnUngroupedColumn()
    {
        assertQuery("SELECT linenumber, suppkey, COUNT(CAST(quantity AS BIGINT)), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), ())",
                "SELECT linenumber, suppkey, COUNT(CAST(quantity AS BIGINT)), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, NULL, COUNT(CAST(quantity AS BIGINT)), SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsMultipleAggregatesWithGroupedColumns()
    {
        assertQuery("SELECT linenumber, suppkey, COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), ())",
                "SELECT linenumber, suppkey, COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, NULL, COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsWithSingleDistinctAndUnion()
    {
        assertQuery("SELECT suppkey, COUNT(DISTINCT linenumber) FROM " +
                        "(SELECT * FROM lineitem WHERE linenumber%2 = 0 UNION ALL SELECT * FROM lineitem WHERE linenumber%2 = 1) " +
                        "GROUP BY GROUPING SETS ((suppkey), ())",
                "SELECT suppkey, COUNT(DISTINCT linenumber) FROM lineitem GROUP BY suppkey UNION ALL " +
                        "SELECT NULL, COUNT(DISTINCT linenumber) FROM lineitem");
    }

    @Test
    public void testGroupingSetsWithSingleDistinctAndUnionGroupedArguments()
    {
        assertQuery("SELECT linenumber, COUNT(DISTINCT linenumber) FROM " +
                        "(SELECT * FROM lineitem WHERE linenumber%2 = 0 UNION ALL SELECT * FROM lineitem WHERE linenumber%2 = 1) " +
                        "GROUP BY GROUPING SETS ((linenumber), ())",
                "SELECT DISTINCT linenumber, 1 FROM lineitem UNION ALL " +
                        "SELECT NULL, COUNT(DISTINCT linenumber) FROM lineitem");
    }

    @Test
    public void testGroupingSetsWithMultipleDistinctAndUnion()
    {
        assertQuery("SELECT linenumber, COUNT(DISTINCT linenumber), SUM(DISTINCT suppkey) FROM " +
                        "(SELECT * FROM lineitem WHERE linenumber%2 = 0 UNION ALL SELECT * FROM lineitem WHERE linenumber%2 = 1) " +
                        "GROUP BY GROUPING SETS ((linenumber), ())",
                "SELECT linenumber, 1, SUM(DISTINCT suppkey) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, COUNT(DISTINCT linenumber), SUM(DISTINCT suppkey) FROM lineitem");
    }

    @Test
    public void testRollup()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY ROLLUP (linenumber, suppkey)",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT linenumber, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testCube()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY CUBE (linenumber, suppkey)",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT linenumber, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY suppkey UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testCubeNoInput()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE quantity < 0 GROUP BY CUBE (linenumber, suppkey)",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE quantity < 0 GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT linenumber, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE quantity < 0 GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE quantity < 0 GROUP BY suppkey UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE quantity < 0");
    }

    @Test
    public void testGroupingCombinationsAll()
    {
        assertQuery("SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, ROLLUP (suppkey, linenumber), CUBE (linenumber)",
                "SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, suppkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, suppkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, NULL, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, suppkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, suppkey, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, suppkey UNION ALL " +
                        "SELECT orderkey, partkey, NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey");
    }

    @Test
    public void testGroupingCombinationsDistinct()
    {
        assertQuery("SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY DISTINCT orderkey, partkey, ROLLUP (suppkey, linenumber), CUBE (linenumber)",
                "SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, suppkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, NULL, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, suppkey, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, suppkey UNION ALL " +
                        "SELECT orderkey, partkey, NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey");
    }

    @Test
    public void testOrderedAggregations()
    {
        assertQuery(
                "SELECT orderpriority, custkey, array_agg(orderstatus ORDER BY orderstatus) FILTER (WHERE custkey > 500)" +
                        "FROM orders " +
                        "WHERE orderkey IN (1, 2, 3, 4, 5) " +
                        "GROUP BY GROUPING SETS ((), (orderpriority), (orderpriority, custkey))",
                "VALUES " +
                        "(NULL, NULL , ARRAY['F', 'O', 'O'])," +
                        "('5-LOW', NULL , ARRAY['F', 'O'])," +
                        "('1-URGENT', NULL, ARRAY['O'])," +
                        "('5-LOW', 370 , NULL)," +
                        "('5-LOW', 1234, ARRAY['F'])," +
                        "('5-LOW', 1369, ARRAY['O'])," +
                        "('5-LOW', 445 , NULL)," +
                        "('1-URGENT', 781 , ARRAY['O'])");
    }

    @Test
    public void testAggregationWithConstantArgumentsOverScalar()
    {
        assertQuery("SELECT count(1) FROM (SELECT count(custkey) FROM orders LIMIT 10) a");
    }

    @Test
    public void testGroupingSetsWithDefaultValue()
    {
        assertQuery(
                "SELECT orderkey, COUNT(DISTINCT k) FROM (SELECT orderkey, 1 k FROM orders) GROUP BY GROUPING SETS ((), orderkey) HAVING orderkey IS NULL",
                "VALUES (null, 1)");
    }

    @Test
    public void testApproxMostFrequentWithLong()
    {
        MaterializedResult actual1 = computeActual("SELECT approx_most_frequent(3, cast(x as bigint), 15) FROM (values 1, 2, 1, 3, 1, 2, 3, 4, 5) t(x)");
        assertThat(actual1.getRowCount()).isEqualTo(1);
        assertThat(actual1.getMaterializedRows().get(0).getFields().get(0)).isEqualTo(ImmutableMap.of(1L, 3L, 2L, 2L, 3L, 2L));

        MaterializedResult actual2 = computeActual("SELECT approx_most_frequent(2, cast(x as bigint), 15) FROM (values 1, 2, 1, 3, 1, 2, 3, 4, 5) t(x)");
        assertThat(actual2.getRowCount()).isEqualTo(1);
        assertThat(actual2.getMaterializedRows().get(0).getFields().get(0)).isEqualTo(ImmutableMap.of(1L, 3L, 2L, 2L));
    }

    @Test
    public void testApproxMostFrequentWithVarchar()
    {
        MaterializedResult actual1 = computeActual("SELECT approx_most_frequent(3, x, 15) FROM (values 'A', 'B', 'A', 'C', 'A', 'B', 'C', 'D', 'E') t(x)");
        assertThat(actual1.getRowCount()).isEqualTo(1);
        assertThat(actual1.getMaterializedRows().get(0).getFields().get(0)).isEqualTo(ImmutableMap.of("A", 3L, "B", 2L, "C", 2L));

        MaterializedResult actual2 = computeActual("SELECT approx_most_frequent(2, x, 15) FROM (values 'A', 'B', 'A', 'C', 'A', 'B', 'C', 'D', 'E') t(x)");
        assertThat(actual2.getRowCount()).isEqualTo(1);
        assertThat(actual2.getMaterializedRows().get(0).getFields().get(0)).isEqualTo(ImmutableMap.of("A", 3L, "B", 2L));
    }

    @Test
    public void testApproxMostFrequentWithLongGroupBy()
    {
        MaterializedResult actual1 = computeActual("SELECT k, approx_most_frequent(3, cast(v as bigint), 15) FROM (values ('a', 1), ('b', 2), ('a', 1), ('c', 3), ('a', 1), ('b', 2), ('c', 3), ('a', 4), ('b', 5)) t(k, v) GROUP BY 1 ORDER BY 1");
        assertThat(actual1.getRowCount()).isEqualTo(3);
        assertThat(actual1.getMaterializedRows().get(0).getFields().get(0)).isEqualTo("a");
        assertThat(actual1.getMaterializedRows().get(0).getFields().get(1)).isEqualTo(ImmutableMap.of(1L, 3L, 4L, 1L));
        assertThat(actual1.getMaterializedRows().get(1).getFields().get(0)).isEqualTo("b");
        assertThat(actual1.getMaterializedRows().get(1).getFields().get(1)).isEqualTo(ImmutableMap.of(2L, 2L, 5L, 1L));
        assertThat(actual1.getMaterializedRows().get(2).getFields().get(0)).isEqualTo("c");
        assertThat(actual1.getMaterializedRows().get(2).getFields().get(1)).isEqualTo(ImmutableMap.of(3L, 2L));
    }

    @Test
    public void testApproxMostFrequentWithStringGroupBy()
    {
        MaterializedResult actual1 = computeActual("SELECT k, approx_most_frequent(3, v, 15) FROM (values ('a', 'A'), ('b', 'B'), ('a', 'A'), ('c', 'C'), ('a', 'A'), ('b', 'B'), ('c', 'C'), ('a', 'D'), ('b', 'E')) t(k, v) GROUP BY 1 ORDER BY 1");
        assertThat(actual1.getRowCount()).isEqualTo(3);
        assertThat(actual1.getMaterializedRows().get(0).getFields().get(0)).isEqualTo("a");
        assertThat(actual1.getMaterializedRows().get(0).getFields().get(1)).isEqualTo(ImmutableMap.of("A", 3L, "D", 1L));
        assertThat(actual1.getMaterializedRows().get(1).getFields().get(0)).isEqualTo("b");
        assertThat(actual1.getMaterializedRows().get(1).getFields().get(1)).isEqualTo(ImmutableMap.of("B", 2L, "E", 1L));
        assertThat(actual1.getMaterializedRows().get(2).getFields().get(0)).isEqualTo("c");
        assertThat(actual1.getMaterializedRows().get(2).getFields().get(1)).isEqualTo(ImmutableMap.of("C", 2L));
    }

    @Test
    public void testLongDecimalAggregations()
    {
        assertQuery(
                """
                SELECT avg(value_big), sum(value_big), avg(value_small), sum(value_small)
                FROM (
                    SELECT orderkey as id, CAST(power(2, 65) as DECIMAL(38, 0)) as value_big, CAST(1 as DECIMAL(38, 0)) as value_small
                    FROM orders
                    LIMIT 10)
                GROUP BY id
                """);
    }
}
