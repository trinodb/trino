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

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.plugin.tpch.TpchMetadata;
import io.trino.testing.tpch.TpchIndexSpec;
import io.trino.testing.tpch.TpchIndexSpec.Builder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestIndexedQueries
        extends AbstractTestQueryFramework
{
    // Generate the indexed data sets
    public static final TpchIndexSpec INDEX_SPEC = new Builder()
            .addIndex("orders", TpchMetadata.TINY_SCALE_FACTOR, ImmutableSet.of("orderkey"))
            .addIndex("orders", TpchMetadata.TINY_SCALE_FACTOR, ImmutableSet.of("orderkey", "orderstatus"))
            .addIndex("orders", TpchMetadata.TINY_SCALE_FACTOR, ImmutableSet.of("orderkey", "custkey"))
            .addIndex("orders", TpchMetadata.TINY_SCALE_FACTOR, ImmutableSet.of("orderstatus", "shippriority"))
            .build();

    @Test
    public void testExampleSystemTable()
    {
        assertQuery("SELECT name FROM sys.example", "SELECT 'test' AS name");

        MaterializedResult result = computeActual("SHOW SCHEMAS");
        assertThat(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of("sf100", "tiny", "sys"))).isTrue();

        result = computeActual("SHOW TABLES FROM sys");
        assertThat(result.getOnlyColumnAsSet()).isEqualTo(ImmutableSet.of("example"));
    }

    @Test
    public void testExplainAnalyzeIndexJoin()
    {
        assertQuerySucceeds(getSession(),
                """
                EXPLAIN ANALYZE
                 SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                JOIN orders o
                  ON l.orderkey = o.orderkey
                """);
    }

    @Test
    public void testBasicIndexJoin()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                JOIN orders o
                  ON l.orderkey = o.orderkey
                """);
    }

    @Test
    public void testBasicIndexJoinReverseCandidates()
    {
        assertQuery(
                """
                SELECT *
                FROM orders o
                JOIN (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                  ON o.orderkey = l.orderkey
                """);
    }

    @Test
    public void testBasicIndexJoinWithNullKeys()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT CASE WHEN suppkey % 2 = 0 THEN orderkey ELSE NULL END AS orderkey
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                JOIN orders o
                  ON l.orderkey = o.orderkey
                """);
    }

    @Test
    public void testMultiKeyIndexJoinAligned()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT orderkey, CASE WHEN suppkey % 2 = 0 THEN 'F' ELSE 'O' END AS orderstatus
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                JOIN orders o
                  ON l.orderkey = o.orderkey AND l.orderstatus = o.orderstatus
                """);
    }

    @Test
    public void testMultiKeyIndexJoinUnaligned()
    {
        // This test a join order that is different from the inner select column ordering
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT orderkey, CASE WHEN suppkey % 2 = 0 THEN 'F' ELSE 'O' END AS orderstatus
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                JOIN orders o
                  ON l.orderstatus = o.orderstatus AND l.orderkey = o.orderkey
                """);
    }

    @Test
    public void testJoinWithNonJoinExpression()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey = 1");
    }

    @Test
    public void testPredicateDerivedKey()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT orderkey
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                JOIN orders o
                  ON l.orderkey = o.orderkey
                WHERE o.orderstatus = 'F'
                """);
    }

    @Test
    public void testCompoundPredicateDerivedKey()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT orderkey
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                JOIN orders o
                  ON l.orderkey = o.orderkey
                WHERE o.orderstatus = 'F'
                  AND o.custkey % 2 = 0
                """);
    }

    @Test
    public void testChainedIndexJoin()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT orderkey, CASE WHEN suppkey % 2 = 0 THEN 'F' ELSE 'O' END AS orderstatus
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                JOIN orders o1
                  ON l.orderkey = o1.orderkey AND l.orderstatus = o1.orderstatus
                JOIN orders o2
                  ON o1.custkey % 1024 = o2.orderkey
                """);
    }

    @Test
    public void testBasicLeftIndexJoin()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                LEFT JOIN orders o
                  ON l.orderkey = o.orderkey
                """);
    }

    @Test
    public void testNonIndexLeftJoin()
    {
        assertQuery(
                """
                SELECT *
                FROM orders o
                LEFT JOIN (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                  ON o.orderkey = l.orderkey
                """);
    }

    @Test
    public void testBasicRightIndexJoin()
    {
        assertQuery(
                """
                SELECT COUNT(*)
                FROM orders o
                RIGHT JOIN (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                  ON o.orderkey = l.orderkey
                """);
    }

    @Test
    public void testNonIndexRightJoin()
    {
        assertQuery(
                """
                SELECT COUNT(*)
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                RIGHT JOIN orders o
                  ON l.orderkey = o.orderkey
                """);
    }

    @Test
    public void testIndexJoinThroughAggregation()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                JOIN (
                  SELECT orderkey, COUNT(*)
                  FROM orders
                  WHERE custkey % 8 = 0
                  GROUP BY orderkey
                  ORDER BY orderkey) o
                  ON l.orderkey = o.orderkey
                """);
    }

    @Test
    public void testIndexJoinThroughMultiKeyAggregation()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                JOIN (
                  SELECT shippriority, orderkey, COUNT(*)
                  FROM orders
                  WHERE custkey % 8 = 0
                  GROUP BY shippriority, orderkey
                  ORDER BY orderkey) o
                  ON l.orderkey = o.orderkey
                """);
    }

    @Test
    public void testNonIndexableKeys()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                JOIN (
                  SELECT orderkey % 2 as orderkey
                  FROM orders) o
                  ON l.orderkey = o.orderkey
                """);
    }

    @Test
    public void testComposableIndexJoins()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) x
                JOIN (
                  SELECT o1.orderkey as orderkey, o2.custkey as custkey
                  FROM orders o1
                  JOIN orders o2
                    ON o1.orderkey = o2.orderkey) y
                  ON x.orderkey = y.orderkey
                """);
    }

    @Test
    public void testNonComposableIndexJoins()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) x
                JOIN (
                  SELECT l.orderkey as orderkey, o.custkey as custkey
                  FROM lineitem l
                  JOIN orders o
                    ON l.orderkey = o.orderkey) y
                  ON x.orderkey = y.orderkey
                """);
    }

    @Test
    public void testOverlappingIndexJoinLookupSymbol()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                JOIN orders o
                  ON l.orderkey % 1024 = o.orderkey AND l.partkey % 1024 = o.orderkey
                """);
    }

    @Test
    public void testOverlappingSourceOuterIndexJoinLookupSymbol()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                LEFT JOIN orders o
                  ON l.orderkey % 1024 = o.orderkey AND l.partkey % 1024 = o.orderkey
                """);
    }

    @Test
    public void testOverlappingIndexJoinProbeSymbol()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                JOIN orders o
                  ON l.orderkey = o.orderkey AND l.orderkey = o.custkey
                """);
    }

    @Test
    public void testOverlappingSourceOuterIndexJoinProbeSymbol()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                LEFT JOIN orders o
                  ON l.orderkey = o.orderkey AND l.orderkey = o.custkey
                """);
    }

    @Test
    public void testRepeatedIndexJoinClause()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                JOIN orders o
                  ON l.orderkey = o.orderkey AND l.orderkey = o.orderkey
                """);
    }

    /**
     * Assure nulls in probe readahead does not leak into connectors.
     */
    @Test
    public void testProbeNullInReadahead()
    {
        assertQuery(
                "select count(*) from (values (1), (cast(null as bigint))) x(orderkey) join orders using (orderkey)",
                "select count(*) from orders where orderkey = 1");
    }

    @Test
    public void testHighCardinalityIndexJoinResult()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM orders
                  WHERE orderkey % 10000 = 0) o1
                JOIN (
                  SELECT *
                  FROM orders
                  WHERE orderkey % 4 = 0) o2
                  ON o1.orderstatus = o2.orderstatus AND o1.shippriority = o2.shippriority
                """);
    }

    @Test
    public void testReducedIndexProjection()
    {
        assertQuery(
                """
                SELECT *
                FROM lineitem l
                INNER JOIN (
                    SELECT orderkey, (orderkey + custkey) % 107 some_projection
                    FROM orders
                ) o
                ON l.orderkey = o.orderkey AND l.linenumber % 407 = o.some_projection
                """);
    }

    @Test
    public void testReducedIndexAggregation()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT orderkey % 64 AS a, suppkey % 107 AS b
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                JOIN (
                  SELECT orderkey AS a, SUM(LENGTH(comment)) % 407 AS b
                  FROM orders
                  GROUP BY orderkey) o
                  ON l.a = o.a AND l.b = o.b
                """);
    }

    @Test
    public void testReducedIndexWindow()
    {
        assertQuery(
                """
                SELECT *
                FROM lineitem l
                INNER JOIN (
                    SELECT
                      orderkey,
                      SUM(custkey) OVER (PARTITION BY orderkey) % 107 some_window
                    FROM orders
                ) o
                ON l.orderkey = o.orderkey AND l.linenumber % 407 = o.some_window
                """);
    }

    @Test
    public void testReducedIndexProbeKeyNegativeCaching()
    {
        // Not every column 'b' can be matched through the join
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT orderkey % 64 AS a, (suppkey % 2) + 1 AS b
                  FROM lineitem
                  WHERE partkey % 8 = 0) l
                JOIN (
                  SELECT orderkey AS a, SUM(LENGTH(comment)) % 2 AS b
                  FROM orders
                  GROUP BY orderkey) o
                  ON l.a = o.a AND l.b = o.b
                """);
    }

    @Test
    public void testHighCardinalityReducedIndexProbeKey()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *, custkey % 4 AS x, custkey % 2 AS y
                  FROM orders
                  WHERE orderkey % 10000 = 0) o1
                JOIN (
                  SELECT *, custkey % 5 AS x, custkey % 3 AS y
                  FROM orders
                  WHERE orderkey % 4 = 0) o2
                  ON o1.orderstatus = o2.orderstatus AND o1.shippriority = o2.shippriority AND o1.x = o2.x AND o1.y = o2.y
                """);
    }

    @Test
    public void testReducedIndexProbeKeyComplexQueryShapes()
    {
        // Reduce the probe key through projections, aggregations, and joins
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT orderkey % 64 AS a, suppkey % 2 AS b, orderkey AS c, linenumber % 2 AS d
                  FROM lineitem
                  WHERE partkey % 7 = 0) l
                JOIN (
                  SELECT t1.a AS a, t1.b AS b, t2.orderkey AS c, SUM(LENGTH(t2.comment)) % 2 AS d
                  FROM (
                    SELECT orderkey AS a, custkey % 3 AS b
                    FROM orders
                  ) t1
                  JOIN orders t2 ON t1.a = (t2.orderkey % 1000)
                  WHERE t1.a % 1000 = 0
                  GROUP BY t1.a, t1.b, t2.orderkey) o
                  ON l.a = o.a AND l.b = o.b AND l.c = o.c AND l.d = o.d
                """);
    }

    @Test
    public void testIndexJoinConstantPropagation()
    {
        assertQuery(
                """
                SELECT x, y, COUNT(*)
                FROM (SELECT orderkey, 0 AS x FROM orders) a\s
                JOIN (SELECT orderkey, 1 AS y FROM orders) b\s
                ON a.orderkey = b.orderkey
                GROUP BY 1, 2
                """);
    }

    @Test
    public void testIndexJoinThroughWindow()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 16 = 0) l
                JOIN (
                  SELECT *, COUNT(*) OVER (PARTITION BY orderkey)
                  FROM orders) o
                  ON l.orderkey = o.orderkey
                """,
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 16 = 0) l
                JOIN (
                  SELECT *, 1
                  FROM orders) o
                  ON l.orderkey = o.orderkey
                """);
    }

    @Test
    public void testIndexJoinThroughWindowDoubleAggregation()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 16 = 0) l
                JOIN (
                  SELECT *, COUNT(*) OVER (PARTITION BY orderkey), SUM(orderkey) OVER (PARTITION BY orderkey)
                  FROM orders) o
                  ON l.orderkey = o.orderkey
                """,
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 16 = 0) l
                JOIN (
                  SELECT *, 1, orderkey as o
                  FROM orders) o
                  ON l.orderkey = o.orderkey
                """);
    }

    @Test
    public void testIndexJoinThroughWindowPartialPartition()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 16 = 0) l
                JOIN (
                  SELECT *, COUNT(*) OVER (PARTITION BY orderkey, custkey)
                  FROM orders) o
                  ON l.orderkey = o.orderkey
                """,
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 16 = 0) l
                JOIN (
                  SELECT *, 1
                  FROM orders) o
                  ON l.orderkey = o.orderkey
                """);
    }

    @Test
    public void testNoIndexJoinThroughWindowWithRowNumberFunction()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 16 = 0) l
                JOIN (
                  SELECT *, row_number() OVER (PARTITION BY orderkey)
                  FROM orders) o
                  ON l.orderkey = o.orderkey
                """,
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 16 = 0) l
                JOIN (
                  SELECT *, 1
                  FROM orders) o
                  ON l.orderkey = o.orderkey
                """);
    }

    @Test
    public void testNoIndexJoinThroughWindowWithOrderBy()
    {
        assertQuery(
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 16 = 0) l
                JOIN (
                  SELECT *, COUNT(*) OVER (PARTITION BY orderkey ORDER BY custkey)
                  FROM orders) o
                  ON l.orderkey = o.orderkey
                """,
                """
                SELECT *
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 16 = 0) l
                JOIN (
                  SELECT *, 1
                  FROM orders) o
                  ON l.orderkey = o.orderkey
                """);
    }

    @Test
    public void testNoIndexJoinThroughWindowWithRowFrame()
    {
        assertQuery(
                """
                SELECT l.orderkey, o.c
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 16 = 0) l
                JOIN (
                  SELECT *, COUNT(*) OVER (PARTITION BY orderkey ROWS 1 PRECEDING) as c
                  FROM orders) o
                  ON l.orderkey = o.orderkey
                """,
                """
                SELECT l.orderkey, o.c
                FROM (
                  SELECT *
                  FROM lineitem
                  WHERE partkey % 16 = 0) l
                JOIN (
                  SELECT *, 1 as c
                  FROM orders) o
                  ON l.orderkey = o.orderkey
                """);
    }

    @Test
    public void testNoIndexJoinOverNondeterministicProjection()
    {
        // One random() value per index source row, shared by every probe row matching it
        assertQuery(
                nondeterministicIndexSourceSession(),
                """
                SELECT count(*)
                FROM (
                    SELECT count(DISTINCT o.sample) AS variants
                    FROM (
                        SELECT orderkey FROM lineitem
                        UNION ALL
                        SELECT orderkey FROM lineitem) l
                    JOIN (SELECT orderkey, random() AS sample FROM orders) o
                      ON l.orderkey = o.orderkey
                    GROUP BY l.orderkey)
                WHERE variants <> 1
                """,
                "SELECT 0");
    }

    @Test
    public void testNoIndexJoinOverNondeterministicFilter()
    {
        // One sampling decision per index source row, so a surviving orderkey matches both probe branches
        assertQuery(
                nondeterministicIndexSourceSession(),
                """
                SELECT count(*)
                FROM (
                    SELECT min(l.branch) AS lo, max(l.branch) AS hi
                    FROM (
                        SELECT orderkey, 1 AS branch FROM lineitem
                        UNION ALL
                        SELECT orderkey, 2 AS branch FROM lineitem) l
                    JOIN (SELECT orderkey, custkey FROM orders WHERE random() < 0.5) o
                      ON l.orderkey = o.orderkey
                    GROUP BY l.orderkey, o.custkey)
                WHERE lo <> 1 OR hi <> 2
                """,
                "SELECT 0");
    }

    private Session nondeterministicIndexSourceSession()
    {
        return Session.builder(getSession())
                .setSystemProperty("task_concurrency", "4")
                .setSystemProperty("task_share_index_loading", "false")
                .build();
    }

    @Test
    public void testOuterNonEquiJoins()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM orders RIGHT OUTER JOIN lineitem ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 WHERE orders.orderkey IS NULL");
    }

    @Test
    public void testNonEquiJoin()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity + length(orders.comment) > 7");
    }
}
