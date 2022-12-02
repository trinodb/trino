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
package io.trino.sql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.LogicalPlanner;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.WindowNode;
import org.testng.annotations.Test;

import static io.trino.SystemSessionProperties.USE_QUERY_FUSION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static org.testng.Assert.assertFalse;

public class TestSharedScanOptimizer
        extends BasePlanTest
{
    TestSharedScanOptimizer()
    {
        super(ImmutableMap.of(USE_QUERY_FUSION, "true"));
    }

    @Test
    public void testJoinGbRule()
    {
        // Join(q, Agg(q)) -> Window(q)
        assertPlan("WITH customer_total_return AS (\n" +
                        "  SELECT shippriority AS ctr_store_sk,\n" +
                        "         sum(orderkey) AS ctr_total_return\n" +
                        "  FROM orders\n" +
                        "  GROUP BY shippriority)\n" +
                        "\n" +
                        "  SELECT (ctr_total_return)\n" +
                        "  FROM customer_total_return, orders\n" +
                        "  WHERE shippriority = ctr_store_sk",
                anyTree(node(WindowNode.class,
                        anyTree(tableScan("orders", ImmutableMap.of("orderkey", "orderkey", "shippriority", "shippriority"))))));

        // Join(Filter(q), Agg(Filter(q))) -> Window(Filter(q))
        assertPlan("SELECT sc.orderkey,sb.ave\n" +
                        "FROM\n" +
                        "  (SELECT orderkey, AVG(totalprice) AS ave\n" +
                        "   FROM (SELECT orderkey, totalprice\n" +
                        "         FROM orders WHERE custkey BETWEEN 1 AND 100\n" +
                        "   ) sa\n" +
                        "  GROUP BY orderkey) sb,\n" +
                        "(SELECT orderkey, totalprice\n" +
                        "         FROM orders WHERE custkey BETWEEN 1 AND 100\n" +
                        "   ) sc\n" +
                        "WHERE sb.orderkey = sc.orderkey",
                anyTree(
                        node(WindowNode.class,
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey", "O_TOTALPRICE", "totalprice", "O_CUSTKEY", "custkey"))))));

        // Join(Project(q), Agg(Project(q)) -> Window(Project(q))
        assertPlan("SELECT sb.orderkey, sb.ave\n" +
                        "FROM\n" +
                        "  (SELECT orderkey, AVG(custkey) AS ave\n" +
                        "   FROM (SELECT orderkey ,custkey, totalprice+1 as totalprice_plus1\n" +
                        "         FROM orders\n" +
                        "   ) sa\n" +
                        "  GROUP BY orderkey) sb,\n" +
                        "(SELECT orderkey ,totalprice+1 as totalprice_plus1\n" +
                        "         FROM orders\n" +
                        "   ) sc\n" +
                        "WHERE sb.orderkey = sc.orderkey",
                anyTree(
                        node(WindowNode.class,
                                anyTree(
                                        tableScan("orders")))));

        // Join(Agg(q), Agg(Agg(q)) -> Window(Agg(q))
        assertPlan("SELECT sb.orderkey, sb.ave, sc.revenue\n" +
                        "FROM  \n" +
                        "  (SELECT orderkey, AVG(revenue) AS ave\n" +
                        "   FROM (SELECT orderkey, custkey, sum(totalprice) AS revenue\n" +
                        "         FROM orders\n" +
                        "   GROUP BY orderkey, custkey) sa\n" +
                        "  GROUP BY orderkey) sb,\n" +
                        "(SELECT orderkey, custkey, sum(totalprice) AS revenue\n" +
                        "         FROM orders\n" +
                        "   GROUP BY orderkey, custkey) sc\n" +
                        "WHERE sb.orderkey = sc.orderkey",
                anyTree(node(WindowNode.class,
                        anyTree(tableScan("orders")))));

        // Join(Join(p, q), Agg(Join(p, q))) -> Window(Join(p, q))
        assertPlan("SELECT sb.orderkey, sb.ave FROM\n" +
                        "  (SELECT orderkey, AVG(totalprice) AS ave\n" +
                        "   FROM (SELECT orderkey, totalprice\n" +
                        "         FROM orders, customer\n" +
                        "         WHERE orders.custkey = customer.custkey ) sa\n" +
                        "  GROUP BY orderkey) sb,\n" +
                        "(SELECT orderkey, totalprice\n" +
                        "         FROM orders, customer\n" +
                        "         WHERE orders.custkey = customer.custkey ) sc\n" +
                        "WHERE sb.orderkey = sc.orderkey",
                anyTree(
                        node(WindowNode.class,
                                anyTree(join(INNER, builder -> builder
                                        .equiCriteria("C_CUSTKEY", "O_CUSTKEY")
                                        .left(
                                                anyTree(
                                                        tableScan("customer", ImmutableMap.of("C_CUSTKEY", "custkey"))))
                                        .right(
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("O_CUSTKEY", "custkey", "O_ORDERKEY", "orderkey", "O_TOTALPRICE", "totalprice")))))))));

        // Join(q, Agg(q)) -> Window(q) even if the subquery contains more than one aggregation functions
        assertPlan("SELECT 1, o2.*\n" +
                        "FROM orders o1\n" +
                        "JOIN (SELECT orderdate, COUNT(*), MIN(custkey) \n" +
                        "      FROM orders GROUP BY orderdate) o2\n" +
                        "on o1.orderdate = o2.orderdate",
                anyTree(node(WindowNode.class,
                        anyTree(node(TableScanNode.class)))));

        // verify TransformSimpleCorrelatedGlobalAggregation helps facilitate for fusion
        assertPlan("SELECT ctr1.totalprice\n" +
                        "FROM orders ctr1\n" +
                        "WHERE ctr1.totalprice > (\n" +
                        "  SELECT avg(totalprice)\n" +
                        "  FROM orders ctr2\n" +
                        "  WHERE ctr1.orderkey = ctr2.orderkey)",
                anyTree(node(WindowNode.class,
                        anyTree(tableScan("orders")))));
    }

    @Test
    public void testGbJoinGbRule()
    {
        // basic case, the Join should disappear
        assertFalse(
                searchFrom(plan("WITH customer_total_return AS (\n" +
                        "    SELECT shippriority,\n" +
                        "              sum(totalprice) AS ctr_total_price\n" +
                        "      FROM orders\n" +
                        "      GROUP BY shippriority)\n" +
                        "      SELECT c1.ctr_total_price\n" +
                        "      FROM customer_total_return as c1, customer_total_return as c2\n" +
                        "      WHERE c1.shippriority = c2.shippriority", LogicalPlanner.Stage.OPTIMIZED).getRoot())
                        .where(root -> root instanceof JoinNode)
                        .matches());

        // a filter on aggregation
        assertFalse(
                searchFrom(plan("WITH customer_total_return AS (\n" +
                        "    SELECT orderkey ,\n" +
                        "              sum(totalprice) AS ctr_total_price\n" +
                        "      FROM orders\n" +
                        "      GROUP BY orderkey)\n" +
                        "      SELECT c1.orderkey,c1.ctr_total_price, c2.ctr_total_price\n" +
                        "      FROM customer_total_return as c1, customer_total_return as c2\n" +
                        "      WHERE c1.orderkey = c2.orderkey and c2.ctr_total_price > 20000 and c1.ctr_total_price > 30000", LogicalPlanner.Stage.OPTIMIZED).getRoot())
                        .where(root -> root instanceof JoinNode)
                        .matches());

        // a filter, a project on aggregation and extra left expr and right expr
        assertFalse(
                searchFrom(plan("WITH c1 AS (\n" +
                        "    SELECT orderkey,\n" +
                        "              2*sum(totalprice) AS ctr_total_price\n" +
                        "      FROM orders where totalprice>3000\n" +
                        "      GROUP BY orderkey),\n" +
                        "      c2 AS (\n" +
                        "    SELECT orderkey,\n" +
                        "              3*sum(totalprice) AS ctr_total_price\n" +
                        "      FROM orders where totalprice>4000\n" +
                        "      GROUP BY orderkey)\n" +
                        "      SELECT c1.orderkey,c1.ctr_total_price, 2*c2.ctr_total_price\n" +
                        "      FROM c1, c2\n" +
                        "      WHERE c1.orderkey = c2.orderkey and c2.ctr_total_price > 20000 and c1.ctr_total_price>30000", LogicalPlanner.Stage.OPTIMIZED).getRoot())
                        .where(root -> root instanceof JoinNode)
                        .matches());

        // on aggregation
        assertFalse(
                searchFrom(plan("SELECT sb.orderkey, sb.ave, sc.ave\n" +
                        "  FROM  \n" +
                        "    (SELECT orderkey, AVG(revenue) AS ave\n" +
                        "      FROM (SELECT orderkey, custkey, 2*sum(totalprice) AS revenue\n" +
                        "            FROM orders where orderkey>20000\n" +
                        "      GROUP BY orderkey, custkey) where revenue > 20000\n" +
                        "    GROUP BY orderkey) sb,\n" +
                        "    (SELECT orderkey, AVG(revenue) AS ave\n" +
                        "      FROM (SELECT orderkey, custkey, 3*sum(totalprice) AS revenue\n" +
                        "            FROM orders where orderkey>30000\n" +
                        "      GROUP BY orderkey, custkey) where revenue > 40000\n" +
                        "    GROUP BY orderkey) sc\n" +
                        "  WHERE sb.orderkey = sc.orderkey", LogicalPlanner.Stage.OPTIMIZED).getRoot())
                        .where(root -> root instanceof JoinNode)
                        .matches());

        // join with extra left and right expr
        assertPlan("SELECT sb.orderkey, sb.ave\n" +
                        "  FROM  \n" +
                        "    (SELECT orderkey, AVG(totalprice) AS ave\n" +
                        "      FROM (SELECT orders.orderkey, totalprice\n" +
                        "            FROM orders, lineitem\n" +
                        "            WHERE orders.orderkey = lineitem.orderkey and lineitem.linenumber>3 and orders.custkey>500) \n" +
                        "    GROUP BY orderkey) sb,\n" +
                        "     (SELECT orderkey, AVG(totalprice) AS ave\n" +
                        "      FROM (SELECT orders.orderkey, totalprice\n" +
                        "            FROM orders, lineitem \n" +
                        "            WHERE orders.orderkey = lineitem.orderkey and lineitem.linenumber>2 and orders.custkey>1000)\n" +
                        "    GROUP BY orderkey) sc\n" +
                        "  WHERE sb.orderkey = sc.orderkey\n",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("O_ORDERKEY", "L_ORDERKEY")
                                .left(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("O_CUSTKEY", "custkey", "O_ORDERKEY", "orderkey", "O_TOTALPRICE", "totalprice"))))
                                .right(
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey")))))));

        // SGB(Q) CROSS SGB'(Q) -> SGB''(Q)
        assertPlan("SELECT * from (SELECT  avg(totalprice) FROM orders) cross join (SELECT  min(totalprice)   FROM orders)",
                anyTree(aggregation(
                        ImmutableMap.of(
                                "avg_1", PlanMatchPattern.functionCall("avg", ImmutableList.of("O_totalprice")),
                                "min_1", PlanMatchPattern.functionCall("min", ImmutableList.of("O_totalprice"))), PARTIAL,
                        (tableScan("orders", ImmutableMap.of("O_totalprice", "totalprice"))))));
    }

    @Test
    public void testPushUnionBelowJoin()
    {
        assertPlan("select ps.comment\n" +
                        "from partsupp ps\n" +
                        "where ps.suppkey in (select suppkey from supplier)\n" +
                        "union all\n" +
                        "select o.comment\n" +
                        "from orders o\n" +
                        "where o.custkey in (select suppkey from supplier)",
                anyTree(node(JoinNode.class,
                        anyTree(tableScan("supplier")),
                        exchange(
                                anyTree(tableScan("partsupp")),
                                anyTree(tableScan("orders"))))));

        assertPlan("select ps.comment\n" +
                        "from partsupp ps, lineitem l\n" +
                        "where ps.partkey = l.orderkey\n" +
                        "and ps.suppkey in (select suppkey from supplier)\n" +
                        "union all\n" +
                        "select o.comment\n" +
                        "from orders o, lineitem l\n" +
                        "where o.orderkey = l.orderkey\n" +
                        "and o.custkey in (select suppkey from supplier)",
                anyTree(node(JoinNode.class,
                        anyTree(node(JoinNode.class,
                                anyTree(tableScan("supplier")),
                                exchange(
                                        anyTree(tableScan("partsupp")),
                                        anyTree(tableScan("orders"))))),
                        anyTree(tableScan("lineitem")))));

        assertPlan("select ps.comment\n" +
                        "from partsupp ps, lineitem l\n" +
                        "where ps.partkey = l.orderkey\n" +
                        "and ps.suppkey in (select suppkey from supplier)\n" +
                        "and ps.availqty in (select size from part)\n" +
                        "union all\n" +
                        "select o.comment\n" +
                        "from orders o, lineitem l\n" +
                        "where o.orderkey = l.orderkey\n" +
                        "and o.custkey in (select suppkey from supplier)\n" +
                        "and o.shippriority in (select size from part)",
                anyTree(node(JoinNode.class,
                        anyTree(node(JoinNode.class,
                                anyTree(tableScan("supplier")),
                                anyTree(node(JoinNode.class,
                                        anyTree(tableScan("part")),
                                        exchange(
                                                anyTree(tableScan("partsupp")),
                                                anyTree(tableScan("orders"))))))),
                        anyTree(tableScan("lineitem")))));

        assertPlan("select ps.comment\n" +
                        "from partsupp ps, lineitem l\n" +
                        "where ps.partkey = l.orderkey\n" +
                        "union all\n" +
                        "select o.comment\n" +
                        "from orders o, lineitem l\n" +
                        "where o.orderkey = l.orderkey",
                anyTree(node(JoinNode.class,
                        anyTree(tableScan("lineitem")),
                        exchange(
                                anyTree(tableScan("partsupp")),
                                anyTree(tableScan("orders"))))));
    }

    @Test
    public void testFusionPrecedence()
    {
        // Verify MarkDistinctNode takes precedence over FilterNode in tryFuse() when we need to break a tie and decide what compensating node to put on, for which side
        assertFalse(
                searchFrom(plan("SELECT\n" +
                        "(SELECT COUNT(DISTINCT totalprice) / COUNT(orderkey)\n" +
                        "FROM orders\n" +
                        "WHERE custkey = 1),\n" +
                        "(SELECT COUNT(totalprice)\n" +
                        "FROM orders\n" +
                        "WHERE custkey = 2)", LogicalPlanner.Stage.OPTIMIZED).getRoot())
                        .where(root -> root instanceof JoinNode)
                        .matches());

        assertPlan("SELECT\n" +
                        "(SELECT COUNT(DISTINCT totalprice) / COUNT(orderkey)\n" +
                        "FROM orders\n" +
                        "WHERE custkey = 1),\n" +
                        "(SELECT COUNT(totalprice)\n" +
                        "FROM orders\n" +
                        "WHERE custkey = 2)",
                anyTree(node(ProjectNode.class,
                        node(FilterNode.class,
                                node(TableScanNode.class)))));
    }

    @Test
    public void testFusionNegative()
    {
        // We don't support the transformation when subquery is a SSQ
        assertPlan("SELECT * FROM nation\n" +
                        "WHERE nationkey = (SELECT MAX(nationkey) FROM nation)",
                anyTree(node(JoinNode.class,
                        anyTree(tableScan("nation")),
                        anyTree(node(AggregationNode.class,
                                anyTree(tableScan("nation")))))));

        // Verify no fusion is done for outer join branches
        assertPlan("SELECT * FROM\n" +
                        "(\n" +
                        "SELECT orders.orderkey, sum(totalprice) as primary_aggregate\n" +
                        "FROM orders LEFT JOIN lineitem ON orders.orderkey = lineitem.orderkey\n" +
                        "WHERE (totalprice > 3000)\n" +
                        "GROUP BY orders.orderkey\n" +
                        ") AS a\n" +
                        "INNER JOIN\n" +
                        "(\n" +
                        "SELECT orders.orderkey, sum(totalprice) as primary_aggregate\n" +
                        "FROM orders LEFT JOIN lineitem ON orders.orderkey = lineitem.orderkey\n" +
                        "WHERE (totalprice < 4000)\n" +
                        "GROUP BY orders.orderkey\n" +
                        ") AS b\n" +
                        "ON a.orderkey = b.orderkey",
                anyTree(
                        node(JoinNode.class,
                                anyTree(node(JoinNode.class,
                                        anyTree(node(TableScanNode.class)),
                                        anyTree(node(TableScanNode.class)))),
                                anyTree(node(JoinNode.class,
                                        anyTree(node(TableScanNode.class)),
                                        anyTree(node(TableScanNode.class)))))));
    }
}
