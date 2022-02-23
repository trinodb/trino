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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.cost.OptimizerConfig.JoinDistributionType;
import io.trino.cost.OptimizerConfig.JoinReorderingStrategy;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.TableScanNode;
import org.testng.annotations.Test;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.columnReference;
import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictOutput;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPlanMatchingFramework
        extends BasePlanTest
{
    @Test
    public void testOutput()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey FROM lineitem",
                node(OutputNode.class,
                        node(TableScanNode.class).withAlias("ORDERKEY", columnReference("lineitem", "orderkey")))
                        .withOutputs(ImmutableList.of("ORDERKEY")));
    }

    @Test
    public void testOutputSameColumnMultipleTimes()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey, orderkey FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "ORDERKEY"),
                        tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey"))));
    }

    @Test
    public void testOutputSameColumnMultipleTimesWithOtherOutputs()
    {
        assertMinimallyOptimizedPlan("SELECT extendedprice, orderkey, discount, orderkey, linenumber FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "ORDERKEY"),
                        tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey"))));
    }

    @Test
    public void testStrictOutput()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey, extendedprice FROM lineitem",
                strictOutput(ImmutableList.of("ORDERKEY", "EXTENDEDPRICE"),
                        tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey",
                                "EXTENDEDPRICE", "extendedprice"))));
    }

    @Test
    public void testStrictTableScan()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey, extendedprice FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "EXTENDEDPRICE"),
                        strictTableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey",
                                "EXTENDEDPRICE", "extendedprice"))));
    }

    @Test
    public void testUnreferencedSymbolsDontNeedBinding()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey, 2 FROM lineitem",
                output(ImmutableList.of("ORDERKEY"),
                        anyTree(
                                tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey")))));
    }

    @Test
    public void testAliasConstantFromProject()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey, 2 FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "TWO"),
                        project(ImmutableMap.of("TWO", expression("2")),
                                tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey")))));
    }

    @Test
    public void testAliasExpressionFromProject()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey, 1 + orderkey FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "EXPRESSION"),
                        project(ImmutableMap.of("EXPRESSION", expression("CAST(1 AS bigint) + ORDERKEY")),
                                tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey")))));
    }

    @Test
    public void testStrictProject()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey, 1 + orderkey FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "EXPRESSION"),
                        strictProject(ImmutableMap.of("EXPRESSION", expression("CAST(1 AS BIGINT) + ORDERKEY"), "ORDERKEY", expression("ORDERKEY")),
                                tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey")))));
    }

    @Test
    public void testIdentityAliasFromProject()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey, 1 + orderkey FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "EXPRESSION"),
                        project(ImmutableMap.of("ORDERKEY", expression("ORDERKEY"), "EXPRESSION", expression("CAST(1 AS bigint) + ORDERKEY")),
                                tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey")))));
    }

    @Test
    public void testTableScan()
    {
        assertMinimallyOptimizedPlan("SELECT orderkey FROM lineitem",
                output(ImmutableList.of("ORDERKEY"),
                        tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey"))));
    }

    @Test
    public void testJoinMatcher()
    {
        assertPlan(
                "SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey",
                noJoinReordering(),
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                anyTree(
                                        tableScan("orders").withAlias("ORDERS_OK", columnReference("orders", "orderkey"))),
                                anyTree(
                                        tableScan("lineitem").withAlias("LINEITEM_OK", columnReference("lineitem", "orderkey"))))));
    }

    @Test
    public void testSelfJoin()
    {
        assertPlan("SELECT l.orderkey FROM orders l, orders r WHERE l.orderkey = r.orderkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("L_ORDERS_OK", "R_ORDERS_OK")),
                                anyTree(
                                        tableScan("orders").withAlias("L_ORDERS_OK", columnReference("orders", "orderkey"))),
                                anyTree(
                                        tableScan("orders").withAlias("R_ORDERS_OK", columnReference("orders", "orderkey"))))));
    }

    @Test
    public void testAggregation()
    {
        assertMinimallyOptimizedPlan("SELECT COUNT(nationkey) FROM nation",
                output(ImmutableList.of("COUNT"),
                        aggregation(ImmutableMap.of("COUNT", functionCall("count", ImmutableList.of("NATIONKEY"))),
                                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey")))));
    }

    @Test
    public void testValues()
    {
        assertMinimallyOptimizedPlan("SELECT * from (VALUES 1, 2)",
                output(ImmutableList.of("VALUE"),
                        values(ImmutableMap.of("VALUE", 0))));
    }

    @Test
    public void testAliasNonexistentColumn()
    {
        assertThatThrownBy(() -> assertMinimallyOptimizedPlan("SELECT orderkey FROM lineitem",
                node(OutputNode.class,
                        node(TableScanNode.class).withAlias("ORDERKEY", columnReference("lineitem", "NXCOLUMN")))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageMatching(".* doesn't have column .*");
    }

    @Test
    public void testReferenceNonexistentAlias()
    {
        assertThatThrownBy(() -> assertMinimallyOptimizedPlan("SELECT orderkey FROM lineitem",
                output(ImmutableList.of("NXALIAS"),
                        tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey")))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageMatching("missing expression for alias .*");
    }

    @Test
    public void testStrictOutputExtraSymbols()
    {
        assertThatThrownBy(() -> assertMinimallyOptimizedPlan("SELECT orderkey, extendedprice FROM lineitem",
                strictOutput(ImmutableList.of("ORDERKEY"),
                        tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey",
                                "EXTENDEDPRICE", "extendedprice")))))
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("Plan does not match");
    }

    @Test
    public void testStrictTableScanExtraSymbols()
    {
        assertThatThrownBy(() -> assertMinimallyOptimizedPlan("SELECT orderkey, extendedprice FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "EXTENDEDPRICE"),
                        strictTableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey")))))
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("Plan does not match");
    }

    @Test
    public void testStrictProjectExtraSymbols()
    {
        assertThatThrownBy(() -> assertMinimallyOptimizedPlan("SELECT discount, orderkey, 1 + orderkey FROM lineitem",
                output(ImmutableList.of("ORDERKEY", "EXPRESSION"),
                        strictProject(ImmutableMap.of("EXPRESSION", expression("1 + ORDERKEY"), "ORDERKEY", expression("ORDERKEY")),
                                tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey"))))))
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("Plan does not match");
    }

    @Test
    public void testDuplicateAliases()
    {
        assertThatThrownBy(() -> assertPlan(
                "SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey",
                noJoinReordering(),
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("LINEITEM_OK", "ORDERS_OK")),
                                anyTree(
                                        tableScan("orders").withAlias("ORDERS_OK", columnReference("orders", "orderkey"))),
                                anyTree(
                                        tableScan("lineitem").withAlias("ORDERS_OK", columnReference("lineitem", "orderkey")))))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageMatching(".*already bound to expression.*");
    }

    @Test
    public void testProjectLimitsScope()
    {
        assertThatThrownBy(() -> assertMinimallyOptimizedPlan("SELECT 1 + orderkey FROM lineitem",
                output(ImmutableList.of("ORDERKEY"),
                        project(ImmutableMap.of("EXPRESSION", expression("CAST(1 AS bigint) + ORDERKEY")),
                                tableScan("lineitem", ImmutableMap.of("ORDERKEY", "orderkey"))))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageMatching("missing expression for alias .*");
    }

    private Session noJoinReordering()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .build();
    }
}
