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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.cost.StatsProvider;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.assertions.MatchResult;
import io.prestosql.sql.planner.assertions.Matcher;
import io.prestosql.sql.planner.assertions.SymbolAliases;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyNot;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.JoinNode.Type.LEFT;

public class TestDynamicFilter
        extends BasePlanTest
{
    public TestDynamicFilter()
    {
        super(ImmutableMap.of(
                ENABLE_DYNAMIC_FILTERING, "true",
                JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name(),
                JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name()));
    }

    @Test
    public void testNonInnerJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o LEFT JOIN lineitem l ON l.orderkey = o.orderkey",
                anyTree(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                ImmutableMap.of(),
                                project(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                exchange(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testEmptyJoinCriteria()
    {
        assertPlan("SELECT o.orderkey FROM orders o CROSS JOIN lineitem l",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(),
                                ImmutableMap.of(),
                                tableScan("orders"),
                                exchange(
                                        tableScan("lineitem")))));
    }

    @Test
    public void testJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                ImmutableMap.of("ORDERS_OK", "LINEITEM_OK"),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                exchange(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testJoinOnCast()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE cast(l.orderkey as int) = cast(o.orderkey as int)",
                anyTree(
                        node(
                                JoinNode.class,
                                anyTree(
                                        project(
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))));
    }

    @Test
    public void testJoinMultipleEquiJoinClauses()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey AND l.partkey = o.custkey",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(
                                        equiJoinClause("ORDERS_OK", "LINEITEM_OK"),
                                        equiJoinClause("ORDERS_CK", "LINEITEM_PK")),
                                ImmutableMap.of("ORDERS_OK", "LINEITEM_OK", "ORDERS_CK", "LINEITEM_PK"),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey", "ORDERS_CK", "custkey"))),
                                exchange(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey", "LINEITEM_PK", "partkey")))))));
    }

    @Test
    public void testJoinWithOrderBySameKey()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey ORDER BY l.orderkey ASC, o.orderkey ASC",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                ImmutableMap.of("ORDERS_OK", "LINEITEM_OK"),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                exchange(
                                        project(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testUncorrelatedSubqueries()
    {
        assertPlan("SELECT * FROM orders WHERE orderkey = (SELECT orderkey FROM lineitem ORDER BY orderkey LIMIT 1)",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("X", "Y")),
                                ImmutableMap.of("X", "Y"),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                project(
                                        node(
                                                EnforceSingleRowNode.class,
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));
    }

    @Test
    public void testInnerInequalityJoinWithEquiJoinConjuncts()
    {
        assertPlan("SELECT 1 FROM orders o JOIN lineitem l ON o.shippriority = l.linenumber AND o.orderkey < l.orderkey",
                anyTree(
                        anyNot(
                                FilterNode.class,
                                join(
                                        INNER,
                                        ImmutableList.of(equiJoinClause("O_SHIPPRIORITY", "L_LINENUMBER")),
                                        Optional.of("O_ORDERKEY < L_ORDERKEY"),
                                        Optional.of(ImmutableMap.of("O_SHIPPRIORITY", "L_LINENUMBER")),
                                        Optional.empty(),
                                        Optional.empty(),
                                        anyTree(tableScan("orders", ImmutableMap.of(
                                                "O_SHIPPRIORITY", "shippriority",
                                                "O_ORDERKEY", "orderkey"))),
                                        anyTree(tableScan("lineitem", ImmutableMap.of(
                                                "L_LINENUMBER", "linenumber",
                                                "L_ORDERKEY", "orderkey")))))));
    }

    @Test
    public void testSubTreeJoinDFOnProbeSide()
    {
        assertPlan(
                "SELECT part.partkey from part JOIN (lineitem JOIN orders ON lineitem.orderkey = orders.orderkey) ON part.partkey = lineitem.orderkey",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("PART_PK", "LINEITEM_OK")),
                                ImmutableMap.of("PART_PK", "LINEITEM_OK"),
                                anyTree(
                                        tableScan("part", ImmutableMap.of("PART_PK", "partkey"))),
                                anyTree(
                                        join(
                                                INNER,
                                                ImmutableList.of(equiJoinClause("LINEITEM_OK", "ORDERS_OK")),
                                                ImmutableMap.of("LINEITEM_OK", "ORDERS_OK"),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))),
                                                exchange(
                                                        project(tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))))))));
    }

    @Test
    public void testSubTreeJoinDFOnBuildSide()
    {
        assertPlan(
                "SELECT part.partkey from (lineitem JOIN orders ON lineitem.orderkey = orders.orderkey) JOIN part ON lineitem.orderkey = part.partkey",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("LINEITEM_OK", "PART_PK")),
                                ImmutableMap.of("LINEITEM_OK", "PART_PK", "ORDERS_OK", "PART_PK"),
                                join(
                                        INNER,
                                        ImmutableList.of(equiJoinClause("LINEITEM_OK", "ORDERS_OK")),
                                        ImmutableMap.of("LINEITEM_OK", "ORDERS_OK"),
                                        anyTree(node(FilterNode.class,
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))
                                                .with(numberOfDynamicFilters(2))),
                                        anyTree(node(FilterNode.class,
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))
                                                .with(numberOfDynamicFilters(1)))),
                                exchange(
                                        project(tableScan("part", ImmutableMap.of("PART_PK", "partkey")))))));
    }

    @Test
    public void testNestedDynamicFiltersRemoval()
    {
        assertPlan(
                "WITH t AS (" +
                        "  SELECT o.clerk FROM (" +
                        "    (orders o LEFT JOIN orders o1 ON o1.clerk = o.clerk) " +
                        "      LEFT JOIN orders o2 ON o2.clerk = o1.clerk)" +
                        ") " +
                        "SELECT t.clerk " +
                        "FROM orders o3 JOIN t ON t.clerk = o3.clerk",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("ORDERS_CK", "ORDERS_CK6")),
                                ImmutableMap.of("ORDERS_CK", "ORDERS_CK6"),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("ORDERS_CK", "clerk"))),
                                anyTree(
                                        join(
                                                LEFT,
                                                ImmutableList.of(equiJoinClause("ORDERS_CK16", "ORDERS_CK27")),
                                                anyTree(
                                                        join(
                                                                LEFT,
                                                                ImmutableList.of(equiJoinClause("ORDERS_CK6", "ORDERS_CK16")),
                                                                project(
                                                                        tableScan("orders", ImmutableMap.of("ORDERS_CK6", "clerk"))),
                                                                exchange(
                                                                        project(
                                                                                tableScan("orders", ImmutableMap.of("ORDERS_CK16", "clerk")))))),
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("ORDERS_CK27", "clerk"))))))));
    }

    @Test
    public void testNonPushedDownJoinFilterRemoval()
    {
        assertPlan(
                "SELECT 1 FROM part t0, part t1, part t2 " +
                        "WHERE t0.partkey = t1.partkey AND t0.partkey = t2.partkey " +
                        "AND t0.size + t1.size = t2.size",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("K0", "K2"), equiJoinClause("S", "V2")),
                                Optional.empty(), // Should be ImmutableMap.of("K0", "K2", "K1", "K2") but K1 symbol is not available when matching current join node
                                project(
                                        project(
                                                ImmutableMap.of("S", expression("V0 + V1")),
                                                join(
                                                        INNER,
                                                        ImmutableList.of(equiJoinClause("K0", "K1")),
                                                        ImmutableMap.of("K0", "K1"),
                                                        project(
                                                                node(
                                                                        FilterNode.class,
                                                                        tableScan("part", ImmutableMap.of("K0", "partkey", "V0", "size")))
                                                                        .with(numberOfDynamicFilters(2))),
                                                        exchange(
                                                                project(
                                                                        node(
                                                                                FilterNode.class,
                                                                                tableScan("part", ImmutableMap.of("K1", "partkey", "V1", "size")))
                                                                                .with(numberOfDynamicFilters(1))))))),
                                exchange(
                                        project(
                                                tableScan("part", ImmutableMap.of("K2", "partkey", "V2", "size")))))));
    }

    @Test
    public void testSemiJoin()
    {
        assertPlan(
                "SELECT * FROM orders WHERE orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber % 4 = 0)",
                anyTree(
                        filter("S",
                                project(
                                        semiJoin("X", "Y", "S", true,
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));
    }

    @Test
    public void testNonFilteringSemiJoin()
    {
        // Dynamic filtering is not applied to non-filtering semi-join queries
        assertPlan(
                "SELECT * FROM orders WHERE orderkey NOT IN (SELECT orderkey FROM lineitem WHERE linenumber < 0)",
                anyTree(
                        filter("NOT S",
                                project(
                                        semiJoin("X", "Y", "S", false,
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));

        assertPlan(
                "SELECT orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber < 0) FROM orders",
                anyTree(
                        semiJoin("X", "Y", "S", false,
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))));
    }

    @Test
    public void testSemiJoinWithStaticFiltering()
    {
        assertPlan(
                "SELECT * FROM orders WHERE orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber % 4 = 0) AND orderkey > 0",
                anyTree(
                        filter("S",
                                project(
                                        semiJoin("X", "Y", "S", true,
                                                anyTree(
                                                        filter("X > BIGINT '0'",
                                                                tableScan("orders", ImmutableMap.of("X", "orderkey")))),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));
    }

    @Test
    public void testMultiSemiJoin()
    {
        assertPlan(
                "SELECT part.partkey FROM part WHERE part.partkey IN " +
                        "(SELECT lineitem.partkey FROM lineitem WHERE lineitem.orderkey IN (SELECT orders.orderkey FROM orders))",
                anyTree(
                        filter("S0",
                                project(
                                        semiJoin("PART_PK", "LINEITEM_PK", "S0", true,
                                                anyTree(
                                                        tableScan("part", ImmutableMap.of("PART_PK", "partkey"))),
                                                anyTree(
                                                        filter("S1",
                                                                project(
                                                                        semiJoin("LINEITEM_OK", "ORDERS_OK", "S1", true,
                                                                                anyTree(
                                                                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_PK", "partkey", "LINEITEM_OK", "orderkey"))),
                                                                                anyTree(
                                                                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))))))))))));
    }

    @Test
    public void testSemiJoinUnsupportedDynamicFilterRemoval()
    {
        // Dynamic filters are supported only after a table scan
        assertPlan(
                "WITH t AS (SELECT lineitem.partkey + 1000 partkey FROM lineitem) " +
                        "SELECT t.partkey FROM t WHERE t.partkey IN (SELECT part.partkey FROM part)",
                anyTree(
                        filter("S0",
                                project(
                                        semiJoin("LINEITEM_PK_PLUS_1000", "PART_PK", "S0", false,
                                                anyTree(
                                                        project(ImmutableMap.of("LINEITEM_PK_PLUS_1000", expression("(LINEITEM_PK + BIGINT '1000')")),
                                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_PK", "partkey")))),
                                                anyTree(
                                                        tableScan("part", ImmutableMap.of("PART_PK", "partkey"))))))));
    }

    private Matcher numberOfDynamicFilters(int numberOfDynamicFilters)
    {
        return new Matcher()
        {
            @Override
            public boolean shapeMatches(PlanNode node)
            {
                return node instanceof FilterNode;
            }

            @Override
            public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
            {
                FilterNode filterNode = (FilterNode) node;
                return new MatchResult(DynamicFilters.extractDynamicFilters(filterNode.getPredicate()).getDynamicConjuncts().size() == numberOfDynamicFilters);
            }
        };
    }
}
