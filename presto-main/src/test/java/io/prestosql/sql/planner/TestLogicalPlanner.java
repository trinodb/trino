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
import io.prestosql.spi.block.SortOrder;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.assertions.ExpressionMatcher;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.assertions.RowNumberSymbolMatcher;
import io.prestosql.sql.planner.optimizations.AddLocalExchanges;
import io.prestosql.sql.planner.optimizations.CheckSubqueryNodesAreRewritten;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.CorrelatedJoinNode;
import io.prestosql.sql.planner.plan.DistinctLimitNode;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.IndexJoinNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.MarkDistinctNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.StatisticsWriterNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.TopNNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.tests.QueryTemplate;
import io.prestosql.util.MorePredicates;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.SystemSessionProperties.DISTRIBUTED_SORT;
import static io.prestosql.SystemSessionProperties.FORCE_SINGLE_NODE_OUTPUT;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static io.prestosql.SystemSessionProperties.TASK_CONCURRENCY;
import static io.prestosql.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static io.prestosql.spi.predicate.Domain.singleValue;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.any;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyNot;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.apply;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.constrainedTableScan;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.constrainedTableScanWithTableLayout;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.enforceSingleRow;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.identityProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.join;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.limit;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.markDistinct;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.output;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.sort;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.specification;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.topN;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.topNRowNumber;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.window;
import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.prestosql.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.prestosql.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.JoinNode.Type.LEFT;
import static io.prestosql.sql.tree.SortItem.NullOrdering.LAST;
import static io.prestosql.sql.tree.SortItem.Ordering.ASCENDING;
import static io.prestosql.sql.tree.SortItem.Ordering.DESCENDING;
import static io.prestosql.tests.QueryTemplate.queryTemplate;
import static io.prestosql.util.MorePredicates.isInstanceOfAny;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestLogicalPlanner
        extends BasePlanTest
{
    @Test
    public void testAnalyze()
    {
        assertDistributedPlan("ANALYZE orders",
                anyTree(
                        node(StatisticsWriterNode.class,
                                anyTree(
                                        exchange(REMOTE, GATHER,
                                                node(AggregationNode.class,
                                                        anyTree(
                                                                exchange(REMOTE, GATHER,
                                                                        node(AggregationNode.class,
                                                                                tableScan("orders", ImmutableMap.of()))))))))));
    }

    @Test
    public void testAggregation()
    {
        // simple group by
        assertDistributedPlan("SELECT orderstatus, sum(totalprice) FROM orders GROUP BY orderstatus",
                anyTree(
                        aggregation(
                                ImmutableMap.of("final_sum", functionCall("sum", ImmutableList.of("partial_sum"))),
                                FINAL,
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE, REPARTITION,
                                                aggregation(
                                                        ImmutableMap.of("partial_sum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                        PARTIAL,
                                                        anyTree(tableScan("orders", ImmutableMap.of("totalprice", "totalprice")))))))));

        // simple group by over filter that keeps at most one group
        assertDistributedPlan("SELECT orderstatus, sum(totalprice) FROM orders WHERE orderstatus='O' GROUP BY orderstatus",
                anyTree(
                        aggregation(
                                ImmutableMap.of("final_sum", functionCall("sum", ImmutableList.of("partial_sum"))),
                                FINAL,
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE, REPARTITION,
                                                aggregation(
                                                        ImmutableMap.of("partial_sum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                        PARTIAL,
                                                        anyTree(tableScan("orders", ImmutableMap.of("totalprice", "totalprice")))))))));
    }

    @Test
    public void testAllFieldsDereferenceOnSubquery()
    {
        assertPlan("SELECT (SELECT (min(regionkey), max(name)) FROM nation).*",
                any(
                        project(
                                ImmutableMap.of(
                                        "output_1", expression("CAST(\"row\" AS ROW(f0 bigint,f1 varchar(25))).f0"),
                                        "output_2", expression("CAST(\"row\" AS ROW(f0 bigint,f1 varchar(25))).f1")),
                                enforceSingleRow(
                                        project(
                                                ImmutableMap.of("row", expression("ROW(min, max)")),
                                                aggregation(
                                                        ImmutableMap.of(
                                                                "min", functionCall("min", ImmutableList.of("min_regionkey")),
                                                                "max", functionCall("max", ImmutableList.of("max_name"))),
                                                        FINAL,
                                                        any(
                                                                aggregation(
                                                                        ImmutableMap.of(
                                                                                "min_regionkey", functionCall("min", ImmutableList.of("REGIONKEY")),
                                                                                "max_name", functionCall("max", ImmutableList.of("NAME"))),
                                                                        PARTIAL,
                                                                        tableScan("nation", ImmutableMap.of("NAME", "name", "REGIONKEY", "regionkey"))))))))));
    }

    @Test
    public void testAllFieldsDereferenceFromNonDeterministic()
    {
        assertPlan("SELECT (x, x).* FROM (SELECT rand()) T(x)",
                any(
                        project(
                                ImmutableMap.of(
                                        "output_1", expression("CAST(row AS ROW(f0 double,f1 double)).f0"),
                                        "output_2", expression("CAST(row AS ROW(f0 double,f1 double)).f1")),
                                project(
                                        ImmutableMap.of("row", expression("ROW(\"rand\", \"rand\")")),
                                        project(
                                                ImmutableMap.of("rand", expression("rand()")),
                                                values())))));
    }

    @Test
    public void testDistinctLimitOverInequalityJoin()
    {
        assertPlan("SELECT DISTINCT o.orderkey FROM orders o JOIN lineitem l ON o.orderkey < l.orderkey LIMIT 1",
                anyTree(
                        node(DistinctLimitNode.class,
                                anyTree(
                                        filter("O_ORDERKEY < L_ORDERKEY",
                                                join(INNER, ImmutableList.of(), Optional.empty(),
                                                        tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey")),
                                                        any(tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey"))))
                                                        .withExactOutputs(ImmutableList.of("O_ORDERKEY", "L_ORDERKEY")))))));

        assertPlan("SELECT DISTINCT o.orderkey FROM orders o JOIN lineitem l ON o.shippriority = l.linenumber AND o.orderkey < l.orderkey LIMIT 1",
                anyTree(
                        node(DistinctLimitNode.class,
                                anyTree(
                                        join(INNER,
                                                ImmutableList.of(equiJoinClause("O_SHIPPRIORITY", "L_LINENUMBER")),
                                                Optional.of("O_ORDERKEY < L_ORDERKEY"),
                                                any(tableScan("orders", ImmutableMap.of(
                                                        "O_SHIPPRIORITY", "shippriority",
                                                        "O_ORDERKEY", "orderkey"))),
                                                anyTree(tableScan("lineitem", ImmutableMap.of(
                                                        "L_LINENUMBER", "linenumber",
                                                        "L_ORDERKEY", "orderkey"))))
                                                .withExactOutputs(ImmutableList.of("O_ORDERKEY"))))));
    }

    @Test
    public void testDistinctOverConstants()
    {
        assertPlan("SELECT count(*), count(distinct orderstatus) FROM (SELECT * FROM orders WHERE orderstatus = 'F')",
                anyTree(
                        markDistinct(
                                "is_distinct",
                                ImmutableList.of("orderstatus"),
                                "hash",
                                anyTree(
                                        project(ImmutableMap.of("hash", expression("combine_hash(bigint '0', coalesce(\"$operator$hash_code\"(orderstatus), 0))")),
                                                tableScan("orders", ImmutableMap.of("orderstatus", "orderstatus")))))));
    }

    @Test
    public void testInnerInequalityJoinNoEquiJoinConjuncts()
    {
        assertPlan("SELECT 1 FROM orders o JOIN lineitem l ON o.orderkey < l.orderkey",
                anyTree(
                        filter("O_ORDERKEY < L_ORDERKEY",
                                join(INNER, ImmutableList.of(), Optional.empty(),
                                        tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey")),
                                        any(tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey")))))));
    }

    @Test
    public void testInnerInequalityJoinWithEquiJoinConjuncts()
    {
        assertPlan("SELECT 1 FROM orders o JOIN lineitem l ON o.shippriority = l.linenumber AND o.orderkey < l.orderkey",
                anyTree(
                        anyNot(FilterNode.class,
                                join(INNER,
                                        ImmutableList.of(equiJoinClause("O_SHIPPRIORITY", "L_LINENUMBER")),
                                        Optional.of("O_ORDERKEY < L_ORDERKEY"),
                                        any(tableScan("orders", ImmutableMap.of(
                                                "O_SHIPPRIORITY", "shippriority",
                                                "O_ORDERKEY", "orderkey"))),
                                        anyTree(tableScan("lineitem", ImmutableMap.of(
                                                "L_LINENUMBER", "linenumber",
                                                "L_ORDERKEY", "orderkey")))))));
    }

    @Test
    public void testLeftConvertedToInnerInequalityJoinNoEquiJoinConjuncts()
    {
        assertPlan("SELECT 1 FROM orders o LEFT JOIN lineitem l ON o.orderkey < l.orderkey WHERE l.orderkey IS NOT NULL",
                anyTree(
                        filter("O_ORDERKEY < L_ORDERKEY",
                                join(INNER, ImmutableList.of(), Optional.empty(),
                                        tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey")),
                                        any(
                                                filter("NOT (L_ORDERKEY IS NULL)",
                                                        tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey"))))))));
    }

    @Test
    public void testJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                any(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))));
    }

    @Test
    public void testJoinWithOrderBySameKey()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey ORDER BY l.orderkey ASC, o.orderkey ASC",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("ORDERS_OK", "LINEITEM_OK")),
                                any(
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                anyTree(
                                        tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))));
    }

    @Test
    public void testTopNPushdownToJoinSource()
    {
        assertPlan("SELECT n.name, r.name FROM nation n LEFT JOIN region r ON n.regionkey = r.regionkey ORDER BY n.comment LIMIT 1",
                anyTree(
                        project(
                                topN(1, ImmutableList.of(sort("N_COMM", ASCENDING, LAST)), TopNNode.Step.FINAL,
                                        anyTree(
                                                join(LEFT, ImmutableList.of(equiJoinClause("N_KEY", "R_KEY")),
                                                        project(
                                                                topN(1, ImmutableList.of(sort("N_COMM", ASCENDING, LAST)), TopNNode.Step.PARTIAL,
                                                                        tableScan("nation", ImmutableMap.of("N_NAME", "name", "N_KEY", "regionkey", "N_COMM", "comment")))),
                                                        anyTree(
                                                                tableScan("region", ImmutableMap.of("R_NAME", "name", "R_KEY", "regionkey")))))))));
    }

    @Test
    public void testUncorrelatedSubqueries()
    {
        assertPlan("SELECT * FROM orders WHERE orderkey = (SELECT orderkey FROM lineitem ORDER BY orderkey LIMIT 1)",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("X", "Y")),
                                project(
                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                project(
                                        node(EnforceSingleRowNode.class,
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));

        assertPlan("SELECT * FROM orders WHERE orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber % 4 = 0)",
                anyTree(
                        filter("S",
                                project(
                                        semiJoin("X", "Y", "S",
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));

        assertPlan("SELECT * FROM orders WHERE orderkey NOT IN (SELECT orderkey FROM lineitem WHERE linenumber < 0)",
                anyTree(
                        filter("NOT S",
                                project(
                                        semiJoin("X", "Y", "S",
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));
    }

    @Test
    public void testPushDownJoinConditionConjunctsToInnerSideBasedOnInheritedPredicate()
    {
        assertPlan(
                "SELECT nationkey FROM nation LEFT OUTER JOIN region " +
                        "ON nation.regionkey = region.regionkey and nation.name = region.name WHERE nation.name = 'blah'",
                anyTree(
                        join(LEFT, ImmutableList.of(equiJoinClause("NATION_NAME", "REGION_NAME"), equiJoinClause("NATION_REGIONKEY", "REGION_REGIONKEY")),
                                anyTree(
                                        filter("NATION_NAME = CAST ('blah' AS varchar(25))",
                                                constrainedTableScan(
                                                        "nation",
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(
                                                                "NATION_NAME", "name",
                                                                "NATION_REGIONKEY", "regionkey")))),
                                anyTree(
                                        filter("REGION_NAME = CAST ('blah' AS varchar(25))",
                                                constrainedTableScan(
                                                        "region",
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(
                                                                "REGION_NAME", "name",
                                                                "REGION_REGIONKEY", "regionkey")))))));
    }

    @Test
    public void testSameScalarSubqueryIsAppliedOnlyOnce()
    {
        // three subqueries with two duplicates (coerced to two different types), only two scalar joins should be in plan
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT * FROM orders WHERE CAST(orderkey AS INTEGER) = (SELECT 1) AND custkey = (SELECT 2) AND CAST(custkey as REAL) != (SELECT 1)"),
                        EnforceSingleRowNode.class::isInstance),
                2);
        // same query used for left, right and complex join condition
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey = (SELECT 1) AND o2.orderkey = (SELECT 1) AND o1.orderkey + o2.orderkey = (SELECT 1)"),
                        EnforceSingleRowNode.class::isInstance),
                1);
    }

    @Test
    public void testSameInSubqueryIsAppliedOnlyOnce()
    {
        // same IN query used for left, right and complex condition
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey IN (SELECT 1) AND (o1.orderkey IN (SELECT 1) OR o1.orderkey IN (SELECT 1))"),
                        SemiJoinNode.class::isInstance),
                1);

        // one subquery used for "1 IN (SELECT 1)", one subquery used for "2 IN (SELECT 1)"
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT 1 IN (SELECT 1), 2 IN (SELECT 1) WHERE 1 IN (SELECT 1)"),
                        SemiJoinNode.class::isInstance),
                2);
    }

    @Test
    public void testSameQualifiedSubqueryIsAppliedOnlyOnce()
    {
        // same ALL query used for left, right and complex condition
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey <= ALL(SELECT 1) AND (o1.orderkey <= ALL(SELECT 1) OR o1.orderkey <= ALL(SELECT 1))"),
                        AggregationNode.class::isInstance),
                1);

        // one subquery used for "1 <= ALL(SELECT 1)", one subquery used for "2 <= ALL(SELECT 1)"
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT 1 <= ALL(SELECT 1), 2 <= ALL(SELECT 1) WHERE 1 <= ALL(SELECT 1)"),
                        AggregationNode.class::isInstance),
                2);
    }

    private static int countOfMatchingNodes(Plan plan, Predicate<PlanNode> predicate)
    {
        return searchFrom(plan.getRoot()).where(predicate).count();
    }

    @Test
    public void testRemoveUnreferencedScalarInputApplyNodes()
    {
        assertPlanContainsNoApplyOrAnyJoin("SELECT (SELECT 1)");
    }

    @Test
    public void testSubqueryPruning()
    {
        List<QueryTemplate.Parameter> subqueries = QueryTemplate.parameter("subquery").of(
                "orderkey IN (SELECT orderkey FROM lineitem WHERE orderkey % 2 = 0)",
                "EXISTS(SELECT orderkey FROM lineitem WHERE orderkey % 2 = 0)",
                "0 = (SELECT orderkey FROM lineitem WHERE orderkey % 2 = 0)");

        queryTemplate("SELECT COUNT(*) FROM (SELECT %subquery% FROM orders)")
                .replaceAll(subqueries)
                .forEach(this::assertPlanContainsNoApplyOrAnyJoin);

        queryTemplate("SELECT * FROM orders WHERE true OR %subquery%")
                .replaceAll(subqueries)
                .forEach(this::assertPlanContainsNoApplyOrAnyJoin);
    }

    @Test
    public void testJoinOutputPruning()
    {
        assertPlan("SELECT nationkey FROM nation JOIN region ON nation.regionkey = region.regionkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("REGIONKEY_LEFT", "REGIONKEY_RIGHT")),
                                anyTree(
                                        tableScan("nation", ImmutableMap.of("REGIONKEY_LEFT", "regionkey", "NATIONKEY", "nationkey"))),
                                anyTree(
                                        tableScan("region", ImmutableMap.of("REGIONKEY_RIGHT", "regionkey")))))
                        .withNumberOfOutputColumns(1)
                        .withOutputs(ImmutableList.of("NATIONKEY")));
    }

    private void assertPlanContainsNoApplyOrAnyJoin(String sql)
    {
        assertPlanDoesNotContain(sql, ApplyNode.class, JoinNode.class, IndexJoinNode.class, SemiJoinNode.class, CorrelatedJoinNode.class);
    }

    private void assertPlanDoesNotContain(String sql, Class<?>... classes)
    {
        assertFalse(
                searchFrom(plan(sql, OPTIMIZED).getRoot())
                        .where(isInstanceOfAny(classes))
                        .matches(),
                "Unexpected node for query: " + sql);
    }

    @Test
    public void testCorrelatedSubqueries()
    {
        assertPlan(
                "SELECT orderkey FROM orders WHERE 3 = (SELECT orderkey)",
                OPTIMIZED,
                any(
                        filter(
                                "X = BIGINT '3'",
                                tableScan("orders", ImmutableMap.of("X", "orderkey")))));
    }

    @Test
    public void testCorrelatedJoinWithLimit()
    {
        // rewrite Limit to RowNumberNode
        assertPlan(
                "SELECT regionkey, n.name FROM region LEFT JOIN LATERAL (SELECT name FROM nation WHERE region.regionkey = regionkey LIMIT 2) n ON TRUE",
                any(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("region_regionkey", "nation_regionkey")),
                                any(tableScan("region", ImmutableMap.of("region_regionkey", "regionkey"))),
                                any(rowNumber(
                                        pattern -> pattern
                                                .partitionBy(ImmutableList.of("nation_regionkey"))
                                                .maxRowCountPerPartition(Optional.of(2)),
                                        anyTree(tableScan("nation", ImmutableMap.of("nation_name", "name", "nation_regionkey", "regionkey"))))))));

        // rewrite Limit to decorrelated Limit
        assertPlan("SELECT regionkey, n.nationkey FROM region LEFT JOIN LATERAL (SELECT nationkey FROM nation WHERE region.regionkey = 3 LIMIT 2) n ON TRUE",
                any(
                        join(
                                LEFT,
                                ImmutableList.of(),
                                Optional.of("region_regionkey = BIGINT '3'"),
                                tableScan("region", ImmutableMap.of("region_regionkey", "regionkey")),
                                limit(
                                        2,
                                        any(tableScan("nation", ImmutableMap.of("nation_nationkey", "nationkey")))))));
    }

    @Test
    public void testCorrelatedJoinWithTopN()
    {
        // rewrite TopN to TopNRowNumberNode
        assertPlan(
                "SELECT regionkey, n.name FROM region LEFT JOIN LATERAL (SELECT name FROM nation WHERE region.regionkey = regionkey ORDER BY name LIMIT 2) n ON TRUE",
                any(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("region_regionkey", "nation_regionkey")),
                                any(tableScan("region", ImmutableMap.of("region_regionkey", "regionkey"))),
                                any(topNRowNumber(
                                        pattern -> pattern
                                                .specification(
                                                        ImmutableList.of("nation_regionkey"),
                                                        ImmutableList.of("nation_name"),
                                                        ImmutableMap.of("nation_name", SortOrder.ASC_NULLS_LAST))
                                                .maxRowCountPerPartition(2)
                                                .partial(false),
                                        anyTree(tableScan("nation", ImmutableMap.of("nation_name", "name", "nation_regionkey", "regionkey"))))))));

        // rewrite TopN to RowNumberNode
        assertPlan(
                "SELECT regionkey, n.name FROM region LEFT JOIN LATERAL (SELECT name FROM nation WHERE region.regionkey = regionkey ORDER BY regionkey LIMIT 2) n ON TRUE",
                any(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("region_regionkey", "nation_regionkey")),
                                any(tableScan("region", ImmutableMap.of("region_regionkey", "regionkey"))),
                                any(rowNumber(
                                        pattern -> pattern
                                                .partitionBy(ImmutableList.of("nation_regionkey"))
                                                .maxRowCountPerPartition(Optional.of(2)),
                                        anyTree(tableScan("nation", ImmutableMap.of("nation_name", "name", "nation_regionkey", "regionkey"))))))));
    }

    @Test
    public void testCorrelatedScalarSubqueryInSelect()
    {
        assertDistributedPlan("SELECT name, (SELECT name FROM region WHERE regionkey = nation.regionkey) FROM nation",
                anyTree(
                        filter(format("CASE \"is_distinct\" WHEN true THEN true ELSE CAST(fail(%s, 'Scalar sub-query has returned multiple rows') AS boolean) END", SUBQUERY_MULTIPLE_ROWS.toErrorCode().getCode()),
                                markDistinct("is_distinct", ImmutableList.of("unique"),
                                        join(LEFT, ImmutableList.of(equiJoinClause("n_regionkey", "r_regionkey")),
                                                assignUniqueId("unique",
                                                        exchange(REMOTE, REPARTITION,
                                                                anyTree(tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey"))))),
                                                anyTree(
                                                        tableScan("region", ImmutableMap.of("r_regionkey", "regionkey"))))))));
    }

    @Test
    public void testStreamingAggregationForCorrelatedSubquery()
    {
        // Use equi-clause to trigger hash partitioning of the join sources
        assertDistributedPlan(
                "SELECT name, (SELECT max(name) FROM region WHERE regionkey = nation.regionkey AND length(name) > length(nation.name)) FROM nation",
                anyTree(
                        aggregation(
                                singleGroupingSet("n_name", "n_regionkey", "unique"),
                                ImmutableMap.of(Optional.of("max"), functionCall("max", ImmutableList.of("r_name"))),
                                ImmutableList.of("n_name", "n_regionkey", "unique"),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                node(JoinNode.class,
                                        assignUniqueId("unique",
                                                exchange(REMOTE, REPARTITION,
                                                        anyTree(
                                                                tableScan("nation", ImmutableMap.of("n_name", "name", "n_regionkey", "regionkey"))))),
                                        anyTree(
                                                tableScan("region", ImmutableMap.of("r_name", "name")))))));

        // Don't use equi-clauses to trigger replicated join
        assertDistributedPlan(
                "SELECT name, (SELECT max(name) FROM region WHERE regionkey > nation.regionkey) FROM nation",
                anyTree(
                        aggregation(
                                singleGroupingSet("n_name", "n_regionkey", "unique"),
                                ImmutableMap.of(Optional.of("max"), functionCall("max", ImmutableList.of("r_name"))),
                                ImmutableList.of("n_name", "n_regionkey", "unique"),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                node(JoinNode.class,
                                        assignUniqueId("unique",
                                                tableScan("nation", ImmutableMap.of("n_name", "name", "n_regionkey", "regionkey"))),
                                        anyTree(
                                                tableScan("region", ImmutableMap.of("r_name", "name")))))));
    }

    @Test
    public void testStreamingAggregationOverJoin()
    {
        // "orders" table is naturally grouped on orderkey
        // this grouping should survive inner and left joins and allow for streaming aggregation later
        // this grouping should not survive a cross join

        // inner join -> streaming aggregation
        assertPlan("SELECT o.orderkey, count(*) FROM orders o, lineitem l WHERE o.orderkey=l.orderkey GROUP BY 1",
                anyTree(
                        aggregation(
                                singleGroupingSet("o_orderkey"),
                                ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                                ImmutableList.of("o_orderkey"), // streaming
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                join(INNER, ImmutableList.of(equiJoinClause("o_orderkey", "l_orderkey")),
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("o_orderkey", "orderkey"))),
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey")))))));

        // left join -> streaming aggregation
        assertPlan("SELECT o.orderkey, count(*) FROM orders o LEFT JOIN lineitem l ON o.orderkey=l.orderkey GROUP BY 1",
                anyTree(
                        aggregation(
                                singleGroupingSet("o_orderkey"),
                                ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                                ImmutableList.of("o_orderkey"), // streaming
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                join(LEFT, ImmutableList.of(equiJoinClause("o_orderkey", "l_orderkey")),
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("o_orderkey", "orderkey"))),
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey")))))));

        // cross join - no streaming
        assertPlan("SELECT o.orderkey, count(*) FROM orders o, lineitem l GROUP BY 1",
                anyTree(
                        aggregation(
                                singleGroupingSet("orderkey"),
                                ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                                ImmutableList.of(), // not streaming
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                join(INNER, ImmutableList.of(),
                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey")),
                                        anyTree(
                                                node(TableScanNode.class))))));
    }

    /**
     * Handling of correlated IN pulls up everything possible to the generated outer join condition.
     * This test ensures uncorrelated conditions are pushed back down.
     */
    @Test
    public void testCorrelatedInUncorrelatedFiltersPushDown()
    {
        assertPlan(
                "SELECT orderkey, comment IN (SELECT clerk FROM orders s WHERE s.orderkey = o.orderkey AND s.orderkey < 7) FROM lineitem o",
                anyTree(
                        node(JoinNode.class,
                                anyTree(tableScan("lineitem")),
                                anyTree(
                                        filter("orderkey < BIGINT '7'", // pushed down
                                                tableScan("orders", ImmutableMap.of("orderkey", "orderkey")))))));
    }

    /**
     * Handling of correlated in predicate involves group by over all symbols from source. Once aggregation is added to the plan,
     * it prevents pruning of the unreferenced symbols. However, the aggregation's result doesn't actually depended on those symbols
     * and this test makes sure the symbols are pruned first.
     */
    @Test
    public void testSymbolsPrunedInCorrelatedInPredicateSource()
    {
        assertPlan(
                "SELECT orderkey, comment IN (SELECT clerk FROM orders s WHERE s.orderkey = o.orderkey AND s.orderkey < 7) FROM lineitem o",
                anyTree(
                        node(JoinNode.class,
                                anyTree(strictTableScan("lineitem", ImmutableMap.of(
                                        "orderkey", "orderkey",
                                        "comment", "comment"))),
                                anyTree(tableScan("orders")))));
    }

    @Test
    public void testDoubleNestedCorrelatedSubqueries()
    {
        assertPlan(
                "SELECT orderkey FROM orders o " +
                        "WHERE 3 IN (SELECT o.custkey FROM lineitem l WHERE (SELECT l.orderkey = o.orderkey))",
                OPTIMIZED,
                anyTree(
                        filter("OUTER_FILTER",
                                apply(ImmutableList.of("C", "O"),
                                        ImmutableMap.of("OUTER_FILTER", expression("THREE IN (C)")),
                                        project(ImmutableMap.of("THREE", expression("BIGINT '3'")),
                                                tableScan("orders", ImmutableMap.of(
                                                        "O", "orderkey",
                                                        "C", "custkey"))),
                                        project(
                                                any(
                                                        any(
                                                                tableScan("lineitem", ImmutableMap.of("L", "orderkey")))))))),
                MorePredicates.<PlanOptimizer>isInstanceOfAny(AddLocalExchanges.class, CheckSubqueryNodesAreRewritten.class).negate());
    }

    @Test
    public void testCorrelatedScalarAggregationRewriteToLeftOuterJoin()
    {
        assertPlan(
                "SELECT orderkey FROM orders WHERE EXISTS(SELECT 1 WHERE orderkey = 3)", // EXISTS maps to count(*) > 0
                anyTree(
                        filter("FINAL_COUNT > BIGINT '0'",
                                aggregation(ImmutableMap.of("FINAL_COUNT", functionCall("count", ImmutableList.of("NON_NULL"))),
                                        join(LEFT, ImmutableList.of(), Optional.of("BIGINT '3' = ORDERKEY"),
                                                any(
                                                        tableScan("orders", ImmutableMap.of("ORDERKEY", "orderkey"))),
                                                project(ImmutableMap.of("NON_NULL", expression("true")),
                                                        node(ValuesNode.class)))))));
    }

    @Test
    public void testRemovesTrivialFilters()
    {
        assertPlan(
                "SELECT * FROM nation WHERE 1 = 1",
                output(
                        tableScan("nation")));
        assertPlan(
                "SELECT * FROM nation WHERE 1 = 0",
                output(
                        values("nationkey", "name", "regionkey", "comment")));
    }

    @Test
    public void testPruneCountAggregationOverScalar()
    {
        assertPlan(
                "SELECT count(*) FROM (SELECT sum(orderkey) FROM orders)",
                output(
                        values(ImmutableList.of("_col0"), ImmutableList.of(ImmutableList.of(new LongLiteral("1"))))));
        assertPlan(
                "SELECT count(s) FROM (SELECT sum(orderkey) AS s FROM orders)",
                anyTree(
                        tableScan("orders")));
        assertPlan(
                "SELECT count(*) FROM (SELECT sum(orderkey) FROM orders GROUP BY custkey)",
                anyTree(
                        tableScan("orders")));
    }

    @Test
    public void testPickTableLayoutWithFilter()
    {
        assertPlan(
                "SELECT orderkey FROM orders WHERE orderkey=5",
                output(
                        filter("orderkey = BIGINT '5'",
                                constrainedTableScanWithTableLayout(
                                        "orders",
                                        ImmutableMap.of(),
                                        ImmutableMap.of("orderkey", "orderkey")))));
        assertPlan(
                "SELECT orderkey FROM orders WHERE orderstatus='F'",
                output(
                        constrainedTableScanWithTableLayout(
                                "orders",
                                ImmutableMap.of("orderstatus", singleValue(createVarcharType(1), utf8Slice("F"))),
                                ImmutableMap.of("orderkey", "orderkey"))));
    }

    @Test
    public void testBroadcastCorrelatedSubqueryAvoidsRemoteExchangeBeforeAggregation()
    {
        Session broadcastJoin = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name())
                .setSystemProperty(FORCE_SINGLE_NODE_OUTPUT, Boolean.toString(false))
                .build();

        // make sure there is a remote exchange on the build side
        PlanMatchPattern joinBuildSideWithRemoteExchange =
                anyTree(
                        node(JoinNode.class,
                                anyTree(
                                        node(TableScanNode.class)),
                                anyTree(
                                        exchange(REMOTE, ExchangeNode.Type.REPLICATE,
                                                anyTree(
                                                        node(TableScanNode.class))))));

        // validates that there exists only one remote exchange
        Consumer<Plan> validateSingleRemoteExchange = plan -> assertEquals(
                countOfMatchingNodes(
                        plan,
                        node -> node instanceof ExchangeNode && ((ExchangeNode) node).getScope() == REMOTE),
                1);

        Consumer<Plan> validateSingleStreamingAggregation = plan -> assertEquals(
                countOfMatchingNodes(
                        plan,
                        node -> node instanceof AggregationNode
                                && ((AggregationNode) node).getGroupingKeys().contains(new Symbol("unique"))
                                && ((AggregationNode) node).isStreamable()),
                1);

        // region is unpartitioned, AssignUniqueId should provide satisfying partitioning for count(*) after LEFT JOIN
        assertPlanWithSession(
                "SELECT (SELECT count(*) FROM region r2 WHERE r2.regionkey > r1.regionkey) FROM region r1",
                broadcastJoin,
                false,
                joinBuildSideWithRemoteExchange,
                validateSingleRemoteExchange.andThen(validateSingleStreamingAggregation));

        // orders is naturally partitioned, AssignUniqueId should not overwrite its natural partitioning
        assertPlanWithSession(
                "SELECT count(count) " +
                        "FROM (SELECT o1.orderkey orderkey, (SELECT count(*) FROM orders o2 WHERE o2.orderkey > o1.orderkey) count FROM orders o1) " +
                        "GROUP BY orderkey",
                broadcastJoin,
                false,
                joinBuildSideWithRemoteExchange,
                validateSingleRemoteExchange.andThen(validateSingleStreamingAggregation));
    }

    @Test
    public void testUsesDistributedJoinIfNaturallyPartitionedOnProbeSymbols()
    {
        Session broadcastJoin = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name())
                .setSystemProperty(FORCE_SINGLE_NODE_OUTPUT, Boolean.toString(false))
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, Boolean.toString(false))
                .build();

        // replicated join with naturally partitioned and distributed probe side is rewritten to partitioned join
        assertPlanWithSession(
                "SELECT r1.regionkey FROM (SELECT regionkey FROM region GROUP BY regionkey) r1, region r2 WHERE r2.regionkey = r1.regionkey",
                broadcastJoin,
                false,
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("LEFT_REGIONKEY", "RIGHT_REGIONKEY")), Optional.empty(), Optional.of(PARTITIONED),
                                // the only remote exchange in probe side should be below aggregation
                                aggregation(ImmutableMap.of(),
                                        anyTree(
                                                exchange(REMOTE, REPARTITION,
                                                        anyTree(
                                                                tableScan("region", ImmutableMap.of("LEFT_REGIONKEY", "regionkey")))))),
                                anyTree(
                                        exchange(REMOTE, REPARTITION,
                                                tableScan("region", ImmutableMap.of("RIGHT_REGIONKEY", "regionkey")))))),
                plan -> // make sure there are only two remote exchanges (one in probe and one in build side)
                        assertEquals(
                                countOfMatchingNodes(
                                        plan,
                                        node -> node instanceof ExchangeNode && ((ExchangeNode) node).getScope() == REMOTE),
                                2));

        // replicated join is preserved if probe side is single node
        assertPlanWithSession(
                "SELECT * FROM (VALUES 1, 2, 3) t(a), region r WHERE r.regionkey = t.a",
                broadcastJoin,
                false,
                anyTree(
                        node(JoinNode.class,
                                anyTree(
                                    node(ValuesNode.class)),
                                anyTree(
                                        exchange(REMOTE, GATHER,
                                                        node(TableScanNode.class))))));

        // replicated join is preserved if there are no equality criteria
        assertPlanWithSession(
                "SELECT * FROM (SELECT regionkey FROM region GROUP BY regionkey) r1, region r2 WHERE r2.regionkey > r1.regionkey",
                broadcastJoin,
                false,
                anyTree(
                        join(INNER, ImmutableList.of(), Optional.empty(), Optional.of(REPLICATED),
                                anyTree(
                                        node(TableScanNode.class)),
                                anyTree(
                                        exchange(REMOTE, REPLICATE,
                                                node(TableScanNode.class))))));
    }

    @Test
    public void testDistributedSort()
    {
        ImmutableList<PlanMatchPattern.Ordering> orderBy = ImmutableList.of(sort("ORDERKEY", DESCENDING, LAST));
        assertDistributedPlan(
                "SELECT orderkey FROM orders ORDER BY orderkey DESC",
                output(
                        exchange(REMOTE, GATHER, orderBy,
                                exchange(LOCAL, GATHER, orderBy,
                                        sort(orderBy,
                                                exchange(REMOTE, REPARTITION,
                                                        tableScan("orders", ImmutableMap.of(
                                                                "ORDERKEY", "orderkey"))))))));

        assertDistributedPlan(
                "SELECT orderkey FROM orders ORDER BY orderkey DESC",
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(DISTRIBUTED_SORT, Boolean.toString(false))
                        .build(),
                output(
                        sort(orderBy,
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE, GATHER,
                                                tableScan("orders", ImmutableMap.of(
                                                        "ORDERKEY", "orderkey")))))));
    }

    @Test
    public void testRemoveAggregationInSemiJoin()
    {
        assertPlanDoesNotContain(
                "SELECT custkey FROM orders WHERE custkey IN (SELECT distinct custkey FROM customer)",
                AggregationNode.class);
    }

    @Test
    public void testOrderByFetch()
    {
        assertPlan(
                "SELECT * FROM nation ORDER BY name FETCH FIRST 2 ROWS ONLY",
                anyTree(
                        topN(
                                2,
                                ImmutableList.of(sort("NAME", ASCENDING, LAST)),
                                TopNNode.Step.PARTIAL,
                                tableScan("nation", ImmutableMap.of(
                                        "NAME", "name")))));
    }

    @Test
    public void testFetch()
    {
        assertPlan(
                "SELECT * FROM nation FETCH FIRST 2 ROWS ONLY",
                anyTree(
                        limit(
                                2,
                                any(
                                        tableScan("nation")))));
    }

    @Test
    public void testOffset()
    {
        assertPlan(
                "SELECT name FROM nation OFFSET 2 ROWS",
                any(
                        strictProject(
                                ImmutableMap.of("name", new ExpressionMatcher("name")),
                                filter(
                                        "row_num > BIGINT '2'",
                                        rowNumber(
                                                pattern -> pattern
                                                        .partitionBy(ImmutableList.of()),
                                                any(
                                                        tableScan("nation", ImmutableMap.of("NAME", "name"))))
                                                .withAlias("row_num", new RowNumberSymbolMatcher())))));

        assertPlan(
                "SELECT name FROM nation ORDER BY regionkey OFFSET 2 ROWS",
                any(
                        strictProject(
                                ImmutableMap.of("name", new ExpressionMatcher("name")),
                                filter(
                                        "row_num > BIGINT '2'",
                                        rowNumber(
                                                pattern -> pattern
                                                        .partitionBy(ImmutableList.of()),
                                                anyTree(
                                                        sort(
                                                                ImmutableList.of(sort("regionkey", ASCENDING, LAST)),
                                                                any(
                                                                        tableScan("nation", ImmutableMap.of("NAME", "name", "REGIONKEY", "regionkey"))))))
                                                .withAlias("row_num", new RowNumberSymbolMatcher())))));

        assertPlan(
                "SELECT name FROM nation ORDER BY regionkey OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY",
                any(
                        strictProject(
                                ImmutableMap.of("name", new ExpressionMatcher("name")),
                                filter(
                                        "row_num > BIGINT '2'",
                                        rowNumber(
                                                pattern -> pattern
                                                        .partitionBy(ImmutableList.of()),
                                                any(
                                                        topN(
                                                                7,
                                                                ImmutableList.of(sort("regionkey", ASCENDING, LAST)),
                                                                TopNNode.Step.FINAL,
                                                                anyTree(
                                                                        tableScan("nation", ImmutableMap.of("NAME", "name", "REGIONKEY", "regionkey"))))))
                                                .withAlias("row_num", new RowNumberSymbolMatcher())))));

        assertPlan(
                "SELECT name FROM nation OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY",
                any(
                        strictProject(
                                ImmutableMap.of("name", new ExpressionMatcher("name")),
                                filter(
                                        "row_num > BIGINT '2'",
                                        rowNumber(
                                                pattern -> pattern
                                                        .partitionBy(ImmutableList.of()),
                                                limit(
                                                        7,
                                                        any(
                                                                tableScan("nation", ImmutableMap.of("NAME", "name")))))
                                                .withAlias("row_num", new RowNumberSymbolMatcher())))));
    }

    @Test
    public void testWithTies()
    {
        assertPlan(
                "SELECT name, regionkey FROM nation ORDER BY regionkey FETCH FIRST 6 ROWS WITH TIES",
                any(
                        strictProject(
                                ImmutableMap.of("name", new ExpressionMatcher("name"), "regionkey", new ExpressionMatcher("regionkey")),
                                filter(
                                        "rank_num <= BIGINT '6'",
                                        window(
                                                windowMatcherBuilder -> windowMatcherBuilder
                                                        .specification(specification(
                                                                ImmutableList.of(),
                                                                ImmutableList.of("regionkey"),
                                                                ImmutableMap.of("regionkey", SortOrder.ASC_NULLS_LAST)))
                                                        .addFunction(
                                                                "rank_num",
                                                                functionCall(
                                                                        "rank",
                                                                        Optional.empty(),
                                                                        ImmutableList.of())),
                                                anyTree(
                                                        sort(
                                                                ImmutableList.of(sort("regionkey", ASCENDING, LAST)),
                                                                any(
                                                                        tableScan(
                                                                                "nation",
                                                                                ImmutableMap.of("NAME", "name", "REGIONKEY", "regionkey"))))))))));

        assertPlan(
                "SELECT name, regionkey FROM nation ORDER BY regionkey OFFSET 10 ROWS FETCH FIRST 6 ROWS WITH TIES",
                any(
                        strictProject(
                                ImmutableMap.of("name", new ExpressionMatcher("name"), "regionkey", new ExpressionMatcher("regionkey")),
                                filter(
                                        "row_num > BIGINT '10'",
                                        rowNumber(
                                                pattern -> pattern
                                                        .partitionBy(ImmutableList.of()),
                                                strictProject(
                                                        ImmutableMap.of("name", new ExpressionMatcher("name"), "regionkey", new ExpressionMatcher("regionkey")),
                                                        filter(
                                                                "rank_num <= BIGINT '16'",
                                                                window(
                                                                        windowMatcherBuilder -> windowMatcherBuilder
                                                                                .specification(specification(
                                                                                        ImmutableList.of(),
                                                                                        ImmutableList.of("regionkey"),
                                                                                        ImmutableMap.of("regionkey", SortOrder.ASC_NULLS_LAST)))
                                                                                .addFunction(
                                                                                        "rank_num",
                                                                                        functionCall(
                                                                                                "rank",
                                                                                                Optional.empty(),
                                                                                                ImmutableList.of())),
                                                                        anyTree(
                                                                                sort(
                                                                                        ImmutableList.of(sort("regionkey", ASCENDING, LAST)),
                                                                                        any(
                                                                                                tableScan(
                                                                                                        "nation",
                                                                                                        ImmutableMap.of("NAME", "name", "REGIONKEY", "regionkey")))))))))
                                                .withAlias("row_num", new RowNumberSymbolMatcher())))));
    }

    @Test
    public void testRedundantLimitNodeRemoval()
    {
        String query = "SELECT count(*) FROM orders LIMIT 10";
        assertFalse(
                searchFrom(plan(query, OPTIMIZED).getRoot())
                        .where(LimitNode.class::isInstance)
                        .matches(),
                format("Unexpected limit node for query: '%s'", query));

        assertPlan(
                "SELECT orderkey, count(*) FROM orders GROUP BY orderkey LIMIT 10",
                output(
                        limit(10,
                                anyTree(
                                        tableScan("orders")))));

        assertPlan(
                "SELECT * FROM (VALUES 1,2,3,4,5,6) AS t1 LIMIT 10",
                output(
                        values(ImmutableList.of("x"))));
    }

    @Test
    public void testRemoveSingleRowSort()
    {
        String query = "SELECT count(*) FROM orders ORDER BY 1";
        assertFalse(
                searchFrom(plan(query, LogicalPlanner.Stage.OPTIMIZED).getRoot())
                        .where(isInstanceOfAny(SortNode.class))
                        .matches(),
                format("Unexpected sort node for query: '%s'", query));

        assertPlan(
                "SELECT orderkey, count(*) FROM orders GROUP BY orderkey ORDER BY 1",
                anyTree(
                        node(SortNode.class,
                                anyTree(
                                        tableScan("orders")))));
    }

    @Test
    public void testRedundantTopNNodeRemoval()
    {
        String query = "SELECT count(*) FROM orders ORDER BY 1 LIMIT 10";
        assertFalse(
                searchFrom(plan(query, LogicalPlanner.Stage.OPTIMIZED).getRoot())
                        .where(isInstanceOfAny(TopNNode.class, SortNode.class))
                        .matches(),
                format("Unexpected TopN node for query: '%s'", query));

        assertPlan(
                "SELECT orderkey, count(*) FROM orders GROUP BY orderkey ORDER BY 1 LIMIT 10",
                output(
                        node(TopNNode.class,
                                anyTree(
                                        tableScan("orders")))));

        assertPlan(
                "SELECT orderkey, count(*) FROM orders GROUP BY orderkey ORDER BY 1 LIMIT 0",
                output(
                        node(ValuesNode.class)));

        assertPlan(
                "SELECT * FROM (VALUES 1,2,3,4,5,6) AS t1 ORDER BY 1 LIMIT 10",
                output(
                        exchange(LOCAL, GATHER,
                                node(SortNode.class,
                                        exchange(LOCAL, REPARTITION,
                                                values(ImmutableList.of("x")))))));
    }

    @Test
    public void testRedundantDistinctLimitNodeRemoval()
    {
        String query = "SELECT distinct(c) FROM (SELECT count(*) as c FROM orders) LIMIT 10";
        assertFalse(
                searchFrom(plan(query, LogicalPlanner.Stage.OPTIMIZED).getRoot())
                        .where(isInstanceOfAny(DistinctLimitNode.class))
                        .matches(),
                format("Unexpected DistinctLimit node for query: '%s'", query));

        assertPlan(
                "SELECT distinct(c) FROM (SELECT count(*) as c FROM orders GROUP BY orderkey) LIMIT 10",
                output(
                        node(DistinctLimitNode.class,
                                anyTree(
                                        tableScan("orders")))));

        assertPlan(
                "SELECT distinct(id) FROM (VALUES 1, 2, 3, 4, 5, 6) as t1 (id) LIMIT 10",
                output(
                        node(ProjectNode.class,
                                node(AggregationNode.class,
                                        node(ProjectNode.class,
                                                values(ImmutableList.of("x")))))));
    }

    @Test
    public void testRedundantHashRemovalForUnionAll()
    {
        assertPlan(
                "SELECT count(*) FROM ((SELECT nationkey FROM customer) UNION ALL (SELECT nationkey FROM customer)) GROUP BY nationkey",
                output(
                        project(
                                node(AggregationNode.class,
                                        exchange(LOCAL, REPARTITION,
                                                project(ImmutableMap.of("hash", expression("combine_hash(bigint '0', coalesce(\"$operator$hash_code\"(nationkey), 0))")),
                                                        node(AggregationNode.class,
                                                                tableScan("customer", ImmutableMap.of("nationkey", "nationkey")))),
                                                project(ImmutableMap.of("hash_1", expression("combine_hash(bigint '0', coalesce(\"$operator$hash_code\"(nationkey_6), 0))")),
                                                        node(AggregationNode.class,
                                                                tableScan("customer", ImmutableMap.of("nationkey_6", "nationkey")))))))));
    }

    @Test
    public void testRedundantHashRemovalForMarkDistinct()
    {
        assertDistributedPlan(
                "select count(*), count(distinct orderkey), count(distinct partkey), count(distinct suppkey) from lineitem",
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(TASK_CONCURRENCY, "16")
                        .build(),
                output(
                        anyTree(
                                identityProject(
                                        node(MarkDistinctNode.class,
                                                anyTree(
                                                        project(ImmutableMap.of(
                                                                "hash_1", expression("combine_hash(bigint '0', coalesce(\"$operator$hash_code\"(suppkey), 0))"),
                                                                "hash_2", expression("combine_hash(bigint '0', coalesce(\"$operator$hash_code\"(partkey), 0))")),
                                                                node(MarkDistinctNode.class,
                                                                        tableScan("lineitem", ImmutableMap.of("suppkey", "suppkey", "partkey", "partkey"))))))))));
    }

    @Test
    public void testRedundantHashRemovalForUnionAllAndMarkDistinct()
    {
        assertDistributedPlan(
                "SELECT count(distinct(custkey)), count(distinct(nationkey)) FROM ((SELECT custkey, nationkey FROM customer) UNION ALL ( SELECT custkey, custkey FROM customer))",
                output(
                        anyTree(
                                node(MarkDistinctNode.class,
                                anyTree(
                                        node(MarkDistinctNode.class,
                                                exchange(LOCAL, REPARTITION,
                                                        exchange(REMOTE, REPARTITION,
                                                                project(ImmutableMap.of("hash_custkey", expression("combine_hash(bigint '0', COALESCE(\"$operator$hash_code\"(custkey), 0))"), "hash_nationkey", expression("combine_hash(bigint '0', COALESCE(\"$operator$hash_code\"(nationkey), 0))")),
                                                                        tableScan("customer", ImmutableMap.of("custkey", "custkey", "nationkey", "nationkey")))),
                                                        exchange(REMOTE, REPARTITION,
                                                                node(ProjectNode.class,
                                                                        node(TableScanNode.class))))))))));
    }
}
