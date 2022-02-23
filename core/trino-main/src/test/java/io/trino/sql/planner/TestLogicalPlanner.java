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
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.cost.OptimizerConfig.JoinDistributionType;
import io.trino.cost.OptimizerConfig.JoinReorderingStrategy;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.RowType;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.ExpressionMatcher;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.assertions.RowNumberSymbolMatcher;
import io.trino.sql.planner.optimizations.AddLocalExchanges;
import io.trino.sql.planner.optimizations.CheckSubqueryNodesAreRewritten;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SemiJoinNode.DistributionType;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.planner.rowpattern.ir.IrQuantified;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.StringLiteral;
import io.trino.tests.QueryTemplate;
import io.trino.util.MorePredicates;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.DISTRIBUTED_SORT;
import static io.trino.SystemSessionProperties.FILTERING_SEMI_JOIN_TO_INNER;
import static io.trino.SystemSessionProperties.FORCE_SINGLE_NODE_OUTPUT;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.LogicalPlanner.Stage.CREATED;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static io.trino.sql.planner.assertions.PlanMatchPattern.DynamicFilterPattern;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aliasToIndex;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyNot;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.apply;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.constrainedTableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.constrainedTableScanWithTableLayout;
import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.identityProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.markDistinct;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.patternRecognition;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.specification;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictConstrainedTableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topNRanking;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFrame;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.RANK;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.ROW_NUMBER;
import static io.trino.sql.planner.rowpattern.ir.IrQuantifier.oneOrMore;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.WINDOW;
import static io.trino.sql.tree.SortItem.NullOrdering.LAST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.sql.tree.SortItem.Ordering.DESCENDING;
import static io.trino.sql.tree.WindowFrame.Type.ROWS;
import static io.trino.tests.QueryTemplate.queryTemplate;
import static io.trino.util.MorePredicates.isInstanceOfAny;
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
    public void testLikePredicate()
    {
        assertPlan("SELECT type FROM part WHERE type LIKE 'LARGE PLATED %'",
                anyTree(
                        tableScan(
                                tableHandle -> {
                                    Map<ColumnHandle, Domain> domains = ((TpchTableHandle) tableHandle).getConstraint().getDomains()
                                            .orElseThrow(() -> new AssertionError("Unexpected none TupleDomain"));

                                    Domain domain = domains.entrySet().stream()
                                            .filter(entry -> ((TpchColumnHandle) entry.getKey()).getColumnName().equals("type"))
                                            .map(Entry::getValue)
                                            .collect(toOptional())
                                            .orElseThrow(() -> new AssertionError("No domain for 'type'"));

                                    assertEquals(domain, Domain.multipleValues(
                                            createVarcharType(25),
                                            ImmutableList.of("LARGE PLATED BRASS", "LARGE PLATED COPPER", "LARGE PLATED NICKEL", "LARGE PLATED STEEL", "LARGE PLATED TIN").stream()
                                                    .map(Slices::utf8Slice)
                                                    .collect(toImmutableList())));
                                    return true;
                                },
                                TupleDomain.withColumnDomains(ImmutableMap.of(
                                        tableHandle -> ((TpchColumnHandle) tableHandle).getColumnName().equals("type"),
                                        Domain.create(
                                                ValueSet.ofRanges(Range.range(createVarcharType(25), utf8Slice("LARGE PLATED "), true, utf8Slice("LARGE PLATED!"), false)),
                                                false))),
                                ImmutableMap.of())));
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
                                        "output_1", expression("row[1]"),
                                        "output_2", expression("row[2]")),
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
                                                                tableScan("nation", ImmutableMap.of("NAME", "name", "REGIONKEY", "regionkey")))))))));
    }

    @Test
    public void testAllFieldsDereferenceFromNonDeterministic()
    {
        FunctionCall randomFunction = new FunctionCall(
                getQueryRunner().getMetadata().resolveFunction(TEST_SESSION, QualifiedName.of("rand"), ImmutableList.of()).toQualifiedName(),
                ImmutableList.of());

        assertPlan("SELECT (x, x).* FROM (SELECT rand()) T(x)",
                any(
                        project(
                                ImmutableMap.of(
                                        "output_1", expression("row[1]"),
                                        "output_2", expression("row[2]")),
                                project(
                                        ImmutableMap.of("row", expression("ROW(\"rand\", \"rand\")")),
                                        values(
                                                ImmutableList.of("rand"),
                                                ImmutableList.of(ImmutableList.of(randomFunction)))))));

        assertPlan("SELECT (rand(), rand()).* FROM (VALUES 1) t(x)",
                any(
                        project(
                                ImmutableMap.of(
                                        "output_1", expression("r[1]"),
                                        "output_2", expression("r[2]")),
                                values(
                                        ImmutableList.of("r"),
                                        ImmutableList.of(ImmutableList.of(new Row(ImmutableList.of(randomFunction, randomFunction))))))));

        // Ensure the calls to rand() are not duplicated by the ORDER BY clause
        assertPlan("SELECT (rand(), rand()).* FROM (VALUES 1, 2) t(x) ORDER BY 1",
                anyTree(
                        node(SortNode.class,
                                any(
                                        project(
                                                ImmutableMap.of(
                                                        "output_1", expression("row[1]"),
                                                        "output_2", expression("row[2]")),
                                                values(
                                                        ImmutableList.of("row"),
                                                        ImmutableList.of(
                                                                ImmutableList.of(new Row(ImmutableList.of(randomFunction, randomFunction))),
                                                                ImmutableList.of(new Row(ImmutableList.of(randomFunction, randomFunction))))))))));
    }

    @Test
    public void testTrivialFilterOverDuplicateSymbol()
    {
        assertPlan(
                "WITH t AS (SELECT DISTINCT cast(null AS varchar), cast(null AS varchar)) " +
                        "SELECT * FROM t WHERE 1 = 0",
                output(ImmutableList.of("expr", "expr"), values("expr")));

        assertPlan(
                "SELECT * FROM (SELECT DISTINCT 1, 1) WHERE 1 = 0",
                output(ImmutableList.of("expr", "expr"), values("expr")));
    }

    @Test
    public void testDistinctLimitOverInequalityJoin()
    {
        assertPlan("SELECT DISTINCT o.orderkey FROM orders o JOIN lineitem l ON o.orderkey < l.orderkey LIMIT 1",
                anyTree(
                        node(DistinctLimitNode.class,
                                anyTree(
                                        filter("O_ORDERKEY < L_ORDERKEY",
                                                join(INNER,
                                                        ImmutableList.of(),
                                                        ImmutableList.of(new DynamicFilterPattern("O_ORDERKEY", LESS_THAN, "L_ORDERKEY")),
                                                        filter(TRUE_LITERAL,
                                                                tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))),
                                                        any(tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey"))))
                                                        .withExactOutputs(ImmutableList.of("O_ORDERKEY", "L_ORDERKEY")))))));

        assertPlan(
                "SELECT DISTINCT o.orderkey FROM orders o JOIN lineitem l ON o.shippriority = l.linenumber AND o.orderkey < l.orderkey LIMIT 1",
                noJoinReordering(),
                anyTree(
                        node(DistinctLimitNode.class,
                                anyTree(
                                        join(INNER,
                                                ImmutableList.of(equiJoinClause("O_SHIPPRIORITY", "L_LINENUMBER")),
                                                Optional.of("O_ORDERKEY < L_ORDERKEY"),
                                                anyTree(tableScan("orders", ImmutableMap.of(
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
                                join(INNER,
                                        ImmutableList.of(),
                                        ImmutableList.of(new DynamicFilterPattern("O_ORDERKEY", LESS_THAN, "L_ORDERKEY")),
                                        filter(TRUE_LITERAL,
                                                tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))),
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
                                        Optional.of(ImmutableList.of(
                                                new DynamicFilterPattern("O_SHIPPRIORITY", EQUAL, "L_LINENUMBER"),
                                                new DynamicFilterPattern("O_ORDERKEY", LESS_THAN, "L_ORDERKEY"))),
                                        Optional.empty(),
                                        Optional.empty(),
                                        project(
                                                filter(TRUE_LITERAL,
                                                        tableScan("orders", ImmutableMap.of(
                                                                "O_SHIPPRIORITY", "shippriority",
                                                                "O_ORDERKEY", "orderkey")))),
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
                                        filter(TRUE_LITERAL,
                                                tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))),
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
                                anyTree(
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
                                anyTree(
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
                                        filter(TRUE_LITERAL,
                                                tableScan("orders", ImmutableMap.of("X", "orderkey")))),
                                project(
                                        node(EnforceSingleRowNode.class,
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));

        assertPlan("SELECT * FROM orders WHERE orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber % 4 = 0)",
                noSemiJoinRewrite(),
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
                        plan("SELECT * " +
                                "FROM orders " +
                                "WHERE CAST(orderkey AS INTEGER) = (SELECT 1 FROM orders LIMIT 1) " +
                                "AND custkey = (SELECT 2 FROM orders LIMIT 1) " +
                                "AND CAST(custkey as REAL) != (SELECT 1 FROM orders LIMIT 1)"),
                        TableScanNode.class::isInstance),
                3);
        // same query used for left, right and complex join condition
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT * " +
                                "FROM orders o1 " +
                                "JOIN orders o2 ON " +
                                "  o1.orderkey = (SELECT 1 FROM orders LIMIT 1) " +
                                "  AND o2.orderkey = (SELECT 1 FROM orders LIMIT 1) " +
                                "  AND o1.orderkey + o2.orderkey > (SELECT 1 FROM orders LIMIT 1)"),
                        TableScanNode.class::isInstance),
                3);
    }

    @Test
    public void testSameInSubqueryIsAppliedOnlyOnce()
    {
        // same IN query used for left, right and complex condition
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey NOT IN (SELECT 1) AND (o1.orderkey NOT IN (SELECT 1) OR o1.orderkey NOT IN (SELECT 1))"),
                        SemiJoinNode.class::isInstance),
                1);

        // one subquery used for "1 IN (SELECT 1)", one subquery used for "2 IN (SELECT 1)"
        assertEquals(
                countOfMatchingNodes(
                        plan("SELECT 1 NOT IN (SELECT 1), 2 NOT IN (SELECT 1) WHERE 1 NOT IN (SELECT 1)"),
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

    @Test
    public void testSameExistsAppliedOnlyOnce()
    {
        assertPlan(
                "SELECT EXISTS (SELECT 1 FROM orders), EXISTS (SELECT 1 FROM orders)",
                anyTree(
                        node(AggregationNode.class,
                                tableScan("orders"))));
    }

    @Test
    public void testReferenceToSameFieldAppliedOnlyOnce()
    {
        assertEquals(
                countOfMatchingNodes(
                        plan(
                                "SELECT " +
                                        "(SELECT 1 FROM orders WHERE orderkey = x) + " +
                                        "(SELECT 1 FROM orders WHERE orderkey = t.x) + " +
                                        "(SELECT 1 FROM orders WHERE orderkey = T.x) + " +
                                        "(SELECT 1 FROM orders WHERE orderkey = t.X) + " +
                                        "(SELECT 1 FROM orders WHERE orderkey = T.X)" +
                                        "FROM (VALUES 1, 2) t(x)"),
                        JoinNode.class::isInstance),
                1);
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
        assertPlan(
                "SELECT nationkey FROM nation JOIN region ON nation.regionkey = region.regionkey",
                noJoinReordering(),
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
        // rewrite TopN to TopNRankingNode
        assertPlan(
                "SELECT regionkey, n.name FROM region LEFT JOIN LATERAL (SELECT name FROM nation WHERE region.regionkey = regionkey ORDER BY name LIMIT 2) n ON TRUE",
                any(
                        join(
                                LEFT,
                                ImmutableList.of(equiJoinClause("region_regionkey", "nation_regionkey")),
                                any(tableScan("region", ImmutableMap.of("region_regionkey", "regionkey"))),
                                any(topNRanking(
                                        pattern -> pattern
                                                .specification(
                                                        ImmutableList.of("nation_regionkey"),
                                                        ImmutableList.of("nation_name"),
                                                        ImmutableMap.of("nation_name", SortOrder.ASC_NULLS_LAST))
                                                .rankingType(ROW_NUMBER)
                                                .maxRankingPerPartition(2)
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
                noJoinReordering(),
                anyTree(
                        filter(format("CASE \"is_distinct\" WHEN true THEN true ELSE CAST(fail(%s, 'Scalar sub-query has returned multiple rows') AS boolean) END", SUBQUERY_MULTIPLE_ROWS.toErrorCode().getCode()),
                                project(
                                        markDistinct("is_distinct", ImmutableList.of("unique"),
                                                join(LEFT, ImmutableList.of(equiJoinClause("n_regionkey", "r_regionkey")),
                                                        assignUniqueId("unique",
                                                                exchange(REMOTE, REPARTITION,
                                                                        anyTree(tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey"))))),
                                                        anyTree(
                                                                tableScan("region", ImmutableMap.of("r_regionkey", "regionkey")))))))));

        assertDistributedPlan("SELECT name, (SELECT name FROM region WHERE regionkey = nation.regionkey) FROM nation",
                automaticJoinDistribution(),
                anyTree(
                        filter(format("CASE \"is_distinct\" WHEN true THEN true ELSE CAST(fail(%s, 'Scalar sub-query has returned multiple rows') AS boolean) END", SUBQUERY_MULTIPLE_ROWS.toErrorCode().getCode()),
                                project(
                                        markDistinct("is_distinct", ImmutableList.of("unique"),
                                                join(LEFT, ImmutableList.of(equiJoinClause("n_regionkey", "r_regionkey")),
                                                        project(
                                                                assignUniqueId("unique",
                                                                        tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey", "n_name", "name")))),
                                                        anyTree(
                                                                tableScan("region", ImmutableMap.of("r_regionkey", "regionkey")))))))));
    }

    @Test
    public void testStreamingAggregationForCorrelatedSubquery()
    {
        // Use equi-clause to trigger hash partitioning of the join sources
        assertDistributedPlan(
                "SELECT name, (SELECT max(name) FROM region WHERE regionkey = nation.regionkey AND length(name) > length(nation.name)) FROM nation",
                noJoinReordering(),
                anyTree(
                        aggregation(
                                singleGroupingSet("n_name", "n_regionkey", "unique"),
                                ImmutableMap.of(Optional.of("max"), functionCall("max", ImmutableList.of("r_name"))),
                                ImmutableList.of("n_name", "n_regionkey", "unique"),
                                ImmutableList.of("non_null"),
                                Optional.empty(),
                                SINGLE,
                                node(JoinNode.class,
                                        assignUniqueId("unique",
                                                exchange(REMOTE, REPARTITION,
                                                        anyTree(
                                                                tableScan("nation", ImmutableMap.of("n_name", "name", "n_regionkey", "regionkey"))))),
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("non_null", expression("true")),
                                                        tableScan("region", ImmutableMap.of("r_name", "name"))))))));

        // Don't use equi-clauses to trigger replicated join
        assertDistributedPlan(
                "SELECT name, (SELECT max(name) FROM region WHERE regionkey > nation.regionkey) FROM nation",
                anyTree(
                        aggregation(
                                singleGroupingSet("n_name", "n_regionkey", "unique"),
                                ImmutableMap.of(Optional.of("max"), functionCall("max", ImmutableList.of("r_name"))),
                                ImmutableList.of("n_name", "n_regionkey", "unique"),
                                ImmutableList.of("non_null"),
                                Optional.empty(),
                                SINGLE,
                                node(JoinNode.class,
                                        assignUniqueId("unique",
                                                tableScan("nation", ImmutableMap.of("n_name", "name", "n_regionkey", "regionkey"))),
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("non_null", expression("true")),
                                                        tableScan("region", ImmutableMap.of("r_name", "name"))))))));
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
                                project(
                                        apply(ImmutableList.of("O", "C"),
                                                ImmutableMap.of("OUTER_FILTER", expression("THREE IN (C)")),
                                                project(ImmutableMap.of("THREE", expression("BIGINT '3'")),
                                                        tableScan("orders", ImmutableMap.of(
                                                                "O", "orderkey",
                                                                "C", "custkey"))),
                                                project(
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
                                project(
                                        aggregation(
                                                singleGroupingSet("ORDERKEY", "UNIQUE"),
                                                ImmutableMap.of(Optional.of("FINAL_COUNT"), functionCall("count", ImmutableList.of())),
                                                ImmutableList.of("ORDERKEY", "UNIQUE"),
                                                ImmutableList.of("NON_NULL"),
                                                Optional.empty(),
                                                SINGLE,
                                                join(LEFT, ImmutableList.of(), Optional.of("BIGINT '3' = ORDERKEY"),
                                                        assignUniqueId(
                                                                "UNIQUE",
                                                                tableScan("orders", ImmutableMap.of("ORDERKEY", "orderkey"))),
                                                        project(ImmutableMap.of("NON_NULL", expression("true")),
                                                                node(ValuesNode.class))))))));
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
                        values(ImmutableList.of("_col0"), ImmutableList.of(ImmutableList.of(new GenericLiteral("BIGINT", "1"))))));
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
    public void testInlineCountOverLiteral()
    {
        assertPlan(
                "SELECT regionkey, count(1) FROM nation GROUP BY regionkey",
                anyTree(
                        aggregation(
                                ImmutableMap.of("count_0", functionCall("count", ImmutableList.of())),
                                PARTIAL,
                                tableScan("nation", ImmutableMap.of("regionkey", "regionkey")))));
    }

    @Test
    public void testInlineCountOverEffectivelyLiteral()
    {
        assertPlan(
                "SELECT regionkey, count(CAST(DECIMAL '1' AS decimal(8,4))) FROM nation GROUP BY regionkey",
                anyTree(
                        aggregation(
                                ImmutableMap.of("count_0", functionCall("count", ImmutableList.of())),
                                PARTIAL,
                                tableScan("nation", ImmutableMap.of("regionkey", "regionkey")))));
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
                                // No orderstatus constraint, as it has been fully consumed by the connector, and orderstatus column is no longer referenced in the query
                                ImmutableMap.of(),
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
                "SELECT custkey FROM orders WHERE custkey NOT IN (SELECT distinct custkey FROM customer)",
                AggregationNode.class);
    }

    @Test
    public void testFilteringSemiJoinRewriteToInnerJoin()
    {
        assertPlan(
                "SELECT custkey FROM orders WHERE custkey IN (SELECT custkey FROM customer)",
                any(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("CUSTOMER_CUSTKEY", "ORDER_CUSTKEY")),
                                project(
                                        aggregation(
                                                singleGroupingSet("CUSTOMER_CUSTKEY"),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                FINAL,
                                                anyTree(
                                                        tableScan("customer", ImmutableMap.of("CUSTOMER_CUSTKEY", "custkey"))))),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("ORDER_CUSTKEY", "custkey"))))));
    }

    @Test
    public void testCorrelatedIn()
    {
        assertPlan(
                "SELECT name FROM region r WHERE regionkey IN (SELECT regionkey FROM nation WHERE name < r.name)",
                anyTree(
                        filter(
                                "count_matches > BIGINT '0'",
                                project(
                                        aggregation(
                                                singleGroupingSet("region_regionkey", "region_name", "unique"),
                                                ImmutableMap.of(Optional.of("count_matches"), functionCall("count", ImmutableList.of())),
                                                ImmutableList.of("region_regionkey", "region_name", "unique"),
                                                ImmutableList.of("mask"),
                                                Optional.empty(),
                                                SINGLE,
                                                project(
                                                        ImmutableMap.of("mask", expression("((NOT (region_regionkey IS NULL)) AND (NOT (nation_regionkey IS NULL)))")),
                                                        join(
                                                                LEFT,
                                                                ImmutableList.of(),
                                                                Optional.of("(region_regionkey IS NULL OR region_regionkey = nation_regionkey OR nation_regionkey IS NULL) AND nation_name < region_name"),
                                                                assignUniqueId(
                                                                        "unique",
                                                                        tableScan("region", ImmutableMap.of(
                                                                                "region_regionkey", "regionkey",
                                                                                "region_name", "name"))),
                                                                any(
                                                                        tableScan("nation", ImmutableMap.of(
                                                                                "nation_name", "name",
                                                                                "nation_regionkey", "regionkey"))))))))));
    }

    @Test
    public void testCorrelatedExists()
    {
        assertPlan(
                "SELECT regionkey, name FROM region r WHERE EXISTS(SELECT regionkey FROM nation WHERE name < r.name)",
                anyTree(
                        filter(
                                "count_matches > BIGINT '0'",
                                project(
                                        aggregation(
                                                singleGroupingSet("region_regionkey", "region_name", "unique"),
                                                ImmutableMap.of(Optional.of("count_matches"), functionCall("count", ImmutableList.of())),
                                                ImmutableList.of("region_regionkey", "region_name", "unique"),
                                                ImmutableList.of("mask"),
                                                Optional.empty(),
                                                SINGLE,
                                                join(
                                                        LEFT,
                                                        ImmutableList.of(),
                                                        Optional.of("nation_name < region_name"),
                                                        assignUniqueId(
                                                                "unique",
                                                                tableScan("region", ImmutableMap.of(
                                                                        "region_regionkey", "regionkey",
                                                                        "region_name", "name"))),
                                                        any(
                                                                project(
                                                                        ImmutableMap.of("mask", expression("true")),
                                                                        tableScan("nation", ImmutableMap.of("nation_name", "name"))))))))));
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
                                                        tableScan("nation", ImmutableMap.of("name", "name"))))
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
                                                                        tableScan("nation", ImmutableMap.of("name", "name", "regionkey", "regionkey"))))))
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
                                                                        tableScan("nation", ImmutableMap.of("name", "name", "regionkey", "regionkey"))))))
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
                                                                tableScan("nation", ImmutableMap.of("name", "name")))))
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
                                topNRanking(
                                        pattern -> pattern
                                                .specification(
                                                        ImmutableList.of(),
                                                        ImmutableList.of("regionkey"),
                                                        ImmutableMap.of("regionkey", SortOrder.ASC_NULLS_LAST))
                                                .rankingType(RANK)
                                                .maxRankingPerPartition(6)
                                                .partial(false),
                                        anyTree(

                                                tableScan(
                                                        "nation",
                                                        ImmutableMap.of("name", "name", "regionkey", "regionkey")))))));

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
                                                        topNRanking(
                                                                pattern -> pattern
                                                                        .specification(
                                                                                ImmutableList.of(),
                                                                                ImmutableList.of("regionkey"),
                                                                                ImmutableMap.of("regionkey", SortOrder.ASC_NULLS_LAST))
                                                                        .rankingType(RANK)
                                                                        .maxRankingPerPartition(16)
                                                                        .partial(false),
                                                                anyTree(
                                                                        tableScan(
                                                                                "nation",
                                                                                ImmutableMap.of("name", "name", "regionkey", "regionkey"))))))
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
                searchFrom(plan(query, OPTIMIZED).getRoot())
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
                searchFrom(plan(query, OPTIMIZED).getRoot())
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
                searchFrom(plan(query, OPTIMIZED).getRoot())
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

    @Test
    public void testRemoveRedundantFilter()
    {
        assertPlan(
                "SELECT orderkey, t2.s " +
                        "FROM orders " +
                        "JOIN (VALUES CAST('' || 'O' AS varchar(1)), CAST('' || 'F' AS varchar(1))) t2(s) " +
                        "ON orders.orderstatus = t2.s",
                any(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("expr", "ORDER_STATUS")),
                        anyTree(values(ImmutableList.of("expr"), ImmutableList.of(ImmutableList.of(new StringLiteral("O")), ImmutableList.of(new StringLiteral("F"))))),
                        exchange(project(strictConstrainedTableScan(
                                "orders",
                                ImmutableMap.of("ORDER_STATUS", "orderstatus", "ORDER_KEY", "orderkey"),
                                ImmutableMap.of("orderstatus", multipleValues(createVarcharType(1), ImmutableList.of(utf8Slice("F"), utf8Slice("O"))))))))));
    }

    @Test
    public void testRemoveRedundantCrossJoin()
    {
        assertPlan("SELECT regionkey FROM nation, (SELECT 1 as a) temp WHERE regionkey = temp.a",
                output(
                        filter("REGIONKEY = BIGINT '1'",
                                tableScan("nation", ImmutableMap.of("REGIONKEY", "regionkey")))));

        assertPlan("SELECT regionkey FROM (SELECT 1 as a) temp, nation WHERE regionkey > temp.a",
                output(
                        filter("REGIONKEY > BIGINT '1'",
                                tableScan("nation", ImmutableMap.of("REGIONKEY", "regionkey")))));

        assertPlan("SELECT * FROM nation, (SELECT 1 as a) temp WHERE regionkey = a",
                output(
                        project(
                                ImmutableMap.of("expr", expression("1")),
                                filter("REGIONKEY = BIGINT '1'",
                                        tableScan("nation", ImmutableMap.of("REGIONKEY", "regionkey"))))));
    }

    @Test
    public void testRemoveRedundantInnerJoin()
    {
        assertPlan("SELECT regionkey FROM nation INNER JOIN (SELECT nationkey FROM customer LIMIT 0) USING (nationkey)",
                output(
                        values(ImmutableList.of("regionkey"))));

        assertPlan("SELECT regionkey FROM (SELECT * FROM nation LIMIT 0) INNER JOIN customer USING (nationkey)",
                output(
                        values(ImmutableList.of("regionkey"))));
    }

    @Test
    public void testRemoveRedundantLeftJoin()
    {
        assertPlan("SELECT regionkey FROM (SELECT * FROM nation LIMIT 0) LEFT JOIN customer USING (nationkey)",
                output(
                        values(ImmutableList.of("regionkey"))));
    }

    @Test
    public void testRemoveRedundantRightJoin()
    {
        assertPlan("SELECT regionkey FROM nation RIGHT JOIN (SELECT nationkey FROM customer LIMIT 0) USING (nationkey)",
                output(
                        values(ImmutableList.of("regionkey"))));
    }

    @Test
    public void testMergeProjectWithValues()
    {
        assertPlan(
                "SELECT * FROM nation, (SELECT a * 2 FROM (VALUES 1, 2, 3) t(a))",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(),
                                tableScan("nation"),
                                values(ImmutableList.of("a"), ImmutableList.of(
                                        ImmutableList.of(new LongLiteral("2")),
                                        ImmutableList.of(new LongLiteral("4")),
                                        ImmutableList.of(new LongLiteral("6")))))));

        // Constraint is enforced on table scan, based on constant value in the other branch of the join.
        // The scalar constant branch of the join becomes obsolete, and join is removed.
        assertPlan(
                "SELECT orderkey, t2.s " +
                        "FROM orders " +
                        "JOIN (SELECT '' || x FROM (VALUES 'F') t(x)) t2(s) " +
                        "ON orders.orderstatus = t2.s",
                any(project(
                        ImmutableMap.of("cast", expression("CAST(ORDER_STATUS AS varchar)")),
                        strictConstrainedTableScan(
                                "orders",
                                ImmutableMap.of("ORDER_STATUS", "orderstatus", "ORDER_KEY", "orderkey"),
                                ImmutableMap.of("orderstatus", Domain.singleValue(createVarcharType(1), utf8Slice("F")))))));

        // Constraint is enforced on table scan, based on constant values in the other branch of the join.
        assertPlan(
                "SELECT orderkey, t2.s " +
                        "FROM orders " +
                        "JOIN (SELECT CAST('' || x AS varchar(1)) FROM (VALUES 'O', 'F') t(x)) t2(s) " +
                        "ON orders.orderstatus = t2.s",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("expr", "ORDER_STATUS")),
                                project(filter(
                                        "expr IN ('F', 'O')",
                                        values(ImmutableList.of("expr"), ImmutableList.of(ImmutableList.of(new StringLiteral("O")), ImmutableList.of(new StringLiteral("F")))))),
                                exchange(project(strictConstrainedTableScan(
                                        "orders",
                                        ImmutableMap.of("ORDER_STATUS", "orderstatus", "ORDER_KEY", "orderkey"),
                                        ImmutableMap.of("orderstatus", multipleValues(createVarcharType(1), ImmutableList.of(utf8Slice("F"), utf8Slice("O"))))))))));

        // Constraint for the table is derived, based on constant values in the other branch of the join.
        // It is not accepted by the connector, and remains in form of a filter over TableScan.
        assertPlan(
                "SELECT orderstatus, t2.s " +
                        "FROM orders " +
                        "JOIN (SELECT x * 1 FROM (VALUES BIGINT '1', BIGINT '2') t(x)) t2(s) " +
                        "ON orders.orderkey = t2.s",
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("expr", "ORDER_KEY")),
                                project(filter(
                                        "expr IN (BIGINT '1', BIGINT '2')",
                                        values(ImmutableList.of("expr"), ImmutableList.of(ImmutableList.of(new GenericLiteral("BIGINT", "1")), ImmutableList.of(new GenericLiteral("BIGINT", "2")))))),
                                anyTree(filter(
                                        "ORDER_KEY IN (BIGINT '1', BIGINT '2')",
                                        strictConstrainedTableScan(
                                                "orders",
                                                ImmutableMap.of("ORDER_STATUS", "orderstatus", "ORDER_KEY", "orderkey"),
                                                ImmutableMap.of()))))));
    }

    @Test
    public void testReplaceJoinWithProject()
    {
        assertPlan(
                "SELECT * FROM nation, (SELECT a * 2 FROM (VALUES 1) t(a))",
                any(
                        project(
                                ImmutableMap.of("expr", expression("2")),
                                tableScan("nation"))));

        assertPlan(
                "SELECT * FROM nation, (SELECT b * 3 FROM (SELECT a * 2 FROM (VALUES 1) t1(a)) t2(b))",
                any(
                        project(
                                ImmutableMap.of("expr", expression("6")),
                                tableScan("nation"))));
    }

    @Test
    public void testGroupingSetsWithDefaultValue()
    {
        assertDistributedPlan("SELECT orderkey, COUNT(DISTINCT k) FROM (SELECT orderkey, 1 k FROM orders) GROUP BY GROUPING SETS ((), orderkey)",
                output(
                        anyTree(
                                aggregation(
                                        ImmutableMap.of("final_count", functionCall("count", ImmutableList.of("partial_count"))),
                                        FINAL,
                                        exchange(
                                                LOCAL,
                                                REPARTITION,
                                                exchange(
                                                        REMOTE,
                                                        REPARTITION,
                                                        aggregation(
                                                                ImmutableMap.of("partial_count", functionCall("count", ImmutableList.of("CONSTANT"))),
                                                                PARTIAL,
                                                                anyTree(
                                                                        project(
                                                                                ImmutableMap.of("CONSTANT", expression("1")),
                                                                                tableScan("orders"))))))))));
    }

    @Test
    public void testSizeBasedJoin()
    {
        // both local.sf100000.nation and local.sf100000.orders don't provide stats, therefore no reordering happens
        assertDistributedPlan("SELECT custkey FROM local.\"sf42.5\".nation, local.\"sf42.5\".orders WHERE nation.nationkey = orders.custkey",
                automaticJoinDistribution(),
                output(
                        anyTree(
                                join(INNER, ImmutableList.of(equiJoinClause("NATIONKEY", "CUSTKEY")),
                                        anyTree(
                                                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey"))),
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("CUSTKEY", "custkey")))))));

        // values node provides stats
        assertDistributedPlan("SELECT custkey FROM (VALUES CAST(1 AS BIGINT), CAST(2 AS BIGINT)) t(a), local.\"sf42.5\".orders WHERE t.a = orders.custkey",
                automaticJoinDistribution(),
                output(
                        anyTree(
                                join(INNER, ImmutableList.of(equiJoinClause("CUSTKEY", "T_A")), Optional.empty(), Optional.of(REPLICATED),
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("CUSTKEY", "custkey"))),
                                        anyTree(
                                                values("T_A"))))));
    }

    @Test
    public void testSizeBasedSemiJoin()
    {
        // both local.sf100000.nation and local.sf100000.orders don't provide stats, therefore no reordering happens
        assertDistributedPlan("SELECT custkey FROM local.\"sf42.5\".orders WHERE orders.custkey NOT IN (SELECT nationkey FROM local.\"sf42.5\".nation)",
                automaticJoinDistribution(),
                output(
                        anyTree(
                                semiJoin("CUSTKEY", "NATIONKEY", "OUT", Optional.of(DistributionType.PARTITIONED),
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("CUSTKEY", "custkey"))),
                                        anyTree(
                                                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey")))))));

        // values node provides stats
        assertDistributedPlan("SELECT custkey FROM local.\"sf42.5\".orders WHERE orders.custkey NOT IN (SELECT t.a FROM (VALUES CAST(1 AS BIGINT), CAST(2 AS BIGINT)) t(a))",
                automaticJoinDistribution(),
                output(
                        anyTree(
                                semiJoin("CUSTKEY", "T_A", "OUT", Optional.of(DistributionType.REPLICATED),
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("CUSTKEY", "custkey"))),
                                        anyTree(
                                                values("T_A"))))));
    }

    @Test
    public void testExplainAnalyze()
    {
        assertPlan("EXPLAIN ANALYZE SELECT regionkey FROM nation",
                output(
                        node(ExplainAnalyzeNode.class,
                                exchange(LOCAL, GATHER,
                                        strictTableScan("nation", ImmutableMap.of("regionkey", "regionkey"))))));
    }

    @Test
    public void testValuesCoercions()
    {
        assertPlan("VALUES TINYINT '1', REAL '1'",
                CREATED,
                anyTree(
                        values(
                                ImmutableList.of("field"),
                                ImmutableList.of(
                                        ImmutableList.of(new Cast(new GenericLiteral("TINYINT", "1"), toSqlType(REAL))),
                                        ImmutableList.of(new GenericLiteral("REAL", "1"))))));

        // rows coerced by field
        assertPlan("VALUES (TINYINT '1', REAL '1'), (DOUBLE '2', SMALLINT '2')",
                CREATED,
                anyTree(
                        values(
                                ImmutableList.of("field", "field0"),
                                ImmutableList.of(
                                        ImmutableList.of(new Cast(new GenericLiteral("TINYINT", "1"), toSqlType(DOUBLE)), new GenericLiteral("REAL", "1")),
                                        ImmutableList.of(new GenericLiteral("DOUBLE", "2"), new Cast(new GenericLiteral("SMALLINT", "2"), toSqlType(REAL)))))));

        // entry of type other than Row coerced as a whole
        assertPlan("VALUES DOUBLE '1', CAST(ROW(2) AS row(bigint))",
                CREATED,
                anyTree(
                        values(
                                aliasToIndex(ImmutableList.of("field")),
                                Optional.of(1),
                                Optional.of(ImmutableList.of(
                                        new Row(ImmutableList.of(new GenericLiteral("DOUBLE", "1"))),
                                        new Cast(
                                                new Cast(
                                                        new Row(ImmutableList.of(new LongLiteral("2"))),
                                                        toSqlType(RowType.anonymous(ImmutableList.of(BIGINT)))),
                                                toSqlType(RowType.anonymous(ImmutableList.of(DOUBLE)))))))));
    }

    @Test
    public void testDoNotPlanUnreferencedRowPatternMeasures()
    {
        // row pattern measure `label` is not referenced
        assertPlan("SELECT val OVER w " +
                        "          FROM (VALUES (1, 90)) t(id, value) " +
                        "          WINDOW w AS ( " +
                        "                   ORDER BY id " +
                        "                   MEASURES " +
                        "                            RUNNING LAST(value) AS val, " +
                        "                            CLASSIFIER() AS label " +
                        "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                        "                   PATTERN (A+) " +
                        "                   DEFINE A AS true " +
                        "          )",
                output(
                        project(
                                patternRecognition(builder -> builder
                                                .specification(specification(ImmutableList.of(), ImmutableList.of("id"), ImmutableMap.of("id", ASC_NULLS_LAST)))
                                                .addMeasure("val", "LAST(value)", INTEGER)
                                                .rowsPerMatch(WINDOW)
                                                .frame(windowFrame(ROWS, CURRENT_ROW, Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                                                .pattern(new IrQuantified(new IrLabel("A"), oneOrMore(true)))
                                                .addVariableDefinition(new IrLabel("A"), "true"),
                                        values(
                                                ImmutableList.of("id", "value"),
                                                ImmutableList.of(ImmutableList.of(new LongLiteral("1"), new LongLiteral("90"))))))));

        // row pattern measure `label` is not referenced
        assertPlan("SELECT min(value) OVER w " +
                        "          FROM (VALUES (1, 90)) t(id, value) " +
                        "          WINDOW w AS ( " +
                        "                   ORDER BY id " +
                        "                   MEASURES CLASSIFIER() AS label " +
                        "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                        "                   PATTERN (A+) " +
                        "                   DEFINE A AS true " +
                        "          )",
                output(
                        project(
                                patternRecognition(builder -> builder
                                                .specification(specification(ImmutableList.of(), ImmutableList.of("id"), ImmutableMap.of("id", ASC_NULLS_LAST)))
                                                .addFunction("min", functionCall("min", ImmutableList.of("value")))
                                                .rowsPerMatch(WINDOW)
                                                .frame(windowFrame(ROWS, CURRENT_ROW, Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                                                .pattern(new IrQuantified(new IrLabel("A"), oneOrMore(true)))
                                                .addVariableDefinition(new IrLabel("A"), "true"),
                                        values(
                                                ImmutableList.of("id", "value"),
                                                ImmutableList.of(ImmutableList.of(new LongLiteral("1"), new LongLiteral("90"))))))));
    }

    @Test
    public void testPruneUnreferencedRowPatternWindowFunctions()
    {
        // window function `row_number` is not referenced
        assertPlan("SELECT id, min FROM " +
                        "       (SELECT id, min(value) OVER w min, row_number() OVER w " +
                        "          FROM (VALUES (1, 90)) t(id, value) " +
                        "          WINDOW w AS ( " +
                        "                   ORDER BY id " +
                        "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                        "                   PATTERN (A+) " +
                        "                   DEFINE A AS true " +
                        "          )" +
                        "       )",
                output(
                        project(
                                patternRecognition(builder -> builder
                                                .specification(specification(ImmutableList.of(), ImmutableList.of("id"), ImmutableMap.of("id", ASC_NULLS_LAST)))
                                                .addFunction("min", functionCall("min", ImmutableList.of("value")))
                                                .rowsPerMatch(WINDOW)
                                                .frame(windowFrame(ROWS, CURRENT_ROW, Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                                                .pattern(new IrQuantified(new IrLabel("A"), oneOrMore(true)))
                                                .addVariableDefinition(new IrLabel("A"), "true"),
                                        values(
                                                ImmutableList.of("id", "value"),
                                                ImmutableList.of(ImmutableList.of(new LongLiteral("1"), new LongLiteral("90"))))))));
    }

    @Test
    public void testPruneUnreferencedRowPatternMeasures()
    {
        // row pattern measure `label` is not referenced
        assertPlan("SELECT id, val FROM " +
                        "       (SELECT id, val OVER w val, label OVER w " +
                        "          FROM (VALUES (1, 90)) t(id, value) " +
                        "          WINDOW w AS ( " +
                        "                   ORDER BY id " +
                        "                   MEASURES " +
                        "                            RUNNING LAST(value) AS val, " +
                        "                            CLASSIFIER() AS label " +
                        "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                        "                   PATTERN (A+) " +
                        "                   DEFINE A AS true " +
                        "          )" +
                        "       )",
                output(
                        project(
                                patternRecognition(builder -> builder
                                                .specification(specification(ImmutableList.of(), ImmutableList.of("id"), ImmutableMap.of("id", ASC_NULLS_LAST)))
                                                .addMeasure("val", "LAST(value)", INTEGER)
                                                .rowsPerMatch(WINDOW)
                                                .frame(windowFrame(ROWS, CURRENT_ROW, Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                                                .pattern(new IrQuantified(new IrLabel("A"), oneOrMore(true)))
                                                .addVariableDefinition(new IrLabel("A"), "true"),
                                        values(
                                                ImmutableList.of("id", "value"),
                                                ImmutableList.of(ImmutableList.of(new LongLiteral("1"), new LongLiteral("90"))))))));
    }

    @Test
    public void testMergePatternRecognitionNodes()
    {
        // The pattern matching window `w` is referenced in three calls: row pattern measure calls: `val OVER w` and `label OVER w`,
        // and window function call `row_number() OVER w`. They are all planned within a single PatternRecognitionNode.
        assertPlan("SELECT id, val OVER w, label OVER w, row_number() OVER w " +
                        "          FROM (VALUES (1, 90)) t(id, value) " +
                        "          WINDOW w AS ( " +
                        "                   ORDER BY id " +
                        "                   MEASURES " +
                        "                            RUNNING LAST(value) AS val, " +
                        "                            CLASSIFIER() AS label " +
                        "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                        "                   PATTERN (A+) " +
                        "                   DEFINE A AS true " +
                        "          )",
                output(
                        project(
                                patternRecognition(builder -> builder
                                                .specification(specification(ImmutableList.of(), ImmutableList.of("id"), ImmutableMap.of("id", ASC_NULLS_LAST)))
                                                .addMeasure("val", "LAST(value)", INTEGER)
                                                .addMeasure("label", "CLASSIFIER()", VARCHAR)
                                                .addFunction("row_number", functionCall("row_number", ImmutableList.of()))
                                                .rowsPerMatch(WINDOW)
                                                .frame(windowFrame(ROWS, CURRENT_ROW, Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                                                .pattern(new IrQuantified(new IrLabel("A"), oneOrMore(true)))
                                                .addVariableDefinition(new IrLabel("A"), "true"),
                                        values(
                                                ImmutableList.of("id", "value"),
                                                ImmutableList.of(ImmutableList.of(new LongLiteral("1"), new LongLiteral("90"))))))));
    }

    @Test
    public void testMergePatternRecognitionNodesWithProjections()
    {
        // The pattern matching window `w` is referenced in three calls: row pattern measure calls: `value OVER w` and `label OVER w`,
        // and window function call `min(input1) OVER w`. They are all planned within a single PatternRecognitionNode.
        assertPlan("SELECT id, 2 * value OVER w, lower(label OVER w), 1 + min(input1) OVER w " +
                        "          FROM (VALUES (1, 2, 3)) t(id, input1, input2) " +
                        "          WINDOW w AS ( " +
                        "                   ORDER BY id " +
                        "                   MEASURES " +
                        "                            RUNNING LAST(input2) AS value, " +
                        "                            CLASSIFIER() AS label " +
                        "                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING " +
                        "                   PATTERN (A+) " +
                        "                   DEFINE A AS true " +
                        "          )",
                output(
                        project(
                                ImmutableMap.of(
                                        "output1", expression("id"),
                                        "output2", expression("value * 2"),
                                        "output3", expression("lower(label)"),
                                        "output4", expression("min + 1")),
                                project(
                                        ImmutableMap.of(
                                                "id", expression("id"),
                                                "value", expression("value"),
                                                "label", expression("label"),
                                                "min", expression("min")),
                                        patternRecognition(builder -> builder
                                                        .specification(specification(ImmutableList.of(), ImmutableList.of("id"), ImmutableMap.of("id", ASC_NULLS_LAST)))
                                                        .addMeasure("value", "LAST(input2)", INTEGER)
                                                        .addMeasure("label", "CLASSIFIER()", VARCHAR)
                                                        .addFunction("min", functionCall("min", ImmutableList.of("input1")))
                                                        .rowsPerMatch(WINDOW)
                                                        .frame(windowFrame(ROWS, CURRENT_ROW, Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()))
                                                        .pattern(new IrQuantified(new IrLabel("A"), oneOrMore(true)))
                                                        .addVariableDefinition(new IrLabel("A"), "true"),
                                                values(
                                                        ImmutableList.of("id", "input1", "input2"),
                                                        ImmutableList.of(ImmutableList.of(new LongLiteral("1"), new LongLiteral("2"), new LongLiteral("3")))))))));
    }

    private Session noJoinReordering()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .build();
    }

    private Session automaticJoinDistribution()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .build();
    }

    private Session noSemiJoinRewrite()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "false")
                .build();
    }
}
