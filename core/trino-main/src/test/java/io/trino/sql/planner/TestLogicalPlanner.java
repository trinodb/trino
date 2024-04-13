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
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.assertions.RowNumberSymbolMatcher;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan;
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
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.rowpattern.ClassifierValuePointer;
import io.trino.sql.planner.rowpattern.LogicalIndexPointer;
import io.trino.sql.planner.rowpattern.ScalarValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.planner.rowpattern.ir.IrQuantified;
import io.trino.tests.QueryTemplate;
import io.trino.type.Reals;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SystemSessionProperties.DISTRIBUTED_SORT;
import static io.trino.SystemSessionProperties.FILTERING_SEMI_JOIN_TO_INNER;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.LogicalPlanner.Stage.CREATED;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static io.trino.sql.planner.assertions.PlanMatchPattern.DynamicFilterPattern;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aliasToIndex;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyNot;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.apply;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.constrainedTableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.constrainedTableScanWithTableLayout;
import static io.trino.sql.planner.assertions.PlanMatchPattern.correlatedJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
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
import static io.trino.sql.planner.assertions.PlanMatchPattern.setExpression;
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
import static io.trino.sql.planner.assertions.PlanMatchPattern.windowFunction;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_FOLLOWING;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.RowsPerMatch.WINDOW;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.RANK;
import static io.trino.sql.planner.plan.TopNRankingNode.RankingType.ROW_NUMBER;
import static io.trino.sql.planner.plan.WindowFrameType.ROWS;
import static io.trino.sql.planner.rowpattern.ir.IrQuantifier.oneOrMore;
import static io.trino.sql.tree.SortItem.NullOrdering.LAST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.sql.tree.SortItem.Ordering.DESCENDING;
import static io.trino.tests.QueryTemplate.queryTemplate;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLogicalPlanner
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction SUBTRACT_BIGINT = FUNCTIONS.resolveOperator(OperatorType.SUBTRACT, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction MULTIPLY_INTEGER = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(INTEGER, INTEGER));

    private static final ResolvedFunction FAIL = FUNCTIONS.resolveFunction("fail", fromTypes(INTEGER, VARCHAR));
    private static final ResolvedFunction LOWER = FUNCTIONS.resolveFunction("lower", fromTypes(VARCHAR));
    private static final ResolvedFunction COMBINE_HASH = FUNCTIONS.resolveFunction("combine_hash", fromTypes(BIGINT, BIGINT));
    private static final ResolvedFunction HASH_CODE = createTestMetadataManager().resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(BIGINT));

    private static final WindowNode.Frame ROWS_FROM_CURRENT = new WindowNode.Frame(
            ROWS,
            CURRENT_ROW,
            Optional.empty(),
            Optional.empty(),
            UNBOUNDED_FOLLOWING,
            Optional.empty(),
            Optional.empty());

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
                                    Map<ColumnHandle, Domain> domains = ((TpchTableHandle) tableHandle).constraint().getDomains()
                                            .orElseThrow(() -> new AssertionError("Unexpected none TupleDomain"));

                                    Domain domain = domains.entrySet().stream()
                                            .filter(entry -> ((TpchColumnHandle) entry.getKey()).columnName().equals("type"))
                                            .map(Entry::getValue)
                                            .collect(toOptional())
                                            .orElseThrow(() -> new AssertionError("No domain for 'type'"));

                                    assertThat(domain).isEqualTo(multipleValues(
                                            createVarcharType(25),
                                            ImmutableList.of("LARGE PLATED BRASS", "LARGE PLATED COPPER", "LARGE PLATED NICKEL", "LARGE PLATED STEEL", "LARGE PLATED TIN").stream()
                                                    .map(Slices::utf8Slice)
                                                    .collect(toImmutableList())));
                                    return true;
                                },
                                TupleDomain.withColumnDomains(ImmutableMap.of(
                                        tableHandle -> ((TpchColumnHandle) tableHandle).columnName().equals("type"),
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
                                ImmutableMap.of("final_sum", aggregationFunction("sum", ImmutableList.of("partial_sum"))),
                                FINAL,
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE, REPARTITION,
                                                aggregation(
                                                        ImmutableMap.of("partial_sum", aggregationFunction("sum", ImmutableList.of("totalprice"))),
                                                        PARTIAL,
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice"))))))));

        // simple group by over filter that keeps at most one group
        assertDistributedPlan("SELECT orderstatus, sum(totalprice) FROM orders WHERE orderstatus='O' GROUP BY orderstatus",
                anyTree(
                        aggregation(
                                ImmutableMap.of("final_sum", aggregationFunction("sum", ImmutableList.of("partial_sum"))),
                                FINAL,
                                exchange(LOCAL, GATHER,
                                        exchange(REMOTE, REPARTITION,
                                                aggregation(
                                                        ImmutableMap.of("partial_sum", aggregationFunction("sum", ImmutableList.of("totalprice"))),
                                                        PARTIAL,
                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice"))))))));
    }

    @Test
    public void testAllFieldsDereferenceOnSubquery()
    {
        assertPlan("SELECT (SELECT (min(regionkey), max(name)) FROM nation).*",
                any(
                        project(
                                ImmutableMap.of(
                                        "output_1", expression(new FieldReference(new Reference(RowType.anonymousRow(BIGINT, BIGINT), "row"), 0)),
                                        "output_2", expression(new FieldReference(new Reference(RowType.anonymousRow(BIGINT, BIGINT), "row"), 1))),
                                project(
                                        ImmutableMap.of("row", expression(new Row(ImmutableList.of(new Reference(BIGINT, "min"), new Reference(BIGINT, "max"))))),
                                        aggregation(
                                                ImmutableMap.of(
                                                        "min", aggregationFunction("min", ImmutableList.of("min_regionkey")),
                                                        "max", aggregationFunction("max", ImmutableList.of("max_name"))),
                                                FINAL,
                                                any(
                                                        aggregation(
                                                                ImmutableMap.of(
                                                                        "min_regionkey", aggregationFunction("min", ImmutableList.of("REGIONKEY")),
                                                                        "max_name", aggregationFunction("max", ImmutableList.of("NAME"))),
                                                                PARTIAL,
                                                                tableScan("nation", ImmutableMap.of("NAME", "name", "REGIONKEY", "regionkey")))))))));
    }

    @Test
    public void testAllFieldsDereferenceFromNonDeterministic()
    {
        Call randomFunction = new Call(
                getPlanTester().getPlannerContext().getMetadata().resolveBuiltinFunction("rand", ImmutableList.of()),
                ImmutableList.of());

        assertPlan("SELECT (x, x).* FROM (SELECT rand()) T(x)",
                any(
                        project(
                                ImmutableMap.of(
                                        "output_1", expression(new FieldReference(new Reference(RowType.anonymousRow(DOUBLE, DOUBLE), "row"), 0)),
                                        "output_2", expression(new FieldReference(new Reference(RowType.anonymousRow(DOUBLE, DOUBLE), "row"), 1))),
                                project(
                                        ImmutableMap.of("row", expression(new Row(ImmutableList.of(new Reference(DOUBLE, "rand"), new Reference(DOUBLE, "rand"))))),
                                        values(
                                                ImmutableList.of("rand"),
                                                ImmutableList.of(ImmutableList.of(randomFunction)))))));

        assertPlan("SELECT (rand(), rand()).* FROM (VALUES 1) t(x)",
                any(
                        project(
                                ImmutableMap.of(
                                        "output_1", expression(new FieldReference(new Reference(RowType.anonymousRow(DOUBLE, DOUBLE), "r"), 0)),
                                        "output_2", expression(new FieldReference(new Reference(RowType.anonymousRow(DOUBLE, DOUBLE), "r"), 1))),
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
                                                        "output_1", expression(new FieldReference(new Reference(RowType.anonymousRow(DOUBLE, DOUBLE), "row"), 0)),
                                                        "output_2", expression(new FieldReference(new Reference(RowType.anonymousRow(DOUBLE, DOUBLE), "row"), 1))),
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
                                        filter(
                                                new Comparison(LESS_THAN, new Reference(BIGINT, "O_ORDERKEY"), new Reference(BIGINT, "L_ORDERKEY")),
                                                join(INNER, builder -> builder
                                                        .dynamicFilter(ImmutableList.of(new DynamicFilterPattern(new Reference(BIGINT, "O_ORDERKEY"), LESS_THAN, "L_ORDERKEY")))
                                                        .left(
                                                                filter(TRUE,
                                                                        tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))))
                                                        .right(
                                                                any(tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey")))))
                                                        .withExactOutputs(ImmutableList.of("O_ORDERKEY", "L_ORDERKEY")))))));

        assertPlan(
                "SELECT DISTINCT o.orderkey FROM orders o JOIN lineitem l ON o.shippriority = l.linenumber AND o.orderkey < l.orderkey LIMIT 1",
                noJoinReordering(),
                anyTree(
                        node(DistinctLimitNode.class,
                                anyTree(
                                        join(INNER, builder -> builder
                                                .equiCriteria("O_SHIPPRIORITY", "L_LINENUMBER")
                                                .filter(new Comparison(LESS_THAN, new Reference(BIGINT, "O_ORDERKEY"), new Reference(BIGINT, "L_ORDERKEY")))
                                                .left(
                                                        anyTree(tableScan("orders", ImmutableMap.of(
                                                                "O_SHIPPRIORITY", "shippriority",
                                                                "O_ORDERKEY", "orderkey"))))
                                                .right(
                                                        anyTree(tableScan("lineitem", ImmutableMap.of(
                                                                "L_LINENUMBER", "linenumber",
                                                                "L_ORDERKEY", "orderkey")))))
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
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("orderstatus", "orderstatus"))))));
    }

    @Test
    public void testInnerInequalityJoinNoEquiJoinConjuncts()
    {
        assertPlan("SELECT 1 FROM orders o JOIN lineitem l ON o.orderkey < l.orderkey",
                anyTree(
                        filter(
                                new Comparison(LESS_THAN, new Reference(BIGINT, "O_ORDERKEY"), new Reference(BIGINT, "L_ORDERKEY")),
                                join(INNER, builder -> builder
                                        .dynamicFilter(ImmutableList.of(new DynamicFilterPattern(new Reference(BIGINT, "O_ORDERKEY"), LESS_THAN, "L_ORDERKEY")))
                                        .left(
                                                filter(
                                                        TRUE,
                                                        tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))))
                                        .right(
                                                any(tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey"))))))));
    }

    @Test
    public void testInnerInequalityJoinWithEquiJoinConjuncts()
    {
        assertPlan("SELECT 1 FROM orders o JOIN lineitem l ON o.shippriority = l.linenumber AND o.orderkey < l.orderkey",
                anyTree(
                        anyNot(FilterNode.class,
                                join(INNER, builder -> builder
                                        .equiCriteria("L_LINENUMBER", "O_SHIPPRIORITY")
                                        .filter(new Comparison(LESS_THAN, new Reference(BIGINT, "O_ORDERKEY"), new Reference(BIGINT, "L_ORDERKEY")))
                                        .dynamicFilter(
                                                ImmutableList.of(
                                                        new DynamicFilterPattern(new Reference(INTEGER, "L_LINENUMBER"), EQUAL, "O_SHIPPRIORITY"),
                                                        new DynamicFilterPattern(new Reference(BIGINT, "L_ORDERKEY"), GREATER_THAN, "O_ORDERKEY")))
                                        .left(
                                                filter(TRUE,
                                                        tableScan("lineitem",
                                                                ImmutableMap.of(
                                                                        "L_LINENUMBER", "linenumber",
                                                                        "L_ORDERKEY", "orderkey"))))
                                        .right(
                                                anyTree(
                                                        tableScan("orders",
                                                                ImmutableMap.of(
                                                                        "O_SHIPPRIORITY", "shippriority",
                                                                        "O_ORDERKEY", "orderkey"))))))));
    }

    @Test
    public void testLeftConvertedToInnerInequalityJoinNoEquiJoinConjuncts()
    {
        assertPlan("SELECT 1 FROM orders o LEFT JOIN lineitem l ON o.orderkey < l.orderkey WHERE l.orderkey IS NOT NULL",
                anyTree(
                        filter(
                                new Comparison(LESS_THAN, new Reference(BIGINT, "O_ORDERKEY"), new Reference(BIGINT, "L_ORDERKEY")),
                                join(INNER, builder -> builder
                                        .left(
                                                filter(
                                                        TRUE,
                                                        tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))))
                                        .right(
                                                any(
                                                        filter(
                                                                new Not(new IsNull(new Reference(BIGINT, "L_ORDERKEY"))),
                                                                tableScan("lineitem", ImmutableMap.of("L_ORDERKEY", "orderkey")))))))));
    }

    @Test
    public void testJoin()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("LINEITEM_OK", "ORDERS_OK")
                                .left(
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))
                                .right(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))))));
    }

    @Test
    public void testJoinWithOrderBySameKey()
    {
        assertPlan("SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey ORDER BY l.orderkey ASC, o.orderkey ASC",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("LINEITEM_OK", "ORDERS_OK")
                                .left(
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey"))))
                                .right(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey")))))));
    }

    @Test
    public void testInequalityPredicatePushdownWithOuterJoin()
    {
        assertPlan("" +
                        "SELECT o.orderkey " +
                        "FROM orders o LEFT JOIN lineitem l " +
                        "ON o.orderkey = l.orderkey AND o.custkey + 42 < l.partkey + 42 " +
                        "WHERE o.custkey - 24 < COALESCE(l.partkey - 24, 0)",
                anyTree(
                        // predicate above outer join is not pushed to build side
                        filter(
                                new Comparison(LESS_THAN, new Call(SUBTRACT_BIGINT, ImmutableList.of(new Reference(BIGINT, "O_CUSTKEY"), new Constant(BIGINT, 24L))), new Coalesce(new Call(SUBTRACT_BIGINT, ImmutableList.of(new Reference(BIGINT, "L_PARTKEY"), new Constant(BIGINT, 24L))), new Constant(BIGINT, 0L))),
                                join(LEFT, builder -> builder
                                        .equiCriteria("O_ORDERKEY", "L_ORDERKEY")
                                        .filter(new Comparison(LESS_THAN, new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "O_CUSTKEY"), new Constant(BIGINT, 42L))), new Reference(BIGINT, "EXPR")))
                                        .left(
                                                tableScan(
                                                        "orders",
                                                        ImmutableMap.of(
                                                                "O_ORDERKEY", "orderkey",
                                                                "O_CUSTKEY", "custkey")))
                                        .right(
                                                anyTree(
                                                        project(
                                                                ImmutableMap.of("EXPR", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "L_PARTKEY"), new Constant(BIGINT, 42L))))),
                                                                tableScan(
                                                                        "lineitem",
                                                                        ImmutableMap.of(
                                                                                "L_ORDERKEY", "orderkey",
                                                                                "L_PARTKEY", "partkey")))))))));
    }

    @Test
    public void testTopNPushdownToJoinSource()
    {
        assertPlan("SELECT n.name, r.name FROM nation n LEFT JOIN region r ON n.regionkey = r.regionkey ORDER BY n.comment LIMIT 1",
                anyTree(
                        project(
                                topN(1, ImmutableList.of(sort("N_COMM", ASCENDING, LAST)), TopNNode.Step.FINAL,
                                        anyTree(
                                                join(LEFT, builder -> builder
                                                        .equiCriteria("N_KEY", "R_KEY")
                                                        .left(
                                                                topN(1, ImmutableList.of(sort("N_COMM", ASCENDING, LAST)), TopNNode.Step.PARTIAL,
                                                                        tableScan("nation", ImmutableMap.of("N_NAME", "name", "N_KEY", "regionkey", "N_COMM", "comment"))))
                                                        .right(anyTree(
                                                                tableScan("region", ImmutableMap.of("R_NAME", "name", "R_KEY", "regionkey"))))))))));
    }

    @Test
    public void testUncorrelatedSubqueries()
    {
        assertPlan("SELECT * FROM orders WHERE orderkey = (SELECT orderkey FROM lineitem ORDER BY orderkey LIMIT 1)",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("X", "Y")
                                .left(
                                        filter(TRUE,
                                                tableScan("orders", ImmutableMap.of("X", "orderkey"))))
                                .right(
                                        node(EnforceSingleRowNode.class,
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("Y", "orderkey"))))))));

        assertPlan("SELECT * FROM orders WHERE orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber % 4 = 0)",
                noSemiJoinRewrite(),
                anyTree(
                        filter(
                                new Reference(BOOLEAN, "S"),
                                semiJoin("X", "Y", "S",
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("X", "orderkey"))),
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("Y", "orderkey")))))));

        assertPlan("SELECT * FROM orders WHERE orderkey NOT IN (SELECT orderkey FROM lineitem WHERE linenumber < 0)",
                anyTree(
                        filter(
                                new Not(new Reference(BOOLEAN, "S")),
                                semiJoin("X", "Y", "S",
                                        tableScan("orders", ImmutableMap.of("X", "orderkey")),
                                        anyTree(
                                                tableScan("lineitem", ImmutableMap.of("Y", "orderkey")))))));
    }

    @Test
    public void testPushDownJoinConditionConjunctsToInnerSideBasedOnInheritedPredicate()
    {
        assertPlan(
                "SELECT nationkey FROM nation LEFT OUTER JOIN region " +
                        "ON nation.regionkey = region.regionkey and nation.name = region.name WHERE nation.name = 'blah'",
                anyTree(
                        join(LEFT, builder -> builder
                                .equiCriteria(
                                        ImmutableList.of(
                                                equiJoinClause("NATION_NAME", "REGION_NAME"),
                                                equiJoinClause("NATION_REGIONKEY", "REGION_REGIONKEY")))
                                .left(
                                        filter(
                                                new Comparison(EQUAL, new Reference(createVarcharType(25), "NATION_NAME"), new Constant(createVarcharType(25), utf8Slice("blah"))),
                                                constrainedTableScan(
                                                        "nation",
                                                        ImmutableMap.of(),
                                                        ImmutableMap.of(
                                                                "NATION_NAME", "name",
                                                                "NATION_REGIONKEY", "regionkey"))))
                                .right(
                                        anyTree(
                                                filter(
                                                        new Comparison(EQUAL, new Reference(createVarcharType(25), "REGION_NAME"), new Constant(createVarcharType(25), utf8Slice("blah"))),
                                                        constrainedTableScan(
                                                                "region",
                                                                ImmutableMap.of(),
                                                                ImmutableMap.of(
                                                                        "REGION_NAME", "name",
                                                                        "REGION_REGIONKEY", "regionkey"))))))));
    }

    @Test
    public void testSameScalarSubqueryIsAppliedOnlyOnce()
    {
        // three subqueries with two duplicates (coerced to two different types), only two scalar joins should be in plan
        assertThat(countOfMatchingNodes(
                plan("SELECT * " +
                        "FROM orders " +
                        "WHERE CAST(orderkey AS INTEGER) = (SELECT 1 FROM orders LIMIT 1) " +
                        "AND custkey = (SELECT 2 FROM orders LIMIT 1) " +
                        "AND CAST(custkey as REAL) != (SELECT 1 FROM orders LIMIT 1)"),
                TableScanNode.class::isInstance)).isEqualTo(3);
        // same query used for left, right and complex join condition
        assertThat(countOfMatchingNodes(
                plan("SELECT * " +
                        "FROM orders o1 " +
                        "JOIN orders o2 ON " +
                        "  o1.orderkey = (SELECT 1 FROM orders LIMIT 1) " +
                        "  AND o2.orderkey = (SELECT 1 FROM orders LIMIT 1) " +
                        "  AND o1.orderkey + o2.orderkey > (SELECT 1 FROM orders LIMIT 1)"),
                TableScanNode.class::isInstance)).isEqualTo(3);
    }

    @Test
    public void testSameInSubqueryIsAppliedOnlyOnce()
    {
        // same IN query used for left, right and complex condition
        assertThat(countOfMatchingNodes(
                plan("SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey NOT IN (SELECT 1) AND (o1.orderkey NOT IN (SELECT 1) OR o1.orderkey NOT IN (SELECT 1))"),
                SemiJoinNode.class::isInstance)).isEqualTo(1);

        // one subquery used for "1 IN (SELECT 1)", one subquery used for "2 IN (SELECT 1)"
        assertThat(countOfMatchingNodes(
                plan("SELECT 1 NOT IN (SELECT 1), 2 NOT IN (SELECT 1) WHERE 1 NOT IN (SELECT 1)"),
                SemiJoinNode.class::isInstance)).isEqualTo(2);
    }

    @Test
    public void testSameQualifiedSubqueryIsAppliedOnlyOnce()
    {
        // same ALL query used for left, right and complex condition
        assertThat(countOfMatchingNodes(
                plan("SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey <= ALL(SELECT 1) AND (o1.orderkey <= ALL(SELECT 1) OR o1.orderkey <= ALL(SELECT 1))"),
                AggregationNode.class::isInstance)).isEqualTo(1);

        // one subquery used for "1 <= ALL(SELECT 1)", one subquery used for "2 <= ALL(SELECT 1)"
        assertThat(countOfMatchingNodes(
                plan("SELECT 1 <= ALL(SELECT 1), 2 <= ALL(SELECT 1) WHERE 1 <= ALL(SELECT 1)"),
                AggregationNode.class::isInstance)).isEqualTo(2);
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
        assertThat(countOfMatchingNodes(
                plan(
                        "SELECT " +
                                "(SELECT 1 FROM orders WHERE orderkey = x) + " +
                                "(SELECT 1 FROM orders WHERE orderkey = t.x) + " +
                                "(SELECT 1 FROM orders WHERE orderkey = T.x) + " +
                                "(SELECT 1 FROM orders WHERE orderkey = t.X) + " +
                                "(SELECT 1 FROM orders WHERE orderkey = T.X)" +
                                "FROM (VALUES 1, 2) t(x)"),
                JoinNode.class::isInstance)).isEqualTo(1);
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
                        join(INNER, builder -> builder
                                .equiCriteria("REGIONKEY_LEFT", "REGIONKEY_RIGHT")
                                .left(
                                        anyTree(
                                                tableScan("nation", ImmutableMap.of("REGIONKEY_LEFT", "regionkey", "NATIONKEY", "nationkey"))))
                                .right(
                                        anyTree(
                                                tableScan("region", ImmutableMap.of("REGIONKEY_RIGHT", "regionkey"))))))
                        .withNumberOfOutputColumns(1)
                        .withOutputs(ImmutableList.of("NATIONKEY")));
    }

    private void assertPlanContainsNoApplyOrAnyJoin(String sql)
    {
        assertPlanDoesNotContain(sql, ApplyNode.class, JoinNode.class, IndexJoinNode.class, SemiJoinNode.class, CorrelatedJoinNode.class);
    }

    @SafeVarargs
    private void assertPlanDoesNotContain(String sql, Class<? extends PlanNode>... classes)
    {
        assertThat(searchFrom(plan(sql, OPTIMIZED).getRoot())
                .whereIsInstanceOfAny(classes)
                .matches())
                .describedAs("Unexpected node for query: " + sql)
                .isFalse();
    }

    @Test
    public void testCorrelatedSubqueries()
    {
        assertPlan(
                "SELECT orderkey FROM orders WHERE 3 = (SELECT orderkey)",
                OPTIMIZED,
                any(
                        filter(
                                new Comparison(EQUAL, new Reference(BIGINT, "X"), new Constant(BIGINT, 3L)),
                                tableScan("orders", ImmutableMap.of("X", "orderkey")))));
    }

    @Test
    public void testCorrelatedJoinWithLimit()
    {
        // rewrite Limit to RowNumberNode
        assertPlan(
                "SELECT regionkey, n.name FROM region LEFT JOIN LATERAL (SELECT name FROM nation WHERE region.regionkey = regionkey LIMIT 2) n ON TRUE",
                any(
                        join(LEFT, builder -> builder
                                .equiCriteria("region_regionkey", "nation_regionkey")
                                .left(tableScan("region", ImmutableMap.of("region_regionkey", "regionkey")))
                                .right(any(rowNumber(
                                        pattern -> pattern
                                                .partitionBy(ImmutableList.of("nation_regionkey"))
                                                .maxRowCountPerPartition(Optional.of(2)),
                                        anyTree(tableScan("nation", ImmutableMap.of("nation_name", "name", "nation_regionkey", "regionkey")))))))));

        // rewrite Limit to decorrelated Limit
        assertPlan("SELECT regionkey, n.nationkey FROM region LEFT JOIN LATERAL (SELECT nationkey FROM nation WHERE region.regionkey = 3 LIMIT 2) n ON TRUE",
                any(
                        join(LEFT, builder -> builder
                                .filter(new Comparison(EQUAL, new Reference(BIGINT, "region_regionkey"), new Constant(BIGINT, 3L)))
                                .left(tableScan("region", ImmutableMap.of("region_regionkey", "regionkey")))
                                .right(
                                        limit(
                                                2,
                                                any(tableScan("nation", ImmutableMap.of("nation_nationkey", "nationkey"))))))));
    }

    @Test
    public void testCorrelatedJoinWithTopN()
    {
        // rewrite TopN to TopNRankingNode
        assertPlan(
                "SELECT regionkey, n.name FROM region LEFT JOIN LATERAL (SELECT name FROM nation WHERE region.regionkey = regionkey ORDER BY name LIMIT 2) n ON TRUE",
                any(
                        join(LEFT, builder -> builder
                                .equiCriteria("region_regionkey", "nation_regionkey")
                                .left(tableScan("region", ImmutableMap.of("region_regionkey", "regionkey")))
                                .right(any(topNRanking(
                                        pattern -> pattern
                                                .specification(
                                                        ImmutableList.of("nation_regionkey"),
                                                        ImmutableList.of("nation_name"),
                                                        ImmutableMap.of("nation_name", SortOrder.ASC_NULLS_LAST))
                                                .rankingType(ROW_NUMBER)
                                                .maxRankingPerPartition(2)
                                                .partial(false),
                                        anyTree(tableScan("nation", ImmutableMap.of("nation_name", "name", "nation_regionkey", "regionkey")))))))));

        // rewrite TopN to RowNumberNode
        assertPlan(
                "SELECT regionkey, n.name FROM region LEFT JOIN LATERAL (SELECT name FROM nation WHERE region.regionkey = regionkey ORDER BY regionkey LIMIT 2) n ON TRUE",
                any(
                        join(LEFT, builder -> builder
                                .equiCriteria("region_regionkey", "nation_regionkey")
                                .left(tableScan("region", ImmutableMap.of("region_regionkey", "regionkey")))
                                .right(any(rowNumber(
                                        pattern -> pattern
                                                .partitionBy(ImmutableList.of("nation_regionkey"))
                                                .maxRowCountPerPartition(Optional.of(2)),
                                        anyTree(tableScan("nation", ImmutableMap.of("nation_name", "name", "nation_regionkey", "regionkey")))))))));
    }

    @Test
    public void testCorrelatedJoinWithNullCondition()
    {
        assertPlan(
                "SELECT regionkey, n.name FROM region LEFT JOIN LATERAL (SELECT name FROM nation) n ON NULL",
                CREATED,
                anyTree(
                        correlatedJoin(
                                List.of("r_row_number", "r_regionkey", "r_name", "r_comment"),
                                new Constant(BOOLEAN, null),
                                tableScan("region", Map.of(
                                        "r_row_number", "row_number",
                                        "r_regionkey", "regionkey",
                                        "r_name", "name",
                                        "r_comment", "comment")),
                                anyTree(tableScan("nation")))));
        assertPlan(
                "SELECT regionkey, n.name FROM region LEFT JOIN LATERAL (SELECT name FROM nation) n ON NULL",
                any(
                        join(LEFT, builder -> builder
                                .equiCriteria(List.of())
                                .left(tableScan("region"))
                                .right(values("name")))));
    }

    @Test
    public void testCorrelatedScalarSubqueryInSelect()
    {
        assertDistributedPlan("SELECT name, (SELECT name FROM region WHERE regionkey = nation.regionkey) FROM nation",
                noJoinReordering(),
                anyTree(
                        filter(
                                new Switch(
                                        new Reference(BOOLEAN, "is_distinct"),
                                        ImmutableList.of(new WhenClause(TRUE, TRUE)),
                                        new Cast(new Call(FAIL, ImmutableList.of(new Constant(INTEGER, (long) SUBQUERY_MULTIPLE_ROWS.toErrorCode().getCode()), new Constant(VARCHAR, utf8Slice("Scalar sub-query has returned multiple rows")))), BOOLEAN)),
                                project(
                                        markDistinct("is_distinct", ImmutableList.of("unique"),
                                                join(LEFT, builder -> builder
                                                        .equiCriteria("n_regionkey", "r_regionkey")
                                                        .left(assignUniqueId("unique",
                                                                exchange(REMOTE, REPARTITION,
                                                                        tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey")))))
                                                        .right(anyTree(
                                                                tableScan("region", ImmutableMap.of("r_regionkey", "regionkey"))))))))));

        assertDistributedPlan("SELECT name, (SELECT name FROM region WHERE regionkey = nation.regionkey) FROM nation",
                automaticJoinDistribution(),
                anyTree(
                        filter(
                                new Switch(
                                        new Reference(BOOLEAN, "is_distinct"),
                                        ImmutableList.of(new WhenClause(TRUE, TRUE)),
                                        new Cast(new Call(FAIL, ImmutableList.of(new Constant(INTEGER, (long) SUBQUERY_MULTIPLE_ROWS.toErrorCode().getCode()), new Constant(VARCHAR, utf8Slice("Scalar sub-query has returned multiple rows")))), BOOLEAN)),
                                project(
                                        markDistinct("is_distinct", ImmutableList.of("unique"),
                                                join(LEFT, builder -> builder
                                                        .equiCriteria("n_regionkey", "r_regionkey")
                                                        .left(
                                                                assignUniqueId("unique",
                                                                        tableScan("nation", ImmutableMap.of("n_regionkey", "regionkey", "n_name", "name"))))
                                                        .right(
                                                                anyTree(
                                                                        tableScan("region", ImmutableMap.of("r_regionkey", "regionkey"))))))))));
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
                                ImmutableMap.of(Optional.of("max"), aggregationFunction("max", ImmutableList.of("r_name"))),
                                ImmutableList.of("n_name", "n_regionkey", "unique"),
                                ImmutableList.of("non_null"),
                                Optional.empty(),
                                SINGLE,
                                node(JoinNode.class,
                                        assignUniqueId("unique",
                                                exchange(REMOTE, REPARTITION,
                                                        tableScan("nation", ImmutableMap.of("n_name", "name", "n_regionkey", "regionkey")))),
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("non_null", expression(TRUE)),
                                                        tableScan("region", ImmutableMap.of("r_name", "name"))))))));

        // Don't use equi-clauses to trigger replicated join
        assertDistributedPlan(
                "SELECT name, (SELECT max(name) FROM region WHERE regionkey > nation.regionkey) FROM nation",
                anyTree(
                        aggregation(
                                singleGroupingSet("n_name", "n_regionkey", "unique"),
                                ImmutableMap.of(Optional.of("max"), aggregationFunction("max", ImmutableList.of("r_name"))),
                                ImmutableList.of("n_name", "n_regionkey", "unique"),
                                ImmutableList.of("non_null"),
                                Optional.empty(),
                                SINGLE,
                                node(JoinNode.class,
                                        assignUniqueId("unique",
                                                tableScan("nation", ImmutableMap.of("n_name", "name", "n_regionkey", "regionkey"))),
                                        anyTree(
                                                project(
                                                        ImmutableMap.of("non_null", expression(TRUE)),
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
                                singleGroupingSet("l_orderkey"),
                                ImmutableMap.of(Optional.empty(), aggregationFunction("count", ImmutableList.of())),
                                ImmutableList.of("l_orderkey"), // streaming
                                Optional.empty(),
                                SINGLE,
                                join(INNER, builder -> builder
                                        .equiCriteria("l_orderkey", "o_orderkey")
                                        .left(
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey"))))
                                        .right(
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("o_orderkey", "orderkey"))))))));

        // left join -> streaming aggregation
        assertPlan("SELECT o.orderkey, count(*) FROM orders o LEFT JOIN lineitem l ON o.orderkey=l.orderkey GROUP BY 1",
                anyTree(
                        aggregation(
                                singleGroupingSet("o_orderkey"),
                                ImmutableMap.of(Optional.empty(), aggregationFunction("count", ImmutableList.of())),
                                ImmutableList.of("o_orderkey"), // streaming
                                Optional.empty(),
                                SINGLE,
                                join(LEFT, builder -> builder
                                        .equiCriteria("o_orderkey", "l_orderkey")
                                        .left(
                                                tableScan("orders", ImmutableMap.of("o_orderkey", "orderkey")))
                                        .right(
                                                anyTree(
                                                        tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey"))))))));

        // cross join - no streaming
        assertPlan("SELECT o.orderkey, count(*) FROM orders o, lineitem l GROUP BY 1",
                anyTree(
                        aggregation(
                                singleGroupingSet("orderkey"),
                                ImmutableMap.of(Optional.empty(), aggregationFunction("count", ImmutableList.of())),
                                ImmutableList.of(), // not streaming
                                Optional.empty(),
                                SINGLE,
                                join(INNER, builder -> builder
                                        .left(tableScan("orders", ImmutableMap.of("orderkey", "orderkey")))
                                        .right(
                                                anyTree(
                                                        node(TableScanNode.class)))))));
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
                                        filter(
                                                new Comparison(LESS_THAN, new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 7L)), // pushed down
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
                        filter(
                                new Reference(BOOLEAN, "OUTER_FILTER"),
                                project(
                                        apply(ImmutableList.of("O", "C"),
                                                ImmutableMap.of("OUTER_FILTER", setExpression(new ApplyNode.In(new Symbol(UNKNOWN, "THREE"), new Symbol(UNKNOWN, "C")))),
                                                project(ImmutableMap.of("THREE", expression(new Constant(BIGINT, 3L))),
                                                        tableScan("orders", ImmutableMap.of(
                                                                "O", "orderkey",
                                                                "C", "custkey"))),
                                                project(
                                                        any(
                                                                tableScan("lineitem", ImmutableMap.of("L", "orderkey")))))))),
                optimizer -> !
                        (optimizer instanceof AddLocalExchanges
                                || optimizer instanceof CheckSubqueryNodesAreRewritten
                                || isPushPredicateIntoTableScanWithPrunePredicateOperation(optimizer)));
    }

    private boolean isPushPredicateIntoTableScanWithPrunePredicateOperation(PlanOptimizer optimizer)
    {
        if (optimizer instanceof IterativeOptimizer iterativeOptimizer) {
            return iterativeOptimizer.getRules().stream().anyMatch(rule -> {
                if (rule instanceof PushPredicateIntoTableScan pushPredicateIntoTableScan) {
                    return pushPredicateIntoTableScan.getPruneWithPredicateExpression();
                }
                return false;
            });
        }

        return false;
    }

    @Test
    public void testCorrelatedScalarAggregationRewriteToLeftOuterJoin()
    {
        assertPlan(
                "SELECT orderkey, EXISTS(SELECT 1 WHERE orderkey = 3) FROM orders", // EXISTS maps to count(*) > 0
                output(
                        strictProject(
                                ImmutableMap.of(
                                        "ORDERKEY", expression(new Reference(BIGINT, "ORDERKEY")),
                                        "exists", expression(new Comparison(GREATER_THAN, new Reference(BIGINT, "FINAL_COUNT"), new Constant(BIGINT, 0L)))),
                                aggregation(
                                        singleGroupingSet("ORDERKEY", "UNIQUE"),
                                        ImmutableMap.of(Optional.of("FINAL_COUNT"), aggregationFunction("count", ImmutableList.of())),
                                        ImmutableList.of("ORDERKEY", "UNIQUE"),
                                        ImmutableList.of("NON_NULL"),
                                        Optional.empty(),
                                        SINGLE,
                                        join(LEFT, builder -> builder
                                                .filter(new Comparison(EQUAL, new Constant(BIGINT, 3L), new Reference(BIGINT, "ORDERKEY")))
                                                .left(
                                                        assignUniqueId(
                                                                "UNIQUE",
                                                                tableScan("orders", ImmutableMap.of("ORDERKEY", "orderkey"))))
                                                .right(
                                                        project(ImmutableMap.of("NON_NULL", expression(TRUE)),
                                                                node(ValuesNode.class))))))));
    }

    @Test
    public void testCorrelatedDistinctAggregationRewriteToLeftOuterJoin()
    {
        assertPlan(
                "SELECT (SELECT count(DISTINCT o.orderkey) FROM orders o WHERE c.custkey = o.custkey), c.custkey FROM customer c",
                output(
                        project(
                                join(INNER, builder -> builder
                                        .left(
                                                join(LEFT, leftJoinBuilder -> leftJoinBuilder
                                                        .equiCriteria("c_custkey", "o_custkey")
                                                        .left(tableScan("customer", ImmutableMap.of("c_custkey", "custkey")))
                                                        .right(aggregation(
                                                                singleGroupingSet("o_custkey"),
                                                                ImmutableMap.of(Optional.of("count"), aggregationFunction("count", ImmutableList.of("o_orderkey"))),
                                                                ImmutableList.of(),
                                                                ImmutableList.of("non_null"),
                                                                Optional.empty(),
                                                                SINGLE,
                                                                project(ImmutableMap.of("non_null", expression(TRUE)),
                                                                        aggregation(
                                                                                singleGroupingSet("o_orderkey", "o_custkey"),
                                                                                ImmutableMap.of(),
                                                                                Optional.empty(),
                                                                                FINAL,
                                                                                anyTree(tableScan("orders", ImmutableMap.of("o_orderkey", "orderkey", "o_custkey", "custkey")))))))))
                                        .right(anyTree(node(ValuesNode.class)))))));
    }

    @Test
    public void testCorrelatedDistinctGroupedAggregationRewriteToLeftOuterJoin()
    {
        assertPlan(
                "SELECT (SELECT count(DISTINCT o.orderkey) FROM orders o WHERE c.custkey = o.custkey GROUP BY o.orderstatus), c.custkey FROM customer c",
                output(
                        project(filter(
                                new Switch(
                                        new Reference(BOOLEAN, "is_distinct"),
                                        ImmutableList.of(new WhenClause(TRUE, TRUE)),
                                        new Cast(new Call(FAIL, ImmutableList.of(new Constant(INTEGER, (long) SUBQUERY_MULTIPLE_ROWS.toErrorCode().getCode()), new Constant(VARCHAR, utf8Slice("Scalar sub-query has returned multiple rows")))), BOOLEAN)),
                                project(markDistinct(
                                        "is_distinct",
                                        ImmutableList.of("unique"),
                                        join(LEFT, builder -> builder
                                                .equiCriteria("c_custkey", "o_custkey")
                                                .left(
                                                        assignUniqueId(
                                                                "unique",
                                                                tableScan("customer", ImmutableMap.of("c_custkey", "custkey"))))
                                                .right(
                                                        project(aggregation(
                                                                singleGroupingSet("o_orderstatus", "o_custkey"),
                                                                ImmutableMap.of(Optional.of("count"), aggregationFunction("count", ImmutableList.of("o_orderkey"))),
                                                                Optional.empty(),
                                                                SINGLE,
                                                                aggregation(
                                                                        singleGroupingSet("o_orderstatus", "o_orderkey", "o_custkey"),
                                                                        ImmutableMap.of(),
                                                                        Optional.empty(),
                                                                        FINAL,
                                                                        anyTree(
                                                                                aggregation(
                                                                                        singleGroupingSet("o_orderstatus", "o_orderkey", "o_custkey"),
                                                                                        ImmutableMap.of(),
                                                                                        Optional.empty(),
                                                                                        PARTIAL,
                                                                                        tableScan(
                                                                                                "orders",
                                                                                                ImmutableMap.of("o_orderkey", "orderkey", "o_orderstatus", "orderstatus", "o_custkey", "custkey")))))))))))))));
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
    public void testRemovesNullFilter()
    {
        assertPlan(
                "SELECT * FROM nation WHERE null",
                output(
                        values("nationkey", "name", "regionkey", "comment")));
        assertPlan(
                "SELECT * FROM nation WHERE NOT null",
                output(
                        values("nationkey", "name", "regionkey", "comment")));
        assertPlan(
                "SELECT * FROM nation WHERE CAST(null AS BOOLEAN)",
                output(
                        values("nationkey", "name", "regionkey", "comment")));
        assertPlan(
                "SELECT * FROM nation WHERE NOT CAST(null AS BOOLEAN)",
                output(
                        values("nationkey", "name", "regionkey", "comment")));
        assertPlan(
                "SELECT * FROM nation WHERE nationkey = null",
                output(
                        values("nationkey", "name", "regionkey", "comment")));
        assertPlan(
                "SELECT * FROM nation WHERE nationkey = CAST(null AS BIGINT)",
                output(
                        values("nationkey", "name", "regionkey", "comment")));
        assertPlan(
                "SELECT * FROM nation WHERE nationkey < null OR nationkey > null",
                output(
                        values("nationkey", "name", "regionkey", "comment")));
        assertPlan(
                "SELECT * FROM nation WHERE nationkey = 19 AND CAST(null AS BOOLEAN)",
                output(
                        values("nationkey", "name", "regionkey", "comment")));
    }

    @Test
    public void testRemovesFalseFilter()
    {
        // Regression test for https://github.com/trinodb/trino/issues/16515
        assertPlan(
                "SELECT * FROM nation WHERE CAST(name AS varchar(1)) = 'PO'",
                output(
                        values("nationkey", "name", "regionkey", "comment")));
    }

    @Test
    public void testPruneCountAggregationOverScalar()
    {
        assertPlan(
                "SELECT count(*) FROM (SELECT sum(orderkey) FROM orders)",
                output(
                        values(ImmutableList.of("_col0"), ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 1L))))));
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
                                ImmutableMap.of("count_0", aggregationFunction("count", ImmutableList.of())),
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
                                ImmutableMap.of("count_0", aggregationFunction("count", ImmutableList.of())),
                                PARTIAL,
                                tableScan("nation", ImmutableMap.of("regionkey", "regionkey")))));
    }

    @Test
    public void testPickTableLayoutWithFilter()
    {
        assertPlan(
                "SELECT orderkey FROM orders WHERE orderkey=5",
                output(
                        filter(
                                new Comparison(EQUAL, new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 5L)),
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
        Session broadcastJoin = Session.builder(this.getPlanTester().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name())
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
        Consumer<Plan> validateSingleRemoteExchange = plan -> assertThat(countOfMatchingNodes(
                plan,
                node -> node instanceof ExchangeNode && ((ExchangeNode) node).getScope() == REMOTE)).isEqualTo(1);

        Consumer<Plan> validateSingleStreamingAggregation = plan -> assertThat(countOfMatchingNodes(
                plan,
                node -> node instanceof AggregationNode && ((AggregationNode) node).getGroupingKeys().contains(new Symbol(BIGINT, "unique"))
                        && ((AggregationNode) node).isStreamable())).isEqualTo(1);

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
        Session broadcastJoin = Session.builder(this.getPlanTester().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name())
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, Boolean.toString(false))
                .build();

        // replicated join with naturally partitioned and distributed probe side is rewritten to partitioned join
        assertPlanWithSession(
                "SELECT r1.regionkey FROM (SELECT regionkey FROM region GROUP BY regionkey) r1, region r2 WHERE r2.regionkey = r1.regionkey",
                broadcastJoin,
                false,
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("LEFT_REGIONKEY", "RIGHT_REGIONKEY")
                                .distributionType(PARTITIONED)
                                .left(
                                        aggregation(ImmutableMap.of(),
                                                anyTree(
                                                        exchange(REMOTE, REPARTITION,
                                                                anyTree(
                                                                        tableScan("region", ImmutableMap.of("LEFT_REGIONKEY", "regionkey")))))))
                                .right(
                                        anyTree(
                                                exchange(REMOTE, REPARTITION,
                                                        tableScan("region", ImmutableMap.of("RIGHT_REGIONKEY", "regionkey"))))))),
                plan -> // make sure there are only two remote exchanges (one in probe and one in build side)
                        assertThat(countOfMatchingNodes(
                                plan,
                                node -> node instanceof ExchangeNode && ((ExchangeNode) node).getScope() == REMOTE)).isEqualTo(2));

        // replicated join is preserved if probe side is single node
        assertPlanWithSession(
                "SELECT * FROM (VALUES 1, 2, 3) t(a), region r WHERE r.regionkey = t.a",
                Session.builder(broadcastJoin)
                        .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name())
                        .build(),
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
                        join(INNER, builder -> builder
                                .distributionType(REPLICATED)
                                .left(
                                        anyTree(
                                                node(TableScanNode.class)))
                                .right(
                                        anyTree(
                                                exchange(REMOTE, REPLICATE,
                                                        node(TableScanNode.class)))))));
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
                Session.builder(this.getPlanTester().getDefaultSession())
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
    public void testRemoveEmptyGlobalAggregation()
    {
        // unused aggregation result over a table
        assertPlan(
                "SELECT count(*) FROM (SELECT count(*) FROM nation)",
                output(
                        values(List.of("c"), List.of(List.of(new Constant(BIGINT, 1L))))));

        // unused aggregation result over values
        assertPlan(
                "SELECT count(*) FROM (SELECT count(*) FROM (VALUES 1,2,3,4,5,6,7))",
                output(
                        values(List.of("c"), List.of(List.of(new Constant(BIGINT, 1L))))));

        // unused aggregation result over unnest
        assertPlan(
                "SELECT count(*) FROM (SELECT count(*) FROM UNNEST(sequence(1, 10)))",
                output(
                        values(List.of("c"), List.of(List.of(new Constant(BIGINT, 1L))))));

        // no aggregate function at all over a table
        assertPlan(
                "SELECT 1 FROM nation GROUP BY GROUPING SETS (())",
                output(
                        values(List.of("c"), List.of(List.of(new Constant(INTEGER, 1L))))));

        // no aggregate function at all over values
        assertPlan(
                "SELECT 1 FROM (VALUES 1,2,3,4,5,6,7) GROUP BY GROUPING SETS (())",
                output(
                        values(List.of("c"), List.of(List.of(new Constant(INTEGER, 1L))))));

        // no aggregate function at all over unnest
        assertPlan(
                "SELECT 1 FROM UNNEST(sequence(1, 10)) GROUP BY GROUPING SETS (())",
                output(
                        values(List.of("c"), List.of(List.of(new Constant(INTEGER, 1L))))));
    }

    @Test
    public void testFilteringSemiJoinRewriteToInnerJoin()
    {
        assertPlan(
                "SELECT custkey FROM orders WHERE custkey IN (SELECT custkey FROM customer)",
                any(
                        join(INNER, builder -> builder
                                .equiCriteria("ORDER_CUSTKEY", "CUSTOMER_CUSTKEY")
                                .left(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("ORDER_CUSTKEY", "custkey"))))
                                .right(
                                        aggregation(
                                                singleGroupingSet("CUSTOMER_CUSTKEY"),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                FINAL,
                                                anyTree(
                                                        tableScan("customer", ImmutableMap.of("CUSTOMER_CUSTKEY", "custkey"))))))));
    }

    @Test
    public void testCorrelatedIn()
    {
        assertPlan(
                "SELECT name FROM region r WHERE regionkey IN (SELECT regionkey FROM nation WHERE name < r.name)",
                output(
                        project(
                                ImmutableMap.of("region_name", expression(new Reference(VARCHAR, "region_name"))),
                                aggregation(
                                        singleGroupingSet("region_regionkey", "region_name", "unique"),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        project(
                                                ImmutableMap.of(
                                                        "region_regionkey", expression(new Reference(BIGINT, "region_regionkey")),
                                                        "region_name", expression(new Reference(VARCHAR, "region_name")),
                                                        "unique", expression(new Reference(BIGINT, "unique"))),
                                                filter(
                                                        new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new IsNull(new Reference(BIGINT, "region_regionkey")), new Comparison(EQUAL, new Reference(BIGINT, "region_regionkey"), new Reference(BIGINT, "nation_regionkey")), new IsNull(new Reference(BIGINT, "nation_regionkey")))), new Comparison(LESS_THAN, new Reference(VARCHAR, "nation_name"), new Reference(VARCHAR, "region_name")))),
                                                        join(INNER, builder -> builder
                                                                .dynamicFilter(ImmutableList.of(new PlanMatchPattern.DynamicFilterPattern(new Reference(VARCHAR, "region_name"), GREATER_THAN, "nation_name")))
                                                                .left(
                                                                        assignUniqueId(
                                                                                "unique",
                                                                                filter(
                                                                                        new Not(new IsNull(new Reference(BIGINT, "region_regionkey"))),
                                                                                        tableScan("region", ImmutableMap.of(
                                                                                                "region_regionkey", "regionkey",
                                                                                                "region_name", "name")))))
                                                                .right(
                                                                        any(
                                                                                filter(
                                                                                        new Not(new IsNull(new Reference(BIGINT, "nation_regionkey"))),
                                                                                        tableScan("nation", ImmutableMap.of(
                                                                                                "nation_name", "name",
                                                                                                "nation_regionkey", "regionkey"))))))))))));
    }

    @Test
    public void testCorrelatedExists()
    {
        assertPlan(
                "SELECT regionkey, name FROM region r WHERE EXISTS(SELECT regionkey FROM nation WHERE name < r.name)",
                output(
                        project(
                                aggregation(
                                        singleGroupingSet("region_regionkey", "region_name", "unique"),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        project(
                                                filter(
                                                        new Comparison(LESS_THAN, new Reference(VARCHAR, "nation_name"), new Reference(VARCHAR, "region_name")),
                                                        join(INNER, builder -> builder
                                                                .dynamicFilter(ImmutableList.of(new PlanMatchPattern.DynamicFilterPattern(new Reference(VARCHAR, "region_name"), GREATER_THAN, "nation_name")))
                                                                .left(
                                                                        assignUniqueId(
                                                                                "unique",
                                                                                filter(
                                                                                        TRUE,
                                                                                        tableScan("region", ImmutableMap.of(
                                                                                                "region_regionkey", "regionkey",
                                                                                                "region_name", "name")))))
                                                                .right(
                                                                        any(
                                                                                tableScan("nation", ImmutableMap.of("nation_name", "name")))))))))));
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
                                ImmutableMap.of("name", expression(new Reference(VARCHAR, "name"))),
                                filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "row_num"), new Constant(BIGINT, 2L)),
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
                                ImmutableMap.of("name", expression(new Reference(VARCHAR, "name"))),
                                filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "row_num"), new Constant(BIGINT, 2L)),
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
                                ImmutableMap.of("name", expression(new Reference(VARCHAR, "name"))),
                                filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "row_num"), new Constant(BIGINT, 2L)),
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
                                ImmutableMap.of("name", expression(new Reference(VARCHAR, "name"))),
                                filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "row_num"), new Constant(BIGINT, 2L)),
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
                                ImmutableMap.of("name", expression(new Reference(VARCHAR, "name")), "regionkey", expression(new Reference(BIGINT, "regionkey"))),
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
                                ImmutableMap.of("name", expression(new Reference(VARCHAR, "name")), "regionkey", expression(new Reference(BIGINT, "regionkey"))),
                                filter(
                                        new Comparison(GREATER_THAN, new Reference(BIGINT, "row_num"), new Constant(BIGINT, 10L)),
                                        rowNumber(
                                                pattern -> pattern
                                                        .partitionBy(ImmutableList.of()),
                                                strictProject(
                                                        ImmutableMap.of("name", expression(new Reference(VARCHAR, "name")), "regionkey", expression(new Reference(BIGINT, "regionkey"))),
                                                        topNRanking(
                                                                pattern -> pattern
                                                                        .specification(
                                                                                ImmutableList.of(),
                                                                                ImmutableList.of("regionkey"),
                                                                                ImmutableMap.of("regionkey", ASC_NULLS_LAST))
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
        assertThat(searchFrom(plan(query, OPTIMIZED).getRoot())
                .where(LimitNode.class::isInstance)
                .matches())
                .describedAs(format("Unexpected limit node for query: '%s'", query))
                .isFalse();

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
    public void testLimitPushdownThroughUnionNesting()
    {
        assertPlan(
                """
                        SELECT col FROM (
                            SELECT nationkey FROM nation
                            UNION ALL
                            SELECT nationkey FROM nation
                            UNION ALL
                            SELECT nationkey FROM nation
                        ) AS t(col)
                        LIMIT 2""",
                output(
                        limit(
                                2,
                                ImmutableList.of(),
                                false,
                                exchange(
                                        LOCAL,
                                        GATHER,
                                        exchange(
                                                LOCAL,
                                                REPARTITION,
                                                limit(2, ImmutableList.of(), true, tableScan("nation")),
                                                limit(2, ImmutableList.of(), true, tableScan("nation")),
                                                limit(2, ImmutableList.of(), true, tableScan("nation")))))));
    }

    @Test
    public void testRemoveSingleRowSort()
    {
        String query = "SELECT count(*) FROM orders ORDER BY 1";
        assertThat(searchFrom(plan(query, OPTIMIZED).getRoot())
                .whereIsInstanceOfAny(SortNode.class)
                .matches())
                .describedAs(format("Unexpected sort node for query: '%s'", query))
                .isFalse();

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
        assertThat(searchFrom(plan(query, OPTIMIZED).getRoot())
                .whereIsInstanceOfAny(TopNNode.class, SortNode.class)
                .matches())
                .describedAs(format("Unexpected TopN node for query: '%s'", query))
                .isFalse();

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
        assertThat(searchFrom(plan(query, OPTIMIZED).getRoot())
                .whereIsInstanceOfAny(DistinctLimitNode.class)
                .matches())
                .describedAs(format("Unexpected DistinctLimit node for query: '%s'", query))
                .isFalse();

        assertPlan(
                "SELECT distinct(c) FROM (SELECT count(*) as c FROM orders GROUP BY orderkey) LIMIT 10",
                output(
                        node(DistinctLimitNode.class,
                                anyTree(
                                        tableScan("orders")))));

        assertPlan(
                "SELECT distinct(id) FROM (VALUES 1, 2, 3, 4, 5, 6) as t1 (id) LIMIT 10",
                output(
                        node(AggregationNode.class,
                                values(ImmutableList.of("x")))));
    }

    @Test
    public void testRedundantHashRemovalForUnionAll()
    {
        assertPlan(
                "SELECT count(*) FROM ((SELECT nationkey FROM customer) UNION ALL (SELECT nationkey FROM customer)) GROUP BY nationkey",
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(OPTIMIZE_HASH_GENERATION, "true")
                        .build(),
                output(
                        project(
                                node(AggregationNode.class,
                                        exchange(LOCAL, REPARTITION,
                                                project(ImmutableMap.of("hash", expression(new Call(COMBINE_HASH, ImmutableList.of(new Constant(BIGINT, 0L), new Coalesce(new Call(HASH_CODE, ImmutableList.of(new Reference(BIGINT, "nationkey"))), new Constant(BIGINT, 0L)))))),
                                                        node(AggregationNode.class,
                                                                tableScan("customer", ImmutableMap.of("nationkey", "nationkey")))),
                                                project(ImmutableMap.of("hash_1", expression(new Call(COMBINE_HASH, ImmutableList.of(new Constant(BIGINT, 0L), new Coalesce(new Call(HASH_CODE, ImmutableList.of(new Reference(BIGINT, "nationkey_6"))), new Constant(BIGINT, 0L)))))),
                                                        node(AggregationNode.class,
                                                                tableScan("customer", ImmutableMap.of("nationkey_6", "nationkey")))))))));
    }

    @Test
    public void testRedundantHashRemovalForMarkDistinct()
    {
        assertDistributedPlan(
                "select count(*), count(distinct orderkey), count(distinct partkey), count(distinct suppkey) from lineitem",
                Session.builder(this.getPlanTester().getDefaultSession())
                        .setSystemProperty(OPTIMIZE_HASH_GENERATION, "true")
                        .setSystemProperty(TASK_CONCURRENCY, "16")
                        .build(),
                output(
                        anyTree(
                                identityProject(
                                        node(MarkDistinctNode.class,
                                                anyTree(
                                                        project(ImmutableMap.of(
                                                                        "hash_1", expression(new Call(COMBINE_HASH, ImmutableList.of(new Constant(BIGINT, 0L), new Coalesce(new Call(HASH_CODE, ImmutableList.of(new Reference(BIGINT, "suppkey"))), new Constant(BIGINT, 0L))))),
                                                                        "hash_2", expression(new Call(COMBINE_HASH, ImmutableList.of(new Constant(BIGINT, 0L), new Coalesce(new Call(HASH_CODE, ImmutableList.of(new Reference(BIGINT, "partkey"))), new Constant(BIGINT, 0L)))))),
                                                                node(MarkDistinctNode.class,
                                                                        tableScan("lineitem", ImmutableMap.of("suppkey", "suppkey", "partkey", "partkey"))))))))));
    }

    @Test
    public void testRedundantHashRemovalForUnionAllAndMarkDistinct()
    {
        assertDistributedPlan(
                "SELECT count(distinct(custkey)), count(distinct(nationkey)) FROM ((SELECT custkey, nationkey FROM customer) UNION ALL ( SELECT custkey, custkey FROM customer))",
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(OPTIMIZE_HASH_GENERATION, "true")
                        .build(),
                output(
                        anyTree(
                                node(MarkDistinctNode.class,
                                        anyTree(
                                                node(MarkDistinctNode.class,
                                                        exchange(LOCAL, REPARTITION,
                                                                exchange(REMOTE, REPARTITION,
                                                                        project(ImmutableMap.of(
                                                                                        "hash_custkey", expression(new Call(COMBINE_HASH, ImmutableList.of(new Constant(BIGINT, 0L), new Coalesce(new Call(HASH_CODE, ImmutableList.of(new Reference(BIGINT, "custkey"))), new Constant(BIGINT, 0L))))),
                                                                                        "hash_nationkey", expression(new Call(COMBINE_HASH, ImmutableList.of(new Constant(BIGINT, 0L), new Coalesce(new Call(HASH_CODE, ImmutableList.of(new Reference(BIGINT, "nationkey"))), new Constant(BIGINT, 0L)))))),
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
                any(
                        join(INNER, builder -> builder
                                .equiCriteria("ORDER_STATUS", "expr")
                                .left(
                                        filter(TRUE,
                                                strictConstrainedTableScan(
                                                        "orders",
                                                        ImmutableMap.of("ORDER_STATUS", "orderstatus", "ORDER_KEY", "orderkey"),
                                                        ImmutableMap.of("orderstatus", multipleValues(createVarcharType(1), ImmutableList.of(utf8Slice("F"), utf8Slice("O")))))))
                                .right(
                                        filter(
                                                new In(new Reference(createVarcharType(1), "expr"), ImmutableList.of(new Constant(createVarcharType(1), utf8Slice("F")), new Constant(createVarcharType(1), utf8Slice("O")))),
                                                values(
                                                        ImmutableList.of("expr"),
                                                        ImmutableList.of(ImmutableList.of(new Constant(createVarcharType(1), utf8Slice("O"))), ImmutableList.of(new Constant(createVarcharType(1), utf8Slice("F"))))))))));
    }

    @Test
    public void testRemoveRedundantCrossJoin()
    {
        assertPlan("SELECT regionkey FROM nation, (SELECT 1 as a) temp WHERE regionkey = temp.a",
                output(
                        filter(new Comparison(EQUAL, new Reference(BIGINT, "REGIONKEY"), new Constant(BIGINT, 1L)),
                                tableScan("nation", ImmutableMap.of("REGIONKEY", "regionkey")))));

        assertPlan("SELECT regionkey FROM (SELECT 1 as a) temp, nation WHERE regionkey > temp.a",
                output(
                        filter(
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "REGIONKEY"), new Constant(BIGINT, 1L)),
                                tableScan("nation", ImmutableMap.of("REGIONKEY", "regionkey")))));

        assertPlan("SELECT * FROM nation, (SELECT 1 as a) temp WHERE regionkey = a",
                output(
                        project(
                                ImmutableMap.of("expr", expression(new Constant(INTEGER, 1L))),
                                filter(
                                        new Comparison(EQUAL, new Reference(BIGINT, "REGIONKEY"), new Constant(BIGINT, 1L)),
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
                        join(INNER, builder -> builder
                                .left(tableScan("nation"))
                                .right(
                                        values(ImmutableList.of("a"),
                                                ImmutableList.of(
                                                        ImmutableList.of(new Constant(INTEGER, 2L)),
                                                        ImmutableList.of(new Constant(INTEGER, 4L)),
                                                        ImmutableList.of(new Constant(INTEGER, 6L))))))));

        // Constraint is enforced on table scan, based on constant value in the other branch of the join.
        // The scalar constant branch of the join becomes obsolete, and join is removed.
        assertPlan(
                "SELECT orderkey, t2.s " +
                        "FROM orders " +
                        "JOIN (SELECT '' || x FROM (VALUES 'F') t(x)) t2(s) " +
                        "ON orders.orderstatus = t2.s",
                any(project(
                        ImmutableMap.of("cast", expression(new Cast(new Reference(createVarcharType(1), "ORDER_STATUS"), VARCHAR))),
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
                        join(INNER, builder -> builder
                                .equiCriteria("ORDER_STATUS", "expr")
                                .left(
                                        filter(TRUE,
                                                strictConstrainedTableScan(
                                                        "orders",
                                                        ImmutableMap.of("ORDER_STATUS", "orderstatus", "ORDER_KEY", "orderkey"),
                                                        ImmutableMap.of("orderstatus", multipleValues(createVarcharType(1), ImmutableList.of(utf8Slice("F"), utf8Slice("O")))))))
                                .right(
                                        filter(
                                                new In(new Reference(createVarcharType(1), "expr"), ImmutableList.of(new Constant(createVarcharType(1), utf8Slice("F")), new Constant(createVarcharType(1), utf8Slice("O")))),
                                                values(ImmutableList.of("expr"), ImmutableList.of(ImmutableList.of(new Constant(createVarcharType(1), utf8Slice("O"))), ImmutableList.of(new Constant(createVarcharType(1), utf8Slice("F"))))))))));

        // Constraint for the table is derived, based on constant values in the other branch of the join.
        // It is not accepted by the connector, and remains in form of a filter over TableScan.
        assertPlan(
                "SELECT orderstatus, t2.s " +
                        "FROM orders " +
                        "JOIN (SELECT x * 1 FROM (VALUES BIGINT '1', BIGINT '2') t(x)) t2(s) " +
                        "ON orders.orderkey = t2.s",
                anyTree(
                        join(INNER, builder -> builder
                                .equiCriteria("ORDER_KEY", "expr")
                                .left(
                                        filter(
                                                new In(new Reference(BIGINT, "ORDER_KEY"), ImmutableList.of(new Constant(BIGINT, 1L), new Constant(BIGINT, 2L))),
                                                strictConstrainedTableScan(
                                                        "orders",
                                                        ImmutableMap.of("ORDER_STATUS", "orderstatus", "ORDER_KEY", "orderkey"),
                                                        ImmutableMap.of())))
                                .right(
                                        filter(
                                                new In(new Reference(BIGINT, "expr"), ImmutableList.of(new Constant(BIGINT, 1L), new Constant(BIGINT, 2L))),
                                                values(ImmutableList.of("expr"), ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 1L)), ImmutableList.of(new Constant(BIGINT, 2L)))))))));
    }

    @Test
    public void testReplaceJoinWithProject()
    {
        assertPlan(
                "SELECT * FROM nation, (SELECT a * 2 FROM (VALUES 1) t(a))",
                any(
                        project(
                                ImmutableMap.of("expr", expression(new Constant(INTEGER, 2L))),
                                tableScan("nation"))));

        assertPlan(
                "SELECT * FROM nation, (SELECT b * 3 FROM (SELECT a * 2 FROM (VALUES 1) t1(a)) t2(b))",
                any(
                        project(
                                ImmutableMap.of("expr", expression(new Constant(INTEGER, 6L))),
                                tableScan("nation"))));
    }

    @Test
    public void testGroupingSetsWithDefaultValue()
    {
        assertDistributedPlan("SELECT orderkey, COUNT(DISTINCT k) FROM (SELECT orderkey, 1 k FROM orders) GROUP BY GROUPING SETS ((), orderkey)",
                output(
                        anyTree(
                                aggregation(
                                        ImmutableMap.of("final_count", aggregationFunction("count", ImmutableList.of("partial_count"))),
                                        FINAL,
                                        exchange(
                                                LOCAL,
                                                REPARTITION,
                                                exchange(
                                                        REMOTE,
                                                        REPARTITION,
                                                        aggregation(
                                                                ImmutableMap.of("partial_count", aggregationFunction("count", ImmutableList.of("CONSTANT"))),
                                                                PARTIAL,
                                                                anyTree(
                                                                        project(
                                                                                ImmutableMap.of("CONSTANT", expression(new Constant(INTEGER, 1L))),
                                                                                tableScan("orders"))))))))));
    }

    @Test
    public void testSizeBasedJoin()
    {
        // both local.sf100000.nation and local.sf100000.orders don't provide stats, therefore no reordering happens
        assertDistributedPlan("SELECT custkey FROM \"test_catalog\".\"sf42.5\".nation, \"test_catalog\".\"sf42.5\".orders WHERE nation.nationkey = orders.custkey",
                automaticJoinDistribution(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("NATIONKEY", "CUSTKEY")
                                .left(
                                        anyTree(
                                                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey"))))
                                .right(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("CUSTKEY", "custkey")))))));

        assertDistributedPlan("SELECT custkey FROM (VALUES CAST(1 AS BIGINT), CAST(2 AS BIGINT)) t(a), \"test_catalog\".\"sf42.5\".orders WHERE t.a = orders.custkey",
                automaticJoinDistribution(),
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("CUSTKEY", "T_A")
                                .distributionType(REPLICATED)
                                .left(
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("CUSTKEY", "custkey"))))
                                .right(
                                        anyTree(
                                                values("T_A"))))));
    }

    @Test
    public void testSizeBasedSemiJoin()
    {
        // both local.sf100000.nation and local.sf100000.orders don't provide stats, therefore no reordering happens
        assertDistributedPlan("SELECT custkey FROM \"test_catalog\".\"sf42.5\".orders WHERE orders.custkey NOT IN (SELECT nationkey FROM \"test_catalog\".\"sf42.5\".nation)",
                automaticJoinDistribution(),
                output(
                        anyTree(
                                semiJoin("CUSTKEY", "NATIONKEY", "OUT", Optional.of(DistributionType.PARTITIONED),
                                        anyTree(
                                                tableScan("orders", ImmutableMap.of("CUSTKEY", "custkey"))),
                                        anyTree(
                                                tableScan("nation", ImmutableMap.of("NATIONKEY", "nationkey")))))));

        // values node provides stats
        assertDistributedPlan("SELECT custkey FROM \"test_catalog\".\"sf42.5\".orders WHERE orders.custkey NOT IN (SELECT t.a FROM (VALUES CAST(1 AS BIGINT), CAST(2 AS BIGINT)) t(a))",
                automaticJoinDistribution(),
                output(
                        anyTree(
                                semiJoin("CUSTKEY", "T_A", "OUT", Optional.of(DistributionType.REPLICATED),
                                        tableScan("orders", ImmutableMap.of("CUSTKEY", "custkey")),
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
                                        ImmutableList.of(new Cast(new Constant(TINYINT, 1L), REAL)),
                                        ImmutableList.of(new Constant(REAL, Reals.toReal(1f)))))));

        // rows coerced by field
        assertPlan("VALUES (TINYINT '1', REAL '1'), (DOUBLE '2', SMALLINT '2')",
                CREATED,
                anyTree(
                        values(
                                ImmutableList.of("field", "field0"),
                                ImmutableList.of(
                                        ImmutableList.of(new Cast(new Constant(TINYINT, 1L), DOUBLE), new Constant(REAL, Reals.toReal(1f))),
                                        ImmutableList.of(new Constant(DOUBLE, 2.0), new Cast(new Constant(SMALLINT, 2L), REAL))))));

        // entry of type other than Row coerced as a whole
        assertPlan("VALUES DOUBLE '1', CAST(ROW(2) AS row(bigint))",
                CREATED,
                anyTree(
                        values(
                                aliasToIndex(ImmutableList.of("field")),
                                Optional.of(1),
                                Optional.of(ImmutableList.of(
                                        new Row(ImmutableList.of(new Constant(DOUBLE, (double) 1))),
                                        new Cast(
                                                new Cast(
                                                        new Row(ImmutableList.of(new Constant(INTEGER, 2L))),
                                                        RowType.anonymous(ImmutableList.of(BIGINT))),
                                                RowType.anonymous(ImmutableList.of(DOUBLE))))))));
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
                                                .addMeasure(
                                                        "val",
                                                        new Reference(INTEGER, "val"),
                                                        ImmutableMap.of("val", new ScalarValuePointer(
                                                                new LogicalIndexPointer(ImmutableSet.of(), true, true, 0, 0),
                                                                new Symbol(UNKNOWN, "value"))),
                                                        INTEGER)
                                                .rowsPerMatch(WINDOW)
                                                .frame(ROWS_FROM_CURRENT)
                                                .pattern(new IrQuantified(new IrLabel("A"), oneOrMore(true)))
                                                .addVariableDefinition(new IrLabel("A"), TRUE),
                                        values(
                                                ImmutableList.of("id", "value"),
                                                ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 90L))))))));

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
                                                .addFunction("min", windowFunction(
                                                        "min",
                                                        ImmutableList.of("value"),
                                                        ROWS_FROM_CURRENT))
                                                .rowsPerMatch(WINDOW)
                                                .frame(ROWS_FROM_CURRENT)
                                                .pattern(new IrQuantified(new IrLabel("A"), oneOrMore(true)))
                                                .addVariableDefinition(new IrLabel("A"), TRUE),
                                        values(
                                                ImmutableList.of("id", "value"),
                                                ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 90L))))))));
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
                                                .addFunction("min", windowFunction(
                                                        "min",
                                                        ImmutableList.of("value"),
                                                        ROWS_FROM_CURRENT))
                                                .rowsPerMatch(WINDOW)
                                                .frame(ROWS_FROM_CURRENT)
                                                .pattern(new IrQuantified(new IrLabel("A"), oneOrMore(true)))
                                                .addVariableDefinition(new IrLabel("A"), TRUE),
                                        values(
                                                ImmutableList.of("id", "value"),
                                                ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 90L))))))));
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
                                                .addMeasure(
                                                        "val",
                                                        new Reference(INTEGER, "val"),
                                                        ImmutableMap.of("val", new ScalarValuePointer(
                                                                new LogicalIndexPointer(ImmutableSet.of(), true, true, 0, 0),
                                                                new Symbol(UNKNOWN, "value"))),
                                                        INTEGER)
                                                .rowsPerMatch(WINDOW)
                                                .frame(ROWS_FROM_CURRENT)
                                                .pattern(new IrQuantified(new IrLabel("A"), oneOrMore(true)))
                                                .addVariableDefinition(new IrLabel("A"), TRUE),
                                        values(
                                                ImmutableList.of("id", "value"),
                                                ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 90L))))))));
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
                                                .addMeasure(
                                                        "val",
                                                        new Reference(INTEGER, "val"),
                                                        ImmutableMap.of("val", new ScalarValuePointer(
                                                                new LogicalIndexPointer(ImmutableSet.of(), true, true, 0, 0),
                                                                new Symbol(UNKNOWN, "value"))),
                                                        INTEGER)
                                                .addMeasure(
                                                        "label",
                                                        new Reference(VARCHAR, "classy"),
                                                        ImmutableMap.of("classy", new ClassifierValuePointer(
                                                                new LogicalIndexPointer(ImmutableSet.of(), true, true, 0, 0))),
                                                        VARCHAR)
                                                .addFunction("row_number", windowFunction(
                                                        "row_number",
                                                        ImmutableList.of(),
                                                        ROWS_FROM_CURRENT))
                                                .rowsPerMatch(WINDOW)
                                                .frame(ROWS_FROM_CURRENT)
                                                .pattern(new IrQuantified(new IrLabel("A"), oneOrMore(true)))
                                                .addVariableDefinition(new IrLabel("A"), TRUE),
                                        values(
                                                ImmutableList.of("id", "value"),
                                                ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 90L))))))));
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
                                        "output1", expression(new Reference(INTEGER, "id")),
                                        "output2", expression(new Call(MULTIPLY_INTEGER, ImmutableList.of(new Reference(INTEGER, "value"), new Constant(INTEGER, 2L)))),
                                        "output3", expression(new Call(LOWER, ImmutableList.of(new Reference(VARCHAR, "label")))),
                                        "output4", expression(new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "min"), new Constant(INTEGER, 1L))))),
                                project(
                                        ImmutableMap.of(
                                                "id", expression(new Reference(INTEGER, "id")),
                                                "value", expression(new Reference(INTEGER, "value")),
                                                "label", expression(new Reference(VARCHAR, "label")),
                                                "min", expression(new Reference(INTEGER, "min"))),
                                        patternRecognition(builder -> builder
                                                        .specification(specification(ImmutableList.of(), ImmutableList.of("id"), ImmutableMap.of("id", ASC_NULLS_LAST)))
                                                        .addMeasure(
                                                                "value",
                                                                new Reference(INTEGER, "value"),
                                                                ImmutableMap.of("value", new ScalarValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(), true, true, 0, 0),
                                                                        new Symbol(UNKNOWN, "input2"))),
                                                                INTEGER)
                                                        .addMeasure(
                                                                "label",
                                                                new Reference(VARCHAR, "classy"),
                                                                ImmutableMap.of("classy", new ClassifierValuePointer(
                                                                        new LogicalIndexPointer(ImmutableSet.of(), true, true, 0, 0))),
                                                                VARCHAR)
                                                        .addFunction("min", windowFunction(
                                                                "min",
                                                                ImmutableList.of("input1"),
                                                                ROWS_FROM_CURRENT))
                                                        .rowsPerMatch(WINDOW)
                                                        .frame(ROWS_FROM_CURRENT)
                                                        .pattern(new IrQuantified(new IrLabel("A"), oneOrMore(true)))
                                                        .addVariableDefinition(new IrLabel("A"), TRUE),
                                                values(
                                                        ImmutableList.of("id", "input1", "input2"),
                                                        ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 2L), new Constant(INTEGER, 3L)))))))));
    }

    @Test
    public void testDifferentOuterParentScopeSubqueries()
    {
        assertPlan("SELECT customer.custkey AS custkey," +
                        "(SELECT COUNT(*) FROM orders WHERE customer.custkey = orders.custkey) AS count1," +
                        "(SELECT COUNT(*) FROM orders WHERE orders.custkey = customer.custkey) AS count2 " +
                        "FROM customer",
                output(
                        project(
                                join(INNER, builder -> builder
                                        .left(
                                                join(LEFT, leftJoinBuilder -> leftJoinBuilder
                                                        .equiCriteria("CUSTOMER_CUSTKEY", "ORDERS2_CUSTKEY")
                                                        .left(
                                                                project(
                                                                        join(INNER, leftInnerJoinBuilder -> leftInnerJoinBuilder
                                                                                .left(
                                                                                        join(LEFT, innerBuilder -> innerBuilder
                                                                                                .equiCriteria("CUSTOMER_CUSTKEY", "ORDERS_CUSTKEY")
                                                                                                .left(tableScan("customer", ImmutableMap.of("CUSTOMER_CUSTKEY", "custkey")))
                                                                                                .right(anyTree(project(tableScan("orders", ImmutableMap.of("ORDERS_CUSTKEY", "custkey")))))))
                                                                                .right(anyTree(node(ValuesNode.class))))))
                                                        .right(
                                                                anyTree(tableScan("orders", ImmutableMap.of("ORDERS2_CUSTKEY", "custkey"))))))
                                        .right(
                                                anyTree(node(ValuesNode.class)))))));
    }

    @Test
    public void testDecorrelateSingleRowSubquery()
    {
        assertPlan("SELECT * FROM (VALUES 1, 2, 3) t(a), LATERAL (VALUES a * 3)",
                output(
                        values(ImmutableList.of("a", "expr"), ImmutableList.of(
                                ImmutableList.of(new Constant(INTEGER, 1L), new Constant(INTEGER, 3L)),
                                ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 6L)),
                                ImmutableList.of(new Constant(INTEGER, 3L), new Constant(INTEGER, 9L))))));
    }

    @Test
    public void testPruneWindow()
    {
        assertPlan("SELECT count() OVER() c FROM (SELECT 1 WHERE false)",
                output(
                        values("c")));
    }

    private Session noJoinReordering()
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .build();
    }

    private Session automaticJoinDistribution()
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .build();
    }

    private Session noSemiJoinRewrite()
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "false")
                .build();
    }
}
