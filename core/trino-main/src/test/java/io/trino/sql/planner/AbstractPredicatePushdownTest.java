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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.FILTERING_SEMI_JOIN_TO_INNER;
import static io.trino.SystemSessionProperties.ITERATIVE_OPTIMIZER_TIMEOUT;
import static io.trino.SystemSessionProperties.ITERATIVE_PREDICATE_PUSHDOWN_ENABLED;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.NOT_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.constrainedTableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.SemiJoinDynamicFilterProducer.dynamicFilter;
import static io.trino.sql.planner.assertions.SemiJoinDynamicFilterProducer.noDynamicFilter;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractPredicatePushdownTest
        extends BasePlanTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", fromTypes());
    private static final ResolvedFunction RANDOM_INTEGER = FUNCTIONS.resolveFunction("random", fromTypes(INTEGER));
    private static final ResolvedFunction ROUND = FUNCTIONS.resolveFunction("round", fromTypes(DOUBLE));
    private static final ResolvedFunction LENGTH = FUNCTIONS.resolveFunction("length", fromTypes(createVarcharType(1)));
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction MULTIPLY_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction DIVIDE_INTEGER = FUNCTIONS.resolveOperator(OperatorType.DIVIDE, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction SUBTRACT_INTEGER = FUNCTIONS.resolveOperator(OperatorType.SUBTRACT, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction MULTIPLY_DOUBLE = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(DOUBLE, DOUBLE));

    private final boolean enableDynamicFiltering;
    private Map<Boolean, PlanTester> planTesters;

    protected AbstractPredicatePushdownTest(boolean enableDynamicFiltering)
    {
        super(ImmutableMap.of(ENABLE_DYNAMIC_FILTERING, Boolean.toString(enableDynamicFiltering)));
        this.enableDynamicFiltering = enableDynamicFiltering;
    }

    @BeforeAll
    public void setupPlanTesters()
    {
        planTesters = ImmutableMap.of(
                true, getPlanTester(),
                false, createPlanTester(ImmutableMap.of(
                        ENABLE_DYNAMIC_FILTERING, Boolean.toString(enableDynamicFiltering),
                        ITERATIVE_PREDICATE_PUSHDOWN_ENABLED, "false")));
    }

    @AfterAll
    public void closeLegacyPlanTester()
    {
        // BasePlanTest owns and closes the default implementation's tester.
        closeAllRuntimeException(planTesters.get(false));
        planTesters = null;
    }

    protected PlanTester getPlanTester(boolean iterativePredicatePushdown)
    {
        return planTesters.get(iterativePredicatePushdown);
    }

    protected static Stream<Arguments> allJoins()
    {
        return joins(Stream.of(JoinType.values()));
    }

    protected static Stream<Arguments> joinsWithPreservedInput()
    {
        return joins(Stream.of(JoinType.INNER, JoinType.LEFT, JoinType.RIGHT));
    }

    private static Stream<Arguments> joins(Stream<JoinType> joinTypes)
    {
        return joinTypes.flatMap(joinType -> Stream.of(
                Arguments.of(true, joinType),
                Arguments.of(false, joinType)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public abstract void testCoercions(boolean iterativePredicatePushdown);

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPushDownToLhsOfSemiJoin(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(
                planTester,
                "SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders)) " +
                        "WHERE linenumber = 2",
                noSemiJoinRewrite(planTester),
                anyTree(
                        semiJoin("LINE_ORDER_KEY",
                                "ORDERS_ORDER_KEY",
                                "SEMI_JOIN_RESULT",
                                enableDynamicFiltering ? dynamicFilter("DF") : noDynamicFilter(),
                                anyTree(
                                        filter(
                                                comparison(EQUAL, new Reference(INTEGER, "LINE_NUMBER"), new Constant(INTEGER, 2L)),
                                                dynamicFilters -> {
                                                    if (enableDynamicFiltering) {
                                                        dynamicFilters.addConsumer(consumer -> consumer.alias("DF").expression(BIGINT, "LINE_ORDER_KEY"));
                                                    }
                                                    else {
                                                        dynamicFilters.noConsumers();
                                                    }
                                                },
                                                tableScan("lineitem", ImmutableMap.of(
                                                        "LINE_ORDER_KEY", "orderkey",
                                                        "LINE_NUMBER", "linenumber",
                                                        "LINE_QUANTITY", "quantity")))),
                                anyTree(tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey"))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testNonDeterministicPredicatePropagatesOnlyToSourceSideOfSemiJoin(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(planTester,
                "SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders) AND orderkey = random(5)",
                noSemiJoinRewrite(planTester),
                anyTree(
                        semiJoin("LINE_ORDER_KEY",
                                "ORDERS_ORDER_KEY",
                                "SEMI_JOIN_RESULT",
                                enableDynamicFiltering ? dynamicFilter("DF") : noDynamicFilter(),
                                filter(
                                        comparison(EQUAL, new Reference(BIGINT, "LINE_ORDER_KEY"), new Cast(new Call(RANDOM_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L))), BIGINT)),
                                        dynamicFilters -> {
                                            if (enableDynamicFiltering) {
                                                dynamicFilters.addConsumer(consumer -> consumer.alias("DF").expression(BIGINT, "LINE_ORDER_KEY"));
                                            }
                                            else {
                                                dynamicFilters.noConsumers();
                                            }
                                        },
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey"))),
                                node(ExchangeNode.class, // NO filter here
                                        tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey"))))));

        assertPlan(planTester, "SELECT * FROM lineitem WHERE orderkey NOT IN (SELECT orderkey FROM orders) AND orderkey = random(5)",
                anyTree(
                        semiJoin("LINE_ORDER_KEY",
                                "ORDERS_ORDER_KEY",
                                "SEMI_JOIN_RESULT",
                                filter(
                                        comparison(EQUAL, new Reference(BIGINT, "LINE_ORDER_KEY"), new Cast(new Call(RANDOM_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L))), BIGINT)),
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey"))),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey"))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGreaterPredicateFromFilterSidePropagatesToSourceSideOfSemiJoin(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(planTester,
                "SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey > 2))",
                noSemiJoinRewrite(planTester),
                anyTree(
                        semiJoin("LINE_ORDER_KEY",
                                "ORDERS_ORDER_KEY",
                                "SEMI_JOIN_RESULT",
                                enableDynamicFiltering ? dynamicFilter("DF") : noDynamicFilter(),
                                filter(comparison(GREATER_THAN, new Reference(BIGINT, "LINE_ORDER_KEY"), new Constant(BIGINT, 2L)),
                                        dynamicFilters -> {
                                            if (enableDynamicFiltering) {
                                                dynamicFilters.addConsumer(consumer -> consumer.alias("DF").expression(BIGINT, "LINE_ORDER_KEY"));
                                            }
                                            else {
                                                dynamicFilters.noConsumers();
                                            }
                                        },
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey",
                                                "LINE_QUANTITY", "quantity"))),
                                anyTree(
                                        filter(
                                                comparison(GREATER_THAN, new Reference(BIGINT, "ORDERS_ORDER_KEY"), new Constant(BIGINT, 2L)),
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEqualsPredicateFromFilterSidePropagatesToSourceSideOfSemiJoin(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(planTester,
                "SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey = 2))",
                noSemiJoinRewrite(planTester),
                anyTree(
                        semiJoin("LINE_ORDER_KEY",
                                "ORDERS_ORDER_KEY",
                                "SEMI_JOIN_RESULT",
                                enableDynamicFiltering ? dynamicFilter("DF") : noDynamicFilter(),
                                filter(
                                        comparison(EQUAL, new Reference(BIGINT, "LINE_ORDER_KEY"), new Constant(BIGINT, 2L)),
                                        dynamicFilters -> {
                                            if (enableDynamicFiltering) {
                                                dynamicFilters.addConsumer(consumer -> consumer.alias("DF").expression(BIGINT, "LINE_ORDER_KEY"));
                                            }
                                            else {
                                                dynamicFilters.noConsumers();
                                            }
                                        },
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey",
                                                "LINE_QUANTITY", "quantity"))),
                                anyTree(
                                        filter(
                                                comparison(EQUAL, new Reference(BIGINT, "ORDERS_ORDER_KEY"), new Constant(BIGINT, 2L)),
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicateFromFilterSideNotPropagatesToSourceSideOfSemiJoinIfNotIn(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(planTester, "SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey NOT IN (SELECT orderkey FROM orders WHERE orderkey > 2))",
                anyTree(
                        semiJoin("LINE_ORDER_KEY",
                                "ORDERS_ORDER_KEY",
                                "SEMI_JOIN_RESULT",
                                // There should be no Filter above table scan, because we don't know whether SemiJoin's filtering source is empty.
                                // And filter would filter out NULLs from source side which is not what we need then.
                                tableScan("lineitem", ImmutableMap.of(
                                        "LINE_ORDER_KEY", "orderkey",
                                        "LINE_QUANTITY", "quantity")),
                                anyTree(
                                        filter(
                                                comparison(GREATER_THAN, new Reference(BIGINT, "ORDERS_ORDER_KEY"), new Constant(BIGINT, 2L)),
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGreaterPredicateFromSourceSidePropagatesToFilterSideOfSemiJoin(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(planTester,
                "SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders) AND orderkey > 2)",
                noSemiJoinRewrite(planTester),
                anyTree(
                        semiJoin("LINE_ORDER_KEY",
                                "ORDERS_ORDER_KEY",
                                "SEMI_JOIN_RESULT",
                                enableDynamicFiltering ? dynamicFilter("DF") : noDynamicFilter(),
                                filter(
                                        comparison(GREATER_THAN, new Reference(BIGINT, "LINE_ORDER_KEY"), new Constant(BIGINT, 2L)),
                                        dynamicFilters -> {
                                            if (enableDynamicFiltering) {
                                                dynamicFilters.addConsumer(consumer -> consumer.alias("DF").expression(BIGINT, "LINE_ORDER_KEY"));
                                            }
                                            else {
                                                dynamicFilters.noConsumers();
                                            }
                                        },
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey",
                                                "LINE_QUANTITY", "quantity"))),
                                anyTree(
                                        filter(
                                                comparison(GREATER_THAN, new Reference(BIGINT, "ORDERS_ORDER_KEY"), new Constant(BIGINT, 2L)),
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEqualPredicateFromSourceSidePropagatesToFilterSideOfSemiJoin(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(planTester,
                "SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders) AND orderkey = 2)",
                noSemiJoinRewrite(planTester),
                anyTree(
                        semiJoin("LINE_ORDER_KEY",
                                "ORDERS_ORDER_KEY",
                                "SEMI_JOIN_RESULT",
                                enableDynamicFiltering ? dynamicFilter("DF") : noDynamicFilter(),
                                filter(
                                        comparison(EQUAL, new Reference(BIGINT, "LINE_ORDER_KEY"), new Constant(BIGINT, 2L)),
                                        dynamicFilters -> {
                                            if (enableDynamicFiltering) {
                                                dynamicFilters.addConsumer(consumer -> consumer.alias("DF").expression(BIGINT, "LINE_ORDER_KEY"));
                                            }
                                            else {
                                                dynamicFilters.noConsumers();
                                            }
                                        },
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey",
                                                "LINE_QUANTITY", "quantity"))),
                                anyTree(
                                        filter(
                                                comparison(EQUAL, new Reference(BIGINT, "ORDERS_ORDER_KEY"), new Constant(BIGINT, 2L)),
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicateFromSourceSideNotPropagatesToFilterSideOfSemiJoinIfNotIn(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(planTester, "SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey NOT IN (SELECT orderkey FROM orders) AND orderkey > 2)",
                anyTree(
                        semiJoin("LINE_ORDER_KEY",
                                "ORDERS_ORDER_KEY",
                                "SEMI_JOIN_RESULT",
                                filter(
                                        comparison(GREATER_THAN, new Reference(BIGINT, "LINE_ORDER_KEY"), new Constant(BIGINT, 2L)),
                                        tableScan("lineitem", ImmutableMap.of(
                                                "LINE_ORDER_KEY", "orderkey",
                                                "LINE_QUANTITY", "quantity"))),
                                node(ExchangeNode.class, // NO filter here
                                        tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey"))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicateFromFilterSideNotPropagatesToSourceSideOfSemiJoinUsedInProjection(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(planTester, "SELECT orderkey IN (SELECT orderkey FROM orders WHERE orderkey > 2) FROM lineitem",
                anyTree(
                        semiJoin("LINE_ORDER_KEY",
                                "ORDERS_ORDER_KEY",
                                "SEMI_JOIN_RESULT",
                                // NO filter here
                                tableScan("lineitem", ImmutableMap.of(
                                        "LINE_ORDER_KEY", "orderkey")),
                                anyTree(
                                        filter(
                                                comparison(GREATER_THAN, new Reference(BIGINT, "ORDERS_ORDER_KEY"), new Constant(BIGINT, 2L)),
                                                tableScan("orders", ImmutableMap.of("ORDERS_ORDER_KEY", "orderkey")))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFilteredSelectFromPartitionedTable(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        // use all optimizers, including AddExchanges
        List<PlanOptimizer> allOptimizers = planTester.getPlanOptimizers(false);

        assertPlan(
                planTester,
                "SELECT DISTINCT orderstatus FROM orders",
                // TODO this could be optimized to VALUES with values from partitions
                anyTree(
                        tableScan("orders")),
                allOptimizers);

        assertPlan(
                planTester,
                "SELECT orderstatus FROM orders WHERE orderstatus = 'O'",
                // predicate matches exactly single partition, no FilterNode needed
                output(
                        tableScan("orders")),
                allOptimizers);

        assertPlan(
                planTester,
                "SELECT orderstatus FROM orders WHERE orderstatus = 'no_such_partition_value'",
                output(
                        values("orderstatus")),
                allOptimizers);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicatePushDownThroughMarkDistinct(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(planTester,
                "SELECT (SELECT a FROM (VALUES 1, 2, 3) t(a) WHERE a = b) FROM (VALUES 0, 1) p(b) WHERE b = 1",
                // TODO this could be optimized to VALUES with values from partitions
                anyTree(
                        join(LEFT, builder -> builder
                                .equiCriteria("A", "B")
                                .left(assignUniqueId("unique", values("A")))
                                .right(values("B")))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicatePushDownOverProjection(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        // Non-singletons should not be pushed down
        assertPlan(
                planTester,
                "WITH t AS (SELECT orderkey * 2 x FROM orders) " +
                        "SELECT * FROM t WHERE x + x > 1",
                anyTree(
                        filter(
                                comparison(GREATER_THAN, new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "expr"), new Reference(BIGINT, "expr"))), new Constant(BIGINT, 1L)),
                                project(ImmutableMap.of("expr", expression(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 2L))))),
                                        tableScan("orders", ImmutableMap.of("orderkey", "orderkey"))))));

        // constant non-singleton should be pushed down
        assertPlan(
                planTester,
                "with t AS (SELECT orderkey * 2 x, 1 y FROM orders) " +
                        "SELECT * FROM t WHERE x + y + y >1",
                anyTree(
                        project(
                                filter(
                                        comparison(GREATER_THAN, new Call(ADD_BIGINT, ImmutableList.of(new Call(ADD_BIGINT, ImmutableList.of(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 2L))), new Constant(BIGINT, 1L))), new Constant(BIGINT, 1L))), new Constant(BIGINT, 1L)),
                                        tableScan("orders", ImmutableMap.of(
                                                "orderkey", "orderkey"))))));

        // singletons should be pushed down
        assertPlan(
                planTester,
                "WITH t AS (SELECT orderkey * 2 x FROM orders) " +
                        "SELECT * FROM t WHERE x > 1",
                anyTree(
                        project(
                                filter(
                                        comparison(GREATER_THAN, new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 2L))), new Constant(BIGINT, 1L)),
                                        tableScan("orders", ImmutableMap.of(
                                                "orderkey", "orderkey"))))));

        // composite singletons should be pushed down
        assertPlan(
                planTester,
                "with t AS (SELECT orderkey * 2 x, orderkey y FROM orders) " +
                        "SELECT * FROM t WHERE x + y > 1",
                anyTree(
                        project(
                                filter(
                                        comparison(GREATER_THAN, new Call(ADD_BIGINT, ImmutableList.of(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 2L))), new Reference(BIGINT, "orderkey"))), new Constant(BIGINT, 1L)),
                                        tableScan("orders", ImmutableMap.of(
                                                "orderkey", "orderkey"))))));

        // Identities should be pushed down
        assertPlan(
                planTester,
                "WITH t AS (SELECT orderkey x FROM orders) " +
                        "SELECT * FROM t WHERE x >1",
                anyTree(
                        filter(
                                comparison(GREATER_THAN, new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 1L)),
                                tableScan("orders", ImmutableMap.of(
                                        "orderkey", "orderkey")))));

        // Non-deterministic predicate should not be pushed down
        assertPlan(
                planTester,
                "WITH t AS (SELECT rand() * orderkey x FROM orders) " +
                        "SELECT * FROM t WHERE x > 5000",
                anyTree(
                        filter(
                                comparison(GREATER_THAN, new Reference(DOUBLE, "expr"), new Constant(DOUBLE, 5000.0)),
                                project(ImmutableMap.of("expr", expression(new Call(MULTIPLY_DOUBLE, ImmutableList.of(new Call(RANDOM, ImmutableList.of()), new Cast(new Reference(BIGINT, "orderkey"), DOUBLE))))),
                                        tableScan("orders", ImmutableMap.of(
                                                "orderkey", "orderkey"))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicatePushDownOverSymbolReferences(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        // Identities should be pushed down
        assertPlan(
                planTester,
                "WITH t AS (SELECT orderkey x, (orderkey + 1) x2 FROM orders) " +
                        "SELECT * FROM t WHERE x > 1 OR x < 0",
                anyTree(
                        filter(
                                new Logical(OR, ImmutableList.of(comparison(LESS_THAN, new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 0L)), comparison(GREATER_THAN, new Reference(BIGINT, "orderkey"), new Constant(BIGINT, 1L)))),
                                tableScan("orders", ImmutableMap.of(
                                        "orderkey", "orderkey")))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testConjunctsOrder(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(
                planTester,
                "select partkey " +
                        "from (" +
                        "  select" +
                        "    partkey," +
                        "    100/(size-1) x" +
                        "  from part" +
                        "  where size <> 1" +
                        ") " +
                        "where x = 2",
                anyTree(
                        // Order matters: size<>1 should be before 100/(size-1)=2.
                        // In this particular example, reversing the order leads to div-by-zero error.
                        filter(
                                new Logical(AND, ImmutableList.of(comparison(NOT_EQUAL, new Reference(INTEGER, "size"), new Constant(INTEGER, 1L)), comparison(EQUAL, new Call(DIVIDE_INTEGER, ImmutableList.of(new Constant(INTEGER, 100L), new Call(SUBTRACT_INTEGER, ImmutableList.of(new Reference(INTEGER, "size"), new Constant(INTEGER, 1L))))), new Constant(INTEGER, 2L)))),
                                tableScan("part", ImmutableMap.of(
                                        "partkey", "partkey",
                                        "size", "size")))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicateOnPartitionSymbolsPushedThroughWindow(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        PlanMatchPattern tableScan = tableScan(
                "orders",
                ImmutableMap.of(
                        "CUST_KEY", "custkey",
                        "ORDER_KEY", "orderkey"));
        assertPlan(
                planTester,
                "SELECT * FROM (" +
                        "SELECT custkey, orderkey, rank() OVER (PARTITION BY custkey  ORDER BY orderdate ASC)" +
                        "FROM orders" +
                        ") WHERE custkey = 0 AND orderkey > 0",
                anyTree(
                        filter(
                                comparison(GREATER_THAN, new Reference(BIGINT, "ORDER_KEY"), new Constant(BIGINT, 0L)),
                                anyTree(
                                        node(WindowNode.class,
                                                anyTree(
                                                        filter(
                                                                comparison(EQUAL, new Reference(BIGINT, "CUST_KEY"), new Constant(BIGINT, 0L)),
                                                                tableScan)))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicateOnPartitionSymbolsPushedThroughTopNRanking(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(
                planTester,
                "SELECT * FROM (" +
                        "SELECT custkey, orderkey, rank() OVER (PARTITION BY custkey  ORDER BY orderdate ASC) rank " +
                        "FROM orders " +
                        ") WHERE rank < 5 AND custkey = 0 AND orderkey > 0 ",
                anyTree(
                        filter(
                                comparison(GREATER_THAN, new Reference(BIGINT, "ORDER_KEY"), new Constant(BIGINT, 0L)),
                                anyTree(
                                        node(TopNRankingNode.class,
                                                anyTree(
                                                        filter(
                                                                comparison(EQUAL, new Reference(BIGINT, "CUST_KEY"), new Constant(BIGINT, 0L)),
                                                                tableScan(
                                                                        "orders",
                                                                        ImmutableMap.of(
                                                                                "CUST_KEY", "custkey",
                                                                                "ORDER_KEY", "orderkey")))))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicateOnNonDeterministicSymbolsPushedDownThroughWindow(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(
                planTester,
                "SELECT * FROM (" +
                        "SELECT random_column, orderkey, rank() OVER (PARTITION BY random_column  ORDER BY orderdate ASC)" +
                        "FROM (select round(custkey*rand()) random_column, * from orders) " +
                        ") WHERE random_column > 100",
                anyTree(
                        node(WindowNode.class,
                                anyTree(
                                        filter(
                                                comparison(GREATER_THAN, new Reference(DOUBLE, "ROUND"), new Constant(DOUBLE, 100.0)),
                                                project(ImmutableMap.of("ROUND", expression(new Call(ROUND, ImmutableList.of(new Call(MULTIPLY_DOUBLE, ImmutableList.of(new Cast(new Reference(BIGINT, "CUST_KEY"), DOUBLE), new Call(RANDOM, ImmutableList.of()))))))),
                                                        tableScan(
                                                                "orders",
                                                                ImmutableMap.of("CUST_KEY", "custkey"))))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicateOnNonDeterministicSymbolsPushedDownThroughTopNRanking(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(
                planTester,
                "SELECT * FROM (" +
                        "SELECT random_column, orderkey, rank() OVER (PARTITION BY random_column  ORDER BY orderdate ASC) rank " +
                        "FROM (select round(custkey*rand()) random_column, * from orders) " +
                        ") WHERE rank < 5 AND random_column > 100",
                anyTree(
                        node(TopNRankingNode.class,
                                anyTree(
                                        filter(
                                                comparison(GREATER_THAN, new Reference(DOUBLE, "ROUND"), new Constant(DOUBLE, 100.0)),
                                                project(ImmutableMap.of("ROUND", expression(new Call(ROUND, ImmutableList.of(new Call(MULTIPLY_DOUBLE, ImmutableList.of(new Cast(new Reference(BIGINT, "CUST_KEY"), DOUBLE), new Call(RANDOM, ImmutableList.of()))))))),
                                                        tableScan(
                                                                "orders",
                                                                ImmutableMap.of("CUST_KEY", "custkey"))))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testNonDeterministicPredicateNotPushedDownThroughWindow(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(
                planTester,
                "SELECT * FROM (" +
                        "SELECT custkey, orderkey, rank() OVER (PARTITION BY custkey  ORDER BY orderdate ASC)" +
                        "FROM orders" +
                        ") WHERE custkey > 100*rand()",
                anyTree(
                        filter(
                                comparison(GREATER_THAN, new Cast(new Reference(BIGINT, "CUST_KEY"), DOUBLE), new Call(MULTIPLY_DOUBLE, ImmutableList.of(new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 100.0)))),
                                anyTree(
                                        node(WindowNode.class,
                                                anyTree(
                                                        tableScan(
                                                                "orders",
                                                                ImmutableMap.of("CUST_KEY", "custkey"))))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testNonDeterministicPredicateNotPushedDownThroughTopNRanking(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(
                planTester,
                "SELECT * FROM (" +
                        "SELECT custkey, orderkey, rank() OVER (PARTITION BY custkey  ORDER BY orderdate ASC) rank " +
                        "FROM orders" +
                        ") WHERE rank < 5 AND custkey > 100*rand()",
                anyTree(
                        filter(
                                comparison(GREATER_THAN, new Cast(new Reference(BIGINT, "CUST_KEY"), DOUBLE), new Call(MULTIPLY_DOUBLE, ImmutableList.of(new Call(RANDOM, ImmutableList.of()), new Constant(DOUBLE, 100.0)))),
                                anyTree(
                                        node(TopNRankingNode.class,
                                                anyTree(
                                                        tableScan(
                                                                "orders",
                                                                ImmutableMap.of("CUST_KEY", "custkey"))))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRemovesRedundantTableScanPredicate(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(
                planTester,
                "SELECT t1.orderstatus " +
                        "FROM (SELECT orderstatus FROM orders WHERE rand() = orderkey AND orderkey = 123) t1, (VALUES 'F', 'K') t2(col) " +
                        "WHERE t1.orderstatus = t2.col AND (t2.col = 'F' OR t2.col = 'K') AND length(t1.orderstatus) < 42",
                anyTree(
                        node(
                                JoinNode.class,
                                node(ProjectNode.class,
                                        filter(
                                                new Logical(AND, ImmutableList.of(comparison(EQUAL, new Reference(BIGINT, "ORDERKEY"), new Constant(BIGINT, 123L)), comparison(EQUAL, new Call(RANDOM, ImmutableList.of()), new Cast(new Reference(BIGINT, "ORDERKEY"), DOUBLE)), comparison(LESS_THAN, new Call(LENGTH, ImmutableList.of(new Reference(createVarcharType(1), "ORDERSTATUS"))), new Constant(BIGINT, 42L)))),
                                                tableScan(
                                                        "orders",
                                                        ImmutableMap.of(
                                                                "ORDERSTATUS", "orderstatus",
                                                                "ORDERKEY", "orderkey")))),
                                values())));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInferredPredicateThroughCaseProjectionSimplifiesToTrue(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(
                planTester,
                """
                SELECT l.a, r.b
                FROM (SELECT totalprice a FROM orders WHERE totalprice > DOUBLE '0') l
                JOIN (
                    SELECT CASE WHEN custkey > 0 THEN DOUBLE '1' ELSE DOUBLE '2' END b
                    FROM orders
                ) r ON l.a = r.b
                """,
                Session.builder(planTester.getDefaultSession())
                        .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                        .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "3s")
                        .build(),
                anyTree(
                        node(JoinNode.class,
                                anyTree(tableScan("orders")),
                                anyTree(node(ProjectNode.class, tableScan("orders"))))));
    }

    @ParameterizedTest
    @MethodSource("joinsWithPreservedInput")
    public void testInferredPredicateThroughNestedJoin(boolean iterativePredicatePushdown, JoinType joinType)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        String projectedInput = joinType == JoinType.RIGHT ? "r2" : "r1";
        assertPlan(
                planTester,
                """
                SELECT l.a, r.b
                FROM (SELECT totalprice a FROM orders WHERE totalprice > DOUBLE '10') l
                JOIN (
                    SELECT CAST(%s.custkey AS DOUBLE) b
                    FROM orders r1 %s JOIN orders r2 ON r1.custkey = r2.custkey
                ) r ON l.a = r.b
                """.formatted(projectedInput, joinType),
                Session.builder(planTester.getDefaultSession())
                        .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                        .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                        .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "3s")
                        .build(),
                anyTree(
                        node(JoinNode.class,
                                anyTree(tableScan("orders")),
                                anyTree(node(JoinNode.class,
                                        anyTree(tableScan("orders")),
                                        anyTree(tableScan("orders")))))));
    }

    @ParameterizedTest
    @MethodSource("joinsWithPreservedInput")
    public void testStrongerPredicateThroughNestedJoin(boolean iterativePredicatePushdown, JoinType joinType)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        String constrainedInput = "(SELECT * FROM orders WHERE custkey > 20)";
        assertNestedJoinConverges(
                planTester,
                """
                SELECT l.a, r.b
                FROM (SELECT totalprice a FROM orders WHERE totalprice > DOUBLE '10') l
                JOIN (
                    SELECT CAST(%s.custkey AS DOUBLE) b
                    FROM %s r1 %s JOIN %s r2 ON r1.custkey = r2.custkey
                ) r ON l.a = r.b
                """.formatted(
                        joinType == JoinType.RIGHT ? "r2" : "r1",
                        joinType == JoinType.RIGHT ? "orders" : constrainedInput,
                        joinType,
                        joinType == JoinType.RIGHT ? constrainedInput : "orders"));
    }

    @ParameterizedTest
    @MethodSource("allJoins")
    public void testCasePredicateThroughNestedJoin(boolean iterativePredicatePushdown, JoinType joinType)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertNestedJoinConverges(
                planTester,
                """
                SELECT l.a, r.b
                FROM (SELECT totalprice a FROM orders WHERE totalprice > DOUBLE '10') l
                JOIN (
                    SELECT CAST(CASE WHEN r1.custkey > 0 THEN r1.custkey ELSE r2.custkey END AS DOUBLE) b
                    FROM orders r1 %s JOIN orders r2 ON r1.totalprice = r2.totalprice
                ) r ON l.a = r.b
                """.formatted(joinType));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicateEnforcedByNestedJoinFilter(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertNestedJoinConverges(
                planTester,
                """
                SELECT l.a, r.b
                FROM (SELECT totalprice a FROM orders WHERE totalprice > DOUBLE '10') l
                JOIN (
                    SELECT CAST(COALESCE(r1.custkey, r2.custkey) AS DOUBLE) b
                    FROM orders r1 JOIN orders r2 ON r1.totalprice = r2.totalprice
                ) r ON l.a = r.b
                """);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicatePushdownAndColumnPruningConverge(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        Session session = Session.builder(planTester.getDefaultSession())
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "3s")
                .build();
        // The filter on the ranking symbol cannot pass through TopNRanking. Column pruning
        // must not keep reintroducing a projection below the filter as pushdown moves it above.
        Plan plan = planTester.inTransaction(session, transactionSession -> planTester.createPlan(transactionSession,
                """
                SELECT orderkey
                FROM (SELECT orderkey, row_number() OVER (PARTITION BY orderkey ORDER BY custkey) n FROM orders)
                WHERE n = 1
                """));
        assertThat(searchFrom(plan.getRoot()).where(node -> node instanceof TopNRankingNode).count()).isEqualTo(1);
    }

    private void assertNestedJoinConverges(PlanTester planTester, String sql)
    {
        Session session = Session.builder(planTester.getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "3s")
                .build();
        Plan plan = planTester.inTransaction(session, transactionSession -> planTester.createPlan(transactionSession, sql));
        assertThat(searchFrom(plan.getRoot()).where(node -> node instanceof JoinNode).count())
                .as("join count for %s", sql)
                .isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTablePredicateIsExtracted(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        PlanMatchPattern constrainedOrders = constrainedTableScan(
                "orders",
                ImmutableMap.of("orderstatus", Domain.create(ValueSet.ofRanges(Range.range(createVarcharType(1), Slices.utf8Slice("A"), true, Slices.utf8Slice("O"), true)), false)));
        // The connector enforces the original range. Its table properties narrow the
        // effective domain to F and O, which is then propagated to the other input.
        Expression name = new Cast(new Reference(VARCHAR, "NAME"), createVarcharType(1));
        Expression effectivePredicate = new In(name, ImmutableList.of(new Constant(createVarcharType(1), Slices.utf8Slice("F")), new Constant(createVarcharType(1), Slices.utf8Slice("O"))));
        PlanMatchPattern filteredOrders = iterativePredicatePushdown
                ? (enableDynamicFiltering ? filter(TRUE, constrainedOrders) : constrainedOrders)
                : filter(
                new In(new Reference(createVarcharType(1), "ORDERSTATUS"), ImmutableList.of(new Constant(createVarcharType(1), Slices.utf8Slice("F")), new Constant(createVarcharType(1), Slices.utf8Slice("O")))),
                tableScan("orders", ImmutableMap.of("ORDERSTATUS", "orderstatus")));
        assertPlan(planTester,
                "SELECT * FROM orders, nation WHERE orderstatus = CAST(nation.name AS varchar(1)) AND orderstatus BETWEEN 'A' AND 'O'",
                anyTree(
                        node(JoinNode.class,
                                filteredOrders,
                                anyTree(
                                        filter(
                                                iterativePredicatePushdown ? new Logical(AND, ImmutableList.of(
                                                        comparison(LESS_THAN_OR_EQUAL, new Constant(createVarcharType(1), Slices.utf8Slice("A")), name),
                                                        comparison(LESS_THAN_OR_EQUAL, name, new Constant(createVarcharType(1), Slices.utf8Slice("O"))),
                                                        effectivePredicate)) : effectivePredicate,
                                                tableScan(
                                                        "nation",
                                                        ImmutableMap.of("NAME", "name")))))));

        PlanMatchPattern ordersTableScan = tableScan("orders", ImmutableMap.of("ORDERSTATUS", "orderstatus"));
        assertPlan(planTester,
                "SELECT * FROM orders JOIN nation ON orderstatus = CAST(nation.name AS varchar(1))",
                anyTree(
                        node(JoinNode.class,
                                enableDynamicFiltering ? filter(TRUE, ordersTableScan) : ordersTableScan,
                                anyTree(
                                        filter(
                                                new In(new Cast(new Reference(VARCHAR, "NAME"), createVarcharType(1)), ImmutableList.of(new Constant(createVarcharType(1), Slices.utf8Slice("F")), new Constant(createVarcharType(1), Slices.utf8Slice("O")), new Constant(createVarcharType(1), Slices.utf8Slice("P")))),
                                                tableScan(
                                                        "nation",
                                                        ImmutableMap.of("NAME", "name")))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testOnlyNullPredicateIsPushDownThroughJoinFilters(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(
                planTester,
                """
                WITH t(a) AS (VALUES 'a', 'b')
                SELECT *
                FROM t t1 JOIN t t2 ON true
                WHERE t1.a = 'aa'
                """,
                output(values("field", "field_0")));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSimplifyNonInferrableInheritedPredicate(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(planTester, "SELECT * FROM (SELECT * FROM nation WHERE nationkey = regionkey AND regionkey = 5) a, nation b WHERE a.nationkey = b.nationkey AND a.nationkey + 11 > 15",
                output(
                        join(INNER, builder -> builder
                                .equiCriteria(ImmutableList.of())
                                .left(
                                        filter(
                                                comparison(EQUAL, new Reference(BIGINT, "R_NATIONKEY"), new Constant(BIGINT, 5L)),
                                                tableScan("nation", ImmutableMap.of("R_NATIONKEY", "nationkey"))))
                                .right(
                                        anyTree(
                                                filter(
                                                        new Logical(AND, ImmutableList.of(comparison(EQUAL, new Reference(BIGINT, "L_NATIONKEY"), new Reference(BIGINT, "L_REGIONKEY")), comparison(EQUAL, new Reference(BIGINT, "L_REGIONKEY"), new Constant(BIGINT, 5L)))),
                                                        tableScan("nation", ImmutableMap.of("L_NATIONKEY", "nationkey", "L_REGIONKEY", "regionkey"))))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDoesNotCreatePredicateFromInferredPredicate(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(planTester, "SELECT * FROM (SELECT *, nationkey + 1 as nationkey2 FROM nation) a JOIN nation b ON a.nationkey2 = b.nationkey",
                output(
                        join(INNER, builder -> builder
                                .equiCriteria("L_NATIONKEY2", "R_NATIONKEY")
                                .left(
                                        project(ImmutableMap.of("L_NATIONKEY2", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "L_NATIONKEY"), new Constant(BIGINT, 1L))))),
                                                tableScan("nation", ImmutableMap.of("L_NATIONKEY", "nationkey"))))
                                .right(
                                        anyTree(
                                                tableScan("nation", ImmutableMap.of("R_NATIONKEY", "nationkey")))))));

        assertPlan(planTester, "SELECT * FROM (SELECT * FROM nation WHERE nationkey = 5) a JOIN (SELECT * FROM nation WHERE nationkey = 5) b ON a.nationkey = b.nationkey",
                output(
                        join(INNER, builder -> builder
                                .equiCriteria(ImmutableList.of())
                                .left(
                                        filter(
                                                comparison(EQUAL, new Reference(BIGINT, "L_NATIONKEY"), new Constant(BIGINT, 5L)),
                                                tableScan("nation", ImmutableMap.of("L_NATIONKEY", "nationkey"))))
                                .right(
                                        anyTree(
                                                filter(
                                                        comparison(EQUAL, new Reference(BIGINT, "R_NATIONKEY"), new Constant(BIGINT, 5L)),
                                                        tableScan("nation", ImmutableMap.of("R_NATIONKEY", "nationkey"))))))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSimplifiesStraddlingPredicate(boolean iterativePredicatePushdown)
    {
        PlanTester planTester = getPlanTester(iterativePredicatePushdown);
        assertPlan(planTester, "SELECT * FROM (SELECT * FROM NATION WHERE nationkey = 5) a JOIN nation b ON a.nationkey = b.nationkey AND a.nationkey = a.regionkey + b.regionkey",
                output(
                        filter(
                                comparison(EQUAL, new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "L_REGIONKEY"), new Reference(BIGINT, "R_REGIONKEY"))), new Constant(BIGINT, 5L)),
                                join(INNER, builder -> builder
                                        .equiCriteria(ImmutableList.of())
                                        .left(
                                                filter(
                                                        comparison(EQUAL, new Reference(BIGINT, "L_NATIONKEY"), new Constant(BIGINT, 5L)),
                                                        tableScan("nation", ImmutableMap.of("L_NATIONKEY", "nationkey", "L_REGIONKEY", "regionkey"))))
                                        .right(
                                                anyTree(
                                                        filter(
                                                                comparison(EQUAL, new Reference(BIGINT, "R_NATIONKEY"), new Constant(BIGINT, 5L)),
                                                                tableScan("nation", ImmutableMap.of("R_NATIONKEY", "nationkey", "R_REGIONKEY", "regionkey")))))))));
    }

    protected Session noSemiJoinRewrite(PlanTester planTester)
    {
        return Session.builder(planTester.getDefaultSession())
                .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "false")
                .build();
    }
}
