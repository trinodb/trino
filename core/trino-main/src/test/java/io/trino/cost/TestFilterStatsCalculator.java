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
package io.trino.cost;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.base.util.JsonTypeUtil;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.transaction.TestingTransactionManager;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.function.Consumer;

import static io.trino.SystemSessionProperties.FILTER_CONJUNCTION_INDEPENDENCE_FACTOR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.type.JsonType.JSON;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;

public class TestFilterStatsCalculator
{
    private static final TestingTransactionManager TRANSACTION_MANAGER = new TestingTransactionManager();
    private static final PlannerContext PLANNER_CONTEXT = plannerContextBuilder()
            .withTransactionManager(TRANSACTION_MANAGER)
            .build();
    private static final VarcharType MEDIUM_VARCHAR_TYPE = createVarcharType(100);

    private final SymbolStatsEstimate xStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(4.0)
            .setDistinctValuesCount(40.0)
            .setLowValue(-10.0)
            .setHighValue(10.0)
            .setNullsFraction(0.25)
            .build();
    private final SymbolStatsEstimate yStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(4.0)
            .setDistinctValuesCount(20.0)
            .setLowValue(0.0)
            .setHighValue(5.0)
            .setNullsFraction(0.5)
            .build();
    private final SymbolStatsEstimate zStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(4.0)
            .setDistinctValuesCount(5.0)
            .setLowValue(-100.0)
            .setHighValue(100.0)
            .setNullsFraction(0.1)
            .build();
    private final SymbolStatsEstimate leftOpenStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(4.0)
            .setDistinctValuesCount(50.0)
            .setLowValue(NEGATIVE_INFINITY)
            .setHighValue(15.0)
            .setNullsFraction(0.1)
            .build();
    private final SymbolStatsEstimate rightOpenStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(4.0)
            .setDistinctValuesCount(50.0)
            .setLowValue(-15.0)
            .setHighValue(POSITIVE_INFINITY)
            .setNullsFraction(0.1)
            .build();
    private final SymbolStatsEstimate unknownRangeStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(4.0)
            .setDistinctValuesCount(50.0)
            .setLowValue(NEGATIVE_INFINITY)
            .setHighValue(POSITIVE_INFINITY)
            .setNullsFraction(0.1)
            .build();
    private final SymbolStatsEstimate emptyRangeStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(0.0)
            .setDistinctValuesCount(0.0)
            .setLowValue(NaN)
            .setHighValue(NaN)
            .setNullsFraction(NaN)
            .build();
    private final SymbolStatsEstimate mediumVarcharStats = SymbolStatsEstimate.builder()
            .setAverageRowSize(85.0)
            .setDistinctValuesCount(165)
            .setLowValue(NEGATIVE_INFINITY)
            .setHighValue(POSITIVE_INFINITY)
            .setNullsFraction(0.34)
            .build();
    private final FilterStatsCalculator statsCalculator = new FilterStatsCalculator(PLANNER_CONTEXT, new ScalarStatsCalculator(PLANNER_CONTEXT), new StatsNormalizer());
    private final PlanNodeStatsEstimate standardInputStatistics = PlanNodeStatsEstimate.builder()
            .addSymbolStatistics(new Symbol(DOUBLE, "x"), xStats)
            .addSymbolStatistics(new Symbol(DOUBLE, "y"), yStats)
            .addSymbolStatistics(new Symbol(DOUBLE, "z"), zStats)
            .addSymbolStatistics(new Symbol(DOUBLE, "leftOpen"), leftOpenStats)
            .addSymbolStatistics(new Symbol(DOUBLE, "rightOpen"), rightOpenStats)
            .addSymbolStatistics(new Symbol(DOUBLE, "unknownRange"), unknownRangeStats)
            .addSymbolStatistics(new Symbol(DOUBLE, "emptyRange"), emptyRangeStats)
            .addSymbolStatistics(new Symbol(MEDIUM_VARCHAR_TYPE, "mediumVarchar"), mediumVarcharStats)
            .setOutputRowCount(1000.0)
            .build();
    private final PlanNodeStatsEstimate zeroStatistics = PlanNodeStatsEstimate.builder()
            .addSymbolStatistics(new Symbol(DOUBLE, "x"), SymbolStatsEstimate.zero())
            .addSymbolStatistics(new Symbol(DOUBLE, "y"), SymbolStatsEstimate.zero())
            .addSymbolStatistics(new Symbol(DOUBLE, "z"), SymbolStatsEstimate.zero())
            .addSymbolStatistics(new Symbol(DOUBLE, "leftOpen"), SymbolStatsEstimate.zero())
            .addSymbolStatistics(new Symbol(DOUBLE, "rightOpen"), SymbolStatsEstimate.zero())
            .addSymbolStatistics(new Symbol(DOUBLE, "unknownRange"), SymbolStatsEstimate.zero())
            .addSymbolStatistics(new Symbol(DOUBLE, "emptyRange"), SymbolStatsEstimate.zero())
            .addSymbolStatistics(new Symbol(MEDIUM_VARCHAR_TYPE, "mediumVarchar"), SymbolStatsEstimate.zero())
            .setOutputRowCount(0)
            .build();
    private final Session session = testSessionBuilder().build();

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction JSON_ARRAY_CONTAINS = FUNCTIONS.resolveFunction("json_array_contains", fromTypes(JSON, DOUBLE));
    private static final ResolvedFunction SIN = FUNCTIONS.resolveFunction("sin", fromTypes(DOUBLE));
    private static final ResolvedFunction ADD_DOUBLE = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(DOUBLE, DOUBLE));
    private static final ResolvedFunction SUBTRACT_DOUBLE = FUNCTIONS.resolveOperator(OperatorType.SUBTRACT, ImmutableList.of(DOUBLE, DOUBLE));
    private static final ResolvedFunction MULTIPLY_DOUBLE = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(DOUBLE, DOUBLE));
    private static final ResolvedFunction NEGATION_DOUBLE = FUNCTIONS.resolveOperator(OperatorType.NEGATION, ImmutableList.of(DOUBLE));

    @Test
    public void testBooleanLiteralStats()
    {
        assertExpression(TRUE).equalTo(standardInputStatistics);
        assertExpression(FALSE).equalTo(zeroStatistics);
        assertExpression(new Constant(BOOLEAN, null)).equalTo(zeroStatistics);
    }

    @Test
    public void testComparison()
    {
        double lessThan3Rows = 487.5;
        assertExpression(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 3.0)))
                .outputRowsCount(lessThan3Rows)
                .symbolStats(new Symbol(DOUBLE, "x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-10)
                                .highValue(3)
                                .distinctValuesCount(26)
                                .nullsFraction(0.0));

        assertExpression(new Comparison(GREATER_THAN, new Call(NEGATION_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "x"))), new Constant(DOUBLE, -3.0)))
                .outputRowsCount(lessThan3Rows);

        for (Expression minusThree : ImmutableList.of(
                new Constant(createDecimalType(3), Decimals.valueOfShort(new BigDecimal("-3"))),
                new Constant(DOUBLE, -3.0),
                new Call(SUBTRACT_DOUBLE, ImmutableList.of(new Constant(DOUBLE, 4.0), new Constant(DOUBLE, 7.0))), new Cast(new Constant(INTEGER, -3L), createDecimalType(7, 3)))) {
            assertExpression(new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Cast(minusThree, DOUBLE)))
                    .outputRowsCount(18.75)
                    .symbolStats(new Symbol(DOUBLE, "x"), symbolAssert ->
                            symbolAssert.averageRowSize(4.0)
                                    .lowValue(-3)
                                    .highValue(-3)
                                    .distinctValuesCount(1)
                                    .nullsFraction(0.0));

            assertExpression(new Comparison(EQUAL, new Cast(minusThree, DOUBLE), new Reference(DOUBLE, "x")))
                    .outputRowsCount(18.75)
                    .symbolStats(new Symbol(DOUBLE, "x"), symbolAssert ->
                            symbolAssert.averageRowSize(4.0)
                                    .lowValue(-3)
                                    .highValue(-3)
                                    .distinctValuesCount(1)
                                    .nullsFraction(0.0));

            assertExpression(new Comparison(
                    EQUAL,
                    new Coalesce(
                            new Call(MULTIPLY_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "x"), new Constant(DOUBLE, null))),
                            new Reference(DOUBLE, "x")),
                    new Cast(minusThree, DOUBLE)))
                    .outputRowsCount(18.75)
                    .symbolStats(new Symbol(DOUBLE, "x"), symbolAssert ->
                            symbolAssert.averageRowSize(4.0)
                                    .lowValue(-3)
                                    .highValue(-3)
                                    .distinctValuesCount(1)
                                    .nullsFraction(0.0));

            assertExpression(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Cast(minusThree, DOUBLE)))
                    .outputRowsCount(262.5)
                    .symbolStats(new Symbol(DOUBLE, "x"), symbolAssert ->
                            symbolAssert.averageRowSize(4.0)
                                    .lowValue(-10)
                                    .highValue(-3)
                                    .distinctValuesCount(14)
                                    .nullsFraction(0.0));

            assertExpression(new Comparison(GREATER_THAN, new Cast(minusThree, DOUBLE), new Reference(DOUBLE, "x")))
                    .outputRowsCount(262.5)
                    .symbolStats(new Symbol(DOUBLE, "x"), symbolAssert ->
                            symbolAssert.averageRowSize(4.0)
                                    .lowValue(-10)
                                    .highValue(-3)
                                    .distinctValuesCount(14)
                                    .nullsFraction(0.0));
        }
    }

    @Test
    public void testInequalityComparisonApproximation()
    {
        assertExpression(new Comparison(GREATER_THAN, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "emptyRange")))
                .outputRowsCount(0);

        assertExpression(new Comparison(GREATER_THAN, new Reference(DOUBLE, "x"), new Call(ADD_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "y"), new Constant(DOUBLE, 20.0)))))
                .outputRowsCount(0);
        assertExpression(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(DOUBLE, "x"), new Call(ADD_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "y"), new Constant(DOUBLE, 20.0)))))
                .outputRowsCount(0);
        assertExpression(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Call(SUBTRACT_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "y"), new Constant(DOUBLE, 25.0)))))
                .outputRowsCount(0);
        assertExpression(new Comparison(LESS_THAN_OR_EQUAL, new Reference(DOUBLE, "x"), new Call(SUBTRACT_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "y"), new Constant(DOUBLE, 25.0)))))
                .outputRowsCount(0);

        double nullsFractionY = 0.5;
        double inputRowCount = standardInputStatistics.getOutputRowCount();
        double nonNullRowCount = inputRowCount * (1 - nullsFractionY);
        SymbolStatsEstimate nonNullStatsX = xStats.mapNullsFraction(nullsFraction -> 0.0);
        assertExpression(new Comparison(GREATER_THAN, new Reference(DOUBLE, "x"), new Call(SUBTRACT_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "y"), new Constant(DOUBLE, 25.0)))))
                .outputRowsCount(nonNullRowCount)
                .symbolStats("x", symbolAssert -> symbolAssert.isEqualTo(nonNullStatsX));
        assertExpression(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(DOUBLE, "x"), new Call(SUBTRACT_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "y"), new Constant(DOUBLE, 25.0)))))
                .outputRowsCount(nonNullRowCount)
                .symbolStats("x", symbolAssert -> symbolAssert.isEqualTo(nonNullStatsX));
        assertExpression(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Call(ADD_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "y"), new Constant(DOUBLE, 20.0)))))
                .outputRowsCount(nonNullRowCount)
                .symbolStats("x", symbolAssert -> symbolAssert.isEqualTo(nonNullStatsX));
        assertExpression(new Comparison(LESS_THAN_OR_EQUAL, new Reference(DOUBLE, "x"), new Call(ADD_DOUBLE, ImmutableList.of(new Reference(DOUBLE, "y"), new Constant(DOUBLE, 20.0)))))
                .outputRowsCount(nonNullRowCount)
                .symbolStats("x", symbolAssert -> symbolAssert.isEqualTo(nonNullStatsX));
    }

    @Test
    public void testOrStats()
    {
        assertExpression(new Logical(OR, ImmutableList.of(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 0.0)), new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, -7.5)))))
                .outputRowsCount(375)
                .symbolStats(new Symbol(DOUBLE, "x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-10.0)
                                .highValue(0.0)
                                .distinctValuesCount(20.0)
                                .nullsFraction(0.0));

        assertExpression(new Logical(OR, ImmutableList.of(new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 0.0)), new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Constant(DOUBLE, -7.5)))))
                .outputRowsCount(37.5)
                .symbolStats(new Symbol(DOUBLE, "x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-7.5)
                                .highValue(0.0)
                                .distinctValuesCount(2.0)
                                .nullsFraction(0.0));

        assertExpression(new Logical(OR, ImmutableList.of(new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 1.0)), new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 3.0)))))
                .outputRowsCount(37.5)
                .symbolStats(new Symbol(DOUBLE, "x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(1)
                                .highValue(3)
                                .distinctValuesCount(2)
                                .nullsFraction(0));

        assertExpression(new Logical(OR, ImmutableList.of(new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 1.0)), new Comparison(EQUAL, new Constant(VarcharType.VARCHAR, Slices.utf8Slice("a")), new Constant(VarcharType.VARCHAR, Slices.utf8Slice("b"))), new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 3.0)))))
                .outputRowsCount(37.5)
                .symbolStats(new Symbol(DOUBLE, "x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(1)
                                .highValue(3)
                                .distinctValuesCount(2)
                                .nullsFraction(0));

        assertExpression(new Logical(OR, ImmutableList.of(new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 1.0)), new In(new Cast(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("b")), createVarcharType(3)), ImmutableList.of(new Cast(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("a")), createVarcharType(3)), new Cast(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("b")), createVarcharType(3)))), new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 3.0)))))
                .equalTo(standardInputStatistics);
    }

    @Test
    public void testUnsupportedExpression()
    {
        assertExpression(new Call(SIN, ImmutableList.of(new Reference(DOUBLE, "x"))))
                .outputRowsCountUnknown();
        assertExpression(new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Call(SIN, ImmutableList.of(new Reference(DOUBLE, "x")))))
                .outputRowsCountUnknown();
    }

    @Test
    public void testAndStats()
    {
        // unknown input
        assertExpression(new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 0.0)), new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 1.0)))), PlanNodeStatsEstimate.unknown()).outputRowsCountUnknown();
        assertExpression(new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 0.0)), new Comparison(LESS_THAN, new Reference(DOUBLE, "y"), new Constant(DOUBLE, 1.0)))), PlanNodeStatsEstimate.unknown()).outputRowsCountUnknown();
        // zeroStatistics input
        assertExpression(new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 0.0)), new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 1.0)))), zeroStatistics).equalTo(zeroStatistics);
        assertExpression(new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 0.0)), new Comparison(LESS_THAN, new Reference(DOUBLE, "y"), new Constant(DOUBLE, 1.0)))), zeroStatistics).equalTo(zeroStatistics);

        assertExpression(new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 0.0)), new Comparison(GREATER_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 1.0))))).equalTo(zeroStatistics);

        assertExpression(new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 0.0)), new Comparison(GREATER_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, -7.5)))))
                .outputRowsCount(281.25)
                .symbolStats(new Symbol(DOUBLE, "x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-7.5)
                                .highValue(0.0)
                                .distinctValuesCount(15.0)
                                .nullsFraction(0.0));

        // Impossible, with symbol-to-expression comparisons
        assertExpression(new Logical(AND, ImmutableList.of(new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Call(ADD_DOUBLE, ImmutableList.of(new Constant(DOUBLE, 0.0), new Constant(DOUBLE, 1.0)))), new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Call(ADD_DOUBLE, ImmutableList.of(new Constant(DOUBLE, 0.0), new Constant(DOUBLE, 3.0)))))))
                .outputRowsCount(0)
                .symbolStats(new Symbol(DOUBLE, "x"), SymbolStatsAssertion::emptyRange)
                .symbolStats(new Symbol(DOUBLE, "y"), SymbolStatsAssertion::emptyRange);

        // first argument unknown
        assertExpression(new Logical(AND, ImmutableList.of(new Call(JSON_ARRAY_CONTAINS, ImmutableList.of(new Constant(JSON, JsonTypeUtil.jsonParse(Slices.utf8Slice("[]"))), new Reference(DOUBLE, "x"))), new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 0.0)))))
                .outputRowsCount(337.5)
                .symbolStats(new Symbol(DOUBLE, "x"), symbolAssert ->
                        symbolAssert.lowValue(-10)
                                .highValue(0)
                                .distinctValuesCount(20)
                                .nullsFraction(0));

        // second argument unknown
        assertExpression(new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 0.0)), new Call(JSON_ARRAY_CONTAINS, ImmutableList.of(new Constant(JSON, JsonTypeUtil.jsonParse(Slices.utf8Slice("[]"))), new Reference(DOUBLE, "x"))))))
                .outputRowsCount(337.5)
                .symbolStats(new Symbol(DOUBLE, "x"), symbolAssert ->
                        symbolAssert.lowValue(-10)
                                .highValue(0)
                                .distinctValuesCount(20)
                                .nullsFraction(0));

        // both arguments unknown
        assertExpression(new Logical(AND, ImmutableList.of(
                new Call(JSON_ARRAY_CONTAINS, ImmutableList.of(new Constant(JSON, JsonTypeUtil.jsonParse(Slices.utf8Slice("[11]"))), new Reference(DOUBLE, "x"))),
                new Call(JSON_ARRAY_CONTAINS, ImmutableList.of(new Constant(JSON, JsonTypeUtil.jsonParse(Slices.utf8Slice("[13]"))), new Reference(DOUBLE, "x"))))))
                .outputRowsCountUnknown();

        assertExpression(new Logical(AND, ImmutableList.of(new In(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("a")), ImmutableList.of(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("b")), new Constant(VarcharType.VARCHAR, Slices.utf8Slice("c")))), new Comparison(EQUAL, new Reference(DOUBLE, "unknownRange"), new Constant(DOUBLE, 3.0)))))
                .outputRowsCount(0);

        assertExpression(new Logical(AND, ImmutableList.of(new Constant(BOOLEAN, null), new Constant(BOOLEAN, null)))).equalTo(zeroStatistics);
        assertExpression(new Logical(AND, ImmutableList.of(new Constant(BOOLEAN, null), new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 0.0)), new Comparison(GREATER_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 1.0))))))).equalTo(zeroStatistics);

        Consumer<SymbolStatsAssertion> symbolAssertX = symbolAssert -> symbolAssert.averageRowSize(4.0)
                .lowValue(-5.0)
                .highValue(5.0)
                .distinctValuesCount(20.0)
                .nullsFraction(0.0);
        Consumer<SymbolStatsAssertion> symbolAssertY = symbolAssert -> symbolAssert.averageRowSize(4.0)
                .lowValue(1.0)
                .highValue(5.0)
                .distinctValuesCount(16.0)
                .nullsFraction(0.0);

        double inputRowCount = standardInputStatistics.getOutputRowCount();
        double filterSelectivityX = 0.375;
        double inequalityFilterSelectivityY = 0.4;
        assertExpression(
                new Logical(AND, ImmutableList.of(
                        new Between(new Reference(DOUBLE, "x"), new Constant(DOUBLE, -5.0), new Constant(DOUBLE, 5.0)),
                        new Comparison(GREATER_THAN, new Reference(DOUBLE, "y"), new Constant(DOUBLE, 1.0)))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "0").build())
                .outputRowsCount(filterSelectivityX * inputRowCount)
                .symbolStats("x", symbolAssertX)
                .symbolStats("y", symbolAssertY);

        assertExpression(
                new Logical(AND, ImmutableList.of(
                        new Between(new Reference(DOUBLE, "x"), new Cast(new Constant(INTEGER, -5L), DOUBLE), new Cast(new Constant(INTEGER, 5L), DOUBLE)),
                        new Comparison(GREATER_THAN, new Reference(DOUBLE, "y"), new Cast(new Constant(INTEGER, 1L), DOUBLE)))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "1").build())
                .outputRowsCount(filterSelectivityX * inequalityFilterSelectivityY * inputRowCount)
                .symbolStats("x", symbolAssertX)
                .symbolStats("y", symbolAssertY);

        assertExpression(
                new Logical(AND, ImmutableList.of(
                        new Between(new Reference(DOUBLE, "x"), new Cast(new Constant(INTEGER, -5L), DOUBLE), new Cast(new Constant(INTEGER, 5L), DOUBLE)),
                        new Comparison(GREATER_THAN, new Reference(DOUBLE, "y"), new Cast(new Constant(INTEGER, 1L), DOUBLE)))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "0.5").build())
                .outputRowsCount(filterSelectivityX * Math.pow(inequalityFilterSelectivityY, 0.5) * inputRowCount)
                .symbolStats("x", symbolAssertX)
                .symbolStats("y", symbolAssertY);

        double nullFilterSelectivityY = 0.5;
        assertExpression(
                new Logical(AND, ImmutableList.of(
                        new Between(new Reference(DOUBLE, "x"), new Cast(new Constant(INTEGER, -5L), DOUBLE), new Cast(new Constant(INTEGER, 5L), DOUBLE)),
                        new IsNull(new Reference(DOUBLE, "y")))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "1").build())
                .outputRowsCount(filterSelectivityX * nullFilterSelectivityY * inputRowCount)
                .symbolStats("x", symbolAssertX)
                .symbolStats("y", symbolAssert -> symbolAssert.isEqualTo(SymbolStatsEstimate.zero()));

        assertExpression(
                new Logical(AND, ImmutableList.of(
                        new Between(new Reference(DOUBLE, "x"), new Cast(new Constant(INTEGER, -5L), DOUBLE), new Cast(new Constant(INTEGER, 5L), DOUBLE)),
                        new IsNull(new Reference(DOUBLE, "y")))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "0.5").build())
                .outputRowsCount(filterSelectivityX * Math.pow(nullFilterSelectivityY, 0.5) * inputRowCount)
                .symbolStats("x", symbolAssertX)
                .symbolStats("y", symbolAssert -> symbolAssert.isEqualTo(SymbolStatsEstimate.zero()));

        assertExpression(
                new Logical(AND, ImmutableList.of(
                        new Between(new Reference(DOUBLE, "x"), new Cast(new Constant(INTEGER, -5L), DOUBLE), new Cast(new Constant(INTEGER, 5L), DOUBLE)),
                        new IsNull(new Reference(DOUBLE, "y")))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "0").build())
                .outputRowsCount(filterSelectivityX * inputRowCount)
                .symbolStats("x", symbolAssertX)
                .symbolStats("y", symbolAssert -> symbolAssert.isEqualTo(SymbolStatsEstimate.zero()));

        assertExpression(
                new Logical(AND, ImmutableList.of(
                        new Comparison(LESS_THAN, new Reference(DOUBLE, "y"), new Cast(new Constant(INTEGER, 1L), DOUBLE)),
                        new Comparison(LESS_THAN, new Cast(new Constant(INTEGER, 0L), DOUBLE), new Reference(DOUBLE, "y")))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "0.5").build())
                .outputRowsCount(100)
                .symbolStats("y", symbolAssert -> symbolAssert.averageRowSize(4.0)
                        .lowValue(0.0)
                        .highValue(1.0)
                        .distinctValuesCount(4.0)
                        .nullsFraction(0.0));

        assertExpression(
                new Logical(AND, ImmutableList.of(
                        new Comparison(GREATER_THAN, new Reference(DOUBLE, "x"), new Cast(new Constant(INTEGER, 0L), DOUBLE)),
                        new Logical(OR, ImmutableList.of(
                                new Comparison(LESS_THAN, new Reference(DOUBLE, "y"), new Cast(new Constant(INTEGER, 1L), DOUBLE)),
                                new Comparison(GREATER_THAN, new Reference(DOUBLE, "y"), new Cast(new Constant(INTEGER, 2L), DOUBLE)))))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "0.5").build())
                .outputRowsCount(filterSelectivityX * Math.pow(inequalityFilterSelectivityY, 0.5) * inputRowCount)
                .symbolStats("x", symbolAssert -> symbolAssert.averageRowSize(4.0)
                        .lowValue(0.0)
                        .highValue(10.0)
                        .distinctValuesCount(20.0)
                        .nullsFraction(0.0))
                .symbolStats("y", symbolAssert -> symbolAssert.averageRowSize(4.0)
                        .lowValue(0.0)
                        .highValue(5.0)
                        .distinctValuesCount(16.0)
                        .nullsFraction(0.0));

        assertExpression(
                new Logical(AND, ImmutableList.of(
                        new Comparison(GREATER_THAN, new Reference(DOUBLE, "x"), new Cast(new Constant(INTEGER, 0L), DOUBLE)),
                        new Logical(OR, ImmutableList.of(
                                new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Cast(new Constant(INTEGER, 1L), DOUBLE)),
                                new Comparison(GREATER_THAN, new Reference(DOUBLE, "y"), new Cast(new Constant(INTEGER, 1L), DOUBLE)))))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "0.5").build())
                .outputRowsCount(172.0)
                .symbolStats("x", symbolAssert -> symbolAssert.averageRowSize(4.0)
                        .lowValue(0.0)
                        .highValue(10.0)
                        .distinctValuesCount(20.0)
                        .nullsFraction(0.0))
                .symbolStats("y", symbolAssert -> symbolAssert.averageRowSize(4.0)
                        .lowValue(0.0)
                        .highValue(5.0)
                        .distinctValuesCount(20.0)
                        .nullsFraction(0.1053779069));

        assertExpression(
                new Logical(AND, ImmutableList.of(
                        new In(new Reference(DOUBLE, "x"), ImmutableList.of(
                                new Cast(new Constant(INTEGER, 0L), DOUBLE),
                                new Cast(new Constant(INTEGER, 1L), DOUBLE),
                                new Cast(new Constant(INTEGER, 2L), DOUBLE))),
                        new Logical(OR, ImmutableList.of(
                                new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Cast(new Constant(INTEGER, 0L), DOUBLE)),
                                new Logical(AND, ImmutableList.of(
                                        new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Cast(new Constant(INTEGER, 1L), DOUBLE)),
                                        new Comparison(EQUAL, new Reference(DOUBLE, "y"), new Cast(new Constant(INTEGER, 1L), DOUBLE)))),
                                new Logical(AND, ImmutableList.of(
                                        new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Cast(new Constant(INTEGER, 2L), DOUBLE)),
                                        new Comparison(EQUAL, new Reference(DOUBLE, "y"), new Cast(new Constant(INTEGER, 1L), DOUBLE)))))))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "0.5").build())
                .outputRowsCount(20.373798)
                .symbolStats("x", symbolAssert -> symbolAssert.averageRowSize(4.0)
                        .lowValue(0.0)
                        .highValue(2.0)
                        .distinctValuesCount(2.623798)
                        .nullsFraction(0.0))
                .symbolStats("y", symbolAssert -> symbolAssert.averageRowSize(4.0)
                        .lowValue(0.0)
                        .highValue(5.0)
                        .distinctValuesCount(15.686298)
                        .nullsFraction(0.2300749269));

        assertExpression(
                new Logical(AND, ImmutableList.of(
                        new Comparison(GREATER_THAN, new Reference(DOUBLE, "x"), new Cast(new Constant(INTEGER, 0L), DOUBLE)),
                        new Constant(BOOLEAN, null))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "0.5").build())
                .outputRowsCount(filterSelectivityX * inputRowCount * 0.9)
                .symbolStats("x", symbolAssert -> symbolAssert.averageRowSize(4.0)
                        .lowValue(0.0)
                        .highValue(10.0)
                        .distinctValuesCount(20.0)
                        .nullsFraction(0.0));
    }

    @Test
    public void testNotStats()
    {
        assertExpression(new Not(new Comparison(LESS_THAN, new Reference(DOUBLE, "x"), new Constant(DOUBLE, 0.0))))
                .outputRowsCount(625) // FIXME - nulls shouldn't be restored
                .symbolStats(new Symbol(DOUBLE, "x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .distinctValuesCount(20.0)
                                .nullsFraction(0.4)) // FIXME - nulls shouldn't be restored
                .symbolStats(new Symbol(DOUBLE, "y"), symbolAssert -> symbolAssert.isEqualTo(yStats));

        assertExpression(new Not(new IsNull(new Reference(DOUBLE, "x"))))
                .outputRowsCount(750)
                .symbolStats(new Symbol(DOUBLE, "x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .distinctValuesCount(40.0)
                                .nullsFraction(0))
                .symbolStats(new Symbol(DOUBLE, "y"), symbolAssert -> symbolAssert.isEqualTo(yStats));

        assertExpression(new Not(new Call(JSON_ARRAY_CONTAINS, ImmutableList.of(new Constant(JSON, JsonTypeUtil.jsonParse(Slices.utf8Slice("[]"))), new Reference(DOUBLE, "x")))))
                .outputRowsCountUnknown();
    }

    @Test
    public void testIsNullFilter()
    {
        assertExpression(new IsNull(new Reference(DOUBLE, "x")))
                .outputRowsCount(250.0)
                .symbolStats("x", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(0)
                                .emptyRange()
                                .nullsFraction(1.0));

        assertExpression(new IsNull(new Reference(DOUBLE, "emptyRange")))
                .outputRowsCount(1000.0)
                .symbolStats(new Symbol(DOUBLE, "emptyRange"), SymbolStatsAssertion::empty);
    }

    @Test
    public void testIsNotNullFilter()
    {
        assertExpression(new Not(new IsNull(new Reference(DOUBLE, "x"))))
                .outputRowsCount(750.0)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(40.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .nullsFraction(0.0));

        assertExpression(new Not(new IsNull(new Reference(DOUBLE, "emptyRange"))))
                .outputRowsCount(0.0)
                .symbolStats("emptyRange", SymbolStatsAssertion::empty);
    }

    @Test
    public void testBetweenOperatorFilter()
    {
        // Only right side cut
        assertExpression(new Between(new Reference(DOUBLE, "x"), new Constant(DOUBLE, 7.5), new Constant(DOUBLE, 12.0)))
                .outputRowsCount(93.75)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(5.0)
                                .lowValue(7.5)
                                .highValue(10.0)
                                .nullsFraction(0.0));

        // Only left side cut
        assertExpression(new Between(new Reference(DOUBLE, "x"), new Constant(DOUBLE, -12.0), new Constant(DOUBLE, -7.5)))
                .outputRowsCount(93.75)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(5.0)
                                .lowValue(-10)
                                .highValue(-7.5)
                                .nullsFraction(0.0));
        assertExpression(new Between(new Reference(DOUBLE, "x"), new Constant(DOUBLE, -12.0), new Constant(DOUBLE, -7.5)))
                .outputRowsCount(93.75)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(5.0)
                                .lowValue(-10)
                                .highValue(-7.5)
                                .nullsFraction(0.0));

        // Both sides cut
        assertExpression(new Between(new Reference(DOUBLE, "x"), new Constant(DOUBLE, -2.5), new Constant(DOUBLE, 2.5)))
                .outputRowsCount(187.5)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(10.0)
                                .lowValue(-2.5)
                                .highValue(2.5)
                                .nullsFraction(0.0));

        // Both sides cut unknownRange
        assertExpression(new Between(new Reference(DOUBLE, "unknownRange"), new Constant(DOUBLE, 2.72), new Constant(DOUBLE, 3.14)))
                .outputRowsCount(112.5)
                .symbolStats("unknownRange", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(6.25)
                                .lowValue(2.72)
                                .highValue(3.14)
                                .nullsFraction(0.0));

        // Left side open, cut on open side
        assertExpression(new Between(new Reference(DOUBLE, "leftOpen"), new Constant(DOUBLE, -10.0), new Constant(DOUBLE, 10.0)))
                .outputRowsCount(180.0)
                .symbolStats("leftOpen", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(10.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .nullsFraction(0.0));

        // Right side open, cut on open side
        assertExpression(new Between(new Reference(DOUBLE, "rightOpen"), new Constant(DOUBLE, -10.0), new Constant(DOUBLE, 10.0)))
                .outputRowsCount(180.0)
                .symbolStats("rightOpen", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(10.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .nullsFraction(0.0));

        // Filter all
        assertExpression(new Between(new Reference(DOUBLE, "y"), new Constant(DOUBLE, 27.5), new Constant(DOUBLE, 107.0)))
                .outputRowsCount(0.0)
                .symbolStats("y", DOUBLE, SymbolStatsAssertion::empty);

        // Filter nothing
        assertExpression(new Between(new Reference(DOUBLE, "y"), new Constant(DOUBLE, -100.0), new Constant(DOUBLE, 100.0)))
                .outputRowsCount(500.0)
                .symbolStats("y", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(20.0)
                                .lowValue(0.0)
                                .highValue(5.0)
                                .nullsFraction(0.0));

        // Filter non exact match
        assertExpression(new Between(new Reference(DOUBLE, "z"), new Constant(DOUBLE, -100.0), new Constant(DOUBLE, 100.0)))
                .outputRowsCount(900.0)
                .symbolStats("z", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(5.0)
                                .lowValue(-100.0)
                                .highValue(100.0)
                                .nullsFraction(0.0));

        // Expression as value. CAST from DOUBLE to DECIMAL(7,2)
        // Produces row count estimate without updating symbol stats
        assertExpression(new Between(new Cast(new Reference(DOUBLE, "x"), createDecimalType(7, 2)), new Constant(createDecimalType(7, 2), Decimals.valueOfShort(new BigDecimal("-2.50"))), new Constant(createDecimalType(7, 2), Decimals.valueOfShort(new BigDecimal("2.50")))))
                .outputRowsCount(219.726563)
                .symbolStats("x", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(xStats.getDistinctValuesCount())
                                .lowValue(xStats.getLowValue())
                                .highValue(xStats.getHighValue())
                                .nullsFraction(xStats.getNullsFraction()));

        assertExpression(new In(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("a")), ImmutableList.of(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("a")), new Constant(VarcharType.VARCHAR, Slices.utf8Slice("b"))))).equalTo(standardInputStatistics);
        assertExpression(new In(new Constant(createVarcharType(1), Slices.utf8Slice("a")), ImmutableList.of(new Constant(createVarcharType(1), Slices.utf8Slice("a")), new Constant(createVarcharType(1), Slices.utf8Slice("b")), new Constant(createVarcharType(1), null)))).equalTo(standardInputStatistics);
        assertExpression(new In(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("a")), ImmutableList.of(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("b")), new Constant(VarcharType.VARCHAR, Slices.utf8Slice("c"))))).outputRowsCount(0);
        assertExpression(new In(new Constant(createVarcharType(1), Slices.utf8Slice("a")), ImmutableList.of(new Constant(createVarcharType(1), Slices.utf8Slice("b")), new Constant(createVarcharType(1), Slices.utf8Slice("c")), new Constant(createVarcharType(1), null)))).outputRowsCount(0);
        assertExpression(new In(new Cast(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("b")), createVarcharType(3)), ImmutableList.of(new Cast(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("a")), createVarcharType(3)), new Cast(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("b")), createVarcharType(3))))).equalTo(standardInputStatistics);
        assertExpression(new In(new Cast(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("c")), createVarcharType(3)), ImmutableList.of(new Cast(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("a")), createVarcharType(3)), new Cast(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("b")), createVarcharType(3))))).outputRowsCount(0);
    }

    @Test
    public void testSymbolEqualsSameSymbolFilter()
    {
        assertExpression(new Comparison(EQUAL, new Reference(DOUBLE, "x"), new Reference(DOUBLE, "x")))
                .outputRowsCount(750)
                .symbolStats("x", DOUBLE, symbolStats ->
                        SymbolStatsEstimate.builder()
                                .setAverageRowSize(4.0)
                                .setDistinctValuesCount(40.0)
                                .setLowValue(-10.0)
                                .setHighValue(10.0)
                                .build());
    }

    @Test
    public void testInPredicateFilter()
    {
        // One value in range
        assertExpression(new In(new Reference(DOUBLE, "x"), ImmutableList.of(new Constant(DOUBLE, 7.5))))
                .outputRowsCount(18.75)
                .symbolStats("x", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(1.0)
                                .lowValue(7.5)
                                .highValue(7.5)
                                .nullsFraction(0.0));
        assertExpression(new In(new Reference(DOUBLE, "x"), ImmutableList.of(new Constant(DOUBLE, -7.5))))
                .outputRowsCount(18.75)
                .symbolStats("x", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(1.0)
                                .lowValue(-7.5)
                                .highValue(-7.5)
                                .nullsFraction(0.0));
        assertExpression(new In(new Reference(DOUBLE, "x"), ImmutableList.of(new Call(ADD_DOUBLE, ImmutableList.of(new Constant(DOUBLE, 2.0), new Constant(DOUBLE, 5.5))))))
                .outputRowsCount(18.75)
                .symbolStats("x", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(1.0)
                                .lowValue(7.5)
                                .highValue(7.5)
                                .nullsFraction(0.0));
        assertExpression(new In(new Reference(DOUBLE, "x"), ImmutableList.of(new Constant(DOUBLE, -7.5))))
                .outputRowsCount(18.75)
                .symbolStats("x", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(1.0)
                                .lowValue(-7.5)
                                .highValue(-7.5)
                                .nullsFraction(0.0));

        // Multiple values in range
        assertExpression(new In(new Reference(DOUBLE, "x"), ImmutableList.of(new Constant(DOUBLE, 1.5), new Constant(DOUBLE, 2.5), new Constant(DOUBLE, 7.5))))
                .outputRowsCount(56.25)
                .symbolStats("x", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(3.0)
                                .lowValue(1.5)
                                .highValue(7.5)
                                .nullsFraction(0.0))
                .symbolStats("y", DOUBLE, symbolStats ->
                        // Symbol not involved in the comparison should have stats basically unchanged
                        symbolStats.distinctValuesCount(20.0)
                                .lowValue(0.0)
                                .highValue(5)
                                .nullsFraction(0.5));

        // Multiple values some in some out of range
        assertExpression(new In(new Reference(DOUBLE, "x"), ImmutableList.of(new Constant(DOUBLE, -42.0), new Constant(DOUBLE, 1.5), new Constant(DOUBLE, 2.5), new Constant(DOUBLE, 7.5), new Constant(DOUBLE, 314.0))))
                .outputRowsCount(56.25)
                .symbolStats("x", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(3.0)
                                .lowValue(1.5)
                                .highValue(7.5)
                                .nullsFraction(0.0));

        // Multiple values some including NULL
        assertExpression(new In(new Reference(DOUBLE, "x"), ImmutableList.of(new Constant(DOUBLE, -42.0), new Constant(DOUBLE, 1.5), new Constant(DOUBLE, 2.5), new Constant(DOUBLE, 7.5), new Constant(DOUBLE, 314.0), new Constant(DOUBLE, null))))
                .outputRowsCount(56.25)
                .symbolStats("x", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(3.0)
                                .lowValue(1.5)
                                .highValue(7.5)
                                .nullsFraction(0.0));

        // Multiple values in unknown range
        assertExpression(new In(new Reference(DOUBLE, "unknownRange"), ImmutableList.of(new Constant(DOUBLE, -42.0), new Constant(DOUBLE, 1.5), new Constant(DOUBLE, 2.5), new Constant(DOUBLE, 7.5), new Constant(DOUBLE, 314.0))))
                .outputRowsCount(90.0)
                .symbolStats("unknownRange", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(5.0)
                                .lowValue(-42.0)
                                .highValue(314.0)
                                .nullsFraction(0.0));

        // Casted literals as value
        assertExpression(new In(new Reference(MEDIUM_VARCHAR_TYPE, "mediumVarchar"), ImmutableList.of(new Cast(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("abc")), MEDIUM_VARCHAR_TYPE))))
                .outputRowsCount(4)
                .symbolStats("mediumVarchar", MEDIUM_VARCHAR_TYPE, symbolStats ->
                        symbolStats.distinctValuesCount(1)
                                .nullsFraction(0.0));

        assertExpression(new In(new Reference(MEDIUM_VARCHAR_TYPE, "mediumVarchar"), ImmutableList.of(new Cast(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("abc")), createVarcharType(100)), new Cast(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("def")), createVarcharType(100)))))
                .outputRowsCount(8)
                .symbolStats("mediumVarchar", MEDIUM_VARCHAR_TYPE, symbolStats ->
                        symbolStats.distinctValuesCount(2)
                                .nullsFraction(0.0));

        // No value in range
        assertExpression(new In(new Reference(DOUBLE, "y"), ImmutableList.of(new Constant(DOUBLE, -42.0), new Constant(DOUBLE, 6.0), new Constant(DOUBLE, 31.1341), new Constant(DOUBLE, -0.000000002), new Constant(DOUBLE, 314.0))))
                .outputRowsCount(0.0)
                .symbolStats("y", DOUBLE, SymbolStatsAssertion::empty);

        // More values in range than distinct values
        assertExpression(new In(new Reference(DOUBLE, "z"), ImmutableList.of(new Constant(DOUBLE, -1.0), new Constant(DOUBLE, 3.14), new Constant(DOUBLE, 0.0), new Constant(DOUBLE, 1.0), new Constant(DOUBLE, 2.0), new Constant(DOUBLE, 3.0), new Constant(DOUBLE, 4.0), new Constant(DOUBLE, 5.0), new Constant(DOUBLE, 6.0), new Constant(DOUBLE, 7.0), new Constant(DOUBLE, 8.0), new Constant(DOUBLE, -2.0))))
                .outputRowsCount(900.0)
                .symbolStats("z", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(5.0)
                                .lowValue(-2.0)
                                .highValue(8.0)
                                .nullsFraction(0.0));

        // Values in weird order
        assertExpression(new In(new Reference(DOUBLE, "z"), ImmutableList.of(new Constant(DOUBLE, -1.0), new Constant(DOUBLE, 1.0), new Constant(DOUBLE, 0.0))))
                .outputRowsCount(540.0)
                .symbolStats("z", DOUBLE, symbolStats ->
                        symbolStats.distinctValuesCount(3.0)
                                .lowValue(-1.0)
                                .highValue(1.0)
                                .nullsFraction(0.0));
    }

    private PlanNodeStatsAssertion assertExpression(Expression expression)
    {
        return assertExpression(expression, session);
    }

    private PlanNodeStatsAssertion assertExpression(Expression expression, PlanNodeStatsEstimate inputStatistics)
    {
        return assertExpression(expression, session, inputStatistics);
    }

    private PlanNodeStatsAssertion assertExpression(Expression expression, Session session)
    {
        return assertExpression(expression, session, standardInputStatistics);
    }

    private PlanNodeStatsAssertion assertExpression(Expression expression, Session session, PlanNodeStatsEstimate inputStatistics)
    {
        TransactionManager transactionManager = new TestingTransactionManager();
        Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
        return transaction(transactionManager, metadata, new AllowAllAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    return PlanNodeStatsAssertion.assertThat(statsCalculator.filterStats(
                            inputStatistics,
                            expression,
                            transactionSession));
                });
    }
}
