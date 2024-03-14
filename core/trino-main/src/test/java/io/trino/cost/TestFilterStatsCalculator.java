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
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.ArithmeticBinaryExpression;
import io.trino.sql.ir.ArithmeticUnaryExpression;
import io.trino.sql.ir.BetweenPredicate;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.CoalesceExpression;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.DecimalLiteral;
import io.trino.sql.ir.DoubleLiteral;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FunctionCall;
import io.trino.sql.ir.GenericLiteral;
import io.trino.sql.ir.InPredicate;
import io.trino.sql.ir.IsNotNullPredicate;
import io.trino.sql.ir.IsNullPredicate;
import io.trino.sql.ir.LogicalExpression;
import io.trino.sql.ir.LongLiteral;
import io.trino.sql.ir.NotExpression;
import io.trino.sql.ir.NullLiteral;
import io.trino.sql.ir.StringLiteral;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.transaction.TestingTransactionManager;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static io.trino.SystemSessionProperties.FILTER_CONJUNCTION_INDEPENDENCE_FACTOR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.SUBTRACT;
import static io.trino.sql.ir.ArithmeticUnaryExpression.Sign.MINUS;
import static io.trino.sql.ir.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.ir.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.LogicalExpression.Operator.AND;
import static io.trino.sql.ir.LogicalExpression.Operator.OR;
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
    private final FilterStatsCalculator statsCalculator = new FilterStatsCalculator(PLANNER_CONTEXT, new ScalarStatsCalculator(PLANNER_CONTEXT, new IrTypeAnalyzer(PLANNER_CONTEXT)), new StatsNormalizer(), new IrTypeAnalyzer(PLANNER_CONTEXT));
    private final PlanNodeStatsEstimate standardInputStatistics = PlanNodeStatsEstimate.builder()
            .addSymbolStatistics(new Symbol("x"), xStats)
            .addSymbolStatistics(new Symbol("y"), yStats)
            .addSymbolStatistics(new Symbol("z"), zStats)
            .addSymbolStatistics(new Symbol("leftOpen"), leftOpenStats)
            .addSymbolStatistics(new Symbol("rightOpen"), rightOpenStats)
            .addSymbolStatistics(new Symbol("unknownRange"), unknownRangeStats)
            .addSymbolStatistics(new Symbol("emptyRange"), emptyRangeStats)
            .addSymbolStatistics(new Symbol("mediumVarchar"), mediumVarcharStats)
            .setOutputRowCount(1000.0)
            .build();
    private final PlanNodeStatsEstimate zeroStatistics = PlanNodeStatsEstimate.builder()
            .addSymbolStatistics(new Symbol("x"), SymbolStatsEstimate.zero())
            .addSymbolStatistics(new Symbol("y"), SymbolStatsEstimate.zero())
            .addSymbolStatistics(new Symbol("z"), SymbolStatsEstimate.zero())
            .addSymbolStatistics(new Symbol("leftOpen"), SymbolStatsEstimate.zero())
            .addSymbolStatistics(new Symbol("rightOpen"), SymbolStatsEstimate.zero())
            .addSymbolStatistics(new Symbol("unknownRange"), SymbolStatsEstimate.zero())
            .addSymbolStatistics(new Symbol("emptyRange"), SymbolStatsEstimate.zero())
            .addSymbolStatistics(new Symbol("mediumVarchar"), SymbolStatsEstimate.zero())
            .setOutputRowCount(0)
            .build();
    private final TypeProvider standardTypes = TypeProvider.copyOf(ImmutableMap.<Symbol, Type>builder()
            .put(new Symbol("x"), DOUBLE)
            .put(new Symbol("y"), DOUBLE)
            .put(new Symbol("z"), DOUBLE)
            .put(new Symbol("leftOpen"), DOUBLE)
            .put(new Symbol("rightOpen"), DOUBLE)
            .put(new Symbol("unknownRange"), DOUBLE)
            .put(new Symbol("emptyRange"), DOUBLE)
            .put(new Symbol("mediumVarchar"), MEDIUM_VARCHAR_TYPE)
            .buildOrThrow());
    private final Session session = testSessionBuilder().build();

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction JSON_ARRAY_CONTAINS = FUNCTIONS.resolveFunction("json_array_contains", fromTypes(JSON, DOUBLE));
    private static final ResolvedFunction SIN = FUNCTIONS.resolveFunction("sin", fromTypes(DOUBLE));

    @Test
    public void testBooleanLiteralStats()
    {
        assertExpression(TRUE_LITERAL).equalTo(standardInputStatistics);
        assertExpression(FALSE_LITERAL).equalTo(zeroStatistics);
        assertExpression(new Cast(new NullLiteral(), BOOLEAN)).equalTo(zeroStatistics);
    }

    @Test
    public void testComparison()
    {
        double lessThan3Rows = 487.5;
        assertExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral(3.0)))
                .outputRowsCount(lessThan3Rows)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-10)
                                .highValue(3)
                                .distinctValuesCount(26)
                                .nullsFraction(0.0));

        assertExpression(new ComparisonExpression(GREATER_THAN, new ArithmeticUnaryExpression(MINUS, new SymbolReference("x")), new DoubleLiteral(-3.0)))
                .outputRowsCount(lessThan3Rows);

        for (Expression minusThree : ImmutableList.of(
                new DecimalLiteral("-3"),
                new DoubleLiteral(-3.0),
                new ArithmeticBinaryExpression(SUBTRACT, new DoubleLiteral(4.0), new DoubleLiteral(7.0)),
                new Cast(new LongLiteral(-3), createDecimalType(7, 3)))) {
            assertExpression(new ComparisonExpression(EQUAL, new SymbolReference("x"), new Cast(minusThree, DOUBLE)))
                    .outputRowsCount(18.75)
                    .symbolStats(new Symbol("x"), symbolAssert ->
                            symbolAssert.averageRowSize(4.0)
                                    .lowValue(-3)
                                    .highValue(-3)
                                    .distinctValuesCount(1)
                                    .nullsFraction(0.0));

            assertExpression(new ComparisonExpression(EQUAL, new Cast(minusThree, DOUBLE), new SymbolReference("x")))
                    .outputRowsCount(18.75)
                    .symbolStats(new Symbol("x"), symbolAssert ->
                            symbolAssert.averageRowSize(4.0)
                                    .lowValue(-3)
                                    .highValue(-3)
                                    .distinctValuesCount(1)
                                    .nullsFraction(0.0));

            assertExpression(new ComparisonExpression(
                    EQUAL,
                    new CoalesceExpression(
                            new ArithmeticBinaryExpression(
                                    MULTIPLY,
                                    new SymbolReference("x"),
                                    new Cast(new NullLiteral(), DOUBLE)),
                            new SymbolReference("x")),
                    new Cast(minusThree, DOUBLE)))
                    .outputRowsCount(18.75)
                    .symbolStats(new Symbol("x"), symbolAssert ->
                            symbolAssert.averageRowSize(4.0)
                                    .lowValue(-3)
                                    .highValue(-3)
                                    .distinctValuesCount(1)
                                    .nullsFraction(0.0));

            assertExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new Cast(minusThree, DOUBLE)))
                    .outputRowsCount(262.5)
                    .symbolStats(new Symbol("x"), symbolAssert ->
                            symbolAssert.averageRowSize(4.0)
                                    .lowValue(-10)
                                    .highValue(-3)
                                    .distinctValuesCount(14)
                                    .nullsFraction(0.0));

            assertExpression(new ComparisonExpression(GREATER_THAN, new Cast(minusThree, DOUBLE), new SymbolReference("x")))
                    .outputRowsCount(262.5)
                    .symbolStats(new Symbol("x"), symbolAssert ->
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
        assertExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("x"), new SymbolReference("emptyRange")))
                .outputRowsCount(0);

        assertExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("x"), new ArithmeticBinaryExpression(ADD, new SymbolReference("y"), new LongLiteral(20))))
                .outputRowsCount(0);
        assertExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("x"), new ArithmeticBinaryExpression(ADD, new SymbolReference("y"), new LongLiteral(20))))
                .outputRowsCount(0);
        assertExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new ArithmeticBinaryExpression(SUBTRACT, new SymbolReference("y"), new LongLiteral(25))))
                .outputRowsCount(0);
        assertExpression(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("x"), new ArithmeticBinaryExpression(SUBTRACT, new SymbolReference("y"), new LongLiteral(25))))
                .outputRowsCount(0);

        double nullsFractionY = 0.5;
        double inputRowCount = standardInputStatistics.getOutputRowCount();
        double nonNullRowCount = inputRowCount * (1 - nullsFractionY);
        SymbolStatsEstimate nonNullStatsX = xStats.mapNullsFraction(nullsFraction -> 0.0);
        assertExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("x"), new ArithmeticBinaryExpression(SUBTRACT, new SymbolReference("y"), new LongLiteral(25))))
                .outputRowsCount(nonNullRowCount)
                .symbolStats("x", symbolAssert -> symbolAssert.isEqualTo(nonNullStatsX));
        assertExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("x"), new ArithmeticBinaryExpression(SUBTRACT, new SymbolReference("y"), new LongLiteral(25))))
                .outputRowsCount(nonNullRowCount)
                .symbolStats("x", symbolAssert -> symbolAssert.isEqualTo(nonNullStatsX));
        assertExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new ArithmeticBinaryExpression(ADD, new SymbolReference("y"), new LongLiteral(20))))
                .outputRowsCount(nonNullRowCount)
                .symbolStats("x", symbolAssert -> symbolAssert.isEqualTo(nonNullStatsX));
        assertExpression(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("x"), new ArithmeticBinaryExpression(ADD, new SymbolReference("y"), new LongLiteral(20))))
                .outputRowsCount(nonNullRowCount)
                .symbolStats("x", symbolAssert -> symbolAssert.isEqualTo(nonNullStatsX));
    }

    @Test
    public void testOrStats()
    {
        assertExpression(new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral(0.0)), new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new GenericLiteral(DOUBLE, "-7.5")))))
                .outputRowsCount(375)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-10.0)
                                .highValue(0.0)
                                .distinctValuesCount(20.0)
                                .nullsFraction(0.0));

        assertExpression(new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference("x"), new DoubleLiteral(0.0)), new ComparisonExpression(EQUAL, new SymbolReference("x"), new GenericLiteral(DOUBLE, "-7.5")))))
                .outputRowsCount(37.5)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-7.5)
                                .highValue(0.0)
                                .distinctValuesCount(2.0)
                                .nullsFraction(0.0));

        assertExpression(new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference("x"), new DoubleLiteral(1.0)), new ComparisonExpression(EQUAL, new SymbolReference("x"), new DoubleLiteral(3.0)))))
                .outputRowsCount(37.5)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(1)
                                .highValue(3)
                                .distinctValuesCount(2)
                                .nullsFraction(0));

        assertExpression(new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference("x"), new DoubleLiteral(1.0)), new ComparisonExpression(EQUAL, new StringLiteral("a"), new StringLiteral("b")), new ComparisonExpression(EQUAL, new SymbolReference("x"), new DoubleLiteral(3.0)))))
                .outputRowsCount(37.5)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(1)
                                .highValue(3)
                                .distinctValuesCount(2)
                                .nullsFraction(0));

        assertExpression(new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference("x"), new DoubleLiteral(1.0)), new InPredicate(new Cast(new StringLiteral("b"), createVarcharType(3)), ImmutableList.of(new Cast(new StringLiteral("a"), createVarcharType(3)), new Cast(new StringLiteral("b"), createVarcharType(3)))), new ComparisonExpression(EQUAL, new SymbolReference("x"), new DoubleLiteral(3.0)))))
                .equalTo(standardInputStatistics);
    }

    @Test
    public void testUnsupportedExpression()
    {
        assertExpression(new FunctionCall(SIN.toQualifiedName(), ImmutableList.of(new SymbolReference("x"))))
                .outputRowsCountUnknown();
        assertExpression(new ComparisonExpression(EQUAL, new SymbolReference("x"), new FunctionCall(SIN.toQualifiedName(), ImmutableList.of(new SymbolReference("x")))))
                .outputRowsCountUnknown();
    }

    @Test
    public void testAndStats()
    {
        // unknown input
        assertExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral(0.0)), new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral(1.0)))), PlanNodeStatsEstimate.unknown()).outputRowsCountUnknown();
        assertExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral(0.0)), new ComparisonExpression(LESS_THAN, new SymbolReference("y"), new DoubleLiteral(1.0)))), PlanNodeStatsEstimate.unknown()).outputRowsCountUnknown();
        // zeroStatistics input
        assertExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral(0.0)), new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral(1.0)))), zeroStatistics).equalTo(zeroStatistics);
        assertExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral(0.0)), new ComparisonExpression(LESS_THAN, new SymbolReference("y"), new DoubleLiteral(1.0)))), zeroStatistics).equalTo(zeroStatistics);

        assertExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral(0.0)), new ComparisonExpression(GREATER_THAN, new SymbolReference("x"), new DoubleLiteral(1.0))))).equalTo(zeroStatistics);

        assertExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral(0.0)), new ComparisonExpression(GREATER_THAN, new SymbolReference("x"), new GenericLiteral(DOUBLE, "-7.5")))))
                .outputRowsCount(281.25)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-7.5)
                                .highValue(0.0)
                                .distinctValuesCount(15.0)
                                .nullsFraction(0.0));

        // Impossible, with symbol-to-expression comparisons
        assertExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference("x"), new ArithmeticBinaryExpression(ADD, new DoubleLiteral(0.0), new DoubleLiteral(1.0))), new ComparisonExpression(EQUAL, new SymbolReference("x"), new ArithmeticBinaryExpression(ADD, new DoubleLiteral(0.0), new DoubleLiteral(3.0))))))
                .outputRowsCount(0)
                .symbolStats(new Symbol("x"), SymbolStatsAssertion::emptyRange)
                .symbolStats(new Symbol("y"), SymbolStatsAssertion::emptyRange);

        // first argument unknown
        assertExpression(new LogicalExpression(AND, ImmutableList.of(new FunctionCall(JSON_ARRAY_CONTAINS.toQualifiedName(), ImmutableList.of(new GenericLiteral(JSON, "[]"), new SymbolReference("x"))), new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral(0.0)))))
                .outputRowsCount(337.5)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.lowValue(-10)
                                .highValue(0)
                                .distinctValuesCount(20)
                                .nullsFraction(0));

        // second argument unknown
        assertExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral(0.0)), new FunctionCall(JSON_ARRAY_CONTAINS.toQualifiedName(), ImmutableList.of(new GenericLiteral(JSON, "[]"), new SymbolReference("x"))))))
                .outputRowsCount(337.5)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.lowValue(-10)
                                .highValue(0)
                                .distinctValuesCount(20)
                                .nullsFraction(0));

        // both arguments unknown
        assertExpression(new LogicalExpression(AND, ImmutableList.of(
                new FunctionCall(JSON_ARRAY_CONTAINS.toQualifiedName(), ImmutableList.of(new GenericLiteral(JSON, "[11]"), new SymbolReference("x"))),
                new FunctionCall(JSON_ARRAY_CONTAINS.toQualifiedName(), ImmutableList.of(new GenericLiteral(JSON, "[13]"), new SymbolReference("x"))))))
                .outputRowsCountUnknown();

        assertExpression(new LogicalExpression(AND, ImmutableList.of(new InPredicate(new StringLiteral("a"), ImmutableList.of(new StringLiteral("b"), new StringLiteral("c"))), new ComparisonExpression(EQUAL, new SymbolReference("unknownRange"), new DoubleLiteral(3.0)))))
                .outputRowsCount(0);

        assertExpression(new LogicalExpression(AND, ImmutableList.of(new Cast(new NullLiteral(), BOOLEAN), new Cast(new NullLiteral(), BOOLEAN)))).equalTo(zeroStatistics);
        assertExpression(new LogicalExpression(AND, ImmutableList.of(new Cast(new NullLiteral(), BOOLEAN), new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral(0.0)), new ComparisonExpression(GREATER_THAN, new SymbolReference("x"), new DoubleLiteral(1.0))))))).equalTo(zeroStatistics);

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
                new LogicalExpression(AND, ImmutableList.of(
                        new BetweenPredicate(new SymbolReference("x"), new Cast(new LongLiteral(-5), DOUBLE), new Cast(new LongLiteral(5), DOUBLE)),
                        new ComparisonExpression(GREATER_THAN, new SymbolReference("y"), new Cast(new LongLiteral(1), DOUBLE)))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "0").build())
                .outputRowsCount(filterSelectivityX * inputRowCount)
                .symbolStats("x", symbolAssertX)
                .symbolStats("y", symbolAssertY);

        assertExpression(
                new LogicalExpression(AND, ImmutableList.of(
                        new BetweenPredicate(new SymbolReference("x"), new Cast(new LongLiteral(-5), DOUBLE), new Cast(new LongLiteral(5), DOUBLE)),
                        new ComparisonExpression(GREATER_THAN, new SymbolReference("y"), new Cast(new LongLiteral(1), DOUBLE)))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "1").build())
                .outputRowsCount(filterSelectivityX * inequalityFilterSelectivityY * inputRowCount)
                .symbolStats("x", symbolAssertX)
                .symbolStats("y", symbolAssertY);

        assertExpression(
                new LogicalExpression(AND, ImmutableList.of(
                        new BetweenPredicate(new SymbolReference("x"), new Cast(new LongLiteral(-5), DOUBLE), new Cast(new LongLiteral(5), DOUBLE)),
                        new ComparisonExpression(GREATER_THAN, new SymbolReference("y"), new Cast(new LongLiteral(1), DOUBLE)))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "0.5").build())
                .outputRowsCount(filterSelectivityX * Math.pow(inequalityFilterSelectivityY, 0.5) * inputRowCount)
                .symbolStats("x", symbolAssertX)
                .symbolStats("y", symbolAssertY);

        double nullFilterSelectivityY = 0.5;
        assertExpression(
                new LogicalExpression(AND, ImmutableList.of(
                        new BetweenPredicate(new SymbolReference("x"), new Cast(new LongLiteral(-5), DOUBLE), new Cast(new LongLiteral(5), DOUBLE)),
                        new IsNullPredicate(new SymbolReference("y")))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "1").build())
                .outputRowsCount(filterSelectivityX * nullFilterSelectivityY * inputRowCount)
                .symbolStats("x", symbolAssertX)
                .symbolStats("y", symbolAssert -> symbolAssert.isEqualTo(SymbolStatsEstimate.zero()));

        assertExpression(
                new LogicalExpression(AND, ImmutableList.of(
                        new BetweenPredicate(new SymbolReference("x"), new Cast(new LongLiteral(-5), DOUBLE), new Cast(new LongLiteral(5), DOUBLE)),
                        new IsNullPredicate(new SymbolReference("y")))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "0.5").build())
                .outputRowsCount(filterSelectivityX * Math.pow(nullFilterSelectivityY, 0.5) * inputRowCount)
                .symbolStats("x", symbolAssertX)
                .symbolStats("y", symbolAssert -> symbolAssert.isEqualTo(SymbolStatsEstimate.zero()));

        assertExpression(
                new LogicalExpression(AND, ImmutableList.of(
                        new BetweenPredicate(new SymbolReference("x"), new Cast(new LongLiteral(-5), DOUBLE), new Cast(new LongLiteral(5), DOUBLE)),
                        new IsNullPredicate(new SymbolReference("y")))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "0").build())
                .outputRowsCount(filterSelectivityX * inputRowCount)
                .symbolStats("x", symbolAssertX)
                .symbolStats("y", symbolAssert -> symbolAssert.isEqualTo(SymbolStatsEstimate.zero()));

        assertExpression(
                new LogicalExpression(AND, ImmutableList.of(
                        new ComparisonExpression(LESS_THAN, new SymbolReference("y"), new Cast(new LongLiteral(1), DOUBLE)),
                        new ComparisonExpression(LESS_THAN, new Cast(new LongLiteral(0), DOUBLE), new SymbolReference("y")))),
                Session.builder(session).setSystemProperty(FILTER_CONJUNCTION_INDEPENDENCE_FACTOR, "0.5").build())
                .outputRowsCount(100)
                .symbolStats("y", symbolAssert -> symbolAssert.averageRowSize(4.0)
                        .lowValue(0.0)
                        .highValue(1.0)
                        .distinctValuesCount(4.0)
                        .nullsFraction(0.0));

        assertExpression(
                new LogicalExpression(AND, ImmutableList.of(
                        new ComparisonExpression(GREATER_THAN, new SymbolReference("x"), new Cast(new LongLiteral(0), DOUBLE)),
                        new LogicalExpression(OR, ImmutableList.of(
                                new ComparisonExpression(LESS_THAN, new SymbolReference("y"), new Cast(new LongLiteral(1), DOUBLE)),
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("y"), new Cast(new LongLiteral(2), DOUBLE)))))),
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
                new LogicalExpression(AND, ImmutableList.of(
                        new ComparisonExpression(GREATER_THAN, new SymbolReference("x"), new Cast(new LongLiteral(0), DOUBLE)),
                        new LogicalExpression(OR, ImmutableList.of(
                                new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new Cast(new LongLiteral(1), DOUBLE)),
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("y"), new Cast(new LongLiteral(1), DOUBLE)))))),
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
                new LogicalExpression(AND, ImmutableList.of(
                        new InPredicate(new SymbolReference("x"), ImmutableList.of(
                                new Cast(new LongLiteral(0), DOUBLE),
                                new Cast(new LongLiteral(1), DOUBLE),
                                new Cast(new LongLiteral(2), DOUBLE))),
                        new LogicalExpression(OR, ImmutableList.of(
                                new ComparisonExpression(EQUAL, new SymbolReference("x"), new Cast(new LongLiteral(0), DOUBLE)),
                                new LogicalExpression(AND, ImmutableList.of(
                                        new ComparisonExpression(EQUAL, new SymbolReference("x"), new Cast(new LongLiteral(1), DOUBLE)),
                                        new ComparisonExpression(EQUAL, new SymbolReference("y"), new Cast(new LongLiteral(1), DOUBLE)))),
                                new LogicalExpression(AND, ImmutableList.of(
                                        new ComparisonExpression(EQUAL, new SymbolReference("x"), new Cast(new LongLiteral(2), DOUBLE)),
                                        new ComparisonExpression(EQUAL, new SymbolReference("y"), new Cast(new LongLiteral(1), DOUBLE)))))))),
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
                new LogicalExpression(AND, ImmutableList.of(
                        new ComparisonExpression(GREATER_THAN, new SymbolReference("x"), new Cast(new LongLiteral(0), DOUBLE)),
                        new Cast(new NullLiteral(), BOOLEAN))),
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
        assertExpression(new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("x"), new DoubleLiteral(0.0))))
                .outputRowsCount(625) // FIXME - nulls shouldn't be restored
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .distinctValuesCount(20.0)
                                .nullsFraction(0.4)) // FIXME - nulls shouldn't be restored
                .symbolStats(new Symbol("y"), symbolAssert -> symbolAssert.isEqualTo(yStats));

        assertExpression(new NotExpression(new IsNullPredicate(new SymbolReference("x"))))
                .outputRowsCount(750)
                .symbolStats(new Symbol("x"), symbolAssert ->
                        symbolAssert.averageRowSize(4.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .distinctValuesCount(40.0)
                                .nullsFraction(0))
                .symbolStats(new Symbol("y"), symbolAssert -> symbolAssert.isEqualTo(yStats));

        assertExpression(new NotExpression(new FunctionCall(JSON_ARRAY_CONTAINS.toQualifiedName(), ImmutableList.of(new GenericLiteral(JSON, "[]"), new SymbolReference("x")))))
                .outputRowsCountUnknown();
    }

    @Test
    public void testIsNullFilter()
    {
        assertExpression(new IsNullPredicate(new SymbolReference("x")))
                .outputRowsCount(250.0)
                .symbolStats(new Symbol("x"), symbolStats ->
                        symbolStats.distinctValuesCount(0)
                                .emptyRange()
                                .nullsFraction(1.0));

        assertExpression(new IsNullPredicate(new SymbolReference("emptyRange")))
                .outputRowsCount(1000.0)
                .symbolStats(new Symbol("emptyRange"), SymbolStatsAssertion::empty);
    }

    @Test
    public void testIsNotNullFilter()
    {
        assertExpression(new IsNotNullPredicate(new SymbolReference("x")))
                .outputRowsCount(750.0)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(40.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .nullsFraction(0.0));

        assertExpression(new IsNotNullPredicate(new SymbolReference("emptyRange")))
                .outputRowsCount(0.0)
                .symbolStats("emptyRange", SymbolStatsAssertion::empty);
    }

    @Test
    public void testBetweenOperatorFilter()
    {
        // Only right side cut
        assertExpression(new BetweenPredicate(new SymbolReference("x"), new DoubleLiteral(7.5), new DoubleLiteral(12.0)))
                .outputRowsCount(93.75)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(5.0)
                                .lowValue(7.5)
                                .highValue(10.0)
                                .nullsFraction(0.0));

        // Only left side cut
        assertExpression(new BetweenPredicate(new SymbolReference("x"), new GenericLiteral(DOUBLE, "-12"), new GenericLiteral(DOUBLE, "-7.5")))
                .outputRowsCount(93.75)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(5.0)
                                .lowValue(-10)
                                .highValue(-7.5)
                                .nullsFraction(0.0));
        assertExpression(new BetweenPredicate(new SymbolReference("x"), new DoubleLiteral(-12.0), new DoubleLiteral(-7.5)))
                .outputRowsCount(93.75)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(5.0)
                                .lowValue(-10)
                                .highValue(-7.5)
                                .nullsFraction(0.0));

        // Both sides cut
        assertExpression(new BetweenPredicate(new SymbolReference("x"), new GenericLiteral(DOUBLE, "-2.5"), new DoubleLiteral(2.5)))
                .outputRowsCount(187.5)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(10.0)
                                .lowValue(-2.5)
                                .highValue(2.5)
                                .nullsFraction(0.0));

        // Both sides cut unknownRange
        assertExpression(new BetweenPredicate(new SymbolReference("unknownRange"), new DoubleLiteral(2.72), new DoubleLiteral(3.14)))
                .outputRowsCount(112.5)
                .symbolStats("unknownRange", symbolStats ->
                        symbolStats.distinctValuesCount(6.25)
                                .lowValue(2.72)
                                .highValue(3.14)
                                .nullsFraction(0.0));

        // Left side open, cut on open side
        assertExpression(new BetweenPredicate(new SymbolReference("leftOpen"), new GenericLiteral(DOUBLE, "-10"), new DoubleLiteral(10.0)))
                .outputRowsCount(180.0)
                .symbolStats("leftOpen", symbolStats ->
                        symbolStats.distinctValuesCount(10.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .nullsFraction(0.0));

        // Right side open, cut on open side
        assertExpression(new BetweenPredicate(new SymbolReference("rightOpen"), new GenericLiteral(DOUBLE, "-10"), new DoubleLiteral(10.0)))
                .outputRowsCount(180.0)
                .symbolStats("rightOpen", symbolStats ->
                        symbolStats.distinctValuesCount(10.0)
                                .lowValue(-10.0)
                                .highValue(10.0)
                                .nullsFraction(0.0));

        // Filter all
        assertExpression(new BetweenPredicate(new SymbolReference("y"), new DoubleLiteral(27.5), new DoubleLiteral(107.0)))
                .outputRowsCount(0.0)
                .symbolStats("y", SymbolStatsAssertion::empty);

        // Filter nothing
        assertExpression(new BetweenPredicate(new SymbolReference("y"), new GenericLiteral(DOUBLE, "-100"), new DoubleLiteral(100.0)))
                .outputRowsCount(500.0)
                .symbolStats("y", symbolStats ->
                        symbolStats.distinctValuesCount(20.0)
                                .lowValue(0.0)
                                .highValue(5.0)
                                .nullsFraction(0.0));

        // Filter non exact match
        assertExpression(new BetweenPredicate(new SymbolReference("z"), new GenericLiteral(DOUBLE, "-100"), new DoubleLiteral(100.0)))
                .outputRowsCount(900.0)
                .symbolStats("z", symbolStats ->
                        symbolStats.distinctValuesCount(5.0)
                                .lowValue(-100.0)
                                .highValue(100.0)
                                .nullsFraction(0.0));

        // Expression as value. CAST from DOUBLE to DECIMAL(7,2)
        // Produces row count estimate without updating symbol stats
        assertExpression(new BetweenPredicate(new Cast(new SymbolReference("x"), createDecimalType(7, 2)), new Cast(new DecimalLiteral("-2.50"), createDecimalType(7, 2)), new Cast(new DecimalLiteral("2.50"), createDecimalType(7, 2))))
                .outputRowsCount(219.726563)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(xStats.getDistinctValuesCount())
                                .lowValue(xStats.getLowValue())
                                .highValue(xStats.getHighValue())
                                .nullsFraction(xStats.getNullsFraction()));

        assertExpression(new InPredicate(new StringLiteral("a"), ImmutableList.of(new StringLiteral("a"), new StringLiteral("b")))).equalTo(standardInputStatistics);
        assertExpression(new InPredicate(new StringLiteral("a"), ImmutableList.of(new StringLiteral("a"), new StringLiteral("b"), new Cast(new NullLiteral(), createVarcharType(1))))).equalTo(standardInputStatistics);
        assertExpression(new InPredicate(new StringLiteral("a"), ImmutableList.of(new StringLiteral("b"), new StringLiteral("c")))).outputRowsCount(0);
        assertExpression(new InPredicate(new StringLiteral("a"), ImmutableList.of(new StringLiteral("b"), new StringLiteral("c"), new Cast(new NullLiteral(), createVarcharType(1))))).outputRowsCount(0);
        assertExpression(new InPredicate(new Cast(new StringLiteral("b"), createVarcharType(3)), ImmutableList.of(new Cast(new StringLiteral("a"), createVarcharType(3)), new Cast(new StringLiteral("b"), createVarcharType(3))))).equalTo(standardInputStatistics);
        assertExpression(new InPredicate(new Cast(new StringLiteral("c"), createVarcharType(3)), ImmutableList.of(new Cast(new StringLiteral("a"), createVarcharType(3)), new Cast(new StringLiteral("b"), createVarcharType(3))))).outputRowsCount(0);
    }

    @Test
    public void testSymbolEqualsSameSymbolFilter()
    {
        assertExpression(new ComparisonExpression(EQUAL, new SymbolReference("x"), new SymbolReference("x")))
                .outputRowsCount(750)
                .symbolStats("x", symbolStats ->
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
        assertExpression(new InPredicate(new SymbolReference("x"), ImmutableList.of(new DoubleLiteral(7.5))))
                .outputRowsCount(18.75)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(1.0)
                                .lowValue(7.5)
                                .highValue(7.5)
                                .nullsFraction(0.0));
        assertExpression(new InPredicate(new SymbolReference("x"), ImmutableList.of(new GenericLiteral(DOUBLE, "-7.5"))))
                .outputRowsCount(18.75)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(1.0)
                                .lowValue(-7.5)
                                .highValue(-7.5)
                                .nullsFraction(0.0));
        assertExpression(new InPredicate(new SymbolReference("x"), ImmutableList.of(new ArithmeticBinaryExpression(ADD, new GenericLiteral(BIGINT, "2"), new DoubleLiteral(5.5)))))
                .outputRowsCount(18.75)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(1.0)
                                .lowValue(7.5)
                                .highValue(7.5)
                                .nullsFraction(0.0));
        assertExpression(new InPredicate(new SymbolReference("x"), ImmutableList.of(new DoubleLiteral(-7.5))))
                .outputRowsCount(18.75)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(1.0)
                                .lowValue(-7.5)
                                .highValue(-7.5)
                                .nullsFraction(0.0));

        // Multiple values in range
        assertExpression(new InPredicate(new SymbolReference("x"), ImmutableList.of(new DoubleLiteral(1.5), new DoubleLiteral(2.5), new DoubleLiteral(7.5))))
                .outputRowsCount(56.25)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(3.0)
                                .lowValue(1.5)
                                .highValue(7.5)
                                .nullsFraction(0.0))
                .symbolStats("y", symbolStats ->
                        // Symbol not involved in the comparison should have stats basically unchanged
                        symbolStats.distinctValuesCount(20.0)
                                .lowValue(0.0)
                                .highValue(5)
                                .nullsFraction(0.5));

        // Multiple values some in some out of range
        assertExpression(new InPredicate(new SymbolReference("x"), ImmutableList.of(new GenericLiteral(DOUBLE, "-42"), new DoubleLiteral(1.5), new DoubleLiteral(2.5), new DoubleLiteral(7.5), new DoubleLiteral(314.0))))
                .outputRowsCount(56.25)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(3.0)
                                .lowValue(1.5)
                                .highValue(7.5)
                                .nullsFraction(0.0));

        // Multiple values some including NULL
        assertExpression(new InPredicate(new SymbolReference("x"), ImmutableList.of(new GenericLiteral(DOUBLE, "-42"), new DoubleLiteral(1.5), new DoubleLiteral(2.5), new DoubleLiteral(7.5), new DoubleLiteral(314.0), new Cast(new NullLiteral(), DOUBLE))))
                .outputRowsCount(56.25)
                .symbolStats("x", symbolStats ->
                        symbolStats.distinctValuesCount(3.0)
                                .lowValue(1.5)
                                .highValue(7.5)
                                .nullsFraction(0.0));

        // Multiple values in unknown range
        assertExpression(new InPredicate(new SymbolReference("unknownRange"), ImmutableList.of(new GenericLiteral(DOUBLE, "-42"), new DoubleLiteral(1.5), new DoubleLiteral(2.5), new DoubleLiteral(7.5), new DoubleLiteral(314.0))))
                .outputRowsCount(90.0)
                .symbolStats("unknownRange", symbolStats ->
                        symbolStats.distinctValuesCount(5.0)
                                .lowValue(-42.0)
                                .highValue(314.0)
                                .nullsFraction(0.0));

        // Casted literals as value
        assertExpression(new InPredicate(new SymbolReference("mediumVarchar"), ImmutableList.of(new Cast(new StringLiteral("abc"), MEDIUM_VARCHAR_TYPE))))
                .outputRowsCount(4)
                .symbolStats("mediumVarchar", symbolStats ->
                        symbolStats.distinctValuesCount(1)
                                .nullsFraction(0.0));

        assertExpression(new InPredicate(new SymbolReference("mediumVarchar"), ImmutableList.of(new Cast(new StringLiteral("abc"), createVarcharType(100)), new Cast(new StringLiteral("def"), createVarcharType(100)))))
                .outputRowsCount(8)
                .symbolStats("mediumVarchar", symbolStats ->
                        symbolStats.distinctValuesCount(2)
                                .nullsFraction(0.0));

        // No value in range
        assertExpression(new InPredicate(new SymbolReference("y"), ImmutableList.of(new GenericLiteral(DOUBLE, "-42"), new DoubleLiteral(6.0), new DoubleLiteral(31.1341), new GenericLiteral(DOUBLE, "-0.000000002"), new DoubleLiteral(314.0))))
                .outputRowsCount(0.0)
                .symbolStats("y", SymbolStatsAssertion::empty);

        // More values in range than distinct values
        assertExpression(new InPredicate(new SymbolReference("z"), ImmutableList.of(new GenericLiteral(DOUBLE, "-1"), new DoubleLiteral(3.14), new DoubleLiteral(0.0), new DoubleLiteral(1.0), new DoubleLiteral(2.0), new DoubleLiteral(3.0), new DoubleLiteral(4.0), new DoubleLiteral(5.0), new DoubleLiteral(6.0), new DoubleLiteral(7.0), new DoubleLiteral(8.0), new GenericLiteral(DOUBLE, "-2"))))
                .outputRowsCount(900.0)
                .symbolStats("z", symbolStats ->
                        symbolStats.distinctValuesCount(5.0)
                                .lowValue(-2.0)
                                .highValue(8.0)
                                .nullsFraction(0.0));

        // Values in weird order
        assertExpression(new InPredicate(new SymbolReference("z"), ImmutableList.of(new GenericLiteral(DOUBLE, "-1"), new DoubleLiteral(1.0), new DoubleLiteral(0.0))))
                .outputRowsCount(540.0)
                .symbolStats("z", symbolStats ->
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
                            transactionSession,
                            standardTypes));
                });
    }
}
