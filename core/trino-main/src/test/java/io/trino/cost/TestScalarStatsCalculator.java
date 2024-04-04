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
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.VarcharType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.transaction.TestingTransactionManager;
import io.trino.transaction.TransactionManager;
import io.trino.type.UnknownType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Long.MAX_VALUE;

public class TestScalarStatsCalculator
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction SUBTRACT_BIGINT = FUNCTIONS.resolveOperator(OperatorType.SUBTRACT, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction MULTIPLY_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction DIVIDE_BIGINT = FUNCTIONS.resolveOperator(OperatorType.DIVIDE, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction MODULUS_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MODULUS, ImmutableList.of(BIGINT, BIGINT));

    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();
    private final ScalarStatsCalculator calculator = new ScalarStatsCalculator(functionResolution.getPlannerContext());
    private final Session session = testSessionBuilder().build();

    @Test
    public void testLiteral()
    {
        assertCalculate(new Constant(TINYINT, 7L))
                .distinctValuesCount(1.0)
                .lowValue(7)
                .highValue(7)
                .nullsFraction(0.0);

        assertCalculate(new Constant(SMALLINT, 8L))
                .distinctValuesCount(1.0)
                .lowValue(8)
                .highValue(8)
                .nullsFraction(0.0);

        assertCalculate(new Constant(INTEGER, 9L))
                .distinctValuesCount(1.0)
                .lowValue(9)
                .highValue(9)
                .nullsFraction(0.0);

        assertCalculate(new Constant(BIGINT, MAX_VALUE))
                .distinctValuesCount(1.0)
                .lowValue(Long.MAX_VALUE)
                .highValue(Long.MAX_VALUE)
                .nullsFraction(0.0);

        assertCalculate(new Constant(DOUBLE, 7.5))
                .distinctValuesCount(1.0)
                .lowValue(7.5)
                .highValue(7.5)
                .nullsFraction(0.0);

        assertCalculate(new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("75.5"))))
                .distinctValuesCount(1.0)
                .lowValue(75.5)
                .highValue(75.5)
                .nullsFraction(0.0);

        assertCalculate(new Constant(VarcharType.VARCHAR, Slices.utf8Slice("blah")))
                .distinctValuesCount(1.0)
                .lowValueUnknown()
                .highValueUnknown()
                .nullsFraction(0.0);

        assertCalculate(new Constant(UnknownType.UNKNOWN, null))
                .distinctValuesCount(0.0)
                .lowValueUnknown()
                .highValueUnknown()
                .nullsFraction(1.0);
    }

    @Test
    public void testFunctionCall()
    {
        assertCalculate(
                functionResolution
                        .functionCallBuilder("length")
                        .addArgument(createVarcharType(10), new Constant(createVarcharType(10), null))
                        .build())
                .distinctValuesCount(0.0)
                .lowValueUnknown()
                .highValueUnknown()
                .nullsFraction(1.0);

        assertCalculate(
                functionResolution
                        .functionCallBuilder("length")
                        .addArgument(createVarcharType(2), new Reference(createVarcharType(2), "x"))
                        .build(),
                PlanNodeStatsEstimate.unknown())
                .distinctValuesCountUnknown()
                .lowValueUnknown()
                .highValueUnknown()
                .nullsFractionUnknown();
    }

    @Test
    public void testVarbinaryConstant()
    {
        Expression expression = new Constant(VARBINARY, Slices.utf8Slice("ala ma kota"));

        assertCalculate(expression)
                .distinctValuesCount(1.0)
                .lowValueUnknown()
                .highValueUnknown()
                .nullsFraction(0.0);
    }

    @Test
    public void testSymbolReference()
    {
        SymbolStatsEstimate xStats = SymbolStatsEstimate.builder()
                .setLowValue(-1)
                .setHighValue(10)
                .setDistinctValuesCount(4)
                .setNullsFraction(0.1)
                .setAverageRowSize(2.0)
                .build();
        PlanNodeStatsEstimate inputStatistics = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(new Symbol(INTEGER, "x"), xStats)
                .build();

        assertCalculate(new Reference(INTEGER, "x"), inputStatistics).isEqualTo(xStats);
        assertCalculate(new Reference(INTEGER, "y"), inputStatistics).isEqualTo(SymbolStatsEstimate.unknown());
    }

    @Test
    public void testCastDoubleToBigint()
    {
        PlanNodeStatsEstimate inputStatistics = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(new Symbol(BIGINT, "a"), SymbolStatsEstimate.builder()
                        .setNullsFraction(0.3)
                        .setLowValue(1.6)
                        .setHighValue(17.3)
                        .setDistinctValuesCount(10)
                        .setAverageRowSize(2.0)
                        .build())
                .build();

        assertCalculate(
                new Cast(new Reference(BIGINT, "a"), BIGINT),
                inputStatistics)
                .lowValue(2.0)
                .highValue(17.0)
                .distinctValuesCount(10)
                .nullsFraction(0.3)
                .dataSizeUnknown();
    }

    @Test
    public void testCastDoubleToShortRange()
    {
        PlanNodeStatsEstimate inputStatistics = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(new Symbol(BIGINT, "a"), SymbolStatsEstimate.builder()
                        .setNullsFraction(0.3)
                        .setLowValue(1.6)
                        .setHighValue(3.3)
                        .setDistinctValuesCount(10)
                        .setAverageRowSize(2.0)
                        .build())
                .build();

        assertCalculate(
                new Cast(new Reference(BIGINT, "a"), BIGINT),
                inputStatistics)
                .lowValue(2.0)
                .highValue(3.0)
                .distinctValuesCount(2)
                .nullsFraction(0.3)
                .dataSizeUnknown();
    }

    @Test
    public void testCastDoubleToShortRangeUnknownDistinctValuesCount()
    {
        PlanNodeStatsEstimate inputStatistics = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(new Symbol(BIGINT, "a"), SymbolStatsEstimate.builder()
                        .setNullsFraction(0.3)
                        .setLowValue(1.6)
                        .setHighValue(3.3)
                        .setAverageRowSize(2.0)
                        .build())
                .build();

        assertCalculate(
                new Cast(new Reference(BIGINT, "a"), BIGINT),
                inputStatistics)
                .lowValue(2.0)
                .highValue(3.0)
                .distinctValuesCountUnknown()
                .nullsFraction(0.3)
                .dataSizeUnknown();
    }

    @Test
    public void testCastBigintToDouble()
    {
        PlanNodeStatsEstimate inputStatistics = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(new Symbol(DOUBLE, "a"), SymbolStatsEstimate.builder()
                        .setNullsFraction(0.3)
                        .setLowValue(2.0)
                        .setHighValue(10.0)
                        .setDistinctValuesCount(4)
                        .setAverageRowSize(2.0)
                        .build())
                .build();

        assertCalculate(
                new Cast(new Reference(DOUBLE, "a"), DOUBLE),
                inputStatistics)
                .lowValue(2.0)
                .highValue(10.0)
                .distinctValuesCount(4)
                .nullsFraction(0.3)
                .dataSizeUnknown();
    }

    @Test
    public void testCastUnknown()
    {
        assertCalculate(
                new Cast(new Reference(BIGINT, "a"), BIGINT),
                PlanNodeStatsEstimate.unknown())
                .lowValueUnknown()
                .highValueUnknown()
                .distinctValuesCountUnknown()
                .nullsFractionUnknown()
                .dataSizeUnknown();
    }

    private SymbolStatsAssertion assertCalculate(Expression scalarExpression)
    {
        return assertCalculate(scalarExpression, PlanNodeStatsEstimate.unknown());
    }

    private SymbolStatsAssertion assertCalculate(Expression scalarExpression, PlanNodeStatsEstimate inputStatistics)
    {
        TransactionManager transactionManager = new TestingTransactionManager();
        Metadata metadata = MetadataManager.testMetadataManagerBuilder().withTransactionManager(transactionManager).build();
        return transaction(transactionManager, metadata, new AllowAllAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    return SymbolStatsAssertion.assertThat(calculator.calculate(scalarExpression, inputStatistics, transactionSession));
                });
    }

    @Test
    public void testNonDivideArithmeticBinaryExpression()
    {
        PlanNodeStatsEstimate relationStats = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(new Symbol(BIGINT, "x"), SymbolStatsEstimate.builder()
                        .setLowValue(-1)
                        .setHighValue(10)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0.1)
                        .setAverageRowSize(2.0)
                        .build())
                .addSymbolStatistics(new Symbol(BIGINT, "y"), SymbolStatsEstimate.builder()
                        .setLowValue(-2)
                        .setHighValue(5)
                        .setDistinctValuesCount(3)
                        .setNullsFraction(0.2)
                        .setAverageRowSize(2.0)
                        .build())
                .addSymbolStatistics(new Symbol(BIGINT, "unknown"), SymbolStatsEstimate.unknown())
                .setOutputRowCount(10)
                .build();

        assertCalculate(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), relationStats)
                .distinctValuesCount(10.0)
                .lowValue(-3.0)
                .highValue(15.0)
                .nullsFraction(0.28)
                .averageRowSize(2.0);

        assertCalculate(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "unknown"))), relationStats)
                .isEqualTo(SymbolStatsEstimate.unknown());
        assertCalculate(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "unknown"), new Reference(BIGINT, "unknown"))), relationStats)
                .isEqualTo(SymbolStatsEstimate.unknown());

        assertCalculate(new Call(SUBTRACT_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), relationStats)
                .distinctValuesCount(10.0)
                .lowValue(-6.0)
                .highValue(12.0)
                .nullsFraction(0.28)
                .averageRowSize(2.0);

        assertCalculate(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), relationStats)
                .distinctValuesCount(10.0)
                .lowValue(-20.0)
                .highValue(50.0)
                .nullsFraction(0.28)
                .averageRowSize(2.0);
    }

    @Test
    public void testArithmeticBinaryWithAllNullsSymbol()
    {
        SymbolStatsEstimate allNullStats = SymbolStatsEstimate.zero();
        PlanNodeStatsEstimate relationStats = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(new Symbol(BIGINT, "x"), SymbolStatsEstimate.builder()
                        .setLowValue(-1)
                        .setHighValue(10)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0.1)
                        .setAverageRowSize(0)
                        .build())
                .addSymbolStatistics(new Symbol(BIGINT, "all_null"), allNullStats)
                .setOutputRowCount(10)
                .build();

        assertCalculate(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "all_null"))), relationStats)
                .isEqualTo(allNullStats);
        assertCalculate(new Call(SUBTRACT_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "all_null"))), relationStats)
                .isEqualTo(allNullStats);
        assertCalculate(new Call(SUBTRACT_BIGINT, ImmutableList.of(new Reference(BIGINT, "all_null"), new Reference(BIGINT, "x"))), relationStats)
                .isEqualTo(allNullStats);
        assertCalculate(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "all_null"), new Reference(BIGINT, "x"))), relationStats)
                .isEqualTo(allNullStats);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "all_null"))), relationStats)
                .isEqualTo(allNullStats);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "all_null"), new Reference(BIGINT, "x"))), relationStats)
                .isEqualTo(allNullStats);
        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "all_null"))), relationStats)
                .isEqualTo(allNullStats);
        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "all_null"), new Reference(BIGINT, "x"))), relationStats)
                .isEqualTo(allNullStats);
    }

    @Test
    public void testDivideArithmeticBinaryExpression()
    {
        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-11, -3, -5, -4)).lowValue(0.6).highValue(2.75);
        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-11, -3, -5, 4)).lowValue(NEGATIVE_INFINITY).highValue(POSITIVE_INFINITY);
        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-11, -3, 4, 5)).lowValue(-2.75).highValue(-0.6);

        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-11, 0, -5, -4)).lowValue(0).highValue(2.75);
        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-11, 0, -5, 4)).lowValue(NEGATIVE_INFINITY).highValue(POSITIVE_INFINITY);
        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-11, 0, 4, 5)).lowValue(-2.75).highValue(0);

        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-11, 3, -5, -4)).lowValue(-0.75).highValue(2.75);
        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-11, 3, -5, 4)).lowValue(NEGATIVE_INFINITY).highValue(POSITIVE_INFINITY);
        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-11, 3, 4, 5)).lowValue(-2.75).highValue(0.75);

        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(0, 3, -5, -4)).lowValue(-0.75).highValue(0);
        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(0, 3, -5, 4)).lowValue(NEGATIVE_INFINITY).highValue(POSITIVE_INFINITY);
        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(0, 3, 4, 5)).lowValue(0).highValue(0.75);

        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(3, 11, -5, -4)).lowValue(-2.75).highValue(-0.6);
        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(3, 11, -5, 4)).lowValue(NEGATIVE_INFINITY).highValue(POSITIVE_INFINITY);
        assertCalculate(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(3, 11, 4, 5)).lowValue(0.6).highValue(2.75);
    }

    @Test
    public void testModulusArithmeticBinaryExpression()
    {
        // negative
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-1, 0, -6, -4)).lowValue(-1).highValue(0);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-5, 0, -6, -4)).lowValue(-5).highValue(0);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-8, 0, -6, -4)).lowValue(-6).highValue(0);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-8, 0, -6, -4)).lowValue(-6).highValue(0);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-8, 0, -6, 4)).lowValue(-6).highValue(0);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-8, 0, -6, 6)).lowValue(-6).highValue(0);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-8, 0, 4, 6)).lowValue(-6).highValue(0);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-1, 0, 4, 6)).lowValue(-1).highValue(0);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-5, 0, 4, 6)).lowValue(-5).highValue(0);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-8, 0, 4, 6)).lowValue(-6).highValue(0);

        // positive
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(0, 5, -6, -4)).lowValue(0).highValue(5);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(0, 8, -6, -4)).lowValue(0).highValue(6);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(0, 1, -6, 4)).lowValue(0).highValue(1);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(0, 5, -6, 4)).lowValue(0).highValue(5);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(0, 8, -6, 4)).lowValue(0).highValue(6);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(0, 1, 4, 6)).lowValue(0).highValue(1);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(0, 5, 4, 6)).lowValue(0).highValue(5);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(0, 8, 4, 6)).lowValue(0).highValue(6);

        // mix
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-1, 1, -6, -4)).lowValue(-1).highValue(1);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-1, 5, -6, -4)).lowValue(-1).highValue(5);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-5, 1, -6, -4)).lowValue(-5).highValue(1);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-5, 5, -6, -4)).lowValue(-5).highValue(5);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-5, 8, -6, -4)).lowValue(-5).highValue(6);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-8, 5, -6, -4)).lowValue(-6).highValue(5);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-8, 8, -6, -4)).lowValue(-6).highValue(6);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-1, 1, -6, 4)).lowValue(-1).highValue(1);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-1, 5, -6, 4)).lowValue(-1).highValue(5);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-5, 1, -6, 4)).lowValue(-5).highValue(1);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-5, 5, -6, 4)).lowValue(-5).highValue(5);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-5, 8, -6, 4)).lowValue(-5).highValue(6);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-8, 5, -6, 4)).lowValue(-6).highValue(5);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-8, 8, -6, 4)).lowValue(-6).highValue(6);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-1, 1, 4, 6)).lowValue(-1).highValue(1);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-1, 5, 4, 6)).lowValue(-1).highValue(5);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-5, 1, 4, 6)).lowValue(-5).highValue(1);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-5, 5, 4, 6)).lowValue(-5).highValue(5);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-5, 8, 4, 6)).lowValue(-5).highValue(6);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-8, 5, 4, 6)).lowValue(-6).highValue(5);
        assertCalculate(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "x"), new Reference(BIGINT, "y"))), xyStats(-8, 8, 4, 6)).lowValue(-6).highValue(6);
    }

    private PlanNodeStatsEstimate xyStats(double lowX, double highX, double lowY, double highY)
    {
        return PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(new Symbol(BIGINT, "x"), SymbolStatsEstimate.builder()
                        .setLowValue(lowX)
                        .setHighValue(highX)
                        .build())
                .addSymbolStatistics(new Symbol(BIGINT, "y"), SymbolStatsEstimate.builder()
                        .setLowValue(lowY)
                        .setHighValue(highY)
                        .build())
                .build();
    }

    @Test
    public void testCoalesceExpression()
    {
        PlanNodeStatsEstimate relationStats = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(new Symbol(INTEGER, "x"), SymbolStatsEstimate.builder()
                        .setLowValue(-1)
                        .setHighValue(10)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0.1)
                        .setAverageRowSize(2.0)
                        .build())
                .addSymbolStatistics(new Symbol(INTEGER, "y"), SymbolStatsEstimate.builder()
                        .setLowValue(-2)
                        .setHighValue(5)
                        .setDistinctValuesCount(3)
                        .setNullsFraction(0.2)
                        .setAverageRowSize(2.0)
                        .build())
                .setOutputRowCount(10)
                .build();

        assertCalculate(new Coalesce(new Reference(INTEGER, "x"), new Reference(INTEGER, "y")), relationStats)
                .distinctValuesCount(5)
                .lowValue(-2)
                .highValue(10)
                .nullsFraction(0.02)
                .averageRowSize(2.0);

        assertCalculate(new Coalesce(new Reference(INTEGER, "y"), new Reference(INTEGER, "x")), relationStats)
                .distinctValuesCount(5)
                .lowValue(-2)
                .highValue(10)
                .nullsFraction(0.02)
                .averageRowSize(2.0);
    }
}
