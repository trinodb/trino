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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.trino.operator.aggregation.state.Int128State;
import io.trino.operator.aggregation.state.Int128StateFactory;
import io.trino.operator.aggregation.state.LongState;
import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.List;

import static io.trino.operator.aggregation.DecimalAverageAggregation.average;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static java.math.BigInteger.ONE;
import static java.math.BigInteger.TEN;
import static java.math.BigInteger.ZERO;
import static java.math.RoundingMode.HALF_UP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDecimalAverageAggregation
{
    private static final BigInteger TWO = new BigInteger("2");
    private static final BigInteger ONE_HUNDRED = new BigInteger("100");
    private static final BigInteger TWO_HUNDRED = new BigInteger("200");
    private static final DecimalType TYPE = createDecimalType(38, 0);

    private Int128State decimalState;
    private LongState counterState;
    private NullableLongState overflowState;
    private AccumulatorStateFactory<Int128State> int128StateFactory;
    private AccumulatorStateFactory<LongState> longStateFactory = StateCompiler.generateStateFactory(LongState.class);
    private AccumulatorStateFactory<NullableLongState> nullableLongStateFactory = StateCompiler.generateStateFactory(NullableLongState.class);

    @BeforeMethod
    public void setUp()
    {
        int128StateFactory = new Int128StateFactory();
        decimalState = int128StateFactory.createSingleState();
        counterState = longStateFactory.createSingleState();
        overflowState = nullableLongStateFactory.createSingleState();
    }

    @Test
    public void testOverflow()
    {
        addToState(decimalState, counterState, overflowState, TWO.pow(126));
        assertEquals(counterState.getValue(), 1);
        assertEquals(overflowState.getValue(), 0);
        assertEquals(getDecimal(decimalState), Int128.valueOf(TWO.pow(126)));

        addToState(decimalState, counterState, overflowState, TWO.pow(126));

        assertEquals(counterState.getValue(), 2);
        assertEquals(overflowState.getValue(), 1);
        assertEquals(getDecimal(decimalState), Int128.valueOf(1L << 63, 0));
        assertAverageEquals(TWO.pow(126));
    }

    @Test
    public void testUnderflow()
    {
        addToState(decimalState, counterState, overflowState, Decimals.MIN_UNSCALED_DECIMAL.toBigInteger());

        assertEquals(counterState.getValue(), 1);
        assertEquals(overflowState.getValue(), 0);
        assertEquals(getDecimal(decimalState), Decimals.MIN_UNSCALED_DECIMAL);

        addToState(decimalState, counterState, overflowState, Decimals.MIN_UNSCALED_DECIMAL.toBigInteger());

        assertEquals(counterState.getValue(), 2);
        assertEquals(overflowState.getValue(), -1);
        assertEquals(getDecimal(decimalState), Int128.valueOf(0x698966AF4AF2770BL, 0xECEBBB8000000002L));

        assertAverageEquals(Decimals.MIN_UNSCALED_DECIMAL.toBigInteger());
    }

    @Test
    public void testUnderflowAfterOverflow()
    {
        addToState(decimalState, counterState, overflowState, TWO.pow(126));
        addToState(decimalState, counterState, overflowState, TWO.pow(126));
        addToState(decimalState, counterState, overflowState, TWO.pow(125));

        assertEquals(overflowState.getValue(), 1);
        assertEquals(getDecimal(decimalState), Int128.valueOf((1L << 63) | (1L << 61), 0));

        addToState(decimalState, counterState, overflowState, TWO.pow(126).negate());
        addToState(decimalState, counterState, overflowState, TWO.pow(126).negate());
        addToState(decimalState, counterState, overflowState, TWO.pow(126).negate());

        assertEquals(overflowState.getValue(), 0);
        assertEquals(getDecimal(decimalState), Int128.valueOf(TWO.pow(125).negate()));

        assertAverageEquals(TWO.pow(125).negate().divide(BigInteger.valueOf(6)));
    }

    @Test
    public void testCombineOverflow()
    {
        addToState(decimalState, counterState, overflowState, TWO.pow(126));
        addToState(decimalState, counterState, overflowState, TWO.pow(126));

        Int128State otherDecimalState = int128StateFactory.createSingleState();
        LongState otherCounterState = longStateFactory.createSingleState();
        NullableLongState otherOverflowState = nullableLongStateFactory.createSingleState();

        addToState(otherDecimalState, otherCounterState, otherOverflowState, TWO.pow(126));
        addToState(otherDecimalState, otherCounterState, otherOverflowState, TWO.pow(126));

        DecimalAverageAggregation.combine(decimalState, counterState, overflowState, otherDecimalState, otherCounterState, otherOverflowState);
        assertEquals(counterState.getValue(), 4);
        assertEquals(overflowState.getValue(), 1);
        assertEquals(getDecimal(decimalState), Int128.ZERO);

        BigInteger expectedAverage = BigInteger.ZERO
                .add(TWO.pow(126))
                .add(TWO.pow(126))
                .add(TWO.pow(126))
                .add(TWO.pow(126))
                .divide(BigInteger.valueOf(4));
        assertAverageEquals(expectedAverage);
    }

    @Test
    public void testCombineUnderflow()
    {
        addToState(decimalState, counterState, overflowState, TWO.pow(125).negate());
        addToState(decimalState, counterState, overflowState, TWO.pow(126).negate());

        Int128State otherDecimalState = int128StateFactory.createSingleState();
        LongState otherCounterState = longStateFactory.createSingleState();
        NullableLongState otherOverflowState = nullableLongStateFactory.createSingleState();

        addToState(otherDecimalState, otherCounterState, otherOverflowState, TWO.pow(125).negate());
        addToState(otherDecimalState, otherCounterState, otherOverflowState, TWO.pow(126).negate());

        DecimalAverageAggregation.combine(decimalState, counterState, overflowState, otherDecimalState, otherCounterState, otherOverflowState);
        assertEquals(counterState.getValue(), 4);
        assertEquals(overflowState.getValue(), -1);
        assertEquals(getDecimal(decimalState), Int128.valueOf(1L << 62, 0));

        BigInteger expectedAverage = BigInteger.ZERO
                .add(TWO.pow(126))
                .add(TWO.pow(126))
                .add(TWO.pow(125))
                .add(TWO.pow(125))
                .negate()
                .divide(BigInteger.valueOf(4));

        assertAverageEquals(expectedAverage);
    }

    @Test(dataProvider = "testNoOverflowDataProvider")
    public void testNoOverflow(List<BigInteger> numbers)
    {
        testNoOverflow(createDecimalType(38, 0), numbers);
        testNoOverflow(createDecimalType(38, 2), numbers);
    }

    @Test
    public void testCombineNullAndNull()
    {
        Int128State otherDecimalState = int128StateFactory.createSingleState();
        LongState otherCounterState = longStateFactory.createSingleState();
        NullableLongState otherOverflowState = nullableLongStateFactory.createSingleState();

        DecimalAverageAggregation.combine(decimalState, counterState, overflowState, otherDecimalState, otherCounterState, otherOverflowState);

        assertEmptyState(decimalState, counterState, overflowState);
        assertEmptyState(otherDecimalState, otherCounterState, otherOverflowState);
    }

    @Test
    public void testCombineNullAndNonNullWithOverflow()
    {
        Int128State otherDecimalState = int128StateFactory.createSingleState();
        LongState otherCounterState = longStateFactory.createSingleState();
        NullableLongState otherOverflowState = nullableLongStateFactory.createSingleState();

        addToState(otherDecimalState, otherCounterState, otherOverflowState, TWO.pow(126));
        addToState(otherDecimalState, otherCounterState, otherOverflowState, TWO.pow(126));

        AccumulatorState copyOtherDecimalState = otherDecimalState.copy();
        AccumulatorState copyOtherCounterState = otherCounterState.copy();
        AccumulatorState copyOtherOverflowState = otherOverflowState.copy();

        DecimalAverageAggregation.combine(decimalState, counterState, overflowState, otherDecimalState, otherCounterState, otherOverflowState);

        assertEquals(counterState.getValue(), 2);
        assertEquals(overflowState.getValue(), 1);
        assertFalse(overflowState.isNull());

        long[] decimal = decimalState.getArray();
        assertEquals(decimal[decimalState.getArrayOffset()], -9223372036854775808L);
        assertEquals(decimal[decimalState.getArrayOffset() + 1], 0);
        assertTrue(decimalState.isNotNull());

        assertStatesEquals(otherDecimalState, otherCounterState, otherOverflowState, (Int128State) copyOtherDecimalState, (LongState) copyOtherCounterState, (NullableLongState) copyOtherOverflowState);
    }

    @Test
    public void testCombineNullAndNonNullWithoutOverflow()
    {
        Int128State otherDecimalState = int128StateFactory.createSingleState();
        LongState otherCounterState = longStateFactory.createSingleState();
        NullableLongState otherOverflowState = nullableLongStateFactory.createSingleState();

        addToState(otherDecimalState, otherCounterState, otherOverflowState, TWO.pow(126));
        addToState(otherDecimalState, otherCounterState, otherOverflowState, TWO.pow(1));

        AccumulatorState copyOtherDecimalState = otherDecimalState.copy();
        AccumulatorState copyOtherCounterState = otherCounterState.copy();
        AccumulatorState copyOtherOverflowState = otherOverflowState.copy();

        DecimalAverageAggregation.combine(decimalState, counterState, overflowState, otherDecimalState, otherCounterState, otherOverflowState);

        assertEquals(counterState.getValue(), 2);
        assertEquals(overflowState.getValue(), 0);
        assertTrue(overflowState.isNull());
        assertTrue(decimalState.isNotNull());

        assertEquals(getDecimal(decimalState), Int128.valueOf(TWO.pow(126).add(TWO.pow(1))));

        assertStatesEquals(otherDecimalState, otherCounterState, otherOverflowState, (Int128State) copyOtherDecimalState, (LongState) copyOtherCounterState, (NullableLongState) copyOtherOverflowState);
    }

    @Test
    public void testCombineNonNullAndNull()
    {
        Int128State otherDecimalState = int128StateFactory.createSingleState();
        LongState otherCounterState = longStateFactory.createSingleState();
        NullableLongState otherOverflowState = nullableLongStateFactory.createSingleState();

        addToState(decimalState, counterState, overflowState, TWO.pow(126));
        addToState(decimalState, counterState, overflowState, TWO.pow(2));

        AccumulatorState copyOtherDecimalState = otherDecimalState.copy();
        AccumulatorState copyOtherCounterState = otherCounterState.copy();
        AccumulatorState copyOtherOverflowState = otherOverflowState.copy();

        DecimalAverageAggregation.combine(decimalState, counterState, overflowState, otherDecimalState, otherCounterState, otherOverflowState);

        assertEquals(counterState.getValue(), 2);
        assertEquals(overflowState.getValue(), 0);
        assertTrue(overflowState.isNull());
        assertTrue(decimalState.isNotNull());

        assertEquals(getDecimal(decimalState), Int128.valueOf(TWO.pow(126).add(TWO.pow(2))));
        assertStatesEquals(otherDecimalState, otherCounterState, otherOverflowState, (Int128State) copyOtherDecimalState, (LongState) copyOtherCounterState, (NullableLongState) copyOtherOverflowState);
    }

    @Test
    public void testCombineNonNullOverflowAndNonNullUnderflow()
    {
        Int128State otherDecimalState = int128StateFactory.createSingleState();
        LongState otherCounterState = longStateFactory.createSingleState();
        NullableLongState otherOverflowState = nullableLongStateFactory.createSingleState();

        addToState(decimalState, counterState, overflowState, TWO.pow(126));
        addToState(decimalState, counterState, overflowState, TWO.pow(126));
        addToState(decimalState, counterState, overflowState, TWO.pow(126));

        addToState(otherDecimalState, otherCounterState, otherOverflowState, TWO.pow(126).negate());
        addToState(otherDecimalState, otherCounterState, otherOverflowState, TWO.pow(126).negate());
        addToState(otherDecimalState, otherCounterState, otherOverflowState, TWO.pow(126).negate());

        AccumulatorState copyOtherDecimalState = otherDecimalState.copy();
        AccumulatorState copyOtherCounterState = otherCounterState.copy();
        AccumulatorState copyOtherOverflowState = otherOverflowState.copy();

        DecimalAverageAggregation.combine(decimalState, counterState, overflowState, otherDecimalState, otherCounterState, otherOverflowState);

        assertEquals(counterState.getValue(), 6);
        assertEquals(overflowState.getValue(), 0);
        assertFalse(overflowState.isNull());
        assertTrue(decimalState.isNotNull());

        assertEquals(getDecimal(decimalState), Int128.valueOf(TWO.pow(126).add(TWO.pow(126)).add(TWO.pow(126).negate()).add(TWO.pow(126).negate())));
        assertAverageEquals(ZERO);
        assertStatesEquals(otherDecimalState, otherCounterState, otherOverflowState, (Int128State) copyOtherDecimalState, (LongState) copyOtherCounterState, (NullableLongState) copyOtherOverflowState);
    }

    @Test
    public void testCombineNonNullOverflowAndNull()
    {
        Int128State otherDecimalState = int128StateFactory.createSingleState();
        LongState otherCounterState = longStateFactory.createSingleState();
        NullableLongState otherOverflowState = nullableLongStateFactory.createSingleState();

        addToState(decimalState, counterState, overflowState, TWO.pow(126));
        addToState(decimalState, counterState, overflowState, TWO.pow(126));

        assertEquals(counterState.getValue(), 2);
        assertEquals(overflowState.getValue(), 1);
        assertFalse(overflowState.isNull());
        assertTrue(decimalState.isNotNull());

        DecimalAverageAggregation.combine(decimalState, counterState, overflowState, otherDecimalState, otherCounterState, otherOverflowState);

        assertEquals(counterState.getValue(), 2);
        assertEquals(overflowState.getValue(), 1);
        assertFalse(overflowState.isNull());
        assertTrue(decimalState.isNotNull());
    }

    private void testNoOverflow(DecimalType type, List<BigInteger> numbers)
    {
        Int128State decimalState = int128StateFactory.createSingleState();
        LongState counterState = longStateFactory.createSingleState();
        NullableLongState overflowState = nullableLongStateFactory.createSingleState();

        for (BigInteger number : numbers) {
            addToState(type, decimalState, counterState, overflowState, number);
        }

        assertEquals(overflowState.getValue(), 0);
        BigInteger sum = numbers.stream().reduce(BigInteger.ZERO, BigInteger::add);
        assertEquals(getDecimal(decimalState), Int128.valueOf(sum));

        BigDecimal expectedAverage = new BigDecimal(sum, type.getScale()).divide(BigDecimal.valueOf(numbers.size()), type.getScale(), HALF_UP);
        assertEquals(decodeBigDecimal(type, average(decimalState, counterState, overflowState, type)), expectedAverage);
    }

    @DataProvider
    public static Object[][] testNoOverflowDataProvider()
    {
        return new Object[][] {
                {ImmutableList.of(TEN.pow(37), ZERO)},
                {ImmutableList.of(TEN.pow(37).negate(), ZERO)},
                {ImmutableList.of(TWO, ONE)},
                {ImmutableList.of(ZERO, ONE)},
                {ImmutableList.of(TWO.negate(), ONE.negate())},
                {ImmutableList.of(ONE.negate(), ZERO)},
                {ImmutableList.of(ONE.negate(), ZERO, ZERO)},
                {ImmutableList.of(TWO.negate(), ZERO, ZERO)},
                {ImmutableList.of(TWO.negate(), ZERO)},
                {ImmutableList.of(TWO_HUNDRED, ONE_HUNDRED)},
                {ImmutableList.of(ZERO, ONE_HUNDRED)},
                {ImmutableList.of(TWO_HUNDRED.negate(), ONE_HUNDRED.negate())},
                {ImmutableList.of(ONE_HUNDRED.negate(), ZERO)}
        };
    }

    private void assertEmptyState(Int128State decimalState, LongState counterState, NullableLongState overflowState) {
        assertEquals(counterState.getValue(), 0);
        assertEquals(overflowState.getValue(), 0);
        assertTrue(overflowState.isNull());

        long[] decimal = decimalState.getArray();
        assertEquals(decimal[decimalState.getArrayOffset()], 0);
        assertEquals(decimal[decimalState.getArrayOffset() + 1], 0);
        assertFalse(decimalState.isNotNull());
    }

    private void assertStatesEquals(Int128State decimalState, LongState counterState, NullableLongState overflowState, Int128State otherDecimalState, LongState otherCounterState, NullableLongState otherOverflowState)
    {
        assertEquals(counterState.getValue(), otherCounterState.getValue());
        assertEquals(overflowState.getValue(), otherOverflowState.getValue());
        assertEquals(overflowState.isNull(), otherOverflowState.isNull());

        long[] decimal = decimalState.getArray();
        long[] otherDecimal = otherDecimalState.getArray();
        assertEquals(decimal[decimalState.getArrayOffset()], otherDecimal[otherDecimalState.getArrayOffset()]);
        assertEquals(decimal[decimalState.getArrayOffset() + 1], otherDecimal[otherDecimalState.getArrayOffset() + 1]);
        assertEquals(decimalState.isNotNull(), otherDecimalState.isNotNull());
    }

    private static BigDecimal decodeBigDecimal(DecimalType type, Int128 average)
    {
        BigInteger unscaledVal = average.toBigInteger();
        return new BigDecimal(unscaledVal, type.getScale(), new MathContext(type.getPrecision()));
    }

    private void assertAverageEquals(BigInteger expectedAverage)
    {
        assertAverageEquals(expectedAverage, TYPE);
    }

    private void assertAverageEquals(BigInteger expectedAverage, DecimalType type)
    {
        assertEquals(average(decimalState, counterState, overflowState, type).toBigInteger(), expectedAverage);
    }

    private static void addToState(Int128State decimalState, LongState counterState, NullableLongState overflowState, BigInteger value)
    {
        addToState(TYPE, decimalState, counterState, overflowState, value);
    }

    private static void addToState(DecimalType type, Int128State decimalState, LongState counterState, NullableLongState overflowState, BigInteger value)
    {
        BlockBuilder blockBuilder = type.createFixedSizeBlockBuilder(1);
        type.writeObject(blockBuilder, Int128.valueOf(value));
        if (type.isShort()) {
            DecimalAverageAggregation.inputShortDecimal(decimalState, counterState, overflowState, blockBuilder.build(), 0);
        }
        else {
            DecimalAverageAggregation.inputLongDecimal(decimalState, counterState, overflowState, blockBuilder.build(), 0);
        }
    }

    private Int128 getDecimal(Int128State state)
    {
        int offset = state.getArrayOffset();
        return Int128.valueOf(state.getArray()[offset], state.getArray()[offset + 1]);
    }
}
