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

import io.trino.operator.aggregation.state.Int128State;
import io.trino.operator.aggregation.state.Int128StateFactory;
import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigInteger;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDecimalSumAggregation
{
    private static final BigInteger TWO = new BigInteger("2");
    private static final DecimalType TYPE = createDecimalType(38, 0);

    private Int128State decimalState;
    private NullableLongState overflowState;
    AccumulatorStateFactory<Int128State> int128StateFactory;
    AccumulatorStateFactory<NullableLongState> nullableLongStateFactory;

    @BeforeMethod
    public void setUp()
    {
        int128StateFactory = new Int128StateFactory();
        nullableLongStateFactory = StateCompiler.generateStateFactory(NullableLongState.class);
        decimalState = int128StateFactory.createSingleState();
        overflowState = nullableLongStateFactory.createSingleState();
    }

    @Test
    public void testOverflow()
    {
        addToState(decimalState, overflowState, TWO.pow(126));

        assertEquals(overflowState.getValue(), 0);
        assertEquals(getDecimal(decimalState), Int128.valueOf(TWO.pow(126)));

        addToState(decimalState, overflowState, TWO.pow(126));

        assertEquals(overflowState.getValue(), 1);
        assertEquals(getDecimal(decimalState), Int128.valueOf(1L << 63, 0));
    }

    @Test
    public void testUnderflow()
    {
        addToState(decimalState, overflowState, TWO.pow(126).negate());

        assertEquals(overflowState.getValue(), 0);
        assertEquals(getDecimal(decimalState), Int128.valueOf(TWO.pow(126).negate()));

        addToState(decimalState, overflowState, TWO.pow(126).negate());

        assertEquals(overflowState.getValue(), 0);
        assertEquals(getDecimal(decimalState), Int128.valueOf(0x8000000000000000L, 0));
    }

    @Test
    public void testUnderflowAfterOverflow()
    {
        addToState(decimalState, overflowState, TWO.pow(126));
        addToState(decimalState, overflowState, TWO.pow(126));
        addToState(decimalState, overflowState, TWO.pow(125));

        assertEquals(overflowState.getValue(), 1);
        assertEquals(getDecimal(decimalState), Int128.valueOf((1L << 63) | (1L << 61), 0));

        addToState(decimalState, overflowState, TWO.pow(126).negate());
        addToState(decimalState, overflowState, TWO.pow(126).negate());
        addToState(decimalState, overflowState, TWO.pow(126).negate());

        assertEquals(overflowState.getValue(), 0);
        assertEquals(getDecimal(decimalState), Int128.valueOf(TWO.pow(125).negate()));
    }

    @Test
    public void testCombineOverflow()
    {
        addToState(decimalState, overflowState, TWO.pow(125));
        addToState(decimalState, overflowState, TWO.pow(126));

        Int128State otherDecimalState = int128StateFactory.createSingleState();
        NullableLongState otherOverflowState = nullableLongStateFactory.createSingleState();

        addToState(otherDecimalState, otherOverflowState, TWO.pow(125));
        addToState(otherDecimalState, otherOverflowState, TWO.pow(126));

        DecimalSumAggregation.combine(decimalState, overflowState, otherDecimalState, otherOverflowState);
        assertEquals(overflowState.getValue(), 1);
        assertEquals(getDecimal(decimalState), Int128.valueOf(0xC000000000000000L, 0));
    }

    @Test
    public void testCombineUnderflow()
    {
        addToState(decimalState, overflowState, TWO.pow(125).negate());
        addToState(decimalState, overflowState, TWO.pow(126).negate());

        Int128State otherDecimalState = int128StateFactory.createSingleState();
        NullableLongState otherOverflowState = nullableLongStateFactory.createSingleState();

        addToState(otherDecimalState, otherOverflowState, TWO.pow(125).negate());
        addToState(otherDecimalState, otherOverflowState, TWO.pow(126).negate());

        DecimalSumAggregation.combine(decimalState, overflowState, otherDecimalState, otherOverflowState);
        assertEquals(overflowState.getValue(), -1);
        assertEquals(getDecimal(decimalState), Int128.valueOf(0x4000000000000000L, 0));
    }

    @Test
    public void testOverflowOnOutput()
    {
        addToState(decimalState, overflowState, TWO.pow(126));
        addToState(decimalState, overflowState, TWO.pow(126));

        assertEquals(overflowState.getValue(), 1);
        assertThatThrownBy(() -> DecimalSumAggregation.outputLongDecimal(decimalState, overflowState, new VariableWidthBlockBuilder(null, 10, 100)))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("Decimal overflow");
    }

    @Test
    public void testCombineNullAndNull()
    {
        Int128State otherDecimalState = int128StateFactory.createSingleState();
        NullableLongState otherOverflowState = nullableLongStateFactory.createSingleState();

        DecimalSumAggregation.combine(decimalState, overflowState, otherDecimalState, otherOverflowState);

        assertEmptyState(decimalState, overflowState);
        assertEmptyState(otherDecimalState, otherOverflowState);
    }

    @Test
    public void testCombineNullAndNonNullWithOverflow()
    {
        Int128State otherDecimalState = int128StateFactory.createSingleState();
        NullableLongState otherOverflowState = nullableLongStateFactory.createSingleState();

        addToState(otherDecimalState, otherOverflowState, TWO.pow(126));
        addToState(otherDecimalState, otherOverflowState, TWO.pow(126));

        AccumulatorState copyOtherDecimalState = otherDecimalState.copy();
        AccumulatorState copyOtherOverflowState = otherOverflowState.copy();

        DecimalSumAggregation.combine(decimalState, overflowState, otherDecimalState, otherOverflowState);

        assertEquals(overflowState.getValue(), 1);
        assertFalse(overflowState.isNull());
        assertTrue(decimalState.isNotNull());

        assertStatesEquals(otherDecimalState, otherOverflowState, (Int128State) copyOtherDecimalState, (NullableLongState) copyOtherOverflowState);
    }

    @Test
    public void testCombineNullAndNonNullWithoutOverflow()
    {
        Int128State otherDecimalState = int128StateFactory.createSingleState();
        NullableLongState otherOverflowState = nullableLongStateFactory.createSingleState();

        addToState(otherDecimalState, otherOverflowState, TWO.pow(126));
        addToState(otherDecimalState, otherOverflowState, TWO.pow(1));

        AccumulatorState copyOtherDecimalState = otherDecimalState.copy();
        AccumulatorState copyOtherOverflowState = otherOverflowState.copy();

        DecimalSumAggregation.combine(decimalState, overflowState, otherDecimalState, otherOverflowState);

        assertEquals(overflowState.getValue(), 0);
        assertTrue(overflowState.isNull());
        assertTrue(decimalState.isNotNull());

        assertEquals(getDecimal(decimalState), Int128.valueOf(TWO.pow(126).add(TWO.pow(1))));
        assertStatesEquals(otherDecimalState, otherOverflowState, (Int128State) copyOtherDecimalState, (NullableLongState) copyOtherOverflowState);
    }

    @Test
    public void testCombineNonNullAndNull()
    {
        Int128State otherDecimalState = int128StateFactory.createSingleState();
        NullableLongState otherOverflowState = nullableLongStateFactory.createSingleState();

        addToState(decimalState, overflowState, TWO.pow(126));
        addToState(decimalState, overflowState, TWO.pow(2));

        AccumulatorState copyOtherDecimalState = otherDecimalState.copy();
        AccumulatorState copyOtherOverflowState = otherOverflowState.copy();

        DecimalSumAggregation.combine(decimalState, overflowState, otherDecimalState, otherOverflowState);

        assertEquals(overflowState.getValue(), 0);
        assertTrue(overflowState.isNull());
        assertTrue(decimalState.isNotNull());

        assertEquals(getDecimal(decimalState), Int128.valueOf(TWO.pow(126).add(TWO.pow(2))));
        assertStatesEquals(otherDecimalState, otherOverflowState, (Int128State) copyOtherDecimalState, (NullableLongState) copyOtherOverflowState);
    }

    @Test
    public void testCombineNonNullOverflowAndNonNullUnderflow()
    {
        Int128State otherDecimalState = int128StateFactory.createSingleState();
        NullableLongState otherOverflowState = nullableLongStateFactory.createSingleState();

        addToState(decimalState, overflowState, TWO.pow(126));
        addToState(decimalState, overflowState, TWO.pow(126));
        addToState(decimalState, overflowState, TWO.pow(126));

        addToState(otherDecimalState, otherOverflowState, TWO.pow(126).negate());
        addToState(otherDecimalState, otherOverflowState, TWO.pow(126).negate());
        addToState(otherDecimalState, otherOverflowState, TWO.pow(126).negate());

        AccumulatorState copyOtherDecimalState = otherDecimalState.copy();
        AccumulatorState copyOtherOverflowState = otherOverflowState.copy();

        DecimalSumAggregation.combine(decimalState, overflowState, otherDecimalState, otherOverflowState);

        assertEquals(overflowState.getValue(), 0);
        assertFalse(overflowState.isNull());
        assertTrue(decimalState.isNotNull());

        assertEquals(getDecimal(decimalState), Int128.valueOf(BigInteger.ZERO));
        assertStatesEquals(otherDecimalState, otherOverflowState, (Int128State) copyOtherDecimalState, (NullableLongState) copyOtherOverflowState);
    }

    @Test
    public void testCombineNonNullOverflowAndNull()
    {
        Int128State otherDecimalState = int128StateFactory.createSingleState();
        NullableLongState otherOverflowState = nullableLongStateFactory.createSingleState();

        addToState(decimalState, overflowState, TWO.pow(126));
        addToState(decimalState, overflowState, TWO.pow(126));

        assertEquals(overflowState.getValue(), 1);
        assertFalse(overflowState.isNull());
        assertTrue(decimalState.isNotNull());

        DecimalSumAggregation.combine(decimalState, overflowState, otherDecimalState, otherOverflowState);

        assertEquals(overflowState.getValue(), 1);
        assertFalse(overflowState.isNull());
        assertTrue(decimalState.isNotNull());
    }

    private static void addToState(Int128State decimalState, NullableLongState overflowState, BigInteger value)
    {
        BlockBuilder blockBuilder = TYPE.createFixedSizeBlockBuilder(1);
        TYPE.writeObject(blockBuilder, Int128.valueOf(value));
        if (TYPE.isShort()) {
            DecimalSumAggregation.inputShortDecimal(decimalState, overflowState, blockBuilder.build(), 0);
        }
        else {
            DecimalSumAggregation.inputLongDecimal(decimalState, overflowState, blockBuilder.build(), 0);
        }
    }

    private Int128 getDecimal(Int128State state)
    {
        int offset = state.getArrayOffset();
        return Int128.valueOf(state.getArray()[offset], state.getArray()[offset + 1]);
    }

    private void assertEmptyState(Int128State decimalState, NullableLongState overflowState)
    {
        assertEquals(overflowState.getValue(), 0);
        assertTrue(overflowState.isNull());
        assertFalse(decimalState.isNotNull());
        assertEquals(getDecimal(decimalState), Int128.valueOf(0));
    }

    private void assertStatesEquals(Int128State decimalState, NullableLongState overflowState, Int128State otherDecimalState, NullableLongState otherOverflowState)
    {
        assertEquals(overflowState.getValue(), otherOverflowState.getValue());
        assertEquals(overflowState.isNull(), otherOverflowState.isNull());
        assertEquals(decimalState.isNotNull(), otherDecimalState.isNotNull());
        assertEquals(getDecimal(decimalState), getDecimal(otherDecimalState));
        long[] decimal = decimalState.getArray();
        long[] otherDecimal = otherDecimalState.getArray();
        assertEquals(decimal[decimalState.getArrayOffset()], otherDecimal[otherDecimalState.getArrayOffset()]);
        assertEquals(decimal[decimalState.getArrayOffset() + 1], otherDecimal[otherDecimalState.getArrayOffset() + 1]);
    }
}
