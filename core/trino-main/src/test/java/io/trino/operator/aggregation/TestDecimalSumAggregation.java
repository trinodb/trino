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

import io.trino.operator.aggregation.state.LongDecimalWithOverflowState;
import io.trino.operator.aggregation.state.LongDecimalWithOverflowStateFactory;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigInteger;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestDecimalSumAggregation
{
    private static final BigInteger TWO = new BigInteger("2");
    private static final DecimalType TYPE = createDecimalType(38, 0);

    private LongDecimalWithOverflowState state;

    @BeforeMethod
    public void setUp()
    {
        state = new LongDecimalWithOverflowStateFactory().createSingleState();
    }

    @Test
    public void testOverflow()
    {
        addToState(state, TWO.pow(126));

        assertEquals(state.getOverflow(), 0);
        assertEquals(getDecimal(state), Int128.valueOf(TWO.pow(126)));

        addToState(state, TWO.pow(126));

        assertEquals(state.getOverflow(), 1);
        assertEquals(getDecimal(state), Int128.valueOf(1L << 63, 0));
    }

    @Test
    public void testUnderflow()
    {
        addToState(state, TWO.pow(126).negate());

        assertEquals(state.getOverflow(), 0);
        assertEquals(getDecimal(state), Int128.valueOf(TWO.pow(126).negate()));

        addToState(state, TWO.pow(126).negate());

        assertEquals(state.getOverflow(), 0);
        assertEquals(getDecimal(state), Int128.valueOf(0x8000000000000000L, 0));
    }

    @Test
    public void testUnderflowAfterOverflow()
    {
        addToState(state, TWO.pow(126));
        addToState(state, TWO.pow(126));
        addToState(state, TWO.pow(125));

        assertEquals(state.getOverflow(), 1);
        assertEquals(getDecimal(state), Int128.valueOf((1L << 63) | (1L << 61), 0));

        addToState(state, TWO.pow(126).negate());
        addToState(state, TWO.pow(126).negate());
        addToState(state, TWO.pow(126).negate());

        assertEquals(state.getOverflow(), 0);
        assertEquals(getDecimal(state), Int128.valueOf(TWO.pow(125).negate()));
    }

    @Test
    public void testCombineOverflow()
    {
        addToState(state, TWO.pow(125));
        addToState(state, TWO.pow(126));

        LongDecimalWithOverflowState otherState = new LongDecimalWithOverflowStateFactory().createSingleState();

        addToState(otherState, TWO.pow(125));
        addToState(otherState, TWO.pow(126));

        DecimalSumAggregation.combine(state, otherState);
        assertEquals(state.getOverflow(), 1);
        assertEquals(getDecimal(state), Int128.valueOf(0xC000000000000000L, 0));
    }

    @Test
    public void testCombineUnderflow()
    {
        addToState(state, TWO.pow(125).negate());
        addToState(state, TWO.pow(126).negate());

        LongDecimalWithOverflowState otherState = new LongDecimalWithOverflowStateFactory().createSingleState();

        addToState(otherState, TWO.pow(125).negate());
        addToState(otherState, TWO.pow(126).negate());

        DecimalSumAggregation.combine(state, otherState);
        assertEquals(state.getOverflow(), -1);
        assertEquals(getDecimal(state), Int128.valueOf(0x4000000000000000L, 0));
    }

    @Test
    public void testOverflowOnOutput()
    {
        addToState(state, TWO.pow(126));
        addToState(state, TWO.pow(126));

        assertEquals(state.getOverflow(), 1);
        assertThatThrownBy(() -> DecimalSumAggregation.outputLongDecimal(state, new VariableWidthBlockBuilder(null, 10, 100)))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("Decimal overflow");
    }

    private static void addToState(LongDecimalWithOverflowState state, BigInteger value)
    {
        if (TYPE.isShort()) {
            DecimalSumAggregation.inputShortDecimal(state, Int128.valueOf(value).toLongExact());
        }
        else {
            BlockBuilder blockBuilder = TYPE.createFixedSizeBlockBuilder(1);
            TYPE.writeObject(blockBuilder, Int128.valueOf(value));
            DecimalSumAggregation.inputLongDecimal(state, blockBuilder.build(), 0);
        }
    }

    private Int128 getDecimal(LongDecimalWithOverflowState state)
    {
        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();

        return Int128.valueOf(decimal[offset], decimal[offset + 1]);
    }
}
