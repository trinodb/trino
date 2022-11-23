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
package io.trino.operator.aggregation.state;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLongDecimalWithOverflowStateSerializer
{
    private static final LongDecimalWithOverflowStateFactory STATE_FACTORY = new LongDecimalWithOverflowStateFactory();

    @Test(dataProvider = "input")
    public void testSerde(long low, long high, long overflow, int expectedLength)
    {
        LongDecimalWithOverflowState state = STATE_FACTORY.createSingleState();
        state.getDecimalArray()[0] = high;
        state.getDecimalArray()[1] = low;
        state.setOverflow(overflow);
        state.setNotNull();

        LongDecimalWithOverflowState outState = roundTrip(state, expectedLength);

        assertTrue(outState.isNotNull());
        assertEquals(outState.getDecimalArray()[0], high);
        assertEquals(outState.getDecimalArray()[1], low);
        assertEquals(outState.getOverflow(), overflow);
    }

    @Test
    public void testNullSerde()
    {
        // state is created null
        LongDecimalWithOverflowState state = STATE_FACTORY.createSingleState();

        LongDecimalWithOverflowState outState = roundTrip(state, 0);

        assertFalse(outState.isNotNull());
    }

    private LongDecimalWithOverflowState roundTrip(LongDecimalWithOverflowState state, int expectedLength)
    {
        LongDecimalWithOverflowStateSerializer serializer = new LongDecimalWithOverflowStateSerializer();
        BlockBuilder out = new VariableWidthBlockBuilder(null, 1, 0);

        serializer.serialize(state, out);

        Block serialized = out.build();
        assertEquals(serialized.getSliceLength(0), expectedLength * Long.BYTES);
        LongDecimalWithOverflowState outState = STATE_FACTORY.createSingleState();
        serializer.deserialize(serialized, 0, outState);
        return outState;
    }

    @DataProvider
    public Object[][] input()
    {
        return new Object[][] {
                {3, 0, 0, 1},
                {3, 5, 0, 2},
                {3, 5, 7, 3},
                {3, 0, 7, 3},
                {0, 0, 0, 1},
                {0, 5, 0, 2},
                {0, 5, 7, 3},
                {0, 0, 7, 3}
        };
    }
}
