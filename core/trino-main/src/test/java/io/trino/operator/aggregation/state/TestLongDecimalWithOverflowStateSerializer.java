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
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.RowType.field;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLongDecimalWithOverflowStateSerializer
{
    private static final LongDecimalWithOverflowStateFactory STATE_FACTORY = new LongDecimalWithOverflowStateFactory();
    private static final RowType SERIALIZED_TYPE = RowType.rowType(
            field("sum", createDecimalType(38, 0)),
            field("overflow", BIGINT));

    @Test
    public void testSerde()
    {
        testSerde(3, 0, 0);
        testSerde(3, 5, 0);
        testSerde(3, 5, 7);
        testSerde(3, 0, 7);
        testSerde(0, 0, 0);
        testSerde(0, 5, 0);
        testSerde(0, 5, 7);
        testSerde(0, 0, 7);
        testSerde(-1, -1, -1);
    }

    private void testSerde(long low, long high, long overflow)
    {
        testSerde(low, high, overflow, Function.identity());
    }

    private void testSerde(long low, long high, long overflow, Function<Block, Block> serializedModification)
    {
        LongDecimalWithOverflowState state = STATE_FACTORY.createSingleState();
        state.getDecimalArray()[0] = high;
        state.getDecimalArray()[1] = low;
        state.setOverflow(overflow);
        state.setNotNull();

        LongDecimalWithOverflowState outState = roundTrip(state, serializedModification);

        assertThat(outState.isNotNull()).isTrue();
        assertThat(outState.getDecimalArray()[0]).isEqualTo(high);
        assertThat(outState.getDecimalArray()[1]).isEqualTo(low);
        assertThat(outState.getOverflow()).isEqualTo(overflow);
    }

    @Test
    public void testNullSerde()
    {
        // state is created null
        LongDecimalWithOverflowState state = STATE_FACTORY.createSingleState();

        LongDecimalWithOverflowState outState = roundTrip(state, Function.identity());

        assertThat(outState.isNotNull()).isFalse();
    }

    @Test
    public void testDictionaryDeserialization()
    {
        testSerde(3, 0, 0, block -> DictionaryBlock.create(2, block, new int[] {0, 0}));
    }

    @Test
    public void testRleDeserialization()
    {
        testSerde(3, 0, 0, block -> RunLengthEncodedBlock.create(block, 2));
    }

    private LongDecimalWithOverflowState roundTrip(LongDecimalWithOverflowState state, Function<Block, Block> serializedModification)
    {
        LongDecimalWithOverflowStateSerializer serializer = new LongDecimalWithOverflowStateSerializer(SERIALIZED_TYPE);
        RowBlockBuilder out = (RowBlockBuilder) SERIALIZED_TYPE.createBlockBuilder(null, 1);

        serializer.serialize(state, out);

        LongDecimalWithOverflowState outState = STATE_FACTORY.createSingleState();
        serializer.deserialize(serializedModification.apply(out.build()), 0, outState);
        return outState;
    }
}
