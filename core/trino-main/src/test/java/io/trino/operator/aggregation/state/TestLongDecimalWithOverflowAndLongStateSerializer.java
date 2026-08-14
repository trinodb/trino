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
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.RowType.field;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLongDecimalWithOverflowAndLongStateSerializer
{
    private static final LongDecimalWithOverflowAndLongStateFactory STATE_FACTORY = new LongDecimalWithOverflowAndLongStateFactory();
    private static final DecimalType LONG_DECIMAL = createDecimalType(38, 0);
    private static final DecimalType SHORT_DECIMAL = createDecimalType(18, 0);

    @Test
    public void testSerde()
    {
        for (DecimalType type : List.of(LONG_DECIMAL, SHORT_DECIMAL)) {
            testSerde(type, 3, 0, 0, 1);
            testSerde(type, 3, 5, 0, 1);
            testSerde(type, 3, 5, 7, 1);
            testSerde(type, 3, 0, 0, 2);
            testSerde(type, 3, 5, 0, 2);
            testSerde(type, 3, 5, 7, 2);
            testSerde(type, 3, 0, 7, 2);
            testSerde(type, 0, 0, 0, 1);
            testSerde(type, 0, 5, 0, 1);
            testSerde(type, 0, 5, 7, 2);
            testSerde(type, 0, 0, 7, 2);
            testSerde(type, -1, -1, -1, 2);
        }
    }

    private void testSerde(DecimalType type, long low, long high, long overflow, long count)
    {
        testSerde(type, low, high, overflow, count, Function.identity());
    }

    private void testSerde(DecimalType type, long low, long high, long overflow, long count, Function<Block, Block> serializedModification)
    {
        LongDecimalWithOverflowAndLongState state = STATE_FACTORY.createSingleState();
        state.getDecimalArray()[0] = high;
        state.getDecimalArray()[1] = low;
        state.setOverflow(overflow);
        state.setLong(count);

        LongDecimalWithOverflowAndLongState outState = roundTrip(type, state, serializedModification);

        assertThat(outState.getDecimalArray()[0]).isEqualTo(high);
        assertThat(outState.getDecimalArray()[1]).isEqualTo(low);
        assertThat(outState.getOverflow()).isEqualTo(overflow);
        assertThat(outState.getLong()).isEqualTo(count);
    }

    @Test
    public void testNullSerde()
    {
        // state is created null
        LongDecimalWithOverflowAndLongState state = STATE_FACTORY.createSingleState();

        LongDecimalWithOverflowAndLongState outState = roundTrip(LONG_DECIMAL, state, Function.identity());

        assertThat(outState.getLong()).isEqualTo(0);
    }

    @Test
    public void testDictionaryDeserialization()
    {
        testSerde(LONG_DECIMAL, 3, 0, 0, 1, block -> DictionaryBlock.create(2, block, new int[] {0, 0}));
        testSerde(SHORT_DECIMAL, 3, 0, 0, 1, block -> DictionaryBlock.create(2, block, new int[] {0, 0}));
    }

    @Test
    public void testRleDeserialization()
    {
        testSerde(LONG_DECIMAL, 3, 0, 0, 1, block -> RunLengthEncodedBlock.create(block, 2));
        testSerde(SHORT_DECIMAL, 3, 0, 0, 1, block -> RunLengthEncodedBlock.create(block, 2));
    }

    private LongDecimalWithOverflowAndLongState roundTrip(DecimalType type, LongDecimalWithOverflowAndLongState state, Function<Block, Block> serializedModification)
    {
        RowType serializedType = serializedType(type);
        LongDecimalWithOverflowAndLongStateSerializer serializer = new LongDecimalWithOverflowAndLongStateSerializer(serializedType);
        RowBlockBuilder out = (RowBlockBuilder) serializedType.createBlockBuilder(null, 1);

        serializer.serialize(state, out);

        LongDecimalWithOverflowAndLongState outState = STATE_FACTORY.createSingleState();
        serializer.deserialize(serializedModification.apply(out.build()), 0, outState);
        return outState;
    }

    private static RowType serializedType(DecimalType type)
    {
        return RowType.rowType(
                field("sum", type),
                field("high", BIGINT),
                field("overflow", BIGINT),
                field("count", BIGINT));
    }
}
