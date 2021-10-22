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

import io.trino.operator.aggregation.state.LongDecimalWithOverflowAndLongStateFactory.SingleLongDecimalWithOverflowAndLongState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLongDecimalWithOverflowAndLongStateSerializer
{
    @Test
    public void testSerializeDeserialize()
    {
        LongDecimalWithOverflowAndLongStateSerializer serializer = new LongDecimalWithOverflowAndLongStateSerializer();
        BlockBuilder out = serializer.getSerializedType().createBlockBuilder(null, 2);

        LongDecimalWithOverflowAndLongState[] states = {
                createNotNullState(1, 1, 2, 3),
                new SingleLongDecimalWithOverflowAndLongState(),
                createNotNullState(1, 1, 2, 0),
                createNotNullState(1, 2, 3, 4)
        };

        for (LongDecimalWithOverflowAndLongState state : states) {
            serializer.serialize(state, out);
        }
        Block outBlock = out.build();

        for (int i = 0; i < states.length; i++) {
            deserializeAndVerify(serializer, states[i], outBlock, i);
        }
    }

    private LongDecimalWithOverflowAndLongState createNotNullState(long count, int low, int high, int overflow)
    {
        LongDecimalWithOverflowAndLongState state = new SingleLongDecimalWithOverflowAndLongState();
        state.setNotNull();
        state.setLong(count);
        state.getDecimalArray()[0] = low;
        state.getDecimalArray()[1] = high;
        state.setOverflow(overflow);
        return state;
    }

    private void deserializeAndVerify(LongDecimalWithOverflowAndLongStateSerializer serializer, LongDecimalWithOverflowAndLongState state, Block outBlock, int position)
    {
        LongDecimalWithOverflowAndLongState outState = new SingleLongDecimalWithOverflowAndLongState();
        serializer.deserialize(outBlock, position, outState);
        assertThat(outState.isNotNull()).isEqualTo(state.isNotNull());
        assertThat(outState.getLong()).isEqualTo(state.getLong());
        assertThat(outState.getDecimalArray()).containsExactly(state.getDecimalArray());
        assertThat(outState.getOverflow()).isEqualTo(state.getOverflow());
    }
}
