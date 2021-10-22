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

import io.trino.operator.aggregation.state.LongDecimalWithOverflowStateFactory.SingleLongDecimalWithOverflowState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLongDecimalWithOverflowStateSerializer
{
    @Test
    public void testSerializeDeserialize()
    {
        LongDecimalWithOverflowStateSerializer serializer = new LongDecimalWithOverflowStateSerializer();
        BlockBuilder out = serializer.getSerializedType().createBlockBuilder(null, 2);

        LongDecimalWithOverflowState[] states = {
                createNotNullState(1, 2, 3),
                new SingleLongDecimalWithOverflowState(),
                createNotNullState(1, 2, 0),
                createNotNullState(2, 3, 4)
        };

        for (LongDecimalWithOverflowState state : states) {
            serializer.serialize(state, out);
        }
        Block outBlock = out.build();

        for (int i = 0; i < states.length; i++) {
            deserializeAndVerify(serializer, states[i], outBlock, i);
        }
    }

    private LongDecimalWithOverflowState createNotNullState(int low, int high, int overflow)
    {
        LongDecimalWithOverflowState state = new SingleLongDecimalWithOverflowState();
        state.setNotNull();
        state.getDecimalArray()[0] = low;
        state.getDecimalArray()[1] = high;
        state.setOverflow(overflow);
        return state;
    }

    private void deserializeAndVerify(LongDecimalWithOverflowStateSerializer serializer, LongDecimalWithOverflowState state, Block outBlock, int position)
    {
        LongDecimalWithOverflowState outState = new SingleLongDecimalWithOverflowState();
        serializer.deserialize(outBlock, position, outState);
        assertThat(outState.isNotNull()).isEqualTo(state.isNotNull());
        assertThat(outState.getDecimalArray()).containsExactly(state.getDecimalArray());
        assertThat(outState.getOverflow()).isEqualTo(state.getOverflow());
    }
}
