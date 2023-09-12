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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import static io.trino.spi.type.VarbinaryType.VARBINARY;

public class LongDecimalWithOverflowStateSerializer
        implements AccumulatorStateSerializer<LongDecimalWithOverflowState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(LongDecimalWithOverflowState state, BlockBuilder out)
    {
        if (state.isNotNull()) {
            long overflow = state.getOverflow();
            long[] decimal = state.getDecimalArray();
            int offset = state.getDecimalArrayOffset();
            Slice buffer = Slices.allocate(Long.BYTES * 3);
            long low = decimal[offset + 1];
            long high = decimal[offset];
            buffer.setLong(0, low);
            buffer.setLong(Long.BYTES, high);
            buffer.setLong(Long.BYTES * 2, overflow);
            // if high == 0 and overflow == 0 we only write low (bufferLength = 1)
            // if high != 0 and overflow == 0 we write both low and high (bufferLength = 2)
            // if overflow != 0 we write all values (bufferLength = 3)
            int decimalsCount = 1 + (high == 0 ? 0 : 1);
            int bufferLength = overflow == 0 ? decimalsCount : 3;
            VARBINARY.writeSlice(out, buffer, 0, bufferLength * Long.BYTES);
        }
        else {
            out.appendNull();
        }
    }

    @Override
    public void deserialize(Block block, int index, LongDecimalWithOverflowState state)
    {
        if (!block.isNull(index)) {
            long[] decimal = state.getDecimalArray();
            int offset = state.getDecimalArrayOffset();

            long low = block.getLong(index, 0);
            int sliceLength = block.getSliceLength(index);
            long high = 0;
            long overflow = 0;

            switch (sliceLength) {
                case 3 * Long.BYTES:
                    overflow = block.getLong(index, Long.BYTES * 2);
                    // fall through
                case 2 * Long.BYTES:
                    high = block.getLong(index, Long.BYTES);
            }

            decimal[offset + 1] = low;
            decimal[offset] = high;
            state.setOverflow(overflow);
            state.setNotNull();
        }
    }
}
