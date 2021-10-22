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
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import static io.trino.spi.type.DecimalAggregationAccumulatorType.LONG_DECIMAL_WITH_OVERFLOW_AND_LONG;

public class LongDecimalWithOverflowAndLongStateSerializer
        implements AccumulatorStateSerializer<LongDecimalWithOverflowAndLongState>
{
    public static final Type SERIALIZED_TYPE = LONG_DECIMAL_WITH_OVERFLOW_AND_LONG;

    @Override
    public Type getSerializedType()
    {
        return SERIALIZED_TYPE;
    }

    @Override
    public void serialize(LongDecimalWithOverflowAndLongState state, BlockBuilder out)
    {
        if (state.isNotNull()) {
            long count = state.getLong();
            long overflow = state.getOverflow();
            long[] decimal = state.getDecimalArray();
            int offset = state.getDecimalArrayOffset();

            out.writeLong(count);
            out.writeLong(decimal[offset]);
            out.writeLong(decimal[offset + 1]);
            out.writeLong(overflow);
            out.closeEntry();
        }
        else {
            out.appendNull();
        }
    }

    @Override
    public void deserialize(Block block, int index, LongDecimalWithOverflowAndLongState state)
    {
        if (!block.isNull(index)) {
            state.setNotNull();
            long count = block.getLong(index, 0);
            state.setLong(count);

            long[] decimal = state.getDecimalArray();
            int offset = state.getDecimalArrayOffset();
            decimal[offset] = block.getLong(index, Long.BYTES);
            decimal[offset + 1] = block.getLong(index, Long.BYTES * 2);
            long overflow = block.getLong(index, Long.BYTES * 3);
            state.setOverflow(overflow);
        }
    }
}
