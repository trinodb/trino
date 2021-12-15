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
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import static io.trino.spi.type.BigintType.BIGINT;

public class LongDecimalWithOverflowAndLongStateSerializerRowType
        implements AccumulatorStateSerializer<LongDecimalWithOverflowAndLongState>
{
    public static final Type SERIALIZED_TYPE = RowType.rowType(
            RowType.field("count", BIGINT),
            RowType.field("decimalHigh", BIGINT),
            RowType.field("decimalLow", BIGINT),
            RowType.field("overflow", BIGINT)
    );

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

            BlockBuilder entry = out.beginBlockEntry();
            BIGINT.writeLong(entry, count);
            BIGINT.writeLong(entry, decimal[offset]);
            BIGINT.writeLong(entry, decimal[offset + 1]);
            BIGINT.writeLong(entry, overflow);
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
            Block singleRowBlock = block.getObject(index, Block.class);
            long count = singleRowBlock.getLong(0, 0);
            state.setLong(count);

            long[] decimal = state.getDecimalArray();
            int offset = state.getDecimalArrayOffset();
            decimal[offset] = singleRowBlock.getLong(1, 0);
            decimal[offset + 1] = singleRowBlock.getLong(2, 0);
            long overflow = singleRowBlock.getLong(3, 0);
            state.setOverflow(overflow);
        }
    }
}
