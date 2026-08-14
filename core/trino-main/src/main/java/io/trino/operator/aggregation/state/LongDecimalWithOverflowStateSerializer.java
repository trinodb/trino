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
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.Int128ArrayBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import static io.trino.spi.type.BigintType.BIGINT;

/// Serializes the decimal sum state as `row(sum decimal(38, s), overflow bigint)`. The sum field
/// holds the raw 128-bit running sum, which may lie outside the valid decimal range; the overflow
/// field counts the wraps around 2^128. Raw Int128 writes bypass decimal range validation on
/// purpose — the range is only checked when the final output is produced.
public class LongDecimalWithOverflowStateSerializer
        implements AccumulatorStateSerializer<LongDecimalWithOverflowState>
{
    private final RowType serializedType;

    public LongDecimalWithOverflowStateSerializer(@TypeParameter("row(sum decimal(38, s), overflow bigint)") Type serializedType)
    {
        this.serializedType = (RowType) serializedType;
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(LongDecimalWithOverflowState state, BlockBuilder out)
    {
        if (!state.isNotNull()) {
            out.appendNull();
            return;
        }

        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();
        write(decimal[offset], decimal[offset + 1], state.getOverflow(), (RowBlockBuilder) out);
    }

    public static void write(long high, long low, long overflow, RowBlockBuilder out)
    {
        out.buildEntry(fieldBuilders -> {
            ((Int128ArrayBlockBuilder) fieldBuilders.get(0)).writeInt128(high, low);
            BIGINT.writeLong(fieldBuilders.get(1), overflow);
        });
    }

    @Override
    public void deserialize(Block block, int index, LongDecimalWithOverflowState state)
    {
        if (block.isNull(index)) {
            return;
        }

        SqlRow row = (SqlRow) serializedType.getObject(block, index);
        int rawIndex = row.getRawIndex();

        Block sumField = row.getRawFieldBlock(0);
        Int128ArrayBlock sumBlock = (Int128ArrayBlock) sumField.getUnderlyingValueBlock();
        int sumPosition = sumField.getUnderlyingValuePosition(rawIndex);

        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();
        decimal[offset] = sumBlock.getInt128High(sumPosition);
        decimal[offset + 1] = sumBlock.getInt128Low(sumPosition);
        state.setOverflow(BIGINT.getLong(row.getRawFieldBlock(1), rawIndex));
        state.setNotNull();
    }
}
