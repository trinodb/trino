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
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import static io.trino.spi.type.BigintType.BIGINT;

/// Serializes the decimal average state as `row(sum decimal(p, s), high bigint, overflow bigint,
/// count bigint)`. The input type rides in the row so the final step, which is resolved from the
/// intermediate type alone, can re-bind both p and s.
///
/// The sum field holds the input-width low bits of the raw 128-bit running sum; for short decimals
/// the remaining bits ride in the high field, which is zero for long decimals. Sum values may lie
/// outside the valid decimal range — range validation happens only when output is produced. The
/// overflow field counts the wraps around 2^128.
public class LongDecimalWithOverflowAndLongStateSerializer
        implements AccumulatorStateSerializer<LongDecimalWithOverflowAndLongState>
{
    private final RowType serializedType;
    private final boolean shortDecimal;

    public LongDecimalWithOverflowAndLongStateSerializer(@TypeParameter("row(sum decimal(p, s), high bigint, overflow bigint, count bigint)") Type serializedType)
    {
        this.serializedType = (RowType) serializedType;
        this.shortDecimal = ((DecimalType) this.serializedType.getFields().getFirst().getType()).isShort();
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(LongDecimalWithOverflowAndLongState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
            return;
        }

        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();
        write(shortDecimal, decimal[offset], decimal[offset + 1], state.getOverflow(), count, (RowBlockBuilder) out);
    }

    public static void write(boolean shortDecimal, long high, long low, long overflow, long count, RowBlockBuilder out)
    {
        out.buildEntry(fieldBuilders -> {
            if (shortDecimal) {
                ((LongArrayBlockBuilder) fieldBuilders.get(0)).writeLong(low);
                BIGINT.writeLong(fieldBuilders.get(1), high);
            }
            else {
                ((Int128ArrayBlockBuilder) fieldBuilders.get(0)).writeInt128(high, low);
                BIGINT.writeLong(fieldBuilders.get(1), 0);
            }
            BIGINT.writeLong(fieldBuilders.get(2), overflow);
            BIGINT.writeLong(fieldBuilders.get(3), count);
        });
    }

    @Override
    public void deserialize(Block block, int index, LongDecimalWithOverflowAndLongState state)
    {
        if (block.isNull(index)) {
            return;
        }

        SqlRow row = (SqlRow) serializedType.getObject(block, index);
        int rawIndex = row.getRawIndex();

        Block sumField = row.getRawFieldBlock(0);
        int sumPosition = sumField.getUnderlyingValuePosition(rawIndex);
        long high;
        long low;
        if (shortDecimal) {
            low = ((LongArrayBlock) sumField.getUnderlyingValueBlock()).getLong(sumPosition);
            high = BIGINT.getLong(row.getRawFieldBlock(1), rawIndex);
        }
        else {
            Int128ArrayBlock sumBlock = (Int128ArrayBlock) sumField.getUnderlyingValueBlock();
            high = sumBlock.getInt128High(sumPosition);
            low = sumBlock.getInt128Low(sumPosition);
        }

        long[] decimal = state.getDecimalArray();
        int offset = state.getDecimalArrayOffset();
        decimal[offset] = high;
        decimal[offset + 1] = low;
        state.setOverflow(BIGINT.getLong(row.getRawFieldBlock(2), rawIndex));
        state.setLong(BIGINT.getLong(row.getRawFieldBlock(3), rawIndex));
    }
}
