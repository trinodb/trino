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
package io.trino.type;

import io.airlift.slice.Slice;
import io.airlift.stats.TDigest;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.AbstractVariableWidthType;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeSignature;

public class TDigestType
        extends AbstractVariableWidthType
{
    public static final TDigestType TDIGEST = new TDigestType();

    private TDigestType()
    {
        super(new TypeSignature(StandardTypes.TDIGEST), TDigest.class);
    }

    @Override
    public Object getObject(Block block, int position)
    {
        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        return TDigest.deserialize(valueBlock.getSlice(valuePosition));
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        Slice serialized = ((TDigest) value).serialize();
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(serialized);
    }

    @Override
    public Object getObjectValue(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        return new SqlVarbinary(valueBlock.getSlice(valuePosition).getBytes());
    }
}
