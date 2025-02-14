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

package io.trino.type.setdigest;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.AbstractVariableWidthType;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeSignature;

// Layout is <size>:<digest>, where
//   size: is an int describing the length of the set digest bytes
//   digest: is the serialized digest
public class SetDigestType
        extends AbstractVariableWidthType
{
    public static final SetDigestType SET_DIGEST = new SetDigestType();

    private SetDigestType()
    {
        super(new TypeSignature(StandardTypes.SET_DIGEST), Slice.class);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return new SqlVarbinary(getSlice(block, position).getBytes());
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        return valueBlock.getSlice(valuePosition);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(value, offset, length);
    }
}
