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
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.AbstractVariableWidthType;
import io.trino.spi.type.TypeSignature;

public class CodePointsType
        extends AbstractVariableWidthType
{
    public static final CodePointsType CODE_POINTS = new CodePointsType();
    public static final String NAME = "CodePoints";

    private CodePointsType()
    {
        super(new TypeSignature(NAME), int[].class);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        Slice slice = block.getSlice(position, 0, block.getSliceLength(position));
        int[] codePoints = new int[slice.length() / Integer.BYTES];
        slice.getInts(0, codePoints);
        return codePoints;
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        int[] codePoints = (int[]) value;
        Slice slice = Slices.allocate(codePoints.length * Integer.BYTES);
        slice.setInts(0, codePoints);
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(slice);
    }
}
