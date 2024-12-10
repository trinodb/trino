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
package io.trino.spi.type;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.SqlMap;
import io.trino.spi.connector.ConnectorSession;

import static io.trino.spi.type.StandardTypes.MULTISET;

public class MultisetType
        extends AbstractType // TODO complete implementation needed to implement constant Values
{
    private final Type elementType;

    public MultisetType(Type elementType)
    {
        // TODO same as map type?
        super(new TypeSignature(MULTISET, TypeSignatureParameter.typeParameter(elementType.getTypeSignature())), SqlMap.class, MapBlock.class);
        this.elementType = elementType;
    }

    public Type getElementType()
    {
        return elementType;
    }

    @Override
    public String getDisplayName()
    {
        return MULTISET + "(" + elementType.getDisplayName() + ")";
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public int getFlatFixedSize()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public boolean isFlatVariableWidth()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public int getFlatVariableWidthSize(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public int getFlatVariableWidthLength(byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }
}
