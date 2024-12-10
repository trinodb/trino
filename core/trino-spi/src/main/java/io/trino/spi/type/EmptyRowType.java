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

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.connector.ConnectorSession;

import java.util.List;
import java.util.Optional;

public class EmptyRowType
        implements Type
{
    public static final EmptyRowType EMPTY_ROW = new EmptyRowType();

    private final TypeSignature signature = new TypeSignature(StandardTypes.EMPTY_ROW);

    private EmptyRowType() {}

    @Override
    public TypeSignature getTypeSignature()
    {
        return signature;
    }

    @Override
    public String getDisplayName()
    {
        return signature.toString();
    }

    @Override
    public boolean isComparable()
    {
        // TODO this type should be comparable. For now, returning false to enable representing constant EmptyRowType value as NullableValue
        return false;
    }

    @Override
    public boolean isOrderable()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public Class<?> getJavaType()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public Class<? extends ValueBlock> getValueBlockType()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return List.of();
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
    public boolean getBoolean(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public long getLong(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public double getDouble(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public Object getObject(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeBoolean(BlockBuilder blockBuilder, boolean value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeDouble(BlockBuilder blockBuilder, double value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public Optional<Object> getNextValue(Object value)
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

    @Override
    public String toString()
    {
        return getDisplayName();
    }
}
