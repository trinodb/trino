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

/**
 * FixedWidthType is a type that has a fixed size for every value.
 */
public interface FixedWidthType
        extends Type
{
    /**
     * Gets the size of a value of this type in bytes. All values
     * of a FixedWidthType are the same size.
     */
    int getFixedSize();

    @Override
    default BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries);
    }

    /**
     * Creates a block builder for this type sized to hold the specified number
     * of positions.
     */
    BlockBuilder createFixedSizeBlockBuilder(int positionCount);

    @Override
    default boolean isFlatVariableWidth()
    {
        return false;
    }

    @Override
    default int getFlatVariableWidthSize(Block block, int position)
    {
        return 0;
    }

    @Override
    default int getFlatVariableWidthLength(byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        return 0;
    }
}
