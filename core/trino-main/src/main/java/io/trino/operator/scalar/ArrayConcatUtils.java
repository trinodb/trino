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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;

public final class ArrayConcatUtils
{
    private ArrayConcatUtils() {}

    @UsedByGeneratedCode
    public static Block appendElement(Type elementType, Block block, long value)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, block.getPositionCount() + 1);
        ValueBlock valueBlock = block.getUnderlyingValueBlock();
        for (int i = 0; i < block.getPositionCount(); i++) {
            blockBuilder.append(valueBlock, block.getUnderlyingValuePosition(i));
        }

        elementType.writeLong(blockBuilder, value);

        return blockBuilder.build();
    }

    @UsedByGeneratedCode
    public static Block appendElement(Type elementType, Block block, boolean value)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, block.getPositionCount() + 1);
        ValueBlock valueBlock = block.getUnderlyingValueBlock();
        for (int i = 0; i < block.getPositionCount(); i++) {
            blockBuilder.append(valueBlock, block.getUnderlyingValuePosition(i));
        }

        elementType.writeBoolean(blockBuilder, value);

        return blockBuilder.build();
    }

    @UsedByGeneratedCode
    public static Block appendElement(Type elementType, Block block, double value)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, block.getPositionCount() + 1);
        ValueBlock valueBlock = block.getUnderlyingValueBlock();
        for (int i = 0; i < block.getPositionCount(); i++) {
            blockBuilder.append(valueBlock, block.getUnderlyingValuePosition(i));
        }

        elementType.writeDouble(blockBuilder, value);

        return blockBuilder.build();
    }

    @UsedByGeneratedCode
    public static Block appendElement(Type elementType, Block block, Slice value)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, block.getPositionCount() + 1);
        ValueBlock valueBlock = block.getUnderlyingValueBlock();
        for (int i = 0; i < block.getPositionCount(); i++) {
            blockBuilder.append(valueBlock, block.getUnderlyingValuePosition(i));
        }

        elementType.writeSlice(blockBuilder, value);

        return blockBuilder.build();
    }

    @UsedByGeneratedCode
    public static Block appendElement(Type elementType, Block block, Object value)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, block.getPositionCount() + 1);
        ValueBlock valueBlock = block.getUnderlyingValueBlock();
        for (int i = 0; i < block.getPositionCount(); i++) {
            blockBuilder.append(valueBlock, block.getUnderlyingValuePosition(i));
        }

        elementType.writeObject(blockBuilder, value);

        return blockBuilder.build();
    }

    // Usage of prependElement: ElementToArrayConcatFunction
    @UsedByGeneratedCode
    public static Block prependElement(Type elementType, Slice value, Block block)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, block.getPositionCount() + 1);

        elementType.writeSlice(blockBuilder, value);
        ValueBlock valueBlock = block.getUnderlyingValueBlock();
        for (int i = 0; i < block.getPositionCount(); i++) {
            blockBuilder.append(valueBlock, block.getUnderlyingValuePosition(i));
        }

        return blockBuilder.build();
    }

    @UsedByGeneratedCode
    public static Block prependElement(Type elementType, Object value, Block block)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, block.getPositionCount() + 1);

        elementType.writeObject(blockBuilder, value);
        ValueBlock valueBlock = block.getUnderlyingValueBlock();
        for (int i = 0; i < block.getPositionCount(); i++) {
            blockBuilder.append(valueBlock, block.getUnderlyingValuePosition(i));
        }

        return blockBuilder.build();
    }

    @UsedByGeneratedCode
    public static Block prependElement(Type elementType, long value, Block block)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, block.getPositionCount() + 1);

        elementType.writeLong(blockBuilder, value);
        ValueBlock valueBlock = block.getUnderlyingValueBlock();
        for (int i = 0; i < block.getPositionCount(); i++) {
            blockBuilder.append(valueBlock, block.getUnderlyingValuePosition(i));
        }

        return blockBuilder.build();
    }

    @UsedByGeneratedCode
    public static Block prependElement(Type elementType, boolean value, Block block)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, block.getPositionCount() + 1);

        elementType.writeBoolean(blockBuilder, value);
        ValueBlock valueBlock = block.getUnderlyingValueBlock();
        for (int i = 0; i < block.getPositionCount(); i++) {
            blockBuilder.append(valueBlock, block.getUnderlyingValuePosition(i));
        }

        return blockBuilder.build();
    }

    @UsedByGeneratedCode
    public static Block prependElement(Type elementType, double value, Block block)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, block.getPositionCount() + 1);

        elementType.writeDouble(blockBuilder, value);
        ValueBlock valueBlock = block.getUnderlyingValueBlock();
        for (int i = 0; i < block.getPositionCount(); i++) {
            blockBuilder.append(valueBlock, block.getUnderlyingValuePosition(i));
        }

        return blockBuilder.build();
    }
}
