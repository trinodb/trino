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

import io.airlift.units.DataSize;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.StandardErrorCode.EXCEEDED_FUNCTION_MEMORY_LIMIT;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * A set of values stored in preexisting blocks. The data is not copied out of the
 * blocks, and instead a direct reference is kept. This means that all data in a block
 * is retained (including non-distinct values), so this works best when processing
 * preexisting blocks in a single code block. Care should be taken when using this
 * across multiple calls, as the memory will not be freed until the BlockSet is freed.
 * <p>
 * BlockSet does not support rehashing, so the maximum size must be known up front.
 */
public class BlockSet
{
    public static final DataSize MAX_FUNCTION_MEMORY = DataSize.of(4, MEGABYTE);

    private static final float FILL_RATIO = 0.75f;
    private static final int EMPTY_SLOT = -1;

    private final Type elementType;
    private final BlockPositionIsDistinctFrom elementDistinctFromOperator;
    private final BlockPositionHashCode elementHashCodeOperator;

    private final int[] blockPositionByHash;

    private final Block[] elementBlocks;
    private final int[] elementPositions;

    private int size;

    private final int maximumSize;
    private final int hashMask;

    private boolean containsNullElement;

    public BlockSet(
            Type elementType,
            BlockPositionIsDistinctFrom elementDistinctFromOperator,
            BlockPositionHashCode elementHashCodeOperator,
            int maximumSize)
    {
        checkArgument(maximumSize >= 0, "maximumSize must not be negative");
        this.elementType = requireNonNull(elementType, "elementType is null");
        this.elementDistinctFromOperator = requireNonNull(elementDistinctFromOperator, "elementDistinctFromOperator is null");
        this.elementHashCodeOperator = requireNonNull(elementHashCodeOperator, "elementHashCodeOperator is null");
        this.maximumSize = maximumSize;

        int hashCapacity = arraySize(maximumSize, FILL_RATIO);
        this.hashMask = hashCapacity - 1;

        blockPositionByHash = new int[hashCapacity];
        Arrays.fill(blockPositionByHash, EMPTY_SLOT);

        this.elementBlocks = new Block[maximumSize];
        this.elementPositions = new int[maximumSize];

        this.containsNullElement = false;
    }

    /**
     * Does this set contain the value?
     */
    public boolean contains(Block block, int position)
    {
        requireNonNull(block, "block must not be null");
        checkArgument(position >= 0, "position must be >= 0");

        if (block.isNull(position)) {
            return containsNullElement;
        }
        return positionOf(block, position) != EMPTY_SLOT;
    }

    /**
     * Add the value to this set.
     *
     * @return {@code true} if the value was added, or {@code false} if it was
     * already in this set.
     */
    public boolean add(Block block, int position)
    {
        requireNonNull(block, "block must not be null");
        checkArgument(position >= 0, "position must be >= 0");

        // containsNullElement flag is maintained so contains() method can have a shortcut for null value
        if (block.isNull(position)) {
            if (containsNullElement) {
                return false;
            }
            containsNullElement = true;
        }

        int hashPosition = getHashPositionOfElement(block, position);
        if (blockPositionByHash[hashPosition] == EMPTY_SLOT) {
            addNewElement(hashPosition, block, position);
            return true;
        }
        return false;
    }

    /**
     * Returns the number of elements in this set.
     */
    public int size()
    {
        return size;
    }

    /**
     * Return the position of the value within this set, or -1 if the value is not in this set.
     * This method can not get the position of a null value, and an exception will be thrown in that case.
     *
     * @throws IllegalArgumentException if the position is null
     */
    public int positionOf(Block block, int position)
    {
        return blockPositionByHash[getHashPositionOfElement(block, position)];
    }

    /**
     * Writes all values to the block builder checking the memory limit after each element is added.
     */
    public void getAllWithSizeLimit(BlockBuilder blockBuilder, String functionName, DataSize maxFunctionMemory)
    {
        long initialSize = blockBuilder.getSizeInBytes();
        long maxBlockMemoryInBytes = toIntExact(maxFunctionMemory.toBytes());
        for (int i = 0; i < size; i++) {
            elementType.appendTo(elementBlocks[i], elementPositions[i], blockBuilder);
            if (blockBuilder.getSizeInBytes() - initialSize > maxBlockMemoryInBytes) {
                throw new TrinoException(
                        EXCEEDED_FUNCTION_MEMORY_LIMIT,
                        "The input to %s is too large. More than %s of memory is needed to hold the output hash set.".formatted(functionName, maxFunctionMemory));
            }
        }
    }

    /**
     * Get hash slot position of the element. If the element is not in the set, return the position
     * where the element should be inserted.
     */
    private int getHashPositionOfElement(Block block, int position)
    {
        int hashPosition = getMaskedHash(elementHashCodeOperator.hashCodeNullSafe(block, position));
        while (true) {
            int blockPosition = blockPositionByHash[hashPosition];
            if (blockPosition == EMPTY_SLOT) {
                // Doesn't have this element
                return hashPosition;
            }
            if (isNotDistinct(blockPosition, block, position)) {
                // Already has this element
                return hashPosition;
            }

            hashPosition = getMaskedHash(hashPosition + 1);
        }
    }

    private void addNewElement(int hashPosition, Block block, int position)
    {
        checkState(size < maximumSize, "BlockSet is full");

        elementBlocks[size] = block;
        elementPositions[size] = position;

        blockPositionByHash[hashPosition] = size;
        size++;
    }

    private boolean isNotDistinct(int leftPosition, Block rightBlock, int rightPosition)
    {
        return !elementDistinctFromOperator.isDistinctFrom(
                elementBlocks[leftPosition],
                elementPositions[leftPosition],
                rightBlock,
                rightPosition);
    }

    private int getMaskedHash(long rawHash)
    {
        return (int) (rawHash & hashMask);
    }
}
