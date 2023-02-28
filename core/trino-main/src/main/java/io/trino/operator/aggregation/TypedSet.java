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
package io.trino.operator.aggregation;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.units.DataSize;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.StandardErrorCode.EXCEEDED_FUNCTION_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * A set of unique SQL values stored in a {@link Block}.
 *
 * <p>Depending on the factory method used, the values' equality may be
 * determined using SQL equality or {@code IS DISTINCT FROM} semantics.
 */
public class TypedSet
{
    @VisibleForTesting
    public static final DataSize MAX_FUNCTION_MEMORY = DataSize.of(4, MEGABYTE);

    private static final int INSTANCE_SIZE = instanceSize(TypedSet.class);
    private static final int INT_ARRAY_LIST_INSTANCE_SIZE = instanceSize(IntArrayList.class);
    private static final float FILL_RATIO = 0.75f;

    private final Type elementType;
    private final BlockPositionEqual elementEqualOperator;
    private final BlockPositionIsDistinctFrom elementDistinctFromOperator;
    private final BlockPositionHashCode elementHashCodeOperator;
    private final IntArrayList blockPositionByHash;
    private final BlockBuilder elementBlock;
    private final String functionName;
    private final long maxBlockMemoryInBytes;

    private final int initialElementBlockOffset;
    private final long initialElementBlockSizeInBytes;
    // The number of elements added to the TypedSet (including null). Should
    // always be equal to elementBlock.getPositionsCount() - initialElementBlockOffset.
    private int size;
    private int hashCapacity;
    private int maxFill;
    private int hashMask;
    private static final int EMPTY_SLOT = -1;

    private boolean containsNullElement;

    /**
     * Create a {@code TypedSet} that compares its elements using SQL equality
     * comparison.
     */
    public static TypedSet createEqualityTypedSet(
            Type elementType,
            BlockPositionEqual elementEqualOperator,
            BlockPositionHashCode elementHashCodeOperator,
            int expectedSize,
            String functionName)
    {
        return createEqualityTypedSet(
                elementType,
                elementEqualOperator,
                elementHashCodeOperator,
                elementType.createBlockBuilder(null, expectedSize),
                expectedSize,
                functionName);
    }

    /**
     * Create a {@code TypedSet} that compares its elements using SQL equality
     * comparison.
     *
     * <p>The elements of the set will be written in the given {@code BlockBuilder}.
     * If the {@code BlockBuilder} is modified by the caller, the set will stop
     * functioning correctly.
     */
    public static TypedSet createEqualityTypedSet(
            Type elementType,
            BlockPositionEqual elementEqualOperator,
            BlockPositionHashCode elementHashCodeOperator,
            BlockBuilder elementBlock,
            int expectedSize,
            String functionName)
    {
        return new TypedSet(
                elementType,
                elementEqualOperator,
                null,
                elementHashCodeOperator,
                elementBlock,
                expectedSize,
                functionName,
                false);
    }

    /**
     * Create a {@code TypedSet} with no size limit that compares its elements
     * using SQL equality comparison.
     *
     * <p>The elements of the set will be written in the given {@code BlockBuilder}.
     * If the {@code BlockBuilder} is modified by the caller, the set will stop
     * functioning correctly.
     */
    public static TypedSet createUnboundedEqualityTypedSet(
            Type elementType,
            BlockPositionEqual elementEqualOperator,
            BlockPositionHashCode elementHashCodeOperator,
            BlockBuilder elementBlock,
            int expectedSize,
            String functionName)
    {
        return new TypedSet(
                elementType,
                elementEqualOperator,
                null,
                elementHashCodeOperator,
                elementBlock,
                expectedSize,
                functionName,
                true);
    }

    /**
     * Create a {@code TypedSet} that compares its elements using the semantics
     * of {@code IS DISTINCT}.
     */
    public static TypedSet createDistinctTypedSet(
            Type elementType,
            BlockPositionIsDistinctFrom elementDistinctFromOperator,
            BlockPositionHashCode elementHashCodeOperator,
            int expectedSize,
            String functionName)
    {
        return createDistinctTypedSet(
                elementType,
                elementDistinctFromOperator,
                elementHashCodeOperator,
                elementType.createBlockBuilder(null, expectedSize),
                expectedSize,
                functionName);
    }

    /**
     * Create a {@code TypedSet} that compares its elements using the semantics
     * of {@code IS DISTINCT}.
     *
     * <p>The elements of the set will be written in the given {@code BlockBuilder}.
     * If the {@code BlockBuilder} is modified by the caller, the set will stop
     * functioning correctly.
     */
    public static TypedSet createDistinctTypedSet(
            Type elementType,
            BlockPositionIsDistinctFrom elementDistinctFromOperator,
            BlockPositionHashCode elementHashCodeOperator,
            BlockBuilder elementBlock,
            int expectedSize,
            String functionName)
    {
        return new TypedSet(
                elementType,
                null,
                elementDistinctFromOperator,
                elementHashCodeOperator,
                elementBlock,
                expectedSize,
                functionName,
                false);
    }

    private TypedSet(
            Type elementType,
            BlockPositionEqual elementEqualOperator,
            BlockPositionIsDistinctFrom elementDistinctFromOperator,
            BlockPositionHashCode elementHashCodeOperator,
            BlockBuilder elementBlock,
            int expectedSize,
            String functionName,
            boolean unboundedMemory)
    {
        checkArgument(expectedSize >= 0, "expectedSize must not be negative");
        this.elementType = requireNonNull(elementType, "elementType is null");

        checkArgument(elementEqualOperator == null ^ elementDistinctFromOperator == null, "Element equal or distinct_from operator must be provided");
        this.elementEqualOperator = elementEqualOperator;
        this.elementDistinctFromOperator = elementDistinctFromOperator;
        this.elementHashCodeOperator = requireNonNull(elementHashCodeOperator, "elementHashCodeOperator is null");

        this.elementBlock = requireNonNull(elementBlock, "elementBlock must not be null");
        this.functionName = functionName;
        this.maxBlockMemoryInBytes = unboundedMemory ? Long.MAX_VALUE : MAX_FUNCTION_MEMORY.toBytes();

        initialElementBlockOffset = elementBlock.getPositionCount();
        initialElementBlockSizeInBytes = elementBlock.getSizeInBytes();

        this.size = 0;
        this.hashCapacity = arraySize(expectedSize, FILL_RATIO);
        this.maxFill = calculateMaxFill(hashCapacity);
        this.hashMask = hashCapacity - 1;

        blockPositionByHash = new IntArrayList(hashCapacity);
        blockPositionByHash.size(hashCapacity);
        for (int i = 0; i < hashCapacity; i++) {
            blockPositionByHash.set(i, EMPTY_SLOT);
        }

        this.containsNullElement = false;
    }

    /**
     * Returns the retained size of this block in memory, including over-allocations.
     * This method is called from the innermost execution loop and must be fast.
     */
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + INT_ARRAY_LIST_INSTANCE_SIZE
                + elementBlock.getRetainedSizeInBytes()
                + blockPositionByHash.size() * (long) Integer.BYTES;
    }

    /**
     * Return whether this set contains the value at the given {@code position}
     * in the given {@code block}.
     */
    public boolean contains(Block block, int position)
    {
        requireNonNull(block, "block must not be null");
        checkArgument(position >= 0, "position must be >= 0");

        if (block.isNull(position)) {
            return containsNullElement;
        }
        return blockPositionByHash.getInt(getHashPositionOfElement(block, position)) != EMPTY_SLOT;
    }

    /**
     * Add the value at the given {@code position} in the given {@code block}
     * to this set.
     *
     * @return {@code true} if the value was added, or {@code false} if it was
     * already in this set.
     */
    public boolean add(Block block, int position)
    {
        requireNonNull(block, "block must not be null");
        checkArgument(position >= 0, "position must be >= 0");

        // containsNullElement flag is maintained so contains() method can have shortcut for null value
        if (block.isNull(position)) {
            if (containsNullElement) {
                return false;
            }
            containsNullElement = true;
        }

        int hashPosition = getHashPositionOfElement(block, position);
        if (blockPositionByHash.getInt(hashPosition) == EMPTY_SLOT) {
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
     * Return the position in this set's {@code BlockBuilder} of the value at
     * the given {@code position} in the given {@code block}, or -1 if the
     * value is not in this set.
     */
    public int positionOf(Block block, int position)
    {
        return blockPositionByHash.getInt(getHashPositionOfElement(block, position));
    }

    /**
     * Get slot position of element at {@code position} of {@code block}
     */
    private int getHashPositionOfElement(Block block, int position)
    {
        int hashPosition = getMaskedHash(elementHashCodeOperator.hashCodeNullSafe(block, position));
        while (true) {
            int blockPosition = blockPositionByHash.getInt(hashPosition);
            if (blockPosition == EMPTY_SLOT) {
                // Doesn't have this element
                return hashPosition;
            }
            if (isNotDistinct(elementBlock, blockPosition, block, position)) {
                // Already has this element
                return hashPosition;
            }

            hashPosition = getMaskedHash(hashPosition + 1);
        }
    }

    private boolean isNotDistinct(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        if (elementDistinctFromOperator != null) {
            return !elementDistinctFromOperator.isDistinctFrom(leftBlock, leftPosition, rightBlock, rightPosition);
        }
        return elementEqualOperator.equalNullSafe(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    private void addNewElement(int hashPosition, Block block, int position)
    {
        elementType.appendTo(block, position, elementBlock);
        if (elementBlock.getSizeInBytes() - initialElementBlockSizeInBytes > maxBlockMemoryInBytes) {
            throw new TrinoException(
                    EXCEEDED_FUNCTION_MEMORY_LIMIT,
                    format("The input to %s is too large. More than %s of memory is needed to hold the intermediate hash set.\n",
                            functionName,
                            MAX_FUNCTION_MEMORY));
        }
        blockPositionByHash.set(hashPosition, elementBlock.getPositionCount() - 1);

        // increase capacity, if necessary
        size++;
        if (size >= maxFill) {
            rehash();
        }
    }

    private void rehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = (int) newCapacityLong;

        hashCapacity = newCapacity;
        hashMask = newCapacity - 1;
        maxFill = calculateMaxFill(newCapacity);
        blockPositionByHash.size(newCapacity);
        for (int i = 0; i < newCapacity; i++) {
            blockPositionByHash.set(i, EMPTY_SLOT);
        }

        for (int blockPosition = initialElementBlockOffset; blockPosition < elementBlock.getPositionCount(); blockPosition++) {
            blockPositionByHash.set(getHashPositionOfElement(elementBlock, blockPosition), blockPosition);
        }
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    private int getMaskedHash(long rawHash)
    {
        return (int) (rawHash & hashMask);
    }
}
