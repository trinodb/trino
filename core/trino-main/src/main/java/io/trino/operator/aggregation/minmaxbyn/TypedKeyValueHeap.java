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
package io.trino.operator.aggregation.minmaxbyn;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class TypedKeyValueHeap
{
    private static final int INSTANCE_SIZE = instanceSize(TypedKeyValueHeap.class);

    private static final int COMPACT_THRESHOLD_BYTES = 32768;
    private static final int COMPACT_THRESHOLD_RATIO = 3; // when 2/3 of elements in keyBlockBuilder is unreferenced, do compact

    private final boolean min;
    private final MethodHandle compare;
    private final Type keyType;
    private final Type valueType;
    private final int capacity;

    private int positionCount;
    private final int[] heapIndex;
    private BlockBuilder keyBlockBuilder;
    private BlockBuilder valueBlockBuilder;

    public TypedKeyValueHeap(boolean min, MethodHandle compare, Type keyType, Type valueType, int capacity)
    {
        this.min = min;
        this.compare = requireNonNull(compare, "compare is null");
        this.keyType = requireNonNull(keyType, "keyType is null");
        this.valueType = requireNonNull(valueType, "valueType is null");
        this.capacity = capacity;
        this.heapIndex = new int[capacity];
        this.keyBlockBuilder = keyType.createBlockBuilder(null, capacity);
        this.valueBlockBuilder = valueType.createBlockBuilder(null, capacity);
    }

    // for copying
    private TypedKeyValueHeap(boolean min, MethodHandle compare, Type keyType, Type valueType, int capacity, int positionCount, int[] heapIndex, BlockBuilder keyBlockBuilder, BlockBuilder valueBlockBuilder)
    {
        this.min = min;
        this.compare = requireNonNull(compare, "compare is null");
        this.keyType = requireNonNull(keyType, "keyType is null");
        this.valueType = requireNonNull(valueType, "valueType is null");
        this.capacity = capacity;
        this.positionCount = positionCount;
        this.heapIndex = heapIndex;
        this.keyBlockBuilder = keyBlockBuilder;
        this.valueBlockBuilder = valueBlockBuilder;
    }

    public static Type getSerializedType(Type keyType, Type valueType)
    {
        return RowType.anonymous(ImmutableList.of(BIGINT, new ArrayType(keyType), new ArrayType(valueType)));
    }

    public int getCapacity()
    {
        return capacity;
    }

    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + keyBlockBuilder.getRetainedSizeInBytes() + valueBlockBuilder.getRetainedSizeInBytes() + sizeOf(heapIndex);
    }

    public boolean isEmpty()
    {
        return positionCount == 0;
    }

    public void serialize(BlockBuilder out)
    {
        BlockBuilder blockBuilder = out.beginBlockEntry();
        BIGINT.writeLong(blockBuilder, getCapacity());

        BlockBuilder keyElements = blockBuilder.beginBlockEntry();
        for (int i = 0; i < positionCount; i++) {
            keyType.appendTo(keyBlockBuilder, heapIndex[i], keyElements);
        }
        blockBuilder.closeEntry();

        BlockBuilder valueElements = blockBuilder.beginBlockEntry();
        for (int i = 0; i < positionCount; i++) {
            valueType.appendTo(valueBlockBuilder, heapIndex[i], valueElements);
        }
        blockBuilder.closeEntry();

        out.closeEntry();
    }

    public static TypedKeyValueHeap deserialize(boolean min, MethodHandle compare, Type keyType, Type valueType, Block rowBlock)
    {
        int capacity = toIntExact(BIGINT.getLong(rowBlock, 0));
        int[] heapIndex = new int[capacity];

        BlockBuilder keyBlockBuilder = keyType.createBlockBuilder(null, capacity);
        Block keyBlock = new ArrayType(keyType).getObject(rowBlock, 1);
        for (int position = 0; position < keyBlock.getPositionCount(); position++) {
            heapIndex[position] = position;
            keyType.appendTo(keyBlock, position, keyBlockBuilder);
        }

        BlockBuilder valueBlockBuilder = valueType.createBlockBuilder(null, capacity);
        Block valueBlock = new ArrayType(valueType).getObject(rowBlock, 2);
        for (int position = 0; position < valueBlock.getPositionCount(); position++) {
            heapIndex[position] = position;
            if (valueBlock.isNull(position)) {
                valueBlockBuilder.appendNull();
            }
            else {
                valueType.appendTo(valueBlock, position, valueBlockBuilder);
            }
        }

        return new TypedKeyValueHeap(min, compare, keyType, valueType, capacity, keyBlock.getPositionCount(), heapIndex, keyBlockBuilder, valueBlockBuilder);
    }

    public void popAllReverse(BlockBuilder resultBlockBuilder)
    {
        int[] indexes = new int[positionCount];
        while (positionCount > 0) {
            indexes[positionCount - 1] = heapIndex[0];
            positionCount--;
            heapIndex[0] = heapIndex[positionCount];
            siftDown();
        }

        for (int index : indexes) {
            valueType.appendTo(valueBlockBuilder, index, resultBlockBuilder);
        }
    }

    public void popAll(BlockBuilder resultBlockBuilder)
    {
        while (positionCount > 0) {
            pop(resultBlockBuilder);
        }
    }

    public void pop(BlockBuilder resultBlockBuilder)
    {
        valueType.appendTo(valueBlockBuilder, heapIndex[0], resultBlockBuilder);
        remove();
    }

    private void remove()
    {
        positionCount--;
        heapIndex[0] = heapIndex[positionCount];
        siftDown();
    }

    public void add(Block keyBlock, Block valueBlock, int position)
    {
        checkArgument(!keyBlock.isNull(position));
        if (positionCount == capacity) {
            if (keyGreaterThanOrEqual(keyBlockBuilder, heapIndex[0], keyBlock, position)) {
                return; // and new element is not larger than heap top: do not add
            }
            heapIndex[0] = keyBlockBuilder.getPositionCount();
            keyType.appendTo(keyBlock, position, keyBlockBuilder);
            valueType.appendTo(valueBlock, position, valueBlockBuilder);
            siftDown();
        }
        else {
            heapIndex[positionCount] = keyBlockBuilder.getPositionCount();
            positionCount++;
            keyType.appendTo(keyBlock, position, keyBlockBuilder);
            valueType.appendTo(valueBlock, position, valueBlockBuilder);
            siftUp();
        }
        compactIfNecessary();
    }

    public void addAll(TypedKeyValueHeap otherHeap)
    {
        addAll(otherHeap.keyBlockBuilder, otherHeap.valueBlockBuilder);
    }

    public void addAll(Block keysBlock, Block valuesBlock)
    {
        for (int i = 0; i < keysBlock.getPositionCount(); i++) {
            add(keysBlock, valuesBlock, i);
        }
    }

    private void siftDown()
    {
        int position = 0;
        while (true) {
            int leftPosition = position * 2 + 1;
            if (leftPosition >= positionCount) {
                break;
            }
            int rightPosition = leftPosition + 1;
            int smallerChildPosition;
            if (rightPosition >= positionCount) {
                smallerChildPosition = leftPosition;
            }
            else {
                smallerChildPosition = keyGreaterThanOrEqual(keyBlockBuilder, heapIndex[leftPosition], keyBlockBuilder, heapIndex[rightPosition]) ? rightPosition : leftPosition;
            }
            if (keyGreaterThanOrEqual(keyBlockBuilder, heapIndex[smallerChildPosition], keyBlockBuilder, heapIndex[position])) {
                break; // child is larger or equal
            }
            int swapTemp = heapIndex[position];
            heapIndex[position] = heapIndex[smallerChildPosition];
            heapIndex[smallerChildPosition] = swapTemp;
            position = smallerChildPosition;
        }
    }

    private void siftUp()
    {
        int position = positionCount - 1;
        while (position != 0) {
            int parentPosition = (position - 1) / 2;
            if (keyGreaterThanOrEqual(keyBlockBuilder, heapIndex[position], keyBlockBuilder, heapIndex[parentPosition])) {
                break; // child is larger or equal
            }
            int swapTemp = heapIndex[position];
            heapIndex[position] = heapIndex[parentPosition];
            heapIndex[parentPosition] = swapTemp;
            position = parentPosition;
        }
    }

    private void compactIfNecessary()
    {
        // Byte size check is needed. Otherwise, if size * 3 is small, BlockBuilder can be reallocate too often.
        // Position count is needed. Otherwise, for large elements, heap will be compacted every time.
        // Size instead of retained size is needed because default allocation size can be huge for some block builders. And the first check will become useless in such case.
        if (keyBlockBuilder.getSizeInBytes() < COMPACT_THRESHOLD_BYTES || keyBlockBuilder.getPositionCount() / positionCount < COMPACT_THRESHOLD_RATIO) {
            return;
        }
        BlockBuilder newHeapKeyBlockBuilder = keyType.createBlockBuilder(null, keyBlockBuilder.getPositionCount());
        BlockBuilder newHeapValueBlockBuilder = valueType.createBlockBuilder(null, valueBlockBuilder.getPositionCount());
        for (int i = 0; i < positionCount; i++) {
            keyType.appendTo(keyBlockBuilder, heapIndex[i], newHeapKeyBlockBuilder);
            valueType.appendTo(valueBlockBuilder, heapIndex[i], newHeapValueBlockBuilder);
            heapIndex[i] = i;
        }
        keyBlockBuilder = newHeapKeyBlockBuilder;
        valueBlockBuilder = newHeapValueBlockBuilder;
    }

    private boolean keyGreaterThanOrEqual(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        try {
            long result = (long) compare.invokeExact(leftBlock, leftPosition, rightBlock, rightPosition);
            return min ? result <= 0 : result >= 0;
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    public TypedKeyValueHeap copy()
    {
        BlockBuilder keyBlockBuilderCopy = null;
        if (keyBlockBuilder != null) {
            keyBlockBuilderCopy = (BlockBuilder) keyBlockBuilder.copyRegion(0, keyBlockBuilder.getPositionCount());
        }
        BlockBuilder valueBlockBuilderCopy = null;
        if (valueBlockBuilder != null) {
            valueBlockBuilderCopy = (BlockBuilder) valueBlockBuilder.copyRegion(0, valueBlockBuilder.getPositionCount());
        }
        return new TypedKeyValueHeap(
                min,
                compare,
                keyType,
                valueType,
                capacity,
                positionCount,
                heapIndex.clone(),
                keyBlockBuilderCopy,
                valueBlockBuilderCopy);
    }
}
