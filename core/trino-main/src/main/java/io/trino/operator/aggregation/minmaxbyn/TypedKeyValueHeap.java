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
import io.airlift.slice.SizeOf;
import io.trino.operator.VariableWidthData;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrays;
import jakarta.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.operator.VariableWidthData.EMPTY_CHUNK;
import static io.trino.operator.VariableWidthData.POINTER_SIZE;
import static io.trino.operator.VariableWidthData.getChunkOffset;
import static io.trino.operator.aggregation.minmaxn.TypedHeap.compactIfNecessary;
import static java.util.Objects.requireNonNull;

public final class TypedKeyValueHeap
{
    private static final int INSTANCE_SIZE = instanceSize(TypedKeyValueHeap.class);

    private final boolean min;
    private final MethodHandle keyReadFlat;
    private final MethodHandle keyWriteFlat;
    private final MethodHandle valueReadFlat;
    private final MethodHandle valueWriteFlat;
    private final MethodHandle compareFlatFlat;
    private final MethodHandle compareFlatBlock;
    private final Type keyType;
    private final Type valueType;
    private final int capacity;

    private final int recordKeyOffset;
    private final int recordValueOffset;

    private final int recordSize;
    /**
     * The fixed chunk contains an array of records. The records are laid out as follows:
     * <ul>
     *     <li>12 byte optional pointer to variable width data (only present if the key or value is variable width)</li>
     *     <li>4 byte optional integer for variable size of the key (only present if the key is variable width)</li>
     *     <li>1 byte null flag for the value</li>
     *     <li>N byte fixed size data for the key type</li>
     *     <li>N byte fixed size data for the value type</li>
     * </ul>
     * The pointer is placed first to simplify the offset calculations for variable with code.
     * This chunk contains {@code capacity + 1} records. The extra record is used for the swap operation.
     */
    private final byte[] fixedChunk;

    private final boolean keyVariableWidth;
    private final boolean valueVariableWidth;
    private VariableWidthData variableWidthData;

    private int positionCount;

    public TypedKeyValueHeap(
            boolean min,
            MethodHandle keyReadFlat,
            MethodHandle keyWriteFlat,
            MethodHandle valueReadFlat,
            MethodHandle valueWriteFlat,
            MethodHandle compareFlatFlat,
            MethodHandle compareFlatBlock,
            Type keyType,
            Type valueType,
            int capacity)
    {
        this.min = min;
        this.keyReadFlat = requireNonNull(keyReadFlat, "keyReadFlat is null");
        this.keyWriteFlat = requireNonNull(keyWriteFlat, "keyWriteFlat is null");
        this.valueReadFlat = requireNonNull(valueReadFlat, "valueReadFlat is null");
        this.valueWriteFlat = requireNonNull(valueWriteFlat, "valueWriteFlat is null");
        this.compareFlatFlat = requireNonNull(compareFlatFlat, "compareFlatFlat is null");
        this.compareFlatBlock = requireNonNull(compareFlatBlock, "compareFlatBlock is null");
        this.keyType = requireNonNull(keyType, "keyType is null");
        this.valueType = requireNonNull(valueType, "valueType is null");
        this.capacity = capacity;

        keyVariableWidth = keyType.isFlatVariableWidth();
        valueVariableWidth = valueType.isFlatVariableWidth();

        boolean variableWidth = keyVariableWidth || valueVariableWidth;
        variableWidthData = variableWidth ? new VariableWidthData() : null;

        recordKeyOffset = (variableWidth ? POINTER_SIZE : 0) + 1;
        recordValueOffset = recordKeyOffset + keyType.getFlatFixedSize();
        recordSize = recordValueOffset + valueType.getFlatFixedSize();

        // allocate the fixed chunk with on extra slow for use in swap
        fixedChunk = new byte[recordSize * (capacity + 1)];
    }

    public TypedKeyValueHeap(TypedKeyValueHeap typedHeap)
    {
        this.min = typedHeap.min;
        this.keyReadFlat = typedHeap.keyReadFlat;
        this.keyWriteFlat = typedHeap.keyWriteFlat;
        this.valueReadFlat = typedHeap.valueReadFlat;
        this.valueWriteFlat = typedHeap.valueWriteFlat;
        this.compareFlatFlat = typedHeap.compareFlatFlat;
        this.compareFlatBlock = typedHeap.compareFlatBlock;
        this.keyType = typedHeap.keyType;
        this.valueType = typedHeap.valueType;
        this.capacity = typedHeap.capacity;
        this.positionCount = typedHeap.positionCount;

        this.keyVariableWidth = typedHeap.keyVariableWidth;
        this.valueVariableWidth = typedHeap.valueVariableWidth;

        this.recordKeyOffset = typedHeap.recordKeyOffset;
        this.recordValueOffset = typedHeap.recordValueOffset;
        this.recordSize = typedHeap.recordSize;
        this.fixedChunk = Arrays.copyOf(typedHeap.fixedChunk, typedHeap.fixedChunk.length);

        if (typedHeap.variableWidthData != null) {
            this.variableWidthData = new VariableWidthData(typedHeap.variableWidthData);
        }
        else {
            this.variableWidthData = null;
        }
    }

    public Type getKeyType()
    {
        return keyType;
    }

    public Type getValueType()
    {
        return valueType;
    }

    public int getCapacity()
    {
        return capacity;
    }

    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                SizeOf.sizeOf(fixedChunk) +
                (variableWidthData == null ? 0 : variableWidthData.getRetainedSizeBytes());
    }

    public boolean isEmpty()
    {
        return positionCount == 0;
    }

    public void writeAllUnsorted(BlockBuilder keyBuilder, BlockBuilder valueBuilder)
    {
        for (int i = 0; i < positionCount; i++) {
            write(i, keyBuilder, valueBuilder);
        }
    }

    public void writeValuesSorted(BlockBuilder valueBlockBuilder)
    {
        // fully sort the heap
        int[] indexes = new int[positionCount];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = i;
        }
        IntArrays.quickSort(indexes, (a, b) -> compare(a, b));

        for (int index : indexes) {
            write(index, null, valueBlockBuilder);
        }
    }

    private void write(int index, @Nullable BlockBuilder keyBlockBuilder, BlockBuilder valueBlockBuilder)
    {
        int recordOffset = getRecordOffset(index);

        byte[] variableWidthChunk = EMPTY_CHUNK;
        if (variableWidthData != null) {
            variableWidthChunk = variableWidthData.getChunk(fixedChunk, recordOffset);
        }

        if (keyBlockBuilder != null) {
            try {
                keyReadFlat.invokeExact(
                        fixedChunk,
                        recordOffset + recordKeyOffset,
                        variableWidthChunk,
                        keyBlockBuilder);
            }
            catch (Throwable throwable) {
                Throwables.throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
        }
        if (fixedChunk[recordOffset + recordKeyOffset - 1] != 0) {
            valueBlockBuilder.appendNull();
        }
        else {
            try {
                valueReadFlat.invokeExact(
                        fixedChunk,
                        recordOffset + recordValueOffset,
                        variableWidthChunk,
                        valueBlockBuilder);
            }
            catch (Throwable throwable) {
                Throwables.throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
        }
    }

    public void addAll(Block keyBlock, Block valueBlock)
    {
        for (int i = 0; i < keyBlock.getPositionCount(); i++) {
            add(keyBlock, valueBlock, i);
        }
    }

    public void add(Block keyBlock, Block valueBlock, int position)
    {
        checkArgument(!keyBlock.isNull(position));
        if (positionCount == capacity) {
            // is it possible the value is within the top N values?
            if (!shouldConsiderValue(keyBlock, position)) {
                return;
            }
            clear(0);
            set(0, keyBlock, valueBlock, position);
            siftDown();
        }
        else {
            set(positionCount, keyBlock, valueBlock, position);
            positionCount++;
            siftUp();
        }
    }

    private void clear(int index)
    {
        if (variableWidthData == null) {
            return;
        }

        variableWidthData.free(fixedChunk, getRecordOffset(index));
        variableWidthData = compactIfNecessary(
                variableWidthData,
                fixedChunk,
                recordSize,
                0,
                positionCount,
                (fixedSizeOffset, variableWidthChunk, variableWidthChunkOffset) -> {
                    int keyVariableWidth = keyType.relocateFlatVariableWidthOffsets(fixedChunk, fixedSizeOffset + recordKeyOffset, variableWidthChunk, variableWidthChunkOffset);
                    if (fixedChunk[fixedSizeOffset + recordKeyOffset - 1] != 0) {
                        valueType.relocateFlatVariableWidthOffsets(fixedChunk, fixedSizeOffset + recordValueOffset, variableWidthChunk, variableWidthChunkOffset + keyVariableWidth);
                    }
                });
    }

    private void set(int index, Block keyBlock, Block valueBlock, int position)
    {
        int recordOffset = getRecordOffset(index);

        byte[] variableWidthChunk = EMPTY_CHUNK;
        int variableWidthChunkOffset = 0;
        int keyVariableWidthLength = 0;
        if (variableWidthData != null) {
            if (keyVariableWidth) {
                keyVariableWidthLength = keyType.getFlatVariableWidthSize(keyBlock, position);
            }
            int valueVariableWidthLength = valueType.getFlatVariableWidthSize(valueBlock, position);
            variableWidthChunk = variableWidthData.allocate(fixedChunk, recordOffset, keyVariableWidthLength + valueVariableWidthLength);
            variableWidthChunkOffset = getChunkOffset(fixedChunk, recordOffset);
        }

        try {
            keyWriteFlat.invokeExact(keyBlock, position, fixedChunk, recordOffset + recordKeyOffset, variableWidthChunk, variableWidthChunkOffset);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
        if (valueBlock.isNull(position)) {
            fixedChunk[recordOffset + recordKeyOffset - 1] = 1;
        }
        else {
            try {
                valueWriteFlat.invokeExact(
                        valueBlock,
                        position,
                        fixedChunk,
                        recordOffset + recordValueOffset,
                        variableWidthChunk,
                        variableWidthChunkOffset + keyVariableWidthLength);
            }
            catch (Throwable throwable) {
                Throwables.throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
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
                smallerChildPosition = compare(leftPosition, rightPosition) < 0 ? rightPosition : leftPosition;
            }
            if (compare(smallerChildPosition, position) < 0) {
                // child is larger or equal
                break;
            }
            swap(position, smallerChildPosition);
            position = smallerChildPosition;
        }
    }

    private void siftUp()
    {
        int position = positionCount - 1;
        while (position != 0) {
            int parentPosition = (position - 1) / 2;
            if (compare(position, parentPosition) < 0) {
                // child is larger or equal
                break;
            }
            swap(position, parentPosition);
            position = parentPosition;
        }
    }

    private void swap(int leftPosition, int rightPosition)
    {
        int leftOffset = getRecordOffset(leftPosition);
        int rightOffset = getRecordOffset(rightPosition);
        int tempOffset = getRecordOffset(capacity);
        System.arraycopy(fixedChunk, leftOffset, fixedChunk, tempOffset, recordSize);
        System.arraycopy(fixedChunk, rightOffset, fixedChunk, leftOffset, recordSize);
        System.arraycopy(fixedChunk, tempOffset, fixedChunk, rightOffset, recordSize);
    }

    private int compare(int leftPosition, int rightPosition)
    {
        int leftRecordOffset = getRecordOffset(leftPosition);
        int rightRecordOffset = getRecordOffset(rightPosition);

        byte[] leftVariableWidthChunk = EMPTY_CHUNK;
        byte[] rightVariableWidthChunk = EMPTY_CHUNK;
        if (keyVariableWidth) {
            leftVariableWidthChunk = variableWidthData.getChunk(fixedChunk, leftRecordOffset);
            rightVariableWidthChunk = variableWidthData.getChunk(fixedChunk, rightRecordOffset);
        }

        try {
            long result = (long) compareFlatFlat.invokeExact(
                    fixedChunk,
                    leftRecordOffset + recordKeyOffset,
                    leftVariableWidthChunk,
                    fixedChunk,
                    rightRecordOffset + recordKeyOffset,
                    rightVariableWidthChunk);
            return (int) (min ? result : -result);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private boolean shouldConsiderValue(Block right, int rightPosition)
    {
        byte[] leftFixedRecordChunk = fixedChunk;
        int leftRecordOffset = getRecordOffset(0);
        byte[] leftVariableWidthChunk = EMPTY_CHUNK;
        if (keyVariableWidth) {
            leftVariableWidthChunk = variableWidthData.getChunk(leftFixedRecordChunk, leftRecordOffset);
        }

        try {
            long result = (long) compareFlatBlock.invokeExact(
                    leftFixedRecordChunk,
                    leftRecordOffset + recordKeyOffset,
                    leftVariableWidthChunk,
                    right,
                    rightPosition);
            return min ? result > 0 : result < 0;
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private int getRecordOffset(int index)
    {
        return index * recordSize;
    }
}
