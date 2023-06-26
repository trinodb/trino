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
package io.trino.operator.aggregation.minmaxn;

import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import io.airlift.slice.SizeOf;
import io.trino.operator.VariableWidthData;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrays;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.operator.VariableWidthData.EMPTY_CHUNK;
import static io.trino.operator.VariableWidthData.MAX_CHUNK_SIZE;
import static io.trino.operator.VariableWidthData.MIN_CHUNK_SIZE;
import static io.trino.operator.VariableWidthData.POINTER_SIZE;
import static io.trino.operator.VariableWidthData.getChunkOffset;
import static io.trino.operator.VariableWidthData.getValueLength;
import static io.trino.operator.VariableWidthData.writePointer;
import static java.util.Objects.requireNonNull;

public final class TypedHeap
{
    private static final int INSTANCE_SIZE = instanceSize(TypedHeap.class);

    private final boolean min;
    private final MethodHandle readFlat;
    private final MethodHandle writeFlat;
    private final MethodHandle compareFlatFlat;
    private final MethodHandle compareFlatBlock;
    private final Type elementType;
    private final int capacity;

    private final int recordElementOffset;

    private final int recordSize;
    /**
     * The fixed chunk contains an array of records. The records are laid out as follows:
     * <ul>
     *     <li>12 byte optional pointer to variable width data (only present if the type is variable width)</li>
     *     <li>N byte fixed size data for the element type</li>
     * </ul>
     * The pointer is placed first to simplify the offset calculations for variable with code.
     * This chunk contains {@code capacity + 1} records. The extra record is used for the swap operation.
     */
    private final byte[] fixedChunk;

    private VariableWidthData variableWidthData;

    private int positionCount;

    public TypedHeap(
            boolean min,
            MethodHandle readFlat,
            MethodHandle writeFlat,
            MethodHandle compareFlatFlat,
            MethodHandle compareFlatBlock,
            Type elementType,
            int capacity)
    {
        this.min = min;
        this.readFlat = requireNonNull(readFlat, "readFlat is null");
        this.writeFlat = requireNonNull(writeFlat, "writeFlat is null");
        this.compareFlatFlat = requireNonNull(compareFlatFlat, "compareFlatFlat is null");
        this.compareFlatBlock = requireNonNull(compareFlatBlock, "compareFlatBlock is null");
        this.elementType = requireNonNull(elementType, "elementType is null");
        this.capacity = capacity;

        boolean variableWidth = elementType.isFlatVariableWidth();
        variableWidthData = variableWidth ? new VariableWidthData() : null;
        recordElementOffset = (variableWidth ? POINTER_SIZE : 0);

        recordSize = recordElementOffset + elementType.getFlatFixedSize();

        // allocate the fixed chunk with on extra slow for use in swap
        fixedChunk = new byte[recordSize * (capacity + 1)];
    }

    public TypedHeap(TypedHeap typedHeap)
    {
        this.min = typedHeap.min;
        this.readFlat = typedHeap.readFlat;
        this.writeFlat = typedHeap.writeFlat;
        this.compareFlatFlat = typedHeap.compareFlatFlat;
        this.compareFlatBlock = typedHeap.compareFlatBlock;
        this.elementType = typedHeap.elementType;
        this.capacity = typedHeap.capacity;
        this.positionCount = typedHeap.positionCount;

        this.recordElementOffset = typedHeap.recordElementOffset;

        this.recordSize = typedHeap.recordSize;
        this.fixedChunk = Arrays.copyOf(typedHeap.fixedChunk, typedHeap.fixedChunk.length);

        if (typedHeap.variableWidthData != null) {
            this.variableWidthData = new VariableWidthData(typedHeap.variableWidthData);
        }
        else {
            this.variableWidthData = null;
        }
    }

    public Type getElementType()
    {
        return elementType;
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

    public void writeAllSorted(BlockBuilder resultBlockBuilder)
    {
        // fully sort the heap
        int[] indexes = new int[positionCount];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = i;
        }
        IntArrays.quickSort(indexes, this::compare);

        for (int index : indexes) {
            write(index, resultBlockBuilder);
        }
    }

    public void writeAllUnsorted(BlockBuilder elementBuilder)
    {
        for (int i = 0; i < positionCount; i++) {
            write(i, elementBuilder);
        }
    }

    private void write(int index, BlockBuilder blockBuilder)
    {
        int recordOffset = getRecordOffset(index);

        byte[] variableWidthChunk = EMPTY_CHUNK;
        if (variableWidthData != null) {
            variableWidthChunk = variableWidthData.getChunk(fixedChunk, recordOffset);
        }

        try {
            readFlat.invokeExact(
                    fixedChunk,
                    recordOffset + recordElementOffset,
                    variableWidthChunk,
                    blockBuilder);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    public void addAll(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            add(block, i);
        }
    }

    public void add(Block block, int position)
    {
        checkArgument(!block.isNull(position));
        if (positionCount == capacity) {
            // is it possible the value is within the top N values?
            if (!shouldConsiderValue(block, position)) {
                return;
            }
            clear(0);
            set(0, block, position);
            siftDown();
        }
        else {
            set(positionCount, block, position);
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
                (fixedSizeOffset, variableWidthChunk, variableWidthChunkOffset) ->
                        elementType.relocateFlatVariableWidthOffsets(fixedChunk, fixedSizeOffset + recordElementOffset, variableWidthChunk, variableWidthChunkOffset));
    }

    private void set(int index, Block block, int position)
    {
        int recordOffset = getRecordOffset(index);

        byte[] variableWidthChunk = EMPTY_CHUNK;
        int variableWidthChunkOffset = 0;
        if (variableWidthData != null) {
            int variableWidthLength = elementType.getFlatVariableWidthSize(block, position);
            variableWidthChunk = variableWidthData.allocate(fixedChunk, recordOffset, variableWidthLength);
            variableWidthChunkOffset = getChunkOffset(fixedChunk, recordOffset);
        }

        try {
            writeFlat.invokeExact(block, position, fixedChunk, recordOffset + recordElementOffset, variableWidthChunk, variableWidthChunkOffset);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
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
        if (variableWidthData != null) {
            leftVariableWidthChunk = variableWidthData.getChunk(fixedChunk, leftRecordOffset);
            rightVariableWidthChunk = variableWidthData.getChunk(fixedChunk, rightRecordOffset);
        }

        try {
            long result = (long) compareFlatFlat.invokeExact(
                    fixedChunk,
                    leftRecordOffset + recordElementOffset,
                    leftVariableWidthChunk,
                    fixedChunk,
                    rightRecordOffset + recordElementOffset,
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
        if (variableWidthData != null) {
            leftVariableWidthChunk = variableWidthData.getChunk(leftFixedRecordChunk, leftRecordOffset);
        }

        try {
            long result = (long) compareFlatBlock.invokeExact(
                    leftFixedRecordChunk,
                    leftRecordOffset + recordElementOffset,
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

    private static final double MAX_FREE_RATIO = 0.66;

    public interface RelocateVariableWidthOffsets
    {
        void relocate(int fixedSizeOffset, byte[] variableWidthChunk, int variableWidthChunkOffset);
    }

    public static VariableWidthData compactIfNecessary(VariableWidthData data, byte[] fixedSizeChunk, int fixedRecordSize, int fixedRecordPointerOffset, int recordCount, RelocateVariableWidthOffsets relocateVariableWidthOffsets)
    {
        List<byte[]> chunks = data.getAllChunks();
        double freeRatio = 1.0 * data.getFreeBytes() / data.getAllocatedBytes();
        if (chunks.size() <= 1 || freeRatio < MAX_FREE_RATIO) {
            return data;
        }

        // there are obviously much smarter ways to compact the memory, so feel free to improve this
        List<byte[]> newSlices = new ArrayList<>();

        int newSize = 0;
        int indexStart = 0;
        for (int i = 0; i < recordCount; i++) {
            int valueLength = getValueLength(fixedSizeChunk, i * fixedRecordSize + fixedRecordPointerOffset);
            if (newSize + valueLength > MAX_CHUNK_SIZE) {
                moveVariableWidthToNewSlice(data, fixedSizeChunk, fixedRecordSize, fixedRecordPointerOffset, indexStart, i, newSlices, newSize, relocateVariableWidthOffsets);
                indexStart = i;
                newSize = 0;
            }
            newSize += valueLength;
        }

        // remaining data is copied into the open slice
        int openChunkOffset;
        if (newSize > 0) {
            int openSliceSize = newSize;
            if (newSize < MAX_CHUNK_SIZE) {
                openSliceSize = Ints.constrainToRange(Ints.saturatedCast(openSliceSize * 2L), MIN_CHUNK_SIZE, MAX_CHUNK_SIZE);
            }
            moveVariableWidthToNewSlice(data, fixedSizeChunk, fixedRecordSize, fixedRecordPointerOffset, indexStart, recordCount, newSlices, openSliceSize, relocateVariableWidthOffsets);
            openChunkOffset = newSize;
        }
        else {
            openChunkOffset = newSlices.get(newSlices.size() - 1).length;
        }

        return new VariableWidthData(newSlices, openChunkOffset);
    }

    private static void moveVariableWidthToNewSlice(
            VariableWidthData sourceData,
            byte[] fixedSizeChunk,
            int fixedRecordSize,
            int fixedRecordPointerOffset,
            int indexStart,
            int indexEnd,
            List<byte[]> newSlices,
            int newSliceSize,
            RelocateVariableWidthOffsets relocateVariableWidthOffsets)
    {
        int newSliceIndex = newSlices.size();
        byte[] newSlice = new byte[newSliceSize];
        newSlices.add(newSlice);

        int newSliceOffset = 0;
        for (int index = indexStart; index < indexEnd; index++) {
            int fixedChunkOffset = index * fixedRecordSize;
            int pointerOffset = fixedChunkOffset + fixedRecordPointerOffset;

            int variableWidthOffset = getChunkOffset(fixedSizeChunk, pointerOffset);
            byte[] variableWidthChunk = sourceData.getChunk(fixedSizeChunk, pointerOffset);
            int variableWidthLength = getValueLength(fixedSizeChunk, pointerOffset);

            System.arraycopy(variableWidthChunk, variableWidthOffset, newSlice, newSliceOffset, variableWidthLength);
            writePointer(
                    fixedSizeChunk,
                    pointerOffset,
                    newSliceIndex,
                    newSliceOffset,
                    variableWidthLength);
            relocateVariableWidthOffsets.relocate(fixedChunkOffset, newSlice, newSliceOffset);
            newSliceOffset += variableWidthLength;
        }
    }
}
