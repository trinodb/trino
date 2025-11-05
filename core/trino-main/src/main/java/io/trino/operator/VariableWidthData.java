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
package io.trino.operator;

import com.google.common.primitives.Ints;
import io.airlift.slice.SizeOf;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.addExact;
import static java.lang.Math.clamp;
import static java.lang.Math.max;
import static java.lang.Math.subtractExact;
import static java.util.Objects.checkIndex;

/**
 * A related implementation of {@link VariableWidthData} exists in {@link AppendOnlyVariableWidthData} which reduces
 * the size of each variable width pointer at the cost of not supporting {@link VariableWidthData#free}. If your data
 * structure does not need to support moving variable width data after insertion, that implementation will consume
 * less memory and should be used instead.
 */
public final class VariableWidthData
{
    private static final int INSTANCE_SIZE = instanceSize(VariableWidthData.class);

    public static final int MIN_CHUNK_SIZE = 1024;
    public static final int MAX_CHUNK_SIZE = 8 * 1024 * 1024;
    private static final int DOUBLING_CHUNK_THRESHOLD = 512 * 1024;

    public static final int POINTER_SIZE = SIZE_OF_INT + SIZE_OF_INT + SIZE_OF_INT;

    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    public static final byte[] EMPTY_CHUNK = new byte[0];

    private final ObjectArrayList<byte[]> chunks = new ObjectArrayList<>();
    private int openChunkOffset;

    private long chunksRetainedSizeInBytes;

    private long allocatedBytes;
    private long freeBytes;

    public VariableWidthData() {}

    public VariableWidthData(VariableWidthData variableWidthData)
    {
        for (byte[] chunk : variableWidthData.chunks) {
            chunks.add(Arrays.copyOf(chunk, chunk.length));
        }
        this.openChunkOffset = variableWidthData.openChunkOffset;

        this.chunksRetainedSizeInBytes = variableWidthData.chunksRetainedSizeInBytes;

        this.allocatedBytes = variableWidthData.allocatedBytes;
        this.freeBytes = variableWidthData.freeBytes;
    }

    public VariableWidthData(List<byte[]> chunks, int openChunkOffset)
    {
        this.chunks.addAll(chunks);
        this.openChunkOffset = openChunkOffset;
        this.chunksRetainedSizeInBytes = chunks.stream().mapToLong(SizeOf::sizeOf).reduce(0L, Math::addExact);
        this.allocatedBytes = chunks.stream().mapToLong(chunk -> chunk.length).sum();
        this.freeBytes = 0;
    }

    public long getRetainedSizeBytes()
    {
        return addExact(
                INSTANCE_SIZE,
                addExact(chunksRetainedSizeInBytes, sizeOf(chunks.elements())));
    }

    public List<byte[]> getAllChunks()
    {
        return chunks;
    }

    public long getAllocatedBytes()
    {
        return allocatedBytes;
    }

    public long getFreeBytes()
    {
        return freeBytes;
    }

    public byte[] allocate(byte[] pointer, int pointerOffset, int size)
    {
        if (size == 0) {
            writePointer(pointer, pointerOffset, 0, 0, 0);
            return EMPTY_CHUNK;
        }

        byte[] openChunk = chunks.isEmpty() ? EMPTY_CHUNK : chunks.getLast();
        if (openChunk.length - openChunkOffset < size) {
            // record unused space as free bytes
            freeBytes += (openChunk.length - openChunkOffset);

            // allocate enough space for 32 values of the current size, or 1.5x the current chunk size, whichever is larger
            int newSize = Ints.saturatedCast(max(size * 32L, nextChunkSize(openChunk.length)));
            // constrain to be between min and max chunk size
            newSize = clamp(newSize, MIN_CHUNK_SIZE, MAX_CHUNK_SIZE);
            // jumbo rows get a separate allocation
            newSize = max(newSize, size);
            openChunk = new byte[newSize];
            chunks.add(openChunk);
            allocatedBytes += newSize;
            chunksRetainedSizeInBytes = addExact(chunksRetainedSizeInBytes, sizeOf(openChunk));
            openChunkOffset = 0;
        }

        writePointer(
                pointer,
                pointerOffset,
                chunks.size() - 1,
                openChunkOffset,
                size);
        openChunkOffset += size;
        return openChunk;
    }

    public void free(byte[] pointer, int pointerOffset)
    {
        int valueLength = getValueLength(pointer, pointerOffset);
        if (valueLength == 0) {
            return;
        }

        int valueChunkIndex = getChunkIndex(pointer, pointerOffset);
        byte[] valueChunk = chunks.get(valueChunkIndex);

        // if this is the last value in the open byte[], then we can simply back up the open chunk offset
        if (valueChunkIndex == chunks.size() - 1) {
            int valueOffset = getChunkOffset(pointer, pointerOffset);
            if (this.openChunkOffset - valueLength == valueOffset) {
                this.openChunkOffset = valueOffset;
                return;
            }
        }

        // if this is the only value written to the chunk, we can simply replace the chunk with the empty chunk
        if (valueLength == valueChunk.length) {
            chunks.set(valueChunkIndex, EMPTY_CHUNK);
            chunksRetainedSizeInBytes = subtractExact(chunksRetainedSizeInBytes, sizeOf(valueChunk));
            allocatedBytes -= valueChunk.length;
            return;
        }

        freeBytes += valueLength;
    }

    public byte[] getChunk(byte[] pointer, int pointerOffset)
    {
        int chunkIndex = getChunkIndex(pointer, pointerOffset);
        if (chunks.isEmpty()) {
            verify(chunkIndex == 0);
            return EMPTY_CHUNK;
        }
        checkIndex(chunkIndex, chunks.size());
        return chunks.get(chunkIndex);
    }

    // growth factor for each chunk doubles up to 512KB, then increases by 1.5x for each chunk after that
    private static long nextChunkSize(long previousChunkSize)
    {
        if (previousChunkSize < DOUBLING_CHUNK_THRESHOLD) {
            return previousChunkSize * 2;
        }
        return previousChunkSize + (previousChunkSize >> 1);
    }

    private static int getChunkIndex(byte[] pointer, int pointerOffset)
    {
        return (int) INT_HANDLE.get(pointer, pointerOffset);
    }

    public static int getChunkOffset(byte[] pointer, int pointerOffset)
    {
        return (int) INT_HANDLE.get(pointer, pointerOffset + SIZE_OF_INT);
    }

    public static int getValueLength(byte[] pointer, int pointerOffset)
    {
        return (int) INT_HANDLE.get(pointer, pointerOffset + SIZE_OF_LONG);
    }

    public static void writePointer(byte[] pointer, int pointerOffset, int chunkIndex, int chunkOffset, int valueLength)
    {
        INT_HANDLE.set(pointer, pointerOffset, chunkIndex);
        INT_HANDLE.set(pointer, pointerOffset + SIZE_OF_INT, chunkOffset);
        INT_HANDLE.set(pointer, pointerOffset + SIZE_OF_LONG, valueLength);
    }
}
