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
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.addExact;
import static java.lang.Math.clamp;
import static java.lang.Math.max;
import static java.util.Objects.checkIndex;

/**
 * Variation of {@link VariableWidthData} that does not support {@link VariableWidthData#free}. As a result of that
 * limitation, the embedded variable width data pointer only needs to encode the chunk index and chunk offset but not
 * the variable width length. This reduces the number of bytes required in each fixed size record by 4.
 */
public final class AppendOnlyVariableWidthData
{
    private static final int INSTANCE_SIZE = instanceSize(AppendOnlyVariableWidthData.class);

    public static final int MIN_CHUNK_SIZE = 1024;
    public static final int MAX_CHUNK_SIZE = 8 * 1024 * 1024;
    private static final int DOUBLING_CHUNK_THRESHOLD = 512 * 1024;

    public static final int POINTER_SIZE = Integer.BYTES + Integer.BYTES;

    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final byte[] EMPTY_CHUNK = VariableWidthData.EMPTY_CHUNK;

    private final ObjectArrayList<byte[]> chunks = new ObjectArrayList<>();
    private int openChunkOffset;

    private long chunksRetainedSizeInBytes;

    private long allocatedBytes;
    private long freeBytes;

    public AppendOnlyVariableWidthData() {}

    public AppendOnlyVariableWidthData(AppendOnlyVariableWidthData other)
    {
        for (byte[] chunk : other.chunks) {
            chunks.add(Arrays.copyOf(chunk, chunk.length));
        }
        this.openChunkOffset = other.openChunkOffset;

        this.chunksRetainedSizeInBytes = other.chunksRetainedSizeInBytes;

        this.allocatedBytes = other.allocatedBytes;
        this.freeBytes = other.freeBytes;
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
            writePointer(pointer, pointerOffset, 0, 0);
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
                openChunkOffset);
        openChunkOffset += size;
        return openChunk;
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

    public void freeChunksBefore(byte[] pointer, int pointerOffset)
    {
        int chunkIndex = getChunkIndex(pointer, pointerOffset);
        if (chunks.isEmpty()) {
            verify(chunkIndex == 0);
            return;
        }
        checkIndex(chunkIndex, chunks.size());
        // Release any previous chunks until a null chunk is encountered, which means it and any previous
        // batches have already been released
        int releaseIndex = chunkIndex - 1;
        while (releaseIndex >= 0) {
            byte[] releaseChunk = chunks.set(releaseIndex, null);
            if (releaseChunk == null) {
                break;
            }
            chunksRetainedSizeInBytes -= sizeOf(releaseChunk);
            releaseIndex--;
        }
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
        return (int) INT_HANDLE.get(pointer, pointerOffset + Integer.BYTES);
    }

    public static void writePointer(byte[] pointer, int pointerOffset, int chunkIndex, int chunkOffset)
    {
        INT_HANDLE.set(pointer, pointerOffset, chunkIndex);
        INT_HANDLE.set(pointer, pointerOffset + Integer.BYTES, chunkOffset);
    }
}
