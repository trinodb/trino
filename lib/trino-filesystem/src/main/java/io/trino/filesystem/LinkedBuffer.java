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
package io.trino.filesystem;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;

public final class LinkedBuffer
        implements DataStreamProvider
{
    final int initialBufferSize;
    final int maxBufferSize;
    List<byte[]> parts;
    int currentBufferSize;
    byte[] currentBuffer;
    int currentOffset;
    int allocatedSize;
    int totalSize;

    public LinkedBuffer(int initialBufferSize, int maxBufferSize)
    {
        checkArgument(initialBufferSize <= maxBufferSize, "initialBufferSize must be less than or equal to maxBufferSize");
        this.initialBufferSize = initialBufferSize;
        this.maxBufferSize = maxBufferSize;
        this.parts = new ArrayList<>();
        this.currentBufferSize = initialBufferSize;
        this.currentBuffer = new byte[initialBufferSize];
        this.currentOffset = 0;
        this.allocatedSize = initialBufferSize;
        this.totalSize = 0;
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    public void write(int b)
    {
        if (remainingCapacity() == 0) {
            parts.add(currentBuffer);
            resetBuffer();
        }
        currentBuffer[currentOffset] = (byte) b;
        currentOffset++;
        totalSize++;
    }

    public void write(byte[] bytes, int offset, int length)
    {
        writeInternal(bytes, offset, length);
    }

    @Override
    public int allocated()
    {
        return allocatedSize;
    }

    @Override
    public int size()
    {
        return totalSize;
    }

    @Override
    public List<ByteBuffer> takeByteBuffers()
    {
        ImmutableList.Builder<ByteBuffer> buffers = ImmutableList.builderWithExpectedSize(parts.size() + ((currentOffset == 0) ? 0 : 1));
        for (byte[] part : parts) {
            buffers.add(ByteBuffer.wrap(part));
        }
        if (currentOffset > 0) {
            buffers.add(ByteBuffer.wrap(currentBuffer, 0, currentOffset));
        }
        return buffers.build();
    }

    public LinkedBuffer startNextPart()
    {
        return new LinkedBuffer(currentBufferSize, maxBufferSize);
    }

    @VisibleForTesting
    void reset()
    {
        allocatedSize = initialBufferSize;
        currentBufferSize = initialBufferSize;
        currentBuffer = new byte[currentBufferSize];
        currentOffset = 0;
        totalSize = 0;
        parts = new ArrayList<>();
    }

    private int remainingCapacity()
    {
        return currentBufferSize - currentOffset;
    }

    private void resetBuffer()
    {
        // scale buffer capacity by 2 or up to the maximum allowed
        currentBufferSize = min(currentBufferSize * 2, maxBufferSize);
        currentBuffer = new byte[currentBufferSize];
        currentOffset = 0;
        allocatedSize += currentBufferSize;
    }

    private void writeInternal(byte[] srcBytes, int srcOffset, int srcLength)
    {
        while (srcLength > 0) {
            if (remainingCapacity() == 0) {
                parts.add(currentBuffer);
                resetBuffer();
            }

            int bytesToWrite = min(srcLength, remainingCapacity());

            arraycopy(srcBytes, srcOffset, currentBuffer, currentOffset, bytesToWrite);

            currentOffset += bytesToWrite;
            totalSize += bytesToWrite;
            srcOffset += bytesToWrite;
            srcLength -= bytesToWrite;
        }
    }
}
