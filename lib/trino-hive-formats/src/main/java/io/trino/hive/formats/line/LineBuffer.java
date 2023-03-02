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
package io.trino.hive.formats.line;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.primitives.Ints.constrainToRange;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;

public final class LineBuffer
        extends OutputStream
{
    private static final int INSTANCE_SIZE = instanceSize(LineBuffer.class);

    private final int maxLength;
    private byte[] buffer;
    private int length;

    public LineBuffer(int initialBufferSize, int maxLength)
    {
        checkArgument(initialBufferSize > 0, "initialBufferSize must be at least one byte");
        checkArgument(maxLength > 0, "maxLength must be at least one byte");
        checkArgument(maxLength <= 1024 * 1024 * 1024, "maxLength is greater than 1GB");
        checkArgument(initialBufferSize <= maxLength, "initialBufferSize is greater than maxLength");

        this.maxLength = maxLength;
        this.buffer = new byte[initialBufferSize];
    }

    public byte[] getBuffer()
    {
        return buffer;
    }

    public boolean isEmpty()
    {
        return length == 0;
    }

    public int getLength()
    {
        return length;
    }

    public void reset()
    {
        length = 0;
    }

    public long getRetainedSize()
    {
        return INSTANCE_SIZE + sizeOf(buffer);
    }

    @Override
    public void write(int b)
            throws IOException
    {
        growBufferIfNecessary(1);
        buffer[length] = (byte) b;
        length++;
    }

    @Override
    public void write(byte[] source, int sourceOffset, int sourceLength)
            throws IOException
    {
        growBufferIfNecessary(sourceLength);
        System.arraycopy(source, sourceOffset, buffer, length, sourceLength);
        length += sourceLength;
    }

    public void write(InputStream input, int size)
            throws IOException
    {
        growBufferIfNecessary(size);
        if (input.readNBytes(buffer, length, size) != size) {
            throw new EOFException("Input is too small");
        }
        length += size;
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}

    private void growBufferIfNecessary(int minFreeSpace)
            throws IOException
    {
        int newLength = length + minFreeSpace;
        if (newLength > maxLength) {
            throw new IOException("Max line length exceeded: " + newLength);
        }
        if (buffer.length < newLength) {
            int newSize = constrainToRange(buffer.length * 2, newLength, maxLength);
            buffer = Arrays.copyOf(buffer, newSize);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("maxLength", maxLength)
                .add("capacity", buffer.length)
                .add("length", length)
                .toString();
    }
}
