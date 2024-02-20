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
package io.trino.hive.formats;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.TrinoInputStream;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.addExact;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class TrinoDataInputStream
        extends InputStream
        implements DataInput
{
    private static final int INSTANCE_SIZE = instanceSize(TrinoDataInputStream.class);
    private static final int DEFAULT_BUFFER_SIZE = 4 * 1024;
    private static final int MINIMUM_CHUNK_SIZE = 1024;

    private final TrinoInputStream inputStream;
    private long readTimeNanos;
    private long readBytes;

    private final byte[] buffer;
    private final Slice slice;
    /**
     * Offset of buffer within stream.
     */
    private long bufferOffset;
    /**
     * Current position for reading from buffer.
     */
    private int bufferPosition;

    private int bufferFill;

    public TrinoDataInputStream(TrinoInputStream inputStream)
    {
        this(inputStream, DEFAULT_BUFFER_SIZE);
    }

    public TrinoDataInputStream(TrinoInputStream inputStream, int bufferSize)
    {
        requireNonNull(inputStream, "inputStream is null");
        checkArgument(bufferSize >= MINIMUM_CHUNK_SIZE, "minimum buffer size of " + MINIMUM_CHUNK_SIZE + " required");

        this.inputStream = inputStream;
        this.buffer = new byte[bufferSize];
        this.slice = Slices.wrappedBuffer(buffer);
    }

    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    public long getReadBytes()
    {
        return readBytes;
    }

    public long getPos()
            throws IOException
    {
        return addExact(bufferOffset, bufferPosition);
    }

    public void seek(long newPos)
            throws IOException
    {
        // todo check if new position is within the current buffer

        // drop current buffer
        bufferPosition = 0;
        bufferFill = 0;

        // skip the rest in inputStream
        inputStream.seek(newPos);

        // update buffer offset to the new position
        bufferOffset = newPos;

        verify(newPos == getPos());
    }

    @Override
    public int available()
            throws IOException
    {
        if (bufferPosition < bufferFill) {
            return availableBytes();
        }

        return fillBuffer();
    }

    @Override
    public int skipBytes(int n)
            throws IOException
    {
        return (int) skip(n);
    }

    @Override
    public boolean readBoolean()
            throws IOException
    {
        return readByte() != 0;
    }

    @Override
    public byte readByte()
            throws IOException
    {
        ensureAvailable(SIZE_OF_BYTE);
        byte v = slice.getByte(bufferPosition);
        bufferPosition += SIZE_OF_BYTE;
        return v;
    }

    @Override
    public int readUnsignedByte()
            throws IOException
    {
        return readByte() & 0xFF;
    }

    @Override
    public short readShort()
            throws IOException
    {
        ensureAvailable(SIZE_OF_SHORT);
        short v = slice.getShort(bufferPosition);
        bufferPosition += SIZE_OF_SHORT;
        return v;
    }

    @Override
    public int readUnsignedShort()
            throws IOException
    {
        return readShort() & 0xFFFF;
    }

    @Override
    public int readInt()
            throws IOException
    {
        ensureAvailable(SIZE_OF_INT);
        int v = slice.getInt(bufferPosition);
        bufferPosition += SIZE_OF_INT;
        return v;
    }

    /**
     * Gets an unsigned 32-bit integer at the current {@code position}
     * and increases the {@code position} by {@code 4} in this buffer.
     *
     * @throws IndexOutOfBoundsException if {@code this.available()} is less than {@code 4}
     */
    public long readUnsignedInt()
            throws IOException
    {
        return readInt() & 0xFFFFFFFFL;
    }

    @Override
    public long readLong()
            throws IOException
    {
        ensureAvailable(SIZE_OF_LONG);
        long v = slice.getLong(bufferPosition);
        bufferPosition += SIZE_OF_LONG;
        return v;
    }

    @Override
    public float readFloat()
            throws IOException
    {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble()
            throws IOException
    {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public int read()
            throws IOException
    {
        if (available() == 0) {
            return -1;
        }

        verify(availableBytes() > 0);
        int v = slice.getByte(bufferPosition) & 0xFF;
        bufferPosition += SIZE_OF_BYTE;
        return v;
    }

    @Override
    public long skip(long length)
            throws IOException
    {
        if (length <= 0) {
            return 0;
        }

        int availableBytes = availableBytes();
        // is skip within the current buffer?
        if (availableBytes >= length) {
            bufferPosition = addExact(bufferPosition, toIntExact(length));
            return length;
        }

        // drop current buffer
        bufferPosition = bufferFill;

        // skip the rest in inputStream
        long start = System.nanoTime();
        long inputStreamSkip = inputStream.skip(length - availableBytes);
        readTimeNanos += System.nanoTime() - start;
        readBytes += inputStreamSkip;

        bufferOffset += inputStreamSkip;
        return availableBytes + inputStreamSkip;
    }

    @Override
    public int read(byte[] destination)
            throws IOException
    {
        return read(destination, 0, destination.length);
    }

    @Override
    public int read(byte[] destination, int destinationIndex, int length)
            throws IOException
    {
        if (available() == 0) {
            return -1;
        }

        verify(availableBytes() > 0);
        int batch = Math.min(availableBytes(), length);
        slice.getBytes(bufferPosition, destination, destinationIndex, batch);
        bufferPosition += batch;
        return batch;
    }

    @Override
    public void readFully(byte[] destination)
            throws IOException
    {
        readFully(destination, 0, destination.length);
    }

    @Override
    public void readFully(byte[] destination, int destinationIndex, int length)
            throws IOException
    {
        while (length > 0) {
            int batch = Math.min(availableBytes(), length);
            slice.getBytes(bufferPosition, destination, destinationIndex, batch);

            bufferPosition += batch;
            destinationIndex += batch;
            length -= batch;

            ensureAvailable(Math.min(length, MINIMUM_CHUNK_SIZE));
        }
    }

    public Slice readSlice(int length)
            throws IOException
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }

        Slice newSlice = Slices.allocate(length);
        readFully(newSlice, 0, length);
        return newSlice;
    }

    public void readFully(Slice destination)
            throws IOException
    {
        readFully(destination, 0, destination.length());
    }

    public void readFully(Slice destination, int destinationIndex, int length)
            throws IOException
    {
        while (length > 0) {
            int batch = Math.min(availableBytes(), length);
            slice.getBytes(bufferPosition, destination, destinationIndex, batch);

            bufferPosition += batch;
            destinationIndex += batch;
            length -= batch;

            ensureAvailable(Math.min(length, MINIMUM_CHUNK_SIZE));
        }
    }

    public void readFully(OutputStream out, int length)
            throws IOException
    {
        while (length > 0) {
            int batch = Math.min(availableBytes(), length);
            out.write(buffer, bufferPosition, batch);

            bufferPosition += batch;
            length -= batch;

            ensureAvailable(Math.min(length, MINIMUM_CHUNK_SIZE));
        }
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
    }

    public long getRetainedSize()
    {
        return INSTANCE_SIZE + sizeOf(buffer);
    }

    private int availableBytes()
    {
        return bufferFill - bufferPosition;
    }

    private void ensureAvailable(int size)
            throws IOException
    {
        if (bufferPosition + size < bufferFill) {
            return;
        }

        if (fillBuffer() < size) {
            throw new EOFException("End of stream");
        }
    }

    private int fillBuffer()
            throws IOException
    {
        // Keep the rest
        int rest = bufferFill - bufferPosition;
        // Use System.arraycopy for small copies
        System.arraycopy(buffer, bufferPosition, buffer, 0, rest);

        bufferFill = rest;
        bufferOffset += bufferPosition;
        bufferPosition = 0;
        // Fill buffer with a minimum of bytes
        long start = System.nanoTime();
        while (bufferFill < MINIMUM_CHUNK_SIZE) {
            int bytesRead = inputStream.read(buffer, bufferFill, buffer.length - bufferFill);
            if (bytesRead < 0) {
                break;
            }

            readBytes += bytesRead;
            bufferFill += bytesRead;
        }
        readTimeNanos += System.nanoTime() - start;

        return bufferFill;
    }

    //
    // Unsupported operations
    //

    @Override
    @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
    @Deprecated
    public void mark(int readLimit)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
    @Deprecated
    public void reset()

    {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public boolean markSupported()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public char readChar()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public String readLine()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    @Deprecated
    public String readUTF()
    {
        throw new UnsupportedOperationException();
    }
}
