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

import java.io.Closeable;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.instanceSize;

public final class DataOutputStream
        extends OutputStream
        implements DataOutput
{
    private static final int DEFAULT_BUFFER_SIZE = 4 * 1024;
    private static final int MINIMUM_CHUNK_SIZE = 1024;

    private static final int INSTANCE_SIZE = instanceSize(DataOutputStream.class);

    private final OutputStream outputStream;

    private final Slice slice;
    private final byte[] buffer;

    /**
     * Offset of buffer within stream.
     */
    private long bufferOffset;
    /**
     * Current position for writing in buffer.
     */
    private int bufferPosition;

    public DataOutputStream(OutputStream inputStream)
    {
        this(inputStream, DEFAULT_BUFFER_SIZE);
    }

    public DataOutputStream(OutputStream outputStream, int bufferSize)
    {
        checkArgument(bufferSize >= MINIMUM_CHUNK_SIZE, "minimum buffer size of " + MINIMUM_CHUNK_SIZE + " required");
        if (outputStream == null) {
            throw new NullPointerException("outputStream is null");
        }

        this.outputStream = outputStream;
        this.buffer = new byte[bufferSize];
        this.slice = Slices.wrappedBuffer(buffer);
    }

    @Override
    public void flush()
            throws IOException
    {
        flushBufferToOutputStream();
        outputStream.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closeable ignored = outputStream) {
            flushBufferToOutputStream();
        }
    }

    public long longSize()
    {
        return bufferOffset + bufferPosition;
    }

    public long getRetainedSize()
    {
        return slice.getRetainedSize() + INSTANCE_SIZE;
    }

    @Override
    public void writeBoolean(boolean value)
            throws IOException
    {
        writeByte(value ? 1 : 0);
    }

    @Override
    public void write(int value)
            throws IOException
    {
        writeByte(value);
    }

    @Override
    public void writeByte(int value)
            throws IOException
    {
        ensureWritableBytes(SIZE_OF_BYTE);
        slice.setByte(bufferPosition, value);
        bufferPosition += SIZE_OF_BYTE;
    }

    @Override
    public void writeShort(int value)
            throws IOException
    {
        ensureWritableBytes(SIZE_OF_SHORT);
        slice.setShort(bufferPosition, value);
        bufferPosition += SIZE_OF_SHORT;
    }

    @Override
    public void writeInt(int value)
            throws IOException
    {
        ensureWritableBytes(SIZE_OF_INT);
        slice.setInt(bufferPosition, value);
        bufferPosition += SIZE_OF_INT;
    }

    @Override
    public void writeLong(long value)
            throws IOException
    {
        ensureWritableBytes(SIZE_OF_LONG);
        slice.setLong(bufferPosition, value);
        bufferPosition += SIZE_OF_LONG;
    }

    @Override
    public void writeFloat(float value)
            throws IOException
    {
        writeInt(Float.floatToIntBits(value));
    }

    @Override
    public void writeDouble(double value)
            throws IOException
    {
        writeLong(Double.doubleToLongBits(value));
    }

    public void write(Slice source)
            throws IOException
    {
        write(source, 0, source.length());
    }

    public void write(Slice source, int sourceIndex, int length)
            throws IOException
    {
        // Write huge chunks direct to OutputStream
        if (length >= MINIMUM_CHUNK_SIZE) {
            flushBufferToOutputStream();
            writeToOutputStream(source, sourceIndex, length);
            bufferOffset += length;
        }
        else {
            ensureWritableBytes(length);
            slice.setBytes(bufferPosition, source, sourceIndex, length);
            bufferPosition += length;
        }
    }

    @Override
    public void write(byte[] source)
            throws IOException
    {
        write(source, 0, source.length);
    }

    @Override
    public void write(byte[] source, int sourceIndex, int length)
            throws IOException
    {
        // Write huge chunks direct to OutputStream
        if (length >= MINIMUM_CHUNK_SIZE) {
            flushBufferToOutputStream();
            writeToOutputStream(source, sourceIndex, length);
            bufferOffset += length;
        }
        else {
            ensureWritableBytes(length);
            slice.setBytes(bufferPosition, source, sourceIndex, length);
            bufferPosition += length;
        }
    }

    public void write(InputStream in, int length)
            throws IOException
    {
        while (length > 0) {
            int batch = ensureBatchSize(length);
            slice.setBytes(bufferPosition, in, batch);
            bufferPosition += batch;
            length -= batch;
        }
    }

    public void writeZero(int length)
            throws IOException
    {
        checkArgument(length >= 0, "length must be 0 or greater than 0.");

        while (length > 0) {
            int batch = ensureBatchSize(length);
            Arrays.fill(buffer, bufferPosition, bufferPosition + batch, (byte) 0);
            bufferPosition += batch;
            length -= batch;
        }
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("OutputStreamSliceOutputAdapter{");
        builder.append("outputStream=").append(outputStream);
        builder.append("bufferSize=").append(slice.length());
        builder.append('}');
        return builder.toString();
    }

    private void ensureWritableBytes(int minWritableBytes)
            throws IOException
    {
        if (bufferPosition + minWritableBytes > slice.length()) {
            flushBufferToOutputStream();
        }
    }

    private int ensureBatchSize(int length)
            throws IOException
    {
        ensureWritableBytes(Math.min(MINIMUM_CHUNK_SIZE, length));
        return Math.min(length, slice.length() - bufferPosition);
    }

    private void flushBufferToOutputStream()
            throws IOException
    {
        writeToOutputStream(buffer, 0, bufferPosition);
        bufferOffset += bufferPosition;
        bufferPosition = 0;
    }

    private void writeToOutputStream(byte[] source, int sourceIndex, int length)
            throws IOException
    {
        outputStream.write(source, sourceIndex, length);
    }

    private void writeToOutputStream(Slice source, int sourceIndex, int length)
            throws IOException
    {
        source.getBytes(sourceIndex, outputStream, length);
    }

    //
    // Unsupported operations
    //

    /**
     * Unsupported operation
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    @Deprecated
    public void writeChar(int value)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported operation
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    @Deprecated
    public void writeChars(String s)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported operation
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    @Deprecated
    public void writeUTF(String s)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsupported operation
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    @Deprecated
    public void writeBytes(String s)
    {
        throw new UnsupportedOperationException();
    }
}
