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
package io.trino.filesystem.gcs;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.common.primitives.Ints;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class GcsOutputStream
        extends OutputStream
{
    private static final int BUFFER_SIZE = 8192;

    private final GcsLocation location;
    private final long writeBlockSizeBytes;
    private final LocalMemoryContext memoryContext;
    private final WriteChannel writeChannel;
    private final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
    private long writtenBytes;
    private boolean closed;

    public GcsOutputStream(GcsLocation location, Blob blob, AggregatedMemoryContext memoryContext, long writeBlockSizeBytes)
    {
        this.location = requireNonNull(location, "location is null");
        checkArgument(writeBlockSizeBytes >= 0, "writeBlockSizeBytes is negative");
        this.writeBlockSizeBytes = writeBlockSizeBytes;
        this.memoryContext = memoryContext.newLocalMemoryContext(GcsOutputStream.class.getSimpleName());
        this.writeChannel = blob.writer();
        this.writeChannel.setChunkSize(Ints.saturatedCast(writeBlockSizeBytes));
    }

    @Override
    public void write(int b)
            throws IOException
    {
        ensureOpen();
        if (!buffer.hasRemaining()) {
            flush();
        }
        buffer.put((byte) b);
        recordBytesWritten(1);
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        if (length > BUFFER_SIZE) {
            writeDirect(ByteBuffer.wrap(buffer, offset, length));
        }
        else {
            if (length > this.buffer.remaining()) {
                flush();
            }
            this.buffer.put(buffer, offset, length);
            recordBytesWritten(length);
        }
    }

    private void writeDirect(ByteBuffer buffer)
            throws IOException
    {
        // Flush write buffer in case calls to write(int) are interleaved with calls to this function
        flush();
        int bytesWritten = 0;
        try {
            bytesWritten = writeChannel.write(buffer);
            if (bytesWritten != buffer.remaining()) {
                throw new IOException("Unexpected bytes written length: %s should be %s".formatted(bytesWritten, buffer.remaining()));
            }
        }
        catch (IOException e) {
            throw new IOException("Error writing file: " + location, e);
        }
        recordBytesWritten(bytesWritten);
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Output stream closed: " + location);
        }
    }

    @Override
    public void flush()
            throws IOException
    {
        ensureOpen();
        if (buffer.position() > 0) {
            buffer.flip();
            while (buffer.hasRemaining()) {
                try {
                    // WriteChannel is buffered internally: see com.google.cloud.BaseWriteChannel
                    writeChannel.write(buffer);
                }
                catch (IOException e) {
                    throw new IOException("Error writing file: " + location, e);
                }
            }
            buffer.clear();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            flush();
            closed = true;
            try {
                writeChannel.close();
            }
            catch (IOException e) {
                throw new IOException("Error closing file: " + location, e);
            }
            finally {
                memoryContext.close();
            }
        }
    }

    private void recordBytesWritten(int size)
    {
        if (writtenBytes < writeBlockSizeBytes) {
            // assume that there is only one pending block buffer, and that it grows as written bytes grow
            memoryContext.setBytes(BUFFER_SIZE + min(writtenBytes + size, writeBlockSizeBytes));
        }
        writtenBytes += size;
    }
}
