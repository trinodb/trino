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
package io.trino.filesystem.cache;

import io.trino.filesystem.TrinoInputStream;
import io.trino.spi.cache.Blob;

import java.io.IOException;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

/**
 * Sequential reads over a {@link Blob}, buffering the small arbitrarily sized reads a stream
 * receives so that each one does not turn into a separate lookup in the underlying cache.
 */
final class BlobTrinoInputStream
        extends TrinoInputStream
{
    private static final int BUFFER_SIZE = 8192;

    private final Blob blob;
    private final long length;
    private long position;
    private boolean closed;

    // Buffered content of the blob, covering [bufferStart, bufferStart + bufferLength) of the file
    private byte[] readBuffer;
    private long bufferStart;
    private int bufferLength;

    BlobTrinoInputStream(Blob blob)
            throws IOException
    {
        this.blob = requireNonNull(blob, "blob is null");
        this.length = blob.length();
    }

    @Override
    public long getPosition()
    {
        return position;
    }

    @Override
    public void seek(long newPosition)
            throws IOException
    {
        ensureOpen();
        if (newPosition < 0) {
            throw new IOException("Negative seek offset");
        }
        if (newPosition > length) {
            throw new IOException("Cannot seek to %s. Blob size is %s: %s".formatted(newPosition, length, blob));
        }
        this.position = newPosition;
    }

    @Override
    public int read()
            throws IOException
    {
        ensureOpen();
        if (position >= length) {
            return -1;
        }
        if (bufferedBytes() == 0) {
            fillBuffer();
        }
        int value = readBuffer[toIntExact(position - bufferStart)] & 0xFF;
        position++;
        return value;
    }

    @Override
    public int read(byte[] buffer, int offset, int len)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(offset, len, buffer.length);
        if (len == 0) {
            return 0;
        }
        if (position >= length) {
            return -1;
        }
        int toRead = toIntExact(min(len, length - position));
        int buffered = copyFromBuffer(buffer, offset, toRead);
        if (buffered > 0) {
            return buffered;
        }
        // Reads at least as large as the buffer are served directly: buffering them would only
        // add a copy, and reading ahead beyond them is not what the caller asked for
        if (toRead >= BUFFER_SIZE) {
            blob.read(position, buffer, offset, toRead);
            position += toRead;
            return toRead;
        }
        fillBuffer();
        return copyFromBuffer(buffer, offset, toRead);
    }

    /**
     * Bytes of the buffer that are still ahead of the current position.
     */
    private int bufferedBytes()
    {
        if (readBuffer == null || position < bufferStart || position >= bufferStart + bufferLength) {
            return 0;
        }
        return toIntExact(bufferStart + bufferLength - position);
    }

    private int copyFromBuffer(byte[] buffer, int offset, int length)
    {
        int available = min(bufferedBytes(), length);
        if (available == 0) {
            return 0;
        }
        System.arraycopy(readBuffer, toIntExact(position - bufferStart), buffer, offset, available);
        position += available;
        return available;
    }

    private void fillBuffer()
            throws IOException
    {
        if (readBuffer == null) {
            readBuffer = new byte[BUFFER_SIZE];
        }
        // The caller checked that at least one byte remains, and a blob only serves ranges
        // fully within its content, so the fill is clamped to the end of the file
        int fillLength = toIntExact(min(BUFFER_SIZE, length - position));
        blob.read(position, readBuffer, 0, fillLength);
        bufferStart = position;
        bufferLength = fillLength;
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        ensureOpen();
        if (n <= 0) {
            return 0;
        }
        long skipped = min(n, length - position);
        position += skipped;
        return skipped;
    }

    @Override
    public int available()
            throws IOException
    {
        ensureOpen();
        long remaining = length - position;
        return (remaining > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) remaining;
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        readBuffer = null;
        bufferLength = 0;
        try {
            blob.close();
        }
        catch (Exception e) {
            throw new IOException("Could not close cached blob", e);
        }
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Cached blob is closed: " + blob);
        }
    }
}
