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
package io.trino.filesystem.cache.local;

import com.google.common.primitives.Longs;
import io.trino.filesystem.TrinoInputStream;

import java.io.EOFException;
import java.io.IOException;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class NativeCacheInputStream
        extends TrinoInputStream
{
    private final NativeCacheFile cacheFile;
    private TrinoInputStream inputStream;
    private byte[] pageBuffer;
    private long pageBufferIndex = -1;
    private int pageBufferLength;
    private long position;
    private boolean closed;

    NativeCacheInputStream(NativeCacheFile cacheFile)
    {
        this.cacheFile = requireNonNull(cacheFile, "cacheFile is null");
    }

    @Override
    public int available()
            throws IOException
    {
        ensureOpen();
        return 0;
    }

    @Override
    public long getPosition()
    {
        return position;
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        if (position > cacheFile.fileLength()) {
            throw new IOException("Cannot seek to %s. File size is %s: %s".formatted(position, cacheFile.fileLength(), cacheFile.location()));
        }
        this.position = position;
    }

    @Override
    public int read()
            throws IOException
    {
        ensureOpen();
        byte[] buffer = new byte[1];
        int read = read(buffer, 0, 1);
        if (read == -1) {
            return -1;
        }
        return buffer[0] & 0xFF;
    }

    @Override
    public int read(byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(offset, length, buffer.length);
        if (length == 0) {
            return 0;
        }
        if (position >= cacheFile.fileLength()) {
            return -1;
        }

        int bytesToRead = toIntExact(min(length, cacheFile.fileLength() - position));
        readFullyFromStream(position, buffer, offset, bytesToRead);
        position += bytesToRead;
        return bytesToRead;
    }

    private void readFullyFromStream(long position, byte[] buffer, int bufferOffset, int length)
            throws IOException
    {
        cacheFile.validateRead(position, length);
        int bytesRead = readCached(position, buffer, bufferOffset, length);
        readExternal(position + bytesRead, buffer, bufferOffset + bytesRead, length - bytesRead);
    }

    private int readCached(long position, byte[] buffer, int bufferOffset, int length)
            throws IOException
    {
        return cacheFile.readCached(position, length, () -> {
            int remaining = length;
            long currentPosition = position;
            int currentBufferOffset = bufferOffset;
            while (remaining > 0) {
                long pageIndex = currentPosition / cacheFile.pageSize();
                int pageOffset = (int) (currentPosition % cacheFile.pageSize());
                int expectedPageLength = cacheFile.expectedPageLength(pageIndex);
                int bytesToRead = min(remaining, expectedPageLength - pageOffset);
                if (readPageBuffer(pageIndex, pageOffset, bytesToRead, buffer, currentBufferOffset)) {
                    currentPosition += bytesToRead;
                    currentBufferOffset += bytesToRead;
                    remaining -= bytesToRead;
                    continue;
                }
                if (shouldBufferCacheRead(pageOffset, bytesToRead, expectedPageLength) && fillPageBuffer(pageIndex, expectedPageLength)) {
                    continue;
                }
                if (cacheFile.readPage(pageIndex, pageOffset, bytesToRead, expectedPageLength, buffer, currentBufferOffset)) {
                    currentPosition += bytesToRead;
                    currentBufferOffset += bytesToRead;
                    remaining -= bytesToRead;
                    continue;
                }

                return length - remaining;
            }
            return length;
        });
    }

    private static boolean shouldBufferCacheRead(int pageOffset, int bytesToRead, int expectedPageLength)
    {
        return bytesToRead < expectedPageLength - pageOffset;
    }

    private boolean readPageBuffer(long pageIndex, int pageOffset, int length, byte[] buffer, int bufferOffset)
    {
        if (pageBufferIndex != pageIndex || pageOffset + length > pageBufferLength) {
            return false;
        }
        System.arraycopy(pageBuffer, pageOffset, buffer, bufferOffset, length);
        return true;
    }

    private boolean fillPageBuffer(long pageIndex, int expectedPageLength)
            throws IOException
    {
        if (pageBuffer == null) {
            pageBuffer = new byte[cacheFile.pageSize()];
        }
        if (!cacheFile.readPage(pageIndex, 0, expectedPageLength, expectedPageLength, pageBuffer, 0)) {
            pageBufferIndex = -1;
            pageBufferLength = 0;
            return false;
        }
        pageBufferIndex = pageIndex;
        pageBufferLength = expectedPageLength;
        return true;
    }

    private void readExternal(long position, byte[] buffer, int bufferOffset, int length)
            throws IOException
    {
        if (length == 0) {
            return;
        }
        cacheFile.readExternalStream(position, length, () -> {
            NativeCacheFile.PageAlignedRead aligned = cacheFile.alignRead(position, length);
            byte[] readBuffer = new byte[aligned.length()];
            TrinoInputStream stream = getInputStream();
            if (stream.getPosition() != aligned.pageStart()) {
                stream.seek(aligned.pageStart());
            }
            int bytesRead = stream.readNBytes(readBuffer, 0, readBuffer.length);
            if (bytesRead != readBuffer.length) {
                throw new EOFException("Read %s of %s requested bytes: %s".formatted(bytesRead, readBuffer.length, cacheFile.location()));
            }
            cacheFile.writePages(aligned.pageStart(), readBuffer);
            System.arraycopy(readBuffer, aligned.pageOffset(), buffer, bufferOffset, length);
            cacheFile.recordExternalRead(bytesRead);
        });
    }

    private TrinoInputStream getInputStream()
            throws IOException
    {
        if (inputStream == null) {
            inputStream = cacheFile.delegate().newStream();
        }
        return inputStream;
    }

    @Override
    public long skip(long length)
            throws IOException
    {
        ensureOpen();
        length = Longs.constrainToRange(length, 0, cacheFile.fileLength() - position);
        position += length;
        return length;
    }

    @Override
    public void skipNBytes(long n)
            throws IOException
    {
        ensureOpen();
        if (n <= 0) {
            return;
        }
        long newPosition;
        try {
            newPosition = Math.addExact(position, n);
        }
        catch (ArithmeticException e) {
            throw new EOFException("Unable to skip %s bytes (position=%s, fileSize=%s): %s".formatted(n, position, cacheFile.fileLength(), cacheFile.location()));
        }
        if (newPosition > cacheFile.fileLength()) {
            throw new EOFException("Unable to skip %s bytes (position=%s, fileSize=%s): %s".formatted(n, position, cacheFile.fileLength(), cacheFile.location()));
        }
        position = newPosition;
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input stream closed: " + cacheFile.location());
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }
}
