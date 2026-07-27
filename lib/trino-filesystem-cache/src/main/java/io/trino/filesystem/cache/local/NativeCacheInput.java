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

import io.trino.filesystem.TrinoInput;

import java.io.IOException;

import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class NativeCacheInput
        implements TrinoInput
{
    private final NativeCacheFile cacheFile;
    private TrinoInput input;
    private boolean closed;

    NativeCacheInput(NativeCacheFile cacheFile)
    {
        this.cacheFile = requireNonNull(cacheFile, "cacheFile is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int length)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(bufferOffset, length, buffer.length);
        cacheFile.validateRead(position, length);
        if (length == 0) {
            return;
        }

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

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int maxLength)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(bufferOffset, maxLength, buffer.length);

        int readSize = (int) min(cacheFile.fileLength(), maxLength);
        readFully(cacheFile.fileLength() - readSize, buffer, bufferOffset, readSize);
        return readSize;
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
            getInput().readFully(aligned.pageStart(), readBuffer, 0, readBuffer.length);
            cacheFile.writePages(aligned.pageStart(), readBuffer);
            System.arraycopy(readBuffer, aligned.pageOffset(), buffer, bufferOffset, length);
            cacheFile.recordExternalRead(readBuffer.length);
        });
    }

    private TrinoInput getInput()
            throws IOException
    {
        if (input == null) {
            input = cacheFile.delegate().newInput();
        }
        return input;
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input closed: " + cacheFile.location());
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            if (input != null) {
                input.close();
            }
        }
    }
}
