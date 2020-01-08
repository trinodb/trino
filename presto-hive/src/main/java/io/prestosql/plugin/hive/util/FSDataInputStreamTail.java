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
package io.prestosql.plugin.hive.util;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class FSDataInputStreamTail
{
    public static final int MAX_SUPPORTED_PADDING_BYTES = 64;
    private static final int MAXIMUM_MIN_READ_LENGTH = Integer.MAX_VALUE - (MAX_SUPPORTED_PADDING_BYTES + 1);
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final byte[] tailBuffer;
    private final long fileSize;
    private final int length;

    private FSDataInputStreamTail(long fileSize, byte[] tailBuffer, int length)
    {
        this.tailBuffer = requireNonNull(tailBuffer, "tailBuffer is null");
        this.fileSize = fileSize;
        this.length = length;
        checkArgument(fileSize >= 0, "fileSize must be >= 0, found: %s", fileSize);
        checkArgument(length <= fileSize, "length must be <= fileSize, found: %s > %s", length, fileSize);
        checkPositionIndexes(0, length, tailBuffer.length);
    }

    public static FSDataInputStreamTail readTail(Path path, long paddedFileSize, FSDataInputStream inputStream, int minimumLength)
            throws IOException
    {
        checkArgument(minimumLength >= 0 && minimumLength <= MAXIMUM_MIN_READ_LENGTH, "minimumLength too large, found: %s", minimumLength);
        long readSize = min(paddedFileSize, (minimumLength + MAX_SUPPORTED_PADDING_BYTES));
        long position = paddedFileSize - readSize;
        // Actual read will be 1 byte larger to ensure we encounter an EOF where expected
        byte[] buffer = new byte[toIntExact(readSize + 1)];
        int bytesRead = 0;
        long startPos = inputStream.getPos();
        try {
            inputStream.seek(position);
            while (bytesRead < buffer.length) {
                int n = inputStream.read(buffer, bytesRead, buffer.length - bytesRead);
                if (n < 0) {
                    break;
                }
                bytesRead += n;
            }
        }
        finally {
            inputStream.seek(startPos);
        }
        if (bytesRead > readSize) {
            throw rejectInvalidFileSize(path, paddedFileSize);
        }
        return new FSDataInputStreamTail(position + bytesRead, bytesRead == 0 ? EMPTY_BYTES : buffer, bytesRead);
    }

    public static long readTailForFileSize(Path path, long paddedFileSize, FSDataInputStream inputStream)
            throws IOException
    {
        long position = max(paddedFileSize - MAX_SUPPORTED_PADDING_BYTES, 0);
        long maxEOFAt = paddedFileSize + 1;
        long startPos = inputStream.getPos();
        try {
            inputStream.seek(position);
            int c;
            while (position < maxEOFAt) {
                c = inputStream.read();
                if (c == -1) {
                    return position;
                }
                position++;
            }
            throw rejectInvalidFileSize(path, paddedFileSize);
        }
        finally {
            inputStream.seek(startPos);
        }
    }

    private static PrestoException rejectInvalidFileSize(Path path, long reportedSize)
    {
        throw new PrestoException(HIVE_FILESYSTEM_ERROR, format("Incorrect fileSize %s for file %s, end of stream not reached", reportedSize, path));
    }

    public long getFileSize()
    {
        return fileSize;
    }

    public Slice getTailSlice()
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        return Slices.wrappedBuffer(tailBuffer, 0, length);
    }

    public FSDataInputStream replaceStreamWithContentsIfComplete(FSDataInputStream originalStream)
            throws IOException
    {
        if (fileSize == length) {
            originalStream.close();
            return new FSDataInputStream(new ByteArraySeekablePositionedReadableInputStream(tailBuffer, 0, length));
        }
        return originalStream;
    }

    @VisibleForTesting
    static final class ByteArraySeekablePositionedReadableInputStream
            extends ByteArrayInputStream
            implements Seekable, PositionedReadable
    {
        ByteArraySeekablePositionedReadableInputStream(byte[] array, int offset, int length)
        {
            super(array, offset, length);
        }

        @Override
        public void seek(long pos)
                throws IOException
        {
            if (pos < 0) {
                throw new IOException(FSExceptionMessages.NEGATIVE_SEEK);
            }
            if (pos > count) {
                throw new IOException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
            }
            // Should never overflow since pos <= count
            this.pos = toIntExact(pos);
        }

        @Override
        public long getPos()
        {
            return pos;
        }

        @Override
        public boolean seekToNewSource(long targetPos)
        {
            return false;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length)
        {
            checkArgument(position >= 0 && position < Integer.MAX_VALUE, "Invalid read position: %s", position);
            int posInt = toIntExact(position);
            if (posInt >= count) {
                return -1;
            }
            int readSize = min(length, count - posInt);
            if (readSize <= 0) {
                return 0;
            }
            System.arraycopy(this.buf, posInt, buffer, offset, readSize);
            return readSize;
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length)
                throws IOException
        {
            int bytes = read(position, buffer, offset, length);
            if (bytes < length) {
                throw new EOFException(FSExceptionMessages.EOF_IN_READ_FULLY);
            }
        }

        @Override
        public void readFully(long position, byte[] buffer)
                throws IOException
        {
            readFully(position, buffer, 0, buffer.length);
        }
    }
}
