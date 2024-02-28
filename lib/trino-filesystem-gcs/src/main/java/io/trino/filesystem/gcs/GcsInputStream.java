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

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.common.primitives.Ints;
import io.trino.filesystem.TrinoInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.OptionalLong;

import static io.trino.filesystem.gcs.GcsUtils.getReadChannel;
import static io.trino.filesystem.gcs.GcsUtils.handleGcsException;
import static java.lang.Math.clamp;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public class GcsInputStream
        extends TrinoInputStream
{
    private final GcsLocation location;
    private final Blob blob;
    private final int readBlockSizeBytes;
    private final long fileSize;
    private final OptionalLong predeclaredLength;
    private ReadChannel readChannel;
    // Used for read(). Similar to sun.nio.ch.ChannelInputStream
    private final ByteBuffer readBuffer = ByteBuffer.allocate(1);
    private long currentPosition;
    private long nextPosition;
    private boolean closed;

    public GcsInputStream(GcsLocation location, Blob blob, int readBlockSizeBytes, OptionalLong predeclaredLength)
            throws IOException
    {
        this.location = requireNonNull(location, "location is null");
        this.blob = requireNonNull(blob, "blob is null");
        this.readBlockSizeBytes = readBlockSizeBytes;
        this.predeclaredLength = requireNonNull(predeclaredLength, "predeclaredLength is null");
        this.fileSize = predeclaredLength.orElse(blob.getSize());
        openStream();
    }

    @Override
    public int available()
            throws IOException
    {
        ensureOpen();
        repositionStream();
        return Ints.saturatedCast(fileSize - currentPosition);
    }

    @Override
    public long getPosition()
            throws IOException
    {
        return nextPosition;
    }

    @Override
    public void seek(long newPosition)
            throws IOException
    {
        ensureOpen();
        if (newPosition < 0) {
            throw new IOException("Negative seek offset");
        }
        if (newPosition > fileSize) {
            throw new IOException("Cannot seek to %s. File size is %s: %s".formatted(newPosition, fileSize, location));
        }
        nextPosition = newPosition;
    }

    @Override
    public int read()
            throws IOException
    {
        ensureOpen();
        repositionStream();
        try {
            // Similar to sun.nio.ch.ChannelInputStream::read but uses a byte buffer and is not synchronized
            readBuffer.position(0);
            int bytesRead = readChannel.read(readBuffer);

            if (bytesRead == 1) {
                currentPosition++;
                nextPosition++;
                return readBuffer.get(0) & 0xff;
            }
            return -1;
        }
        catch (IOException e) {
            throw new IOException("Error reading file: " + location, e);
        }
    }

    @Override
    public int read(byte[] buffer, int offset, int length)
            throws IOException
    {
        checkFromIndexSize(offset, length, buffer.length);
        ensureOpen();
        repositionStream();
        ByteBuffer wrappedBuffer = ByteBuffer.wrap(buffer, offset, length);
        try {
            int readSize = readChannel.read(wrappedBuffer);
            if (readSize > 0) {
                currentPosition += readSize;
                nextPosition += readSize;
            }
            return readSize;
        }
        catch (IOException e) {
            throw new IOException("Error reading file: " + location, e);
        }
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        ensureOpen();
        long skipSize = clamp(n, 0, fileSize - nextPosition);
        nextPosition += skipSize;
        return skipSize;
    }

    @Override
    public void skipNBytes(long n)
            throws IOException
    {
        ensureOpen();
        if (n <= 0) {
            return;
        }

        long position = nextPosition + n;
        if ((position < 0) || (position > fileSize)) {
            throw new EOFException("Unable to skip %s bytes (position=%s, fileSize=%s): %s".formatted(n, nextPosition, fileSize, location));
        }
        nextPosition = position;
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            try {
                readChannel.close();
            }
            catch (RuntimeException e) {
                throw handleGcsException(e, "closing file", location);
            }
        }
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Output stream closed: " + location);
        }
    }

    private void openStream()
            throws IOException
    {
        try {
            this.readChannel = getReadChannel(blob, location, 0L, readBlockSizeBytes, predeclaredLength);
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "reading file", location);
        }
    }

    private void repositionStream()
            throws IOException
    {
        if (nextPosition == currentPosition) {
            return;
        }
        readChannel.seek(nextPosition);
        currentPosition = nextPosition;
    }
}
