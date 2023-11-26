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
import com.google.cloud.storage.Storage;
import io.trino.filesystem.TrinoInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.filesystem.gcs.GcsUtils.getBlobOrThrow;
import static io.trino.filesystem.gcs.GcsUtils.getReadChannel;
import static io.trino.filesystem.gcs.GcsUtils.handleGcsException;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class GcsInput
        implements TrinoInput
{
    private final GcsLocation location;
    private final Storage storage;
    private final int readBlockSize;
    private OptionalLong length;
    private boolean closed;

    public GcsInput(GcsLocation location, Storage storage, int readBlockSize, OptionalLong length)
    {
        this.location = requireNonNull(location, "location is null");
        this.storage = requireNonNull(storage, "storage is null");
        checkArgument(readBlockSize >= 0, "readBlockSize is negative");
        this.readBlockSize = readBlockSize;
        this.length = requireNonNull(length, "length is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
        if (bufferLength == 0) {
            return;
        }

        try (ReadChannel readChannel = getReadChannel(getBlobOrThrow(storage, location), location, position, readBlockSize, length)) {
            int readSize = readNBytes(readChannel, buffer, bufferOffset, bufferLength);
            if (readSize != bufferLength) {
                throw new EOFException("End of file reached before reading fully: " + location);
            }
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "reading file", location);
        }
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
        Blob blob = getBlobOrThrow(storage, location);
        long offset = Math.max(0, length.orElse(blob.getSize()) - bufferLength);
        try (ReadChannel readChannel = getReadChannel(blob, location, offset, readBlockSize, length)) {
            return readNBytes(readChannel, buffer, bufferOffset, bufferLength);
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "reading file", location);
        }
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input stream closed: " + location);
        }
    }

    @Override
    public void close()
    {
        closed = true;
    }

    @Override
    public String toString()
    {
        return location.toString();
    }

    private int readNBytes(ReadChannel readChannel, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ByteBuffer wrappedBuffer = ByteBuffer.wrap(buffer, bufferOffset, bufferLength);
        int readSize = 0;
        while (readSize < bufferLength) {
            int bytesRead = readChannel.read(wrappedBuffer);
            if (bytesRead == -1) {
                break;
            }
            readSize += bytesRead;
        }
        return readSize;
    }
}
