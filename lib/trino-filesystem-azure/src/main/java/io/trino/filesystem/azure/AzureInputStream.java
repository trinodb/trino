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
package io.trino.filesystem.azure;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.options.BlobInputStreamOptions;
import com.azure.storage.blob.specialized.BlobInputStream;
import io.trino.filesystem.TrinoInputStream;

import java.io.EOFException;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.primitives.Longs.constrainToRange;
import static io.trino.filesystem.azure.AzureUtils.handleAzureException;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

class AzureInputStream
        extends TrinoInputStream
{
    private final AzureLocation location;
    private final BlobClient blobClient;
    private final int readBlockSizeBytes;
    private final long fileSize;

    private BlobInputStream stream;
    private long currentPosition;
    private long nextPosition;
    private boolean closed;

    public AzureInputStream(AzureLocation location, BlobClient blobClient, int readBlockSizeBytes)
            throws IOException
    {
        this.location = requireNonNull(location, "location is null");
        this.blobClient = requireNonNull(blobClient, "blobClient is null");
        checkArgument(readBlockSizeBytes >= 0, "readBlockSizeBytes is negative");
        this.readBlockSizeBytes = readBlockSizeBytes;
        openStream(0);
        fileSize = stream.getProperties().getBlobSize();
    }

    @Override
    public int available()
            throws IOException
    {
        ensureOpen();
        repositionStream();
        return stream.available();
    }

    @Override
    public long getPosition()
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
            int value = stream.read();
            if (value >= 0) {
                currentPosition++;
                nextPosition++;
            }
            return value;
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "reading file", location);
        }
    }

    @Override
    public int read(byte[] buffer, int offset, int length)
            throws IOException
    {
        checkFromIndexSize(offset, length, buffer.length);

        ensureOpen();
        repositionStream();

        try {
            int readSize = stream.read(buffer, offset, length);
            if (readSize > 0) {
                currentPosition += readSize;
                nextPosition += readSize;
            }
            return readSize;
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "reading file", location);
        }
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        ensureOpen();

        long skipSize = constrainToRange(n, 0, fileSize - nextPosition);
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

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Output stream closed: " + location);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            try {
                stream.close();
            }
            catch (RuntimeException e) {
                throw handleAzureException(e, "closing file", location);
            }
        }
    }

    private void openStream(long offset)
            throws IOException
    {
        try {
            BlobInputStreamOptions options = new BlobInputStreamOptions()
                    .setRange(new BlobRange(offset))
                    .setBlockSize(readBlockSizeBytes);
            stream = blobClient.openInputStream(options);
            currentPosition = offset;
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "reading file", location);
        }
    }

    private void repositionStream()
            throws IOException
    {
        if (nextPosition == currentPosition) {
            return;
        }

        if (nextPosition > currentPosition) {
            long bytesToSkip = nextPosition - currentPosition;
            // this always works because the client simply moves a counter forward and
            // preforms the reposition on the next actual read
            stream.skipNBytes(bytesToSkip);
        }
        else {
            stream.close();
            openStream(nextPosition);
        }

        currentPosition = nextPosition;
    }
}
