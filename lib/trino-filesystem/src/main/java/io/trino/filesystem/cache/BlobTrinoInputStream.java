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

import io.trino.spi.cache.Blob;
import io.trino.spi.filesystem.Location;
import io.trino.spi.filesystem.TrinoInputStream;

import java.io.IOException;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class BlobTrinoInputStream
        extends TrinoInputStream
{
    private final Location location;
    private final Blob blob;
    private final long length;
    private long position;
    private boolean closed;

    BlobTrinoInputStream(Location location, Blob blob)
    {
        this.location = requireNonNull(location, "location is null");
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
            throw new IOException("Cannot seek to %s. Blob size is %s: %s".formatted(newPosition, length, location));
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
        byte[] single = new byte[1];
        blob.readFully(position, single, 0, 1);
        position++;
        return single[0] & 0xFF;
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
        blob.readFully(position, buffer, offset, toRead);
        position += toRead;
        return toRead;
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
        if (!closed) {
            closed = true;
            blob.close();
        }
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream closed: " + location);
        }
    }
}
