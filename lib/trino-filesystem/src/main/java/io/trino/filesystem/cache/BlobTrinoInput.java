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
import io.trino.spi.filesystem.TrinoInput;

import java.io.IOException;

import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class BlobTrinoInput
        implements TrinoInput
{
    private final Location location;
    private final Blob blob;
    private boolean closed;

    BlobTrinoInput(Location location, Blob blob)
    {
        this.location = requireNonNull(location, "location is null");
        this.blob = requireNonNull(blob, "blob is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        blob.readFully(position, buffer, offset, length);
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(offset, length, buffer.length);
        int readSize = (int) min(blob.length(), length);
        blob.readFully(blob.length() - readSize, buffer, offset, readSize);
        return readSize;
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
            throw new IOException("Input closed: " + location);
        }
    }

    @Override
    public String toString()
    {
        return location.toString();
    }
}
