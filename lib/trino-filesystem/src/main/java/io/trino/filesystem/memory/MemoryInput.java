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
package io.trino.filesystem.memory;

import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;

import java.io.EOFException;
import java.io.IOException;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

class MemoryInput
        implements TrinoInput
{
    private final Location location;
    private final Slice data;
    private boolean closed;

    public MemoryInput(Location location, Slice data)
    {
        this.location = requireNonNull(location, "location is null");
        this.data = requireNonNull(data, "data is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        if (position + bufferLength > data.length()) {
            throw new EOFException("Cannot read %s bytes at %s. File size is %s: %s".formatted(position, bufferLength, data.length(), location));
        }

        data.getBytes(toIntExact(position), buffer, bufferOffset, bufferLength);
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);

        int readSize = min(data.length(), bufferLength);
        readFully(data.length() - readSize, buffer, bufferOffset, readSize);
        return readSize;
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
    {
        closed = true;
    }

    @Override
    public String toString()
    {
        return location.toString();
    }
}
