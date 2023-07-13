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
import io.airlift.slice.SliceInput;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

class MemoryInputStream
        extends TrinoInputStream
{
    private final Location location;
    private final SliceInput input;
    private final int length;
    private boolean closed;

    public MemoryInputStream(Location location, Slice data)
    {
        this.location = requireNonNull(location, "location is null");
        this.input = requireNonNull(data, "data is null").getInput();
        this.length = data.length();
    }

    @Override
    public int available()
            throws IOException
    {
        ensureOpen();
        return input.available();
    }

    @Override
    public long getPosition()
    {
        return input.position();
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        if (position > length) {
            throw new IOException("Cannot seek to %s. File size is %s: %s".formatted(position, length, location));
        }
        input.setPosition(position);
    }

    @Override
    public int read()
            throws IOException
    {
        ensureOpen();
        return input.read();
    }

    @Override
    public int read(byte[] destination, int destinationIndex, int length)
            throws IOException
    {
        ensureOpen();
        return input.read(destination, destinationIndex, length);
    }

    @Override
    public long skip(long length)
            throws IOException
    {
        ensureOpen();
        return input.skip(length);
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
            input.close();
        }
    }
}
