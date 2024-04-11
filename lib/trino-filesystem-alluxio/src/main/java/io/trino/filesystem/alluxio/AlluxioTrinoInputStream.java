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
package io.trino.filesystem.alluxio;

import alluxio.client.file.FileInStream;
import alluxio.client.file.URIStatus;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;

import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public final class AlluxioTrinoInputStream
        extends TrinoInputStream
{
    private final Location location;
    private final FileInStream stream;
    private final URIStatus fileStatus;

    private boolean closed;

    public AlluxioTrinoInputStream(Location location, FileInStream stream, URIStatus fileStatus)
    {
        this.location = requireNonNull(location, "location is null");
        this.stream = requireNonNull(stream, "stream is null");
        this.fileStatus = requireNonNull(fileStatus, "fileStatus is null");
    }

    @Override
    public long getPosition()
            throws IOException
    {
        ensureOpen();
        try {
            return stream.getPos();
        }
        catch (IOException e) {
            throw new IOException("Get position for file %s failed: %s".formatted(location, e.getMessage()), e);
        }
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        if (position > fileStatus.getLength()) {
            throw new IOException("Cannot seek to %s. File size is %s: %s".formatted(position, fileStatus.getLength(), location));
        }
        try {
            stream.seek(position);
        }
        catch (IOException e) {
            throw new IOException("Cannot seek to %s: %s".formatted(position, location));
        }
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        ensureOpen();
        try {
            return stream.skip(n);
        }
        catch (IOException e) {
            throw new IOException("Skipping %s bytes of file %s failed: %s".formatted(n, location, e.getMessage()), e);
        }
    }

    @Override
    public int read()
            throws IOException
    {
        ensureOpen();
        try {
            return stream.read();
        }
        catch (IOException e) {
            throw new IOException("Read of file %s failed: %s".formatted(location, e.getMessage()), e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(off, len, b.length);
        try {
            return stream.read(b, off, len);
        }
        catch (IOException e) {
            throw new IOException("Read of file %s failed: %s".formatted(location, e.getMessage()), e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
        stream.close();
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input stream closed: " + location);
        }
    }

    @Override
    public int available()
            throws IOException
    {
        ensureOpen();
        return super.available();
    }
}
