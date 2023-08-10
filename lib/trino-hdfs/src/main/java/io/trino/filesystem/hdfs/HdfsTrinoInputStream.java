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
package io.trino.filesystem.hdfs;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;

import static io.trino.filesystem.hdfs.HdfsFileSystem.withCause;
import static java.util.Objects.requireNonNull;

class HdfsTrinoInputStream
        extends TrinoInputStream
{
    private final Location location;
    private final FSDataInputStream stream;
    private boolean closed;

    HdfsTrinoInputStream(Location location, FSDataInputStream stream)
    {
        this.location = requireNonNull(location, "location is null");
        this.stream = requireNonNull(stream, "stream is null");
    }

    @Override
    public int available()
            throws IOException
    {
        ensureOpen();
        try {
            return stream.available();
        }
        catch (IOException e) {
            throw new IOException("Get available for file %s failed: %s".formatted(location, e.getMessage()), e);
        }
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
        try {
            stream.seek(position);
        }
        catch (IOException e) {
            throw new IOException("Seek to position %s for file %s failed: %s".formatted(position, location, e.getMessage()), e);
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
        catch (FileNotFoundException e) {
            throw withCause(new FileNotFoundException("File %s not found: %s".formatted(location, e.getMessage())), e);
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
        try {
            return stream.read(b, off, len);
        }
        catch (FileNotFoundException e) {
            throw withCause(new FileNotFoundException("File %s not found: %s".formatted(location, e.getMessage())), e);
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
            throw new IOException("Output stream closed: " + location);
        }
    }
}
