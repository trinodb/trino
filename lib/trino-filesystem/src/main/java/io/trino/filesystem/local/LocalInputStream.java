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
package io.trino.filesystem.local;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputStream;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

class LocalInputStream
        extends TrinoInputStream
{
    private final Location location;
    private final File file;
    private final long fileLength;

    private InputStream input;
    private long position;
    private boolean closed;

    public LocalInputStream(Location location, File file)
            throws FileNotFoundException
    {
        this.location = requireNonNull(location, "location is null");
        this.file = requireNonNull(file, "file is null");
        this.fileLength = file.length();
        this.input = new BufferedInputStream(new FileInputStream(file), 4 * 1024);
    }

    @Override
    public int available()
            throws IOException
    {
        ensureOpen();
        return Ints.saturatedCast(fileLength - position);
    }

    @Override
    public long getPosition()
    {
        return position;
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        if (position > fileLength) {
            throw new IOException("Cannot seek to %s. File size is %s: %s".formatted(position, fileLength, location));
        }

        // for negative seek, reopen the file
        if (position < this.position) {
            input.close();
            // it is possible to seek backwards using the original file input stream, but this seems simpler
            input = new BufferedInputStream(new FileInputStream(file), 4 * 1024);
            this.position = 0;
        }

        while (position > this.position) {
            long skip = input.skip(position - this.position);
            if (skip < 0) {
                throw new IOException("Skip returned a negative size");
            }

            if (skip > 0) {
                this.position += skip;
            }
            else {
                if (input.read() == -1) {
                    // This should not happen unless the file size changed
                    throw new EOFException();
                }
                this.position++;
            }
        }

        if (this.position != position) {
            throw new IOException("Seek to %s failed. Current position is %s: %s".formatted(position, this.position, location));
        }
    }

    @Override
    public int read()
            throws IOException
    {
        ensureOpen();
        int read = input.read();
        if (read != -1) {
            position++;
        }
        return read;
    }

    @Override
    public int read(byte[] destination, int destinationIndex, int length)
            throws IOException
    {
        ensureOpen();
        int read = input.read(destination, destinationIndex, length);
        if (read > 0) {
            position += read;
        }
        return read;
    }

    @Override
    public long skip(long length)
            throws IOException
    {
        ensureOpen();

        length = Longs.constrainToRange(length, 0, fileLength - position);
        seek(position + length);
        return length;
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
