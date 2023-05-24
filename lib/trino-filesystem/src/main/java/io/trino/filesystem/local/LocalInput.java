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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static io.trino.filesystem.local.LocalUtils.handleException;
import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

class LocalInput
        implements TrinoInput
{
    private final Location location;
    private final File file;
    private final RandomAccessFile input;
    private boolean closed;

    public LocalInput(Location location, File file)
            throws IOException
    {
        this.location = requireNonNull(location, "location is null");
        this.file = requireNonNull(file, "file is null");
        this.input = new RandomAccessFile(file, "r");
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
        if (position >= file.length()) {
            throw new EOFException("Cannot read at %s. File size is %s: %s".formatted(position, file.length(), location));
        }

        try {
            input.seek(position);
            input.readFully(buffer, bufferOffset, bufferLength);
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);

        int readSize = (int) min(file.length(), bufferLength);
        readFully(file.length() - readSize, buffer, bufferOffset, readSize);
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
            throws IOException
    {
        closed = true;
        input.close();
    }

    @Override
    public String toString()
    {
        return file.getPath();
    }
}
