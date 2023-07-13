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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static io.trino.filesystem.local.LocalUtils.handleException;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

class LocalOutputStream
        extends OutputStream
{
    private final Location location;
    private final OutputStream stream;
    private boolean closed;

    public LocalOutputStream(Location location, OutputStream stream)
    {
        this.location = requireNonNull(location, "location is null");
        this.stream = new BufferedOutputStream(requireNonNull(stream, "stream is null"), 4 * 1024);
    }

    @Override
    public void write(int b)
            throws IOException
    {
        ensureOpen();
        try {
            stream.write(b);
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        checkFromIndexSize(offset, length, buffer.length);

        ensureOpen();
        try {
            stream.write(buffer, offset, length);
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
    }

    @Override
    public void flush()
            throws IOException
    {
        ensureOpen();
        try {
            stream.flush();
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
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
            catch (IOException e) {
                throw handleException(location, e);
            }
        }
    }
}
