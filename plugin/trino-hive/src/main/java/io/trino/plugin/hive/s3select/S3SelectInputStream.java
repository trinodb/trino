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
package io.trino.plugin.hive.s3select;

import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.google.common.primitives.Ints;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

public class S3SelectInputStream
        extends TrinoInputStream
{
    private final Location location;
    private final TrinoS3SelectClient client;
    private final SelectObjectContentRequest request;
    private final long length;

    private InputStream input;
    private long position;
    private boolean closed;

    public S3SelectInputStream(Location location, TrinoS3SelectClient client, SelectObjectContentRequest request, long length)
    {
        this.location = requireNonNull(location, "location is null");
        this.client = requireNonNull(client, "client is null");
        this.request = requireNonNull(request, "request is null");
        this.length = length;

        this.input = client.getRecordsContent(request);
    }

    @Override
    public int available()
            throws IOException
    {
        ensureOpen();
        return Ints.saturatedCast(length - position);
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
        if (position > length) {
            throw new IOException("Cannot seek to %s. File size is %s: %s".formatted(position, length, location));
        }

        // for negative seek, reopen the file
        if (position < this.position) {
            input.close();
            // it is possible to seek backwards using the original file input stream, but this seems simpler
            input = client.getRecordsContent(request);
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
    public int read(byte[] bytes, int offset, int length)
            throws IOException
    {
        ensureOpen();
        int read = input.read(bytes, offset, length);
        if (read > 0) {
            position += read;
        }
        return read;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        closeStream();
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input stream closed: " + location);
        }
    }

    private void closeStream()
    {
        if (input == null) {
            return;
        }

        try (var ignored = input) {
            client.close();
        }
        catch (IOException ignored) {
        }
        finally {
            input = null;
        }
    }
}
