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

import com.amazonaws.AbortedException;
import com.amazonaws.services.s3.model.ScanRange;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;

import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public class S3SelectTrinoInput
        implements TrinoInput
{
    private final Location location;
    private final TrinoS3SelectClient client;
    private final SelectObjectContentRequest request;
    private boolean closed;

    public S3SelectTrinoInput(Location location, TrinoS3SelectClient client, SelectObjectContentRequest request)
    {
        this.location = requireNonNull(location, "location is null");
        this.client = requireNonNull(client, "client is null");
        this.request = requireNonNull(request, "request is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        checkFromIndexSize(offset, length, buffer.length);
        if (length == 0) {
            return;
        }

        SelectObjectContentRequest contentRequest = request.withScanRange(new ScanRange()
                .withStart(position)
                .withEnd(position + length));
        InputStream inputStream = client.getRecordsContent(contentRequest);
        int n = readNBytes(inputStream, buffer, offset, length);
        if (n < length) {
            throw new EOFException("Read %s of %s requested bytes: %s".formatted(n, length, location));
        }
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(offset, length, buffer.length);
        if (length == 0) {
            return 0;
        }

        long start = request.getScanRange().getStart();
        SelectObjectContentRequest contentRequest = request.withScanRange(new ScanRange()
                .withStart(start)
                .withEnd(start + length));
        return readNBytes(client.getRecordsContent(contentRequest), buffer, offset, length);
    }

    @Override
    public void close()
    {
        closed = true;
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input closed: " + location);
        }
    }

    private static int readNBytes(InputStream in, byte[] buffer, int offset, int length)
            throws IOException
    {
        try {
            return in.readNBytes(buffer, offset, length);
        }
        catch (AbortedException e) {
            throw new InterruptedIOException();
        }
    }
}
