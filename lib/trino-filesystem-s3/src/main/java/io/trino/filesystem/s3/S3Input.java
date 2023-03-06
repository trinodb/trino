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
package io.trino.filesystem.s3;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;

import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class S3Input
        implements TrinoInput
{
    private final Location location;
    private final S3Client client;
    private final GetObjectRequest request;
    private boolean closed;

    public S3Input(Location location, S3Client client, GetObjectRequest request)
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

        String range = "bytes=%s-%s".formatted(position, (position + length) - 1);
        GetObjectRequest rangeRequest = request.toBuilder().range(range).build();

        try (InputStream in = getObject(rangeRequest)) {
            int n = readNBytes(in, buffer, offset, length);
            if (n < length) {
                throw new EOFException("Read %s of %s requested bytes: %s".formatted(n, length, location));
            }
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

        String range = "bytes=-%s".formatted(length);
        GetObjectRequest rangeRequest = request.toBuilder().range(range).build();

        try (InputStream in = getObject(rangeRequest)) {
            return readNBytes(in, buffer, offset, length);
        }
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

    private InputStream getObject(GetObjectRequest request)
            throws IOException
    {
        try {
            return client.getObject(request);
        }
        catch (NoSuchKeyException e) {
            throw new FileNotFoundException(location.toString());
        }
        catch (SdkException e) {
            throw new IOException("Failed to open S3 file: " + location, e);
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
