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
import io.trino.filesystem.TrinoFileSystemException;
import io.trino.filesystem.TrinoInputStream;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;

import static java.lang.Math.clamp;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

final class S3InputStream
        extends TrinoInputStream
{
    private static final int MAX_SKIP_BYTES = 1024 * 1024;

    private final Location location;
    private final S3Client client;
    private final GetObjectRequest request;
    private final Long length;

    private boolean closed;
    private ResponseInputStream<GetObjectResponse> in;
    private long streamPosition;
    private long nextReadPosition;

    public S3InputStream(Location location, S3Client client, GetObjectRequest request, Long length)
    {
        this.location = requireNonNull(location, "location is null");
        this.client = requireNonNull(client, "client is null");
        this.request = requireNonNull(request, "request is null");
        this.length = length;
    }

    @Override
    public int available()
            throws IOException
    {
        ensureOpen();
        if ((in != null) && (nextReadPosition == streamPosition)) {
            return getAvailable();
        }
        return 0;
    }

    @Override
    public long getPosition()
    {
        return nextReadPosition;
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        if ((length != null) && (position > length)) {
            throw new IOException("Cannot seek to %s. File size is %s: %s".formatted(position, length, location));
        }

        nextReadPosition = position;
    }

    @Override
    public int read()
            throws IOException
    {
        ensureOpen();
        seekStream(false);

        return reconnectStreamIfNecessary(() -> {
            int value = doRead();
            if (value >= 0) {
                streamPosition++;
                nextReadPosition++;
            }
            return value;
        });
    }

    @Override
    public int read(byte[] bytes, int offset, int length)
            throws IOException
    {
        ensureOpen();
        seekStream(false);

        return reconnectStreamIfNecessary(() -> {
            int n = doRead(bytes, offset, length);
            if (n > 0) {
                streamPosition += n;
                nextReadPosition += n;
            }
            return n;
        });
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        ensureOpen();

        long skipSize = clamp(n, 0, length != null ? length - nextReadPosition : Integer.MAX_VALUE);
        nextReadPosition += skipSize;
        return skipSize;
    }

    @Override
    public void skipNBytes(long n)
            throws IOException
    {
        ensureOpen();

        if (n <= 0) {
            return;
        }

        long position = nextReadPosition + n;
        if ((position < 0) || (length != null && position > length)) {
            throw new EOFException("Unable to skip %s bytes (position=%s, fileSize=%s): %s".formatted(n, nextReadPosition, length, location));
        }
        nextReadPosition = position;
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

    private <T> T reconnectStreamIfNecessary(IOExceptionThrowingSupplier<T> supplier)
            throws IOException
    {
        try {
            return supplier.get();
        }
        catch (IOException e) {
            seekStream(true);
        }

        return supplier.get();
    }

    private interface IOExceptionThrowingSupplier<T>
    {
        T get()
                throws IOException;
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input stream closed: " + location);
        }
    }

    private void seekStream(boolean forceStreamReset)
            throws IOException
    {
        if (!forceStreamReset && (in != null) && (nextReadPosition == streamPosition)) {
            // already at specified position
            return;
        }

        if (!forceStreamReset && (in != null) && (nextReadPosition > streamPosition)) {
            // seeking forwards
            long skip = nextReadPosition - streamPosition;
            if (skip <= max(getAvailable(), MAX_SKIP_BYTES)) {
                // already buffered or seek is small enough
                if (doSkip(skip) == skip) {
                    streamPosition = nextReadPosition;
                    return;
                }
            }
        }

        // close the stream and open at desired position
        streamPosition = nextReadPosition;
        closeStream();

        try {
            GetObjectRequest rangeRequest = request;
            if (nextReadPosition != 0) {
                String range = "bytes=%s-".formatted(nextReadPosition);
                rangeRequest = request.toBuilder().range(range).build();
            }
            in = client.getObject(rangeRequest);
            // a workaround for https://github.com/aws/aws-sdk-java-v2/issues/3538
            if (in.response().contentLength() != null && in.response().contentLength() == 0) {
                in = new ResponseInputStream<>(in.response(), nullInputStream());
            }
            streamPosition = nextReadPosition;
        }
        catch (NoSuchKeyException e) {
            var ex = new FileNotFoundException(location.toString());
            ex.initCause(e);
            throw ex;
        }
        catch (SdkException e) {
            throw new TrinoFileSystemException("Failed to open S3 file: " + location, e);
        }
    }

    private void closeStream()
    {
        if (in == null) {
            return;
        }

        try (var _ = in) {
            in.abort();
        }
        catch (AbortedException | IOException _) {
        }
        finally {
            in = null;
        }
    }

    private int getAvailable()
            throws IOException
    {
        try {
            return in.available();
        }
        catch (AbortedException e) {
            throw new InterruptedIOException();
        }
    }

    private long doSkip(long n)
            throws IOException
    {
        try {
            return in.skip(n);
        }
        catch (AbortedException e) {
            throw new InterruptedIOException();
        }
    }

    private int doRead()
            throws IOException
    {
        try {
            return in.read();
        }
        catch (AbortedException e) {
            throw new InterruptedIOException();
        }
    }

    private int doRead(byte[] bytes, int offset, int length)
            throws IOException
    {
        try {
            return in.read(bytes, offset, length);
        }
        catch (AbortedException e) {
            throw new InterruptedIOException();
        }
    }
}
