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

import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.CacheManager;
import alluxio.conf.AlluxioConfiguration;
import com.google.common.primitives.Longs;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.EOFException;
import java.io.IOException;

import static com.google.common.base.Verify.verify;
import static com.google.common.primitives.Ints.saturatedCast;
import static io.trino.filesystem.alluxio.AlluxioTracing.withTracing;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_LOCATION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_SIZE;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_KEY;
import static java.lang.Integer.max;
import static java.lang.Math.addExact;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public class AlluxioInputStream
        extends TrinoInputStream
{
    private final TrinoInputFile inputFile;
    private final long fileLength;
    private final Location location;
    private final AlluxioCacheStats statistics;
    private final String key;
    private final AlluxioInputHelper helper;
    private final Tracer tracer;
    private TrinoInputStream externalStream;
    private long position;
    private boolean closed;

    public AlluxioInputStream(Tracer tracer, TrinoInputFile inputFile, String key, URIStatus status, CacheManager cacheManager, AlluxioConfiguration configuration, AlluxioCacheStats statistics)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.inputFile = requireNonNull(inputFile, "inputFile is null");
        this.fileLength = requireNonNull(status, "status is null").getLength();
        this.location = inputFile.location();
        this.statistics = requireNonNull(statistics, "statistics is null");
        this.key = requireNonNull(key, "key is null");
        this.helper = new AlluxioInputHelper(tracer, inputFile.location(), key, status, cacheManager, configuration, statistics);
    }

    @Override
    public int available()
            throws IOException
    {
        ensureOpen();

        return saturatedCast(fileLength - position);
    }

    @Override
    public long getPosition()
    {
        return position;
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Output stream closed: " + location);
        }
    }

    @Override
    public int read()
            throws IOException
    {
        ensureOpen();

        byte[] bytes = new byte[1];
        int n = read(bytes, 0, 1);
        if (n == 1) {
            // Converts the byte to an unsigned byte, an integer in the range 0 to 255
            return bytes[0] & 0xff;
        }
        if (n == -1) {
            return -1;
        }
        throw new IOException(format("%d bytes read", n));
    }

    @Override
    public int read(byte[] bytes, int offset, int length)
            throws IOException
    {
        ensureOpen();

        checkFromIndexSize(offset, length, bytes.length);
        if (length == 0) {
            return 0;
        }
        if (position >= fileLength) {
            return -1;
        }
        int bytesRead = doRead(bytes, offset, toIntExact(min(fileLength - position, length)));
        position += bytesRead;
        return bytesRead;
    }

    private int doRead(byte[] bytes, int offset, int length)
            throws IOException
    {
        int bytesRead = helper.doCacheRead(position, bytes, offset, length);
        return addExact(bytesRead, doExternalRead0(position + bytesRead, bytes, offset + bytesRead, length - bytesRead));
    }

    private int doExternalRead0(long readPosition, byte[] buffer, int offset, int length)
            throws IOException
    {
        if (length == 0) {
            return 0;
        }

        Span span = tracer.spanBuilder("Alluxio.readExternal")
                .setAttribute(CACHE_KEY, key)
                .setAttribute(CACHE_FILE_LOCATION, inputFile.location().toString())
                .setAttribute(CACHE_FILE_READ_SIZE, (long) length)
                .setAttribute(CACHE_FILE_READ_POSITION, readPosition)
                .startSpan();

        return withTracing(span, () -> doExternalReadInternal(readPosition, buffer, offset, length));
    }

    private int doExternalReadInternal(long readPosition, byte[] buffer, int offset, int length)
            throws IOException
    {
        verify(length > 0, "zero-length or negative read");
        AlluxioInputHelper.PageAlignedRead aligned = helper.alignRead(readPosition, length);
        if (externalStream == null) {
            externalStream = inputFile.newStream();
        }
        externalStream.seek(aligned.pageStart());
        byte[] readBuffer = new byte[aligned.length()];
        int externalBytesRead = externalStream.readNBytes(readBuffer, 0, aligned.length());
        if (externalBytesRead < 0) {
            throw new IOException("Unexpected end of stream");
        }
        verify(aligned.length() == externalBytesRead, "invalid number of external bytes read");
        helper.putCache(aligned.pageStart(), aligned.pageEnd(), readBuffer, externalBytesRead);
        int bytesToCopy = min(length, max(externalBytesRead - aligned.pageOffset(), 0));
        System.arraycopy(readBuffer, aligned.pageOffset(), buffer, offset, bytesToCopy);
        statistics.recordExternalRead(externalBytesRead);
        return bytesToCopy;
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        ensureOpen();

        n = Longs.constrainToRange(n, 0, fileLength - position);
        position += n;
        return n;
    }

    @Override
    public void skipNBytes(long n)
            throws IOException
    {
        ensureOpen();

        if (n <= 0) {
            return;
        }

        long position;
        try {
            position = addExact(this.position, n);
        }
        catch (ArithmeticException e) {
            throw new EOFException("Unable to skip %s bytes (position=%s, fileSize=%s): %s".formatted(n, this.position, fileLength, location));
        }
        if (position > fileLength) {
            throw new EOFException("Unable to skip %s bytes (position=%s, fileSize=%s): %s".formatted(n, this.position, fileLength, location));
        }
        this.position = position;
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

        this.position = position;
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            if (externalStream != null) {
                externalStream.close();
                externalStream = null;
            }
        }
    }
}
