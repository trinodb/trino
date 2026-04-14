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
package io.trino.blob.cache.alluxio;

import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.CacheManager;
import alluxio.conf.AlluxioConfiguration;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobSource;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class AlluxioBlob
        implements Blob
{
    private final BlobSource source;
    private final String description;
    private final long length;
    private final AlluxioCacheStats statistics;
    private final AlluxioInputHelper helper;

    AlluxioBlob(
            Tracer tracer,
            BlobSource source,
            String cacheKey,
            URIStatus status,
            CacheManager cacheManager,
            AlluxioConfiguration configuration,
            AlluxioCacheStats statistics)
    {
        this.source = requireNonNull(source, "source is null");
        this.description = source.toString();
        this.length = requireNonNull(status, "status is null").getLength();
        this.statistics = requireNonNull(statistics, "statistics is null");
        this.helper = new AlluxioInputHelper(tracer, description, cacheKey, status, cacheManager, configuration, statistics);
    }

    @Override
    public long length()
    {
        return length;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int bufferLength)
            throws IOException
    {
        checkFromIndexSize(offset, bufferLength, buffer.length);
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        if (bufferLength == 0) {
            return;
        }

        int bytesRead = helper.doCacheRead(position, buffer, offset, bufferLength);
        if (bufferLength > bytesRead && position + bytesRead == length) {
            throw new EOFException("Read %s of %s requested bytes at %s".formatted(bytesRead, bufferLength, description));
        }
        doExternalRead(position + bytesRead, buffer, offset + bytesRead, bufferLength - bytesRead);
    }

    private void doExternalRead(long position, byte[] buffer, int offset, int bufferLength)
            throws IOException
    {
        if (bufferLength == 0) {
            return;
        }

        AlluxioInputHelper.PageAlignedRead aligned = helper.alignRead(position, bufferLength);
        byte[] readBuffer = new byte[aligned.length()];
        source.readFully(aligned.pageStart(), readBuffer, 0, readBuffer.length);

        helper.putCache(aligned.pageStart(), aligned.pageEnd(), readBuffer, aligned.length());
        System.arraycopy(readBuffer, aligned.pageOffset(), buffer, offset, bufferLength);
        statistics.recordExternalRead(readBuffer.length);
    }

    @Override
    public InputStream openStream()
    {
        return new BlobInputStream();
    }

    @Override
    public void close() {}

    private final class BlobInputStream
            extends InputStream
    {
        private long position;
        private boolean closed;

        @Override
        public int read()
                throws IOException
        {
            ensureOpen();
            if (position >= length) {
                return -1;
            }
            byte[] single = new byte[1];
            readFully(position, single, 0, 1);
            position++;
            return single[0] & 0xFF;
        }

        @Override
        public int read(byte[] buffer, int offset, int len)
                throws IOException
        {
            ensureOpen();
            checkFromIndexSize(offset, len, buffer.length);
            if (len == 0) {
                return 0;
            }
            if (position >= length) {
                return -1;
            }
            int toRead = (int) min(len, length - position);
            readFully(position, buffer, offset, toRead);
            position += toRead;
            return toRead;
        }

        @Override
        public long skip(long n)
                throws IOException
        {
            ensureOpen();
            if (n <= 0) {
                return 0;
            }
            long skipped = min(n, length - position);
            position += skipped;
            return skipped;
        }

        @Override
        public int available()
                throws IOException
        {
            ensureOpen();
            long remaining = length - position;
            return (remaining > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) remaining;
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
                throw new IOException("Stream closed: " + description);
            }
        }
    }
}
