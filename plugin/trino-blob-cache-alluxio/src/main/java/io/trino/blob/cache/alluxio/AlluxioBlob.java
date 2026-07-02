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

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class AlluxioBlob
        implements Blob
{
    private final BlobSource delegate;
    private final String description;
    private final long length;
    private final AlluxioCacheStats statistics;
    private final AlluxioInputHelper helper;

    AlluxioBlob(
            Tracer tracer,
            BlobSource delegate,
            String cacheKey,
            URIStatus status,
            CacheManager cacheManager,
            AlluxioConfiguration configuration,
            AlluxioCacheStats statistics)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.description = delegate.toString();
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
        if (bufferLength > length - position) {
            throw new EOFException("Read past end of file %s: position %s, length %s, file length %s".formatted(description, position, bufferLength, length));
        }

        int bytesRead = helper.doCacheRead(position, buffer, offset, bufferLength);
        doExternalRead(position + bytesRead, buffer, offset + bytesRead, bufferLength - bytesRead);
    }

    @Override
    public int readTail(byte[] buffer, int offset, int bufferLength)
            throws IOException
    {
        checkFromIndexSize(offset, bufferLength, buffer.length);
        if (bufferLength == 0) {
            return 0;
        }
        if (bufferLength > helper.pageSize()) {
            // Tails larger than a cache page (e.g. whole-file reads of ORC files below the
            // tiny-stripe threshold) are served through the page cache as a positioned read
            // anchored at the reported length, so the pages are populated and reused.
            int readSize = toIntExact(min(bufferLength, length));
            readFully(length - readSize, buffer, offset, readSize);
            return readSize;
        }
        // Tail bytes are cached under a separate (file, length)-scoped identifier so the lookup
        // is independent of the blob's reported length — which may not match the storage object.
        int cached = helper.doTailCacheRead(buffer, offset, bufferLength);
        if (cached == bufferLength) {
            return cached;
        }
        int read = delegate.readTail(buffer, offset, bufferLength);
        statistics.recordExternalRead(read);
        if (read == bufferLength) {
            // A short read means the storage object holds fewer bytes than the reported length;
            // skip caching so a lookup scoped to (key, length) never sees a partial entry.
            helper.putTailCache(buffer, offset, read);
        }
        return read;
    }

    private void doExternalRead(long position, byte[] buffer, int offset, int bufferLength)
            throws IOException
    {
        if (bufferLength == 0) {
            return;
        }

        AlluxioInputHelper.PageAlignedRead aligned = helper.alignRead(position, bufferLength);
        byte[] readBuffer = new byte[aligned.length()];
        delegate.readFully(aligned.pageStart(), readBuffer, 0, readBuffer.length);

        helper.putCache(aligned.pageStart(), aligned.pageEnd(), readBuffer, aligned.length());
        System.arraycopy(readBuffer, aligned.pageOffset(), buffer, offset, bufferLength);
        statistics.recordExternalRead(readBuffer.length);
    }

    @Override
    public String toString()
    {
        return description;
    }

    @Override
    public void close() {}
}
