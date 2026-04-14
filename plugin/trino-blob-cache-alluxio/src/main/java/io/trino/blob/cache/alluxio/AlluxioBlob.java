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
    public void close()
    {
    }
}
