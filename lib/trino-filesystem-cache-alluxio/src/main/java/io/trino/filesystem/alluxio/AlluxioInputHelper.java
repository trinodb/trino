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

import alluxio.client.file.CacheContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import com.google.common.primitives.Ints;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.Location;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.filesystem.alluxio.AlluxioTracing.withTracing;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_LOCATION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_SIZE;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_SIZE;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_KEY;
import static java.lang.Integer.max;
import static java.lang.Math.addExact;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

// Inspired by https://github.com/Alluxio/alluxio/blob/4e39eda0305a0042edaeae649b503b4508623619/dora/core/client/fs/src/main/java/alluxio/client/file/cache/LocalCacheFileInStream.java#L50
// We implement a variant of this class to enable positioned reads
public class AlluxioInputHelper
{
    private final Tracer tracer;
    private final URIStatus status;
    private final String cacheKey;
    private final CacheManager cacheManager;
    private final AlluxioCacheStats statistics;
    private final Location location;
    private final int pageSize;
    private final long fileLength;
    private final int bufferSize;
    private final byte[] readBuffer;

    // Tracks the start and end positions of the portion of the file in the buffer
    private long bufferStartPosition;
    private long bufferEndPosition;

    public AlluxioInputHelper(Tracer tracer, Location location, String cacheKey, URIStatus status, CacheManager cacheManager, AlluxioConfiguration configuration, AlluxioCacheStats statistics)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.status = requireNonNull(status, "status is null");
        this.cacheKey = requireNonNull(cacheKey, "cacheKey is null");
        this.fileLength = status.getLength();
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.pageSize = (int) requireNonNull(configuration, "configuration is null").getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE);
        this.statistics = requireNonNull(statistics, "statistics is null");
        this.location = requireNonNull(location, "location is null");
        // Buffer to reduce the cost of doing page aligned reads for small sequential reads pattern
        this.bufferSize = pageSize;
        this.readBuffer = new byte[bufferSize];
    }

    public int doCacheRead(long position, byte[] bytes, int offset, int length)
    {
        Span span = tracer.spanBuilder("Alluxio.readCached")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_LOCATION, location.toString())
                .setAttribute(CACHE_FILE_READ_SIZE, (long) length)
                .setAttribute(CACHE_FILE_READ_POSITION, position)
                .startSpan();

        return withTracing(span, () -> {
            int bytesRead = doBufferRead(position, bytes, offset, length);
            return addExact(bytesRead, doInternalCacheRead(position + bytesRead, bytes, offset + bytesRead, length - bytesRead));
        });
    }

    private int doBufferRead(long position, byte[] bytes, int offset, int length)
    {
        if (length == 0) {
            return 0;
        }
        if (position < bufferStartPosition || position >= bufferEndPosition) {
            return 0;
        }
        int bytesToCopy = min(length, Ints.saturatedCast(bufferEndPosition - position));
        System.arraycopy(readBuffer, Ints.saturatedCast(position - bufferStartPosition), bytes, offset, bytesToCopy);
        return bytesToCopy;
    }

    private int doInternalCacheRead(long position, byte[] bytes, int offset, int length)
    {
        // TODO: Support reading cache hits from the back as well
        if (length == 0) {
            return 0;
        }
        int remainingLength = length;
        while (remainingLength > 0) {
            int bytesReadFromCache = readPageFromCache(position, bytes, offset, remainingLength);
            // When dealing with concurrent access for a new file, CacheManager#put doesn't guarantee the page is fully written,
            // and trying to access the page could return -1, so we read the data from source and update the cache.
            if (bytesReadFromCache <= 0) {
                break;
            }
            position += bytesReadFromCache;
            remainingLength -= bytesReadFromCache;
            offset += bytesReadFromCache;
        }
        int bytesRead = length - remainingLength;
        statistics.recordCacheRead(bytesRead);
        return bytesRead;
    }

    private int readPageFromCache(long position, byte[] buffer, int offset, int length)
    {
        long currentPage = position / pageSize;
        int currentPageOffset = (int) (position % pageSize);
        int bytesLeftInPage = (int) min(pageSize - currentPageOffset, fileLength - position);
        int bytesToReadInPage = min(bytesLeftInPage, length);
        if (bytesToReadInPage == 0) {
            return 0;
        }
        CacheContext cacheContext = status.getCacheContext();
        PageId pageId = new PageId(cacheContext.getCacheIdentifier(), currentPage);
        if (bytesLeftInPage > length && bufferSize > length) { // Read page into buffer
            int putBytes = putBuffer(position, currentPageOffset, pageId, cacheContext);
            if (putBytes <= 0) {
                return putBytes;
            }
            return doBufferRead(position, buffer, offset, length);
        }
        else {
            return cacheManager.get(pageId, currentPageOffset, bytesToReadInPage, buffer, offset, cacheContext);
        }
    }

    private int putBuffer(long position, int pageOffset, PageId pageId, CacheContext cacheContext)
    {
        pageOffset = min(pageOffset, max(pageSize - bufferSize, 0));
        int bytesToReadInPage = Ints.saturatedCast(min(pageSize - pageOffset, fileLength - position));
        int bytesRead = cacheManager.get(pageId, pageOffset, min(bytesToReadInPage, bufferSize), readBuffer, 0, cacheContext);
        if (bytesRead < 0) {
            // Buffer could be corrupted
            bufferStartPosition = 0;
            bufferEndPosition = 0;
            return bytesRead;
        }
        if (bytesRead == 0) {
            return bytesRead;
        }
        bufferStartPosition = pageOffset + (pageId.getPageIndex() * pageSize);
        bufferEndPosition = bufferStartPosition + bytesRead;
        return bytesRead;
    }

    public record PageAlignedRead(long pageStart, long pageEnd, int pageOffset)
    {
        public int length()
        {
            return (int) (pageEnd - pageStart);
        }
    }

    public PageAlignedRead alignRead(long position, long length)
    {
        long pageStart = position - (position % pageSize);
        int pageOffset = (int) (position % pageSize);
        long readEnd = position + length;
        long alignedReadEnd = readEnd + (pageSize - (readEnd % pageSize)) % pageSize;
        long pageEnd = min(alignedReadEnd, fileLength);
        return new PageAlignedRead(pageStart, pageEnd, pageOffset);
    }

    // Put length bytes from readBuffer into cache between pageStart and pageEnd
    public void putCache(long pageStart, long pageEnd, byte[] readBuffer, int length)
    {
        Span span = tracer.spanBuilder("Alluxio.writeCache")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_LOCATION, location.toString())
                .setAttribute(CACHE_FILE_WRITE_SIZE, (long) length)
                .setAttribute(CACHE_FILE_WRITE_POSITION, pageStart)
                .startSpan();
        withTracing(span, () -> {
            checkArgument(pageStart + length <= pageEnd);
            long end = pageEnd;
            if (pageStart + length < pageEnd) {
                end = pageStart + length - (length % pageSize);
            }
            int offset = 0;
            long currentPageStart = pageStart;
            while (currentPageStart < end) {
                long currentPage = currentPageStart / pageSize;
                int currentPageSize = (int) min(pageSize, pageEnd - currentPageStart);
                PageId pageId = new PageId(status.getCacheContext().getCacheIdentifier(), currentPage);
                cacheManager.put(pageId, ByteBuffer.wrap(readBuffer, offset, currentPageSize));
                currentPageStart += currentPageSize;
                offset += pageSize;
            }
        });
    }
}
