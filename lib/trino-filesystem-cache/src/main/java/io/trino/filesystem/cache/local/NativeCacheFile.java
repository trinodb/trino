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
package io.trino.filesystem.cache.local;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.tracing.Tracing.CheckedSupplier;

import java.io.EOFException;
import java.io.IOException;

import static com.google.common.primitives.Ints.saturatedCast;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_LOCATION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_SIZE;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_SIZE;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_KEY;
import static io.trino.filesystem.tracing.Tracing.CheckedRunnable;
import static io.trino.filesystem.tracing.Tracing.withTracing;
import static java.lang.Math.addExact;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

final class NativeCacheFile
{
    private final Tracer tracer;
    private final TrinoInputFile delegate;
    private final String cacheKey;
    private final NativeCacheDirectory cacheDirectory;
    private final CacheFileLayout.CacheFile cacheFile;
    private final NativeFileSystemCacheStats stats;
    private final int pageSize;
    private final long fileLength;

    NativeCacheFile(Tracer tracer, TrinoInputFile delegate, String cacheKey, NativeCacheDirectory cacheDirectory, NativeFileSystemCacheStats stats, int pageSize, long fileLength)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cacheKey = requireNonNull(cacheKey, "cacheKey is null");
        this.cacheDirectory = requireNonNull(cacheDirectory, "cacheDirectory is null");
        this.cacheFile = cacheDirectory.cacheFile(delegate.location(), cacheKey);
        this.stats = requireNonNull(stats, "stats is null");
        this.pageSize = pageSize;
        this.fileLength = fileLength;
    }

    Location location()
    {
        return delegate.location();
    }

    long fileLength()
    {
        return fileLength;
    }

    TrinoInputFile delegate()
    {
        return delegate;
    }

    int pageSize()
    {
        return pageSize;
    }

    void validateRead(long position, int length)
            throws IOException
    {
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        if (length == 0) {
            return;
        }
        long endPosition;
        try {
            endPosition = addExact(position, length);
        }
        catch (ArithmeticException e) {
            throw new EOFException("Read request overflows file size: " + delegate.location());
        }
        if (endPosition > fileLength) {
            throw new EOFException("Cannot read %s bytes at %s. File size is %s: %s".formatted(length, position, fileLength, delegate.location()));
        }
    }

    boolean readPage(long pageIndex, int pageOffset, int length, int expectedPageLength, byte[] buffer, int bufferOffset)
            throws IOException
    {
        return cacheDirectory.readPage(cacheFile, pageIndex, pageOffset, length, expectedPageLength, buffer, bufferOffset);
    }

    int readCached(long position, int length, CheckedSupplier<Integer, IOException> read)
            throws IOException
    {
        Span span = tracer.spanBuilder("NativeCache.readCached")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_LOCATION, location().toString())
                .setAttribute(CACHE_FILE_READ_SIZE, (long) length)
                .setAttribute(CACHE_FILE_READ_POSITION, position)
                .startSpan();
        return withTracing(span, read);
    }

    void readExternalStream(long position, int length, CheckedRunnable<IOException> read)
            throws IOException
    {
        Span span = tracer.spanBuilder("NativeCache.readExternalStream")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_LOCATION, location().toString())
                .setAttribute(CACHE_FILE_READ_SIZE, (long) length)
                .setAttribute(CACHE_FILE_READ_POSITION, position)
                .startSpan();
        withTracing(span, read);
    }

    void recordExternalRead(long bytes)
    {
        stats.recordExternalRead(bytes);
    }

    void writePages(long pageStart, byte[] readBuffer)
    {
        Span span = tracer.spanBuilder("NativeCache.writeCache")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_LOCATION, location().toString())
                .setAttribute(CACHE_FILE_WRITE_SIZE, (long) readBuffer.length)
                .setAttribute(CACHE_FILE_WRITE_POSITION, pageStart)
                .startSpan();
        withTracing(span, () -> {
            int offset = 0;
            long currentPageStart = pageStart;
            while (offset < readBuffer.length) {
                long pageIndex = currentPageStart / pageSize;
                int pageLength = min(pageSize, readBuffer.length - offset);
                cacheDirectory.writePage(cacheFile, pageIndex, readBuffer, offset, pageLength);
                currentPageStart += pageLength;
                offset += pageLength;
            }
        });
    }

    int expectedPageLength(long pageIndex)
    {
        long pageStart = pageIndex * (long) pageSize;
        return saturatedCast(min(pageSize, fileLength - pageStart));
    }

    PageAlignedRead alignRead(long position, int length)
    {
        long pageStart = position - (position % pageSize);
        int pageOffset = (int) (position % pageSize);
        long readEnd = position + length;
        long alignedReadEnd = readEnd + (pageSize - (readEnd % pageSize)) % pageSize;
        long pageEnd = min(alignedReadEnd, fileLength);
        return new PageAlignedRead(pageStart, pageEnd, pageOffset);
    }

    record PageAlignedRead(long pageStart, long pageEnd, int pageOffset)
    {
        int length()
        {
            return toIntExact(pageEnd - pageStart);
        }
    }
}
