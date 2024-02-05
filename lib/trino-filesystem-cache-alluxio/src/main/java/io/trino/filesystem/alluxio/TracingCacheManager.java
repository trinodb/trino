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
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheUsage;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.exception.PageNotFoundException;
import alluxio.file.ReadTargetBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;
import io.airlift.units.DataSize;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.trino.filesystem.alluxio.AlluxioTracing.withTracing;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_SIZE;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_SIZE;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_KEY;
import static java.util.Objects.requireNonNull;

public class TracingCacheManager
        implements CacheManager
{
    private final Tracer tracer;
    private final String cacheKey;
    private final CacheManager delegate;
    private final long pageSizeBytes;

    public TracingCacheManager(Tracer tracer, String cacheKey, DataSize pageSize, CacheManager delegate)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.pageSizeBytes = requireNonNull(pageSize, "pageSize is null").toBytes();
        this.cacheKey = requireNonNull(cacheKey, "cacheKey is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public boolean put(PageId pageId, ByteBuffer byteBuffer, CacheContext cacheContext)
    {
        Span span = tracer.spanBuilder("AlluxioCacheManager.put")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_WRITE_POSITION, positionInFile(pageId, 0))
                .setAttribute(CACHE_FILE_WRITE_SIZE, (long) byteBuffer.remaining())
                .startSpan();
        return withTracing(span, () -> delegate.put(pageId, byteBuffer, cacheContext));
    }

    @Override
    public int get(PageId pageId, int position, int length, ReadTargetBuffer readTargetBuffer, CacheContext cacheContext)
    {
        Span span = tracer.spanBuilder("AlluxioCacheManager.get")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_READ_POSITION, positionInFile(pageId, position))
                .setAttribute(CACHE_FILE_READ_SIZE, (long) length)
                .startSpan();
        return withTracing(span, () -> delegate.get(pageId, position, length, readTargetBuffer, cacheContext));
    }

    @Override
    public int getAndLoad(PageId pageId, int position, int length, ReadTargetBuffer readTargetBuffer, CacheContext cacheContext, Supplier<byte[]> supplier)
    {
        Span span = tracer.spanBuilder("AlluxioCacheManager.getAndLoad")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_READ_POSITION, positionInFile(pageId, position))
                .setAttribute(CACHE_FILE_READ_SIZE, (long) length)
                .startSpan();
        return withTracing(span, () -> delegate.getAndLoad(pageId, position, length, readTargetBuffer, cacheContext, supplier));
    }

    @Override
    public void deleteFile(String fileId)
    {
        Span span = tracer.spanBuilder("AlluxioCacheManager.deleteFile")
                .setAttribute(CACHE_KEY, cacheKey)
                .startSpan();
        withTracing(span, () -> delegate.deleteFile(fileId));
    }

    @Override
    public void deleteTempFile(String fileId)
    {
        Span span = tracer.spanBuilder("AlluxioCacheManager.deleteTempFile")
                .setAttribute(CACHE_KEY, cacheKey)
                .startSpan();
        withTracing(span, () -> delegate.deleteTempFile(fileId));
    }

    @Override
    public boolean delete(PageId pageId)
    {
        Span span = tracer.spanBuilder("AlluxioCacheManager.delete")
                .setAttribute(CACHE_KEY, cacheKey)
                .startSpan();
        return withTracing(span, () -> delegate.delete(pageId));
    }

    @Override
    public boolean append(PageId pageId, int position, byte[] bytes, CacheContext cacheContext)
    {
        Span span = tracer.spanBuilder("AlluxioCacheManager.append")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_WRITE_POSITION, positionInFile(pageId, position))
                .setAttribute(CACHE_FILE_WRITE_SIZE, (long) bytes.length)
                .startSpan();
        return withTracing(span, () -> delegate.append(pageId, position, bytes, cacheContext));
    }

    @Override
    public void commitFile(String fileId)
    {
        Span span = tracer.spanBuilder("AlluxioCacheManager.commitFile")
                .setAttribute(CACHE_KEY, cacheKey)
                .startSpan();
        withTracing(span, () -> delegate.commitFile(fileId));
    }

    @Override
    public Optional<DataFileChannel> getDataFileChannel(PageId pageId, int position, int length, CacheContext cacheContext)
            throws PageNotFoundException
    {
        Span span = tracer.spanBuilder("AlluxioCacheManager.getDataFileChannel")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_READ_POSITION, positionInFile(pageId, position))
                .setAttribute(CACHE_FILE_READ_POSITION, (long) length)
                .startSpan();
        return withTracing(span, () -> delegate.getDataFileChannel(pageId, position, length, cacheContext));
    }

    @Override
    public void close()
            throws Exception
    {
        delegate.close();
    }

    @Override
    public boolean put(PageId pageId, byte[] page)
    {
        Span span = tracer.spanBuilder("AlluxioCacheManager.put")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_WRITE_POSITION, positionInFile(pageId, 0))
                .setAttribute(CACHE_FILE_WRITE_SIZE, (long) page.length)
                .startSpan();
        return withTracing(span, () -> delegate.put(pageId, page));
    }

    @Override
    public boolean put(PageId pageId, ByteBuffer page)
    {
        Span span = tracer.spanBuilder("AlluxioCacheManager.put")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_WRITE_POSITION, positionInFile(pageId, 0))
                .setAttribute(CACHE_FILE_WRITE_SIZE, (long) page.remaining())
                .startSpan();
        return withTracing(span, () -> delegate.put(pageId, page));
    }

    @Override
    public boolean put(PageId pageId, byte[] page, CacheContext cacheContext)
    {
        Span span = tracer.spanBuilder("AlluxioCacheManager.put")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_WRITE_POSITION, positionInFile(pageId, 0))
                .setAttribute(CACHE_FILE_WRITE_SIZE, (long) page.length)
                .startSpan();
        return withTracing(span, () -> delegate.put(pageId, page, cacheContext));
    }

    @Override
    public int get(PageId pageId, int bytesToRead, byte[] buffer, int offsetInBuffer)
    {
        Span span = tracer.spanBuilder("AlluxioCacheManager.get")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_READ_POSITION, positionInFile(pageId, 0))
                .setAttribute(CACHE_FILE_READ_SIZE, (long) bytesToRead)
                .startSpan();
        return withTracing(span, () -> delegate.get(pageId, bytesToRead, buffer, offsetInBuffer));
    }

    @Override
    public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer, int offsetInBuffer)
    {
        Span span = tracer.spanBuilder("AlluxioCacheManager.get")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_READ_POSITION, positionInFile(pageId, pageOffset))
                .setAttribute(CACHE_FILE_READ_SIZE, (long) bytesToRead)
                .startSpan();
        return withTracing(span, () -> delegate.get(pageId, bytesToRead, buffer, offsetInBuffer));
    }

    @Override
    public int get(PageId pageId, int pageOffset, ReadTargetBuffer buffer, CacheContext cacheContext)
    {
        Span span = tracer.spanBuilder("AlluxioCacheManager.get")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_READ_POSITION, positionInFile(pageId, pageOffset))
                .setAttribute(CACHE_FILE_READ_SIZE, (long) buffer.remaining())
                .startSpan();
        return withTracing(span, () -> delegate.get(pageId, pageOffset, buffer, cacheContext));
    }

    @Override
    public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer, int offsetInBuffer, CacheContext cacheContext)
    {
        Span span = tracer.spanBuilder("AlluxioCacheManager.get")
                .setAttribute(CACHE_KEY, cacheKey)
                .setAttribute(CACHE_FILE_READ_POSITION, positionInFile(pageId, pageOffset))
                .setAttribute(CACHE_FILE_READ_SIZE, (long) bytesToRead)
                .startSpan();
        return withTracing(span, () -> delegate.get(pageId, pageOffset, bytesToRead, buffer, offsetInBuffer, cacheContext));
    }

    @Override
    public List<PageId> getCachedPageIdsByFileId(String fileId, long fileLength)
    {
        return delegate.getCachedPageIdsByFileId(fileId, fileLength);
    }

    @Override
    public boolean hasPageUnsafe(PageId pageId)
    {
        return delegate.hasPageUnsafe(pageId);
    }

    @Override
    public void invalidate(Predicate<PageInfo> predicate)
    {
        delegate.invalidate(predicate);
    }

    @Override
    public Optional<CacheUsage> getUsage()
    {
        return delegate.getUsage();
    }

    @Override
    public State state()
    {
        return delegate.state();
    }

    private long positionInFile(PageId pageId, long position)
    {
        return pageId.getPageIndex() * pageSizeBytes + position;
    }
}
