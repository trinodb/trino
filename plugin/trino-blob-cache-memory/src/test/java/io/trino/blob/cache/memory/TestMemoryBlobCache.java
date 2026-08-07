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
package io.trino.blob.cache.memory;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.cache.CacheFileSystem;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.filesystem.cache.DefaultCacheKeyProvider;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheKey;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.UUID;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMemoryBlobCache
        extends AbstractTestTrinoFileSystem
{
    private static final int MAX_CONTENT_LENGTH = 2 * 1024 * 1024;

    private MemoryFileSystem delegate;
    private CacheFileSystem fileSystem;
    private MemoryBlobCache cache;
    private CacheKeyProvider cacheKeyProvider;

    @BeforeAll
    void beforeAll()
    {
        MemoryBlobCacheConfig configuration = new MemoryBlobCacheConfig()
                .setMaxContentLength(DataSize.ofBytes(MAX_CONTENT_LENGTH))
                .setCacheTtl(new Duration(8, HOURS));
        delegate = new MemoryFileSystem();
        cache = new MemoryBlobCache(configuration);
        cacheKeyProvider = new DefaultCacheKeyProvider();
        fileSystem = new CacheFileSystem(delegate, cache, cacheKeyProvider);
    }

    @Override
    protected boolean isHierarchical()
    {
        return false;
    }

    @Override
    protected TrinoFileSystem getFileSystem()
    {
        return fileSystem;
    }

    @Override
    protected Location getRootLocation()
    {
        return Location.of("memory://");
    }

    @Override
    protected void verifyFileSystemIsEmpty()
    {
        assertThat(delegate.isEmpty()).isTrue();
    }

    @Test
    public void testMaxContentLength()
            throws IOException
    {
        int fileSize = MAX_CONTENT_LENGTH + 200;
        Location location = writeFile(fileSize);
        TrinoInputFile inputFile = getFileSystem().newInputFile(location);
        long largeFileSkippedCount = cache.getLargeFileSkippedCount();
        try (TrinoInput input = inputFile.newInput()) {
            input.readTail(fileSize);
        }
        assertThat(cache.getLargeFileSkippedCount()).isGreaterThan(largeFileSkippedCount);
        assertThat(cache.isCached(cacheKeyProvider.getCacheKey(inputFile).orElseThrow())).isFalse();
        getFileSystem().deleteFile(location);

        fileSize = MAX_CONTENT_LENGTH - 200;
        location = writeFile(fileSize);
        inputFile = getFileSystem().newInputFile(location);
        try (TrinoInput input = inputFile.newInput()) {
            input.readTail(fileSize);
        }
        assertThat(cache.isCached(cacheKeyProvider.getCacheKey(inputFile).orElseThrow())).isTrue();
        getFileSystem().deleteFile(location);
    }

    @Test
    public void testMissLoadsAndHitDoesNot()
            throws IOException
    {
        CacheKey key = CacheKey.of("testMissLoadsAndHitDoesNot", UUID.randomUUID().toString());
        byte[] content = content(1024);
        byte[] buffer = new byte[100];

        TestingBlobSource missSource = new TestingBlobSource(content);
        Blob miss = cache.get(key, missSource);
        assertThat(miss.length()).isEqualTo(content.length);
        // The whole entry is read from the source on a miss, before anything is served from it
        assertThat(miss.loadedSize()).isEqualTo(content.length);
        assertThat(miss.cachedSize()).isEqualTo(0);
        assertThat(missSource.readBytes()).isEqualTo(content.length);
        // The cache owns the source once the entry is cached
        assertThat(missSource.isClosed()).isTrue();

        miss.read(0, buffer, 0, buffer.length);
        assertThat(miss.cachedSize()).isEqualTo(buffer.length);
        miss.close();

        TestingBlobSource hitSource = new TestingBlobSource(content);
        Blob hit = cache.get(key, hitSource);
        // Nothing is fetched on a hit, so the read is not reported as external
        assertThat(hit.loadedSize()).isEqualTo(0);
        assertThat(hitSource.readBytes()).isEqualTo(0);
        assertThat(hitSource.isClosed()).isTrue();

        hit.read(0, buffer, 0, buffer.length);
        assertThat(hit.cachedSize()).isEqualTo(buffer.length);
        assertThat(buffer).isEqualTo(Arrays.copyOf(content, buffer.length));
        assertThat(hit.loadedSize()).isEqualTo(0);
        hit.close();
    }

    @Test
    public void testOversizedContentReadsThroughToSource()
            throws IOException
    {
        CacheKey key = CacheKey.of("testOversizedContentReadsThroughToSource", UUID.randomUUID().toString());
        byte[] content = content(MAX_CONTENT_LENGTH + 200);
        byte[] buffer = new byte[100];

        TestingBlobSource source = new TestingBlobSource(content);
        long largeFileSkippedCount = cache.getLargeFileSkippedCount();
        try (Blob blob = cache.get(key, source)) {
            assertThat(cache.getLargeFileSkippedCount()).isEqualTo(largeFileSkippedCount + 1);
            assertThat(cache.isCached(key)).isFalse();
            // The pass-through blob owns the source, so it stays open for its reads
            assertThat(source.isClosed()).isFalse();
            assertThat(source.readBytes()).isEqualTo(0);

            blob.read(0, buffer, 0, buffer.length);
            // Everything served by a pass-through blob comes from the source
            assertThat(blob.loadedSize()).isEqualTo(buffer.length);
            assertThat(blob.cachedSize()).isEqualTo(0);
        }
        assertThat(source.isClosed()).isTrue();
    }

    private static byte[] content(int size)
    {
        byte[] content = new byte[size];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) i;
        }
        return content;
    }

    private static class TestingBlobSource
            implements BlobSource
    {
        private final byte[] content;
        private long readBytes;
        private boolean closed;

        public TestingBlobSource(byte[] content)
        {
            this.content = content;
        }

        @Override
        public long length()
        {
            return content.length;
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length)
                throws IOException
        {
            if (position < 0 || position > content.length - length) {
                throw new EOFException("Cannot read %s bytes at %s of %s".formatted(length, position, content.length));
            }
            System.arraycopy(content, toIntExact(position), buffer, offset, length);
            readBytes += length;
        }

        @Override
        public void close()
        {
            closed = true;
        }

        public long readBytes()
        {
            return readBytes;
        }

        public boolean isClosed()
        {
            return closed;
        }
    }

    private Location writeFile(int fileSize)
            throws IOException
    {
        Location location = getRootLocation().appendPath("testMaxContentLength-%s".formatted(UUID.randomUUID()));
        getFileSystem().deleteFile(location);
        try (OutputStream outputStream = getFileSystem().newOutputFile(location).create()) {
            byte[] bytes = new byte[8192];
            Arrays.fill(bytes, (byte) 'a');
            int count = 0;
            while (count < fileSize) {
                outputStream.write(bytes, 0, Math.min(bytes.length, fileSize - count));
                count += bytes.length;
            }
        }
        return location;
    }
}
