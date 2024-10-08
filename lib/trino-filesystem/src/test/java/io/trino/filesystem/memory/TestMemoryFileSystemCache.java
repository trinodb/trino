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
package io.trino.filesystem.memory;

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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMemoryFileSystemCache
        extends AbstractTestTrinoFileSystem
{
    private static final int MAX_CONTENT_LENGTH = 2 * 1024 * 1024;

    private MemoryFileSystem delegate;
    private CacheFileSystem fileSystem;
    private MemoryFileSystemCache cache;
    private CacheKeyProvider cacheKeyProvider;

    @BeforeAll
    void beforeAll()
    {
        MemoryFileSystemCacheConfig configuration = new MemoryFileSystemCacheConfig()
                .setMaxContentLength(DataSize.ofBytes(MAX_CONTENT_LENGTH))
                .setCacheTtl(new Duration(8, HOURS));
        delegate = new MemoryFileSystem();
        cache = new MemoryFileSystemCache(configuration);
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
