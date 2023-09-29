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

import alluxio.conf.AlluxioConfiguration;
import io.airlift.units.DataSize;
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.cache.CacheFileSystem;
import io.trino.filesystem.cache.DefaultCacheKeyProvider;
import io.trino.filesystem.memory.MemoryFileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAlluxioCacheFileSystem
        extends AbstractTestTrinoFileSystem
{
    private CacheFileSystem fileSystem;
    private Path tempDirectory;
    private TestingAlluxioFileSystemCache cache;
    private MemoryFileSystem memoryFileSystem;

    @BeforeAll
    void beforeAll()
            throws IOException
    {
        tempDirectory = Files.createTempDirectory("test");
        Path cacheDirectory = tempDirectory.resolve("cache");
        Files.createDirectory(cacheDirectory);
        AlluxioFileSystemCacheConfig configuration = new AlluxioFileSystemCacheConfig()
                .setCacheDirectories(cacheDirectory.toAbsolutePath().toString())
                .setCachePageSize(DataSize.valueOf("32003B"))
                .disableTTL()
                .setMaxCacheSizes("100MB");
        AlluxioConfiguration alluxioConfiguration = AlluxioFileSystemCacheModule.getAlluxioConfiguration(configuration);
        cache = new TestingAlluxioFileSystemCache(alluxioConfiguration, new DefaultCacheKeyProvider()) {
            @Override
            public void expire(Location location)
            {
                // Expire the entire cache on a single invalidation
                clear();
            }
        };
        memoryFileSystem = new MemoryFileSystem();
        fileSystem = new CacheFileSystem(memoryFileSystem, cache, cache.getCacheKeyProvider());
    }

    @AfterEach
    void afterEach()
            throws IOException
    {
        cache.clear();
    }

    @AfterAll
    void afterAll()
            throws IOException
    {
        cleanupFiles(tempDirectory);
        Files.delete(tempDirectory);
    }

    private void cleanupFiles(Path directory)
            throws IOException
    {
        // tests will leave directories
        try (Stream<Path> walk = Files.walk(directory)) {
            Iterator<Path> iterator = walk.sorted(Comparator.reverseOrder()).iterator();
            while (iterator.hasNext()) {
                Path path = iterator.next();
                if (!path.equals(directory)) {
                    Files.delete(path);
                }
            }
        }
    }

    @Override
    protected boolean isHierarchical()
    {
        return false;
    }

    @Override
    protected boolean isFileContentCaching()
    {
        return true;
    }

    @Override
    protected boolean supportsCreateExclusive()
    {
        return true;
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
        assertThat(memoryFileSystem.isEmpty()).isTrue();
    }
}
