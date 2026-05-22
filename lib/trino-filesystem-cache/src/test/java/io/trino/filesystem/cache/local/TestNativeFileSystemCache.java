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

import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.cache.CacheFileSystem;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.filesystem.memory.MemoryFileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.slice.Slices.wrappedBuffer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

final class TestNativeFileSystemCache
{
    private static final CacheKeyProvider PATH_CACHE_KEY_PROVIDER = inputFile -> Optional.of(inputFile.location().path());

    @Test
    void testReadFullyCachesData(@TempDir Path cacheRoot)
            throws Exception
    {
        TestingCacheFileSystem fileSystem = createFileSystem(cacheRoot);
        Location location = Location.of("memory:///hello");
        byte[] content = "hello world".getBytes(UTF_8);
        fileSystem.write(location, content);

        assertThat(fileSystem.readFully(location)).isEqualTo(wrappedBuffer(content));
        assertThat(fileSystem.stats().getExternalReads()).isEqualTo(1);

        assertThat(fileSystem.readFully(location)).isEqualTo(wrappedBuffer(content));
        assertThat(fileSystem.stats().getExternalReads()).isEqualTo(1);
        assertThat(fileSystem.stats().getExternalReadBytes()).isEqualTo(content.length);
        assertThat(fileSystem.stats().getCacheReadBytes()).isEqualTo(content.length);
    }

    @Test
    void testPositionedReadFillsPageAlignedRange(@TempDir Path cacheRoot)
            throws Exception
    {
        TestingCacheFileSystem fileSystem = createFileSystem(cacheRoot);
        Location location = Location.of("memory:///large");
        byte[] content = new byte[70 * 1024];
        for (int index = 0; index < content.length; index++) {
            content[index] = (byte) (index % 251);
        }
        fileSystem.write(location, content);

        TrinoInputFile inputFile = fileSystem.newInputFile(location);
        byte[] buffer = new byte[10];
        try (TrinoInput input = inputFile.newInput()) {
            input.readFully(10, buffer, 0, buffer.length);
        }
        assertThat(buffer).containsExactly(Arrays.copyOfRange(content, 10, 20));
        assertThat(fileSystem.stats().getExternalReadBytes()).isEqualTo(64 * 1024);

        byte[] cachedBuffer = new byte[10];
        try (TrinoInput input = fileSystem.newInputFile(location).newInput()) {
            input.readFully(20, cachedBuffer, 0, cachedBuffer.length);
        }
        assertThat(cachedBuffer).containsExactly(Arrays.copyOfRange(content, 20, 30));
        assertThat(fileSystem.stats().getExternalReads()).isEqualTo(1);
    }

    @Test
    void testStreamCachesData(@TempDir Path cacheRoot)
            throws Exception
    {
        TestingCacheFileSystem fileSystem = createFileSystem(cacheRoot);
        Location location = Location.of("memory:///stream");
        byte[] content = "streamed content".getBytes(UTF_8);
        fileSystem.write(location, content);

        byte[] buffer = new byte[content.length];
        try (TrinoInputStream stream = fileSystem.newInputFile(location).newStream()) {
            assertThat(stream.read(buffer, 0, 6)).isEqualTo(6);
            stream.seek(0);
            assertThat(stream.read(buffer, 0, buffer.length)).isEqualTo(buffer.length);
        }
        assertThat(buffer).isEqualTo(content);
        assertThat(fileSystem.stats().getExternalReads()).isEqualTo(1);

        byte[] cached = new byte[content.length];
        try (TrinoInputStream stream = fileSystem.newInputFile(location).newStream()) {
            assertThat(stream.read(cached, 0, cached.length)).isEqualTo(cached.length);
        }
        assertThat(cached).isEqualTo(content);
        assertThat(fileSystem.stats().getExternalReads()).isEqualTo(1);
    }

    @Test
    void testStreamMissesReuseExternalStream(@TempDir Path cacheRoot)
            throws Exception
    {
        CountingMemoryFileSystem delegate = new CountingMemoryFileSystem();
        TestingCacheFileSystem fileSystem = createFileSystem(cacheRoot, delegate);
        Location location = Location.of("memory:///sequential-stream");
        byte[] content = new byte[150 * 1024];
        for (int index = 0; index < content.length; index++) {
            content[index] = (byte) (index % 251);
        }
        fileSystem.write(location, content);

        byte[] actual = new byte[content.length];
        int offset = 0;
        try (TrinoInputStream stream = fileSystem.newInputFile(location).newStream()) {
            while (offset < actual.length) {
                int read = stream.read(actual, offset, Math.min(1024, actual.length - offset));
                assertThat(read).isGreaterThan(0);
                offset += read;
            }
            assertThat(stream.read()).isEqualTo(-1);
        }

        assertThat(actual).isEqualTo(content);
        assertThat(delegate.newInputCount()).isEqualTo(0);
        assertThat(delegate.newStreamCount()).isEqualTo(1);
    }

    @Test
    void testStreamSmallReadsUsePageBuffer(@TempDir Path cacheRoot)
            throws Exception
    {
        MemoryFileSystem delegate = new MemoryFileSystem();
        TestingCacheFileSystem fileSystem = createFileSystem(cacheRoot, delegate);
        Location location = Location.of("memory:///small-stream-reads");
        byte[] content = new byte[64 * 1024];
        for (int index = 0; index < content.length; index++) {
            content[index] = (byte) (index % 251);
        }
        fileSystem.write(location, content);
        assertThat(fileSystem.readFully(location)).isEqualTo(wrappedBuffer(content));

        TestingCacheFileSystem restartedFileSystem = createFileSystem(cacheRoot, delegate);

        try (TrinoInputStream stream = restartedFileSystem.newInputFile(location).newStream()) {
            for (int index = 0; index < 10; index++) {
                assertThat(stream.read()).isEqualTo(content[index] & 0xFF);
            }
        }

        assertThat(restartedFileSystem.stats().getCacheReads()).isEqualTo(1);
        assertThat(restartedFileSystem.stats().getExternalReads()).isEqualTo(0);
    }

    @Test
    void testExpireRemovesCachedData(@TempDir Path cacheRoot)
            throws Exception
    {
        TestingCacheFileSystem fileSystem = createFileSystem(cacheRoot);
        Location location = Location.of("memory:///expire");
        byte[] content = "cached".getBytes(UTF_8);
        fileSystem.write(location, content);

        assertThat(fileSystem.readFully(location)).isEqualTo(wrappedBuffer(content));
        assertThat(fileSystem.readFully(location)).isEqualTo(wrappedBuffer(content));
        assertThat(fileSystem.stats().getExternalReads()).isEqualTo(1);

        fileSystem.cache().expire(location);

        assertThat(fileSystem.readFully(location)).isEqualTo(wrappedBuffer(content));
        assertThat(fileSystem.stats().getExternalReads()).isEqualTo(2);
    }

    @Test
    void testCacheSurvivesRestart(@TempDir Path cacheRoot)
            throws Exception
    {
        MemoryFileSystem delegate = new MemoryFileSystem();
        TestingCacheFileSystem fileSystem = createFileSystem(cacheRoot, delegate);
        Location location = Location.of("memory:///restart");
        byte[] content = "persistent cache".getBytes(UTF_8);
        fileSystem.write(location, content);

        assertThat(fileSystem.readFully(location)).isEqualTo(wrappedBuffer(content));
        assertThat(fileSystem.stats().getExternalReads()).isEqualTo(1);

        TestingCacheFileSystem restartedFileSystem = createFileSystem(cacheRoot, delegate);

        assertThat(restartedFileSystem.readFully(location)).isEqualTo(wrappedBuffer(content));
        assertThat(restartedFileSystem.stats().getExternalReads()).isEqualTo(0);
        assertThat(restartedFileSystem.stats().getCacheReadBytes()).isEqualTo(content.length);
    }

    @Test
    void testDuplicatePageWriteDoesNotOverwriteExistingPage(@TempDir Path cacheRoot)
            throws Exception
    {
        NativeFileSystemCacheStats stats = new NativeFileSystemCacheStats();
        NativeCacheDirectory cacheDirectory = new NativeCacheDirectory(cacheRoot, stats);
        CacheFileLayout.CacheFile cacheFile = cacheDirectory.cacheFile(Location.of("memory:///duplicate"), "key");
        byte[] first = "first page".getBytes(UTF_8);
        byte[] second = "other page".getBytes(UTF_8);

        cacheDirectory.writePage(cacheFile, 0, first, 0, first.length);
        cacheDirectory.writePage(cacheFile, 0, second, 0, second.length);

        byte[] buffer = new byte[first.length];
        assertThat(cacheDirectory.readPage(cacheFile, 0, 0, first.length, first.length, buffer, 0)).isTrue();
        assertThat(buffer).isEqualTo(first);
        assertThat(stats.getCacheWrites()).isEqualTo(1);
        assertThat(stats.getCacheWriteFailures()).isEqualTo(0);
    }

    private static TestingCacheFileSystem createFileSystem(Path cacheRoot)
    {
        return createFileSystem(cacheRoot, new MemoryFileSystem());
    }

    private static TestingCacheFileSystem createFileSystem(Path cacheRoot, MemoryFileSystem delegate)
    {
        return createFileSystem(cacheRoot, (TrinoFileSystem) delegate);
    }

    private static TestingCacheFileSystem createFileSystem(Path cacheRoot, TrinoFileSystem delegate)
    {
        NativeFileSystemCacheStats stats = new NativeFileSystemCacheStats();
        NativeFileSystemCache cache = new NativeFileSystemCache(List.of(cacheRoot), DataSize.valueOf("64kB"), stats);
        CacheFileSystem fileSystem = new CacheFileSystem(
                delegate,
                cache,
                PATH_CACHE_KEY_PROVIDER);
        return new TestingCacheFileSystem(fileSystem, cache, stats);
    }

    private record TestingCacheFileSystem(CacheFileSystem fileSystem, NativeFileSystemCache cache, NativeFileSystemCacheStats stats)
    {
        TrinoInputFile newInputFile(Location location)
        {
            return fileSystem.newInputFile(location);
        }

        void write(Location location, byte[] content)
                throws IOException
        {
            try (OutputStream output = fileSystem.newOutputFile(location).create()) {
                output.write(content);
            }
        }

        Slice readFully(Location location)
                throws IOException
        {
            TrinoInputFile inputFile = fileSystem.newInputFile(location);
            int length = (int) inputFile.length();
            try (TrinoInput input = inputFile.newInput()) {
                return input.readFully(0, length);
            }
        }
    }

    private static class CountingMemoryFileSystem
            extends MemoryFileSystem
    {
        private final AtomicInteger newInputCount = new AtomicInteger();
        private final AtomicInteger newStreamCount = new AtomicInteger();

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            return new CountingInputFile(super.newInputFile(location));
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            return new CountingInputFile(super.newInputFile(location, length));
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            return new CountingInputFile(super.newInputFile(location, length, lastModified));
        }

        int newInputCount()
        {
            return newInputCount.get();
        }

        int newStreamCount()
        {
            return newStreamCount.get();
        }

        private class CountingInputFile
                implements TrinoInputFile
        {
            private final TrinoInputFile delegate;

            CountingInputFile(TrinoInputFile delegate)
            {
                this.delegate = delegate;
            }

            @Override
            public TrinoInput newInput()
                    throws IOException
            {
                newInputCount.incrementAndGet();
                return delegate.newInput();
            }

            @Override
            public TrinoInputStream newStream()
                    throws IOException
            {
                newStreamCount.incrementAndGet();
                return delegate.newStream();
            }

            @Override
            public long length()
                    throws IOException
            {
                return delegate.length();
            }

            @Override
            public Instant lastModified()
                    throws IOException
            {
                return delegate.lastModified();
            }

            @Override
            public boolean exists()
                    throws IOException
            {
                return delegate.exists();
            }

            @Override
            public Location location()
            {
                return delegate.location();
            }
        }
    }
}
