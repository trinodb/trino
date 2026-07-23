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
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.TracerProvider;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
        NativeCacheDirectory cacheDirectory = createCacheDirectory(cacheRoot, stats);
        cacheDirectory.initialize();
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

    @Test
    void testWritesAreAllowedBeforeInitialScanCompletes(@TempDir Path cacheRoot)
            throws Exception
    {
        NativeFileSystemCacheStats stats = new NativeFileSystemCacheStats();
        NativeCacheDirectory cacheDirectory = createCacheDirectory(cacheRoot, stats);
        CacheFileLayout.CacheFile cacheFile = cacheDirectory.cacheFile(Location.of("memory:///initializing"), "key");
        byte[] page = "page".getBytes(UTF_8);

        cacheDirectory.writePage(cacheFile, 0, page, 0, page.length);

        byte[] buffer = new byte[page.length];
        assertThat(cacheDirectory.readPage(cacheFile, 0, 0, page.length, page.length, buffer, 0)).isTrue();
        assertThat(buffer).isEqualTo(page);
        assertThat(stats.getCacheWrites()).isEqualTo(1);
        assertThat(stats.getCacheWriteSkips()).isEqualTo(0);
        assertThat(stats.getCachedFiles()).isEqualTo(1);

        cacheDirectory.initialize();

        assertThat(cacheDirectory.readPage(cacheFile, 0, 0, page.length, page.length, buffer, 0)).isTrue();
        assertThat(buffer).isEqualTo(page);
        assertThat(stats.getCachedFiles()).isEqualTo(1);
    }

    @Test
    void testReadsAreAllowedUntilInitialScanCompletes(@TempDir Path cacheRoot)
            throws Exception
    {
        NativeFileSystemCacheStats stats = new NativeFileSystemCacheStats();
        NativeCacheDirectory cacheDirectory = createCacheDirectory(cacheRoot, stats);
        cacheDirectory.initialize();
        CacheFileLayout.CacheFile cacheFile = cacheDirectory.cacheFile(Location.of("memory:///initializing-read"), "key");
        byte[] page = "page".getBytes(UTF_8);
        cacheDirectory.writePage(cacheFile, 0, page, 0, page.length);

        NativeFileSystemCacheStats restartStats = new NativeFileSystemCacheStats();
        NativeCacheDirectory restartingCacheDirectory = createCacheDirectory(cacheRoot, restartStats);
        CacheFileLayout.CacheFile newCacheFile = restartingCacheDirectory.cacheFile(Location.of("memory:///initializing-new-write"), "key");
        byte[] newPage = "new-page".getBytes(UTF_8);
        restartingCacheDirectory.writePage(newCacheFile, 0, newPage, 0, newPage.length);

        byte[] buffer = new byte[page.length];
        assertThat(restartingCacheDirectory.readPage(cacheFile, 0, 0, page.length, page.length, buffer, 0)).isTrue();
        assertThat(buffer).isEqualTo(page);
        assertThat(restartStats.getCacheReads()).isEqualTo(1);

        restartingCacheDirectory.initialize();

        byte[] newBuffer = new byte[newPage.length];
        assertThat(restartingCacheDirectory.readPage(newCacheFile, 0, 0, newPage.length, newPage.length, newBuffer, 0)).isTrue();
        assertThat(newBuffer).isEqualTo(newPage);
        assertThat(restartStats.getCachedFiles()).isEqualTo(2);
    }

    @Test
    void testSkipsWriteWhenPageExceedsMaxSize(@TempDir Path cacheRoot)
            throws Exception
    {
        TestingCacheFileSystem fileSystem = createFileSystem(cacheRoot, new MemoryFileSystem(), DataSize.valueOf("64kB"), DataSize.valueOf("10B"), 100, Optional.of(Duration.valueOf("7d")));
        Location location = Location.of("memory:///too-large");
        byte[] content = "content larger than cache".getBytes(UTF_8);
        fileSystem.write(location, content);

        assertThat(fileSystem.readFully(location)).isEqualTo(wrappedBuffer(content));
        assertThat(fileSystem.readFully(location)).isEqualTo(wrappedBuffer(content));

        assertThat(fileSystem.stats().getExternalReads()).isEqualTo(2);
        assertThat(fileSystem.stats().getCacheWrites()).isEqualTo(0);
        assertThat(fileSystem.stats().getCacheWriteSkips()).isEqualTo(2);
    }

    @Test
    void testSkipsWriteWhenFileSystemHasNoUsableSpace(@TempDir Path cacheRoot)
            throws Exception
    {
        NativeFileSystemCacheStats stats = new NativeFileSystemCacheStats();
        NativeCacheDirectory cacheDirectory = new NativeCacheDirectory(
                cacheRoot,
                DataSize.valueOf("1MB").toBytes(),
                100,
                Optional.of(Duration.valueOf("7d")),
                createAccessTracker(cacheRoot, stats),
                Clock.systemUTC(),
                stats,
                2,
                () -> 0);
        cacheDirectory.initialize();
        CacheFileLayout.CacheFile cacheFile = cacheDirectory.cacheFile(Location.of("memory:///no-usable-space"), "key");
        byte[] page = "page".getBytes(UTF_8);

        cacheDirectory.writePage(cacheFile, 0, page, 0, page.length);

        byte[] buffer = new byte[page.length];
        assertThat(cacheDirectory.readPage(cacheFile, 0, 0, page.length, page.length, buffer, 0)).isFalse();
        assertThat(stats.getCacheWrites()).isEqualTo(0);
        assertThat(stats.getCacheWriteSkips()).isEqualTo(1);
        assertThat(stats.getCachedFiles()).isEqualTo(0);
    }

    @Test
    void testAccountingIsReconciledWhenEvictionFindsNoCandidates(@TempDir Path cacheRoot)
            throws Exception
    {
        TestingCacheFileSystem fileSystem = createFileSystem(cacheRoot, new MemoryFileSystem(), DataSize.valueOf("64kB"), DataSize.valueOf("4B"), 100, Optional.of(Duration.valueOf("7d")));
        Location first = Location.of("memory:///accounting-first");
        Location second = Location.of("memory:///accounting-second");
        byte[] firstContent = "abcd".getBytes(UTF_8);
        byte[] secondContent = "wxyz".getBytes(UTF_8);
        fileSystem.write(first, firstContent);
        fileSystem.write(second, secondContent);

        assertThat(fileSystem.readFully(first)).isEqualTo(wrappedBuffer(firstContent));
        assertThat(fileSystem.stats().getCachedFiles()).isEqualTo(1);
        deleteCacheFiles(cacheRoot);

        assertThat(fileSystem.readFully(second)).isEqualTo(wrappedBuffer(secondContent));
        assertThat(fileSystem.stats().getCacheWriteSkips()).isEqualTo(0);
        assertThat(fileSystem.stats().getCachedFiles()).isEqualTo(1);
        assertThat(fileSystem.stats().getCachedBytes()).isEqualTo(secondContent.length);
    }

    @Test
    void testEvictsWhenByteLimitIsExceeded(@TempDir Path cacheRoot)
            throws Exception
    {
        TestingCacheFileSystem fileSystem = createFileSystem(cacheRoot, new MemoryFileSystem(), DataSize.valueOf("64kB"), DataSize.valueOf("96kB"), 100, Optional.of(Duration.valueOf("7d")));
        Location first = Location.of("memory:///first");
        Location second = Location.of("memory:///second");
        byte[] firstContent = page(1);
        byte[] secondContent = page(2);
        fileSystem.write(first, firstContent);
        fileSystem.write(second, secondContent);

        assertThat(fileSystem.readFully(first)).isEqualTo(wrappedBuffer(firstContent));
        assertThat(fileSystem.readFully(second)).isEqualTo(wrappedBuffer(secondContent));
        assertThat(fileSystem.stats().getEvictionCount()).isGreaterThanOrEqualTo(1);

        assertThat(fileSystem.readFully(first)).isEqualTo(wrappedBuffer(firstContent));
        assertThat(fileSystem.stats().getExternalReads()).isEqualTo(3);
    }

    @Test
    void testEvictsWhenFileCountLimitIsExceeded(@TempDir Path cacheRoot)
            throws Exception
    {
        TestingCacheFileSystem fileSystem = createFileSystem(cacheRoot, new MemoryFileSystem(), DataSize.valueOf("64kB"), DataSize.valueOf("1MB"), 1, Optional.of(Duration.valueOf("7d")));
        Location first = Location.of("memory:///first-file-count");
        Location second = Location.of("memory:///second-file-count");
        byte[] firstContent = "first".getBytes(UTF_8);
        byte[] secondContent = "second".getBytes(UTF_8);
        fileSystem.write(first, firstContent);
        fileSystem.write(second, secondContent);

        assertThat(fileSystem.readFully(first)).isEqualTo(wrappedBuffer(firstContent));
        assertThat(fileSystem.readFully(second)).isEqualTo(wrappedBuffer(secondContent));

        assertThat(fileSystem.stats().getEvictionCount()).isGreaterThanOrEqualTo(1);
        assertThat(fileSystem.stats().getCachedFiles()).isEqualTo(1);
    }

    @Test
    void testFileCountEvictionStopsAtLowWatermark(@TempDir Path cacheRoot)
            throws Exception
    {
        TestingCacheFileSystem fileSystem = createFileSystem(cacheRoot, new MemoryFileSystem(), DataSize.valueOf("64kB"), DataSize.valueOf("1MB"), 10, Optional.of(Duration.valueOf("7d")));
        for (int index = 0; index < 11; index++) {
            Location location = Location.of("memory:///file-count-low-watermark-%s".formatted(index));
            fileSystem.write(location, "x".getBytes(UTF_8));
            assertThat(fileSystem.readFully(location)).isEqualTo(wrappedBuffer("x".getBytes(UTF_8)));
        }

        assertThat(fileSystem.stats().getEvictionCount()).isGreaterThanOrEqualTo(1);
        assertThat(fileSystem.stats().getCachedFiles()).isEqualTo(9);
    }

    @Test
    void testFileCountEvictionContinuesAcrossSamplesUntilLowWatermark(@TempDir Path cacheRoot)
            throws Exception
    {
        NativeFileSystemCacheStats stats = new NativeFileSystemCacheStats();
        NativeCacheDirectory cacheDirectory = new NativeCacheDirectory(
                cacheRoot,
                DataSize.valueOf("1MB").toBytes(),
                10,
                Optional.of(Duration.valueOf("7d")),
                createAccessTracker(cacheRoot, stats),
                Clock.systemUTC(),
                stats,
                1,
                () -> Long.MAX_VALUE);
        cacheDirectory.initialize();
        byte[] page = "x".getBytes(UTF_8);
        for (int index = 0; index < 11; index++) {
            CacheFileLayout.CacheFile cacheFile = cacheDirectory.cacheFile(Location.of("memory:///file-count-low-watermark-small-sample-%s".formatted(index)), "key-%s".formatted(index));
            cacheDirectory.writePage(cacheFile, 0, page, 0, page.length);
        }

        assertThat(stats.getEvictionCount()).isGreaterThanOrEqualTo(2);
        assertThat(stats.getCachedFiles()).isEqualTo(9);
    }

    @Test
    void testStartupScanDeletesExpiredFileGroups(@TempDir Path cacheRoot)
            throws Exception
    {
        MemoryFileSystem delegate = new MemoryFileSystem();
        TestingCacheFileSystem fileSystem = createFileSystem(cacheRoot, delegate, DataSize.valueOf("64kB"), DataSize.valueOf("1MB"), 100, Optional.empty());
        Location location = Location.of("memory:///expired");
        byte[] content = "expired content".getBytes(UTF_8);
        fileSystem.write(location, content);
        assertThat(fileSystem.readFully(location)).isEqualTo(wrappedBuffer(content));

        try (var paths = Files.walk(cacheRoot)) {
            for (Path path : paths.filter(Files::isRegularFile).toList()) {
                Files.setLastModifiedTime(path, FileTime.from(Instant.EPOCH));
            }
        }

        TestingCacheFileSystem restartedFileSystem = createFileSystem(cacheRoot, delegate, DataSize.valueOf("64kB"), DataSize.valueOf("1MB"), 100, Optional.of(new Duration(1, TimeUnit.MILLISECONDS)));

        assertThat(restartedFileSystem.stats().getExpiredFiles()).isGreaterThanOrEqualTo(1);
        assertThat(restartedFileSystem.readFully(location)).isEqualTo(wrappedBuffer(content));
        assertThat(restartedFileSystem.stats().getExternalReads()).isEqualTo(1);
    }

    @Test
    void testMaintenanceCursorAdvancesAndWraps(@TempDir Path cacheRoot)
            throws Exception
    {
        NativeFileSystemCacheStats stats = new NativeFileSystemCacheStats();
        NativeCacheDirectory cacheDirectory = new NativeCacheDirectory(
                cacheRoot,
                DataSize.valueOf("1MB").toBytes(),
                100,
                Optional.of(new Duration(1, TimeUnit.MILLISECONDS)),
                createAccessTracker(cacheRoot, stats),
                Clock.systemUTC(),
                stats,
                2,
                () -> Long.MAX_VALUE);
        cacheDirectory.initialize();
        byte[] page = "page".getBytes(UTF_8);
        for (int index = 0; index < 5; index++) {
            CacheFileLayout.CacheFile cacheFile = cacheDirectory.cacheFile(Location.of("memory:///maintenance-%s".formatted(index)), "key");
            cacheDirectory.writePage(cacheFile, 0, page, 0, page.length);
        }
        setCacheFilesLastModifiedTime(cacheRoot, FileTime.from(Instant.EPOCH));

        cacheDirectory.maintenance();
        assertThat(countCacheFiles(cacheRoot)).isEqualTo(3);

        cacheDirectory.maintenance();
        assertThat(countCacheFiles(cacheRoot)).isEqualTo(1);

        cacheDirectory.maintenance();
        assertThat(countCacheFiles(cacheRoot)).isEqualTo(0);
        assertThat(stats.getExpiredFiles()).isEqualTo(5);
    }

    @Test
    void testStartupScanDeletesOnlyStaleTemporaryFiles(@TempDir Path cacheRoot)
            throws Exception
    {
        NativeFileSystemCacheStats stats = new NativeFileSystemCacheStats();
        NativeCacheDirectory cacheDirectory = createCacheDirectory(cacheRoot, stats);
        Path staleTemporaryFile = cacheDirectory.cacheFile(Location.of("memory:///stale-temporary"), "key")
                .temporaryPagePath(0, "stale");
        Path recentTemporaryFile = cacheDirectory.cacheFile(Location.of("memory:///recent-temporary"), "key")
                .temporaryPagePath(0, "recent");
        Files.createDirectories(staleTemporaryFile.getParent());
        Files.createDirectories(recentTemporaryFile.getParent());
        Files.writeString(staleTemporaryFile, "stale");
        Files.writeString(recentTemporaryFile, "recent");
        Files.setLastModifiedTime(staleTemporaryFile, FileTime.fromMillis(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2)));

        cacheDirectory.initialize();

        assertThat(staleTemporaryFile).doesNotExist();
        assertThat(recentTemporaryFile).exists();
        assertThat(stats.getStaleTemporaryFiles()).isEqualTo(1);
    }

    @Test
    void testEvictionPrefersFilesAbsentFromRecentAccessBloomFilter(@TempDir Path cacheRoot)
            throws Exception
    {
        TestingCacheFileSystem fileSystem = createFileSystem(cacheRoot, new MemoryFileSystem(), DataSize.valueOf("64kB"), DataSize.valueOf("256kB"), 100, Optional.of(Duration.valueOf("7d")));
        Location first = Location.of("memory:///recent-first");
        Location second = Location.of("memory:///cold-second");
        Location third = Location.of("memory:///third");
        Location fourth = Location.of("memory:///fourth");
        Location fifth = Location.of("memory:///fifth");
        byte[] firstContent = page(1);
        byte[] secondContent = page(2);
        byte[] thirdContent = page(3);
        byte[] fourthContent = page(4);
        byte[] fifthContent = page(5);
        fileSystem.write(first, firstContent);
        fileSystem.write(second, secondContent);
        fileSystem.write(third, thirdContent);
        fileSystem.write(fourth, fourthContent);
        fileSystem.write(fifth, fifthContent);

        assertThat(fileSystem.readFully(first)).isEqualTo(wrappedBuffer(firstContent));
        assertThat(fileSystem.readFully(second)).isEqualTo(wrappedBuffer(secondContent));
        assertThat(fileSystem.readFully(third)).isEqualTo(wrappedBuffer(thirdContent));
        assertThat(fileSystem.readFully(fourth)).isEqualTo(wrappedBuffer(fourthContent));
        assertThat(fileSystem.readFully(first)).isEqualTo(wrappedBuffer(firstContent));
        assertThat(fileSystem.stats().getExternalReads()).isEqualTo(4);

        assertThat(fileSystem.readFully(fifth)).isEqualTo(wrappedBuffer(fifthContent));
        assertThat(fileSystem.readFully(first)).isEqualTo(wrappedBuffer(firstContent));
        assertThat(fileSystem.stats().getExternalReads()).isEqualTo(5);
        assertThat(fileSystem.stats().getEvictionCount()).isGreaterThanOrEqualTo(2);
        assertThat(fileSystem.stats().getCachedFiles()).isEqualTo(3);
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
        return createFileSystem(cacheRoot, delegate, DataSize.valueOf("64kB"), DataSize.valueOf("1GB"), Long.MAX_VALUE / 2, Optional.of(Duration.valueOf("7d")));
    }

    private static TestingCacheFileSystem createFileSystem(Path cacheRoot, TrinoFileSystem delegate, DataSize pageSize, DataSize maxCacheSize, long maxFiles, Optional<Duration> ttl)
    {
        NativeFileSystemCacheStats stats = new NativeFileSystemCacheStats();
        NativeFileSystemCache cache = new NativeFileSystemCache(
                nativeCacheConfig(cacheRoot, pageSize, maxCacheSize, maxFiles, ttl),
                stats,
                TracerProvider.noop().get("test"));
        CacheFileSystem fileSystem = new CacheFileSystem(
                delegate,
                cache,
                PATH_CACHE_KEY_PROVIDER);
        waitUntilInitialized(cache);
        return new TestingCacheFileSystem(fileSystem, cache, stats);
    }

    private static void waitUntilInitialized(NativeFileSystemCache cache)
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        for (NativeCacheDirectory cacheDirectory : cache.getCacheDirectories()) {
            while (!cacheDirectory.isInitialized()) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("Timed out waiting for native file system cache initialization");
                }
                try {
                    Thread.sleep(10);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static NativeFileSystemCacheConfig nativeCacheConfig(Path cacheRoot, DataSize pageSize, DataSize maxCacheSize, long maxFiles, Optional<Duration> ttl)
    {
        NativeFileSystemCacheConfig config = new NativeFileSystemCacheConfig()
                .setCacheDirectories(List.of(cacheRoot.toString()))
                .setCachePageSize(pageSize)
                .setMaxCacheSizes(List.of(maxCacheSize))
                .setMaxCacheFilesPerDirectory(maxFiles);
        ttl.ifPresentOrElse(config::setCacheTTL, config::disableTTL);
        return config;
    }

    private static NativeCacheDirectory createCacheDirectory(Path cacheRoot, NativeFileSystemCacheStats stats)
            throws IOException
    {
        return new NativeCacheDirectory(cacheRoot, DataSize.valueOf("1MB").toBytes(), 100, Optional.of(Duration.valueOf("7d")), createAccessTracker(cacheRoot, stats), stats);
    }

    private static BloomFilterAccessTracker createAccessTracker(Path cacheRoot, NativeFileSystemCacheStats stats)
            throws IOException
    {
        return new BloomFilterAccessTracker(cacheRoot, Duration.valueOf("24h"), Duration.valueOf("10m"), DataSize.valueOf("1MB"), stats);
    }

    private static byte[] page(int seed)
    {
        byte[] page = new byte[64 * 1024];
        for (int index = 0; index < page.length; index++) {
            page[index] = (byte) ((index + seed) % 251);
        }
        return page;
    }

    private static void deleteCacheFiles(Path cacheRoot)
            throws IOException
    {
        try (var paths = Files.walk(cacheRoot)) {
            for (Path path : paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".cache"))
                    .toList()) {
                Files.delete(path);
            }
        }
    }

    private static long countCacheFiles(Path cacheRoot)
            throws IOException
    {
        try (var paths = Files.walk(cacheRoot)) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".cache"))
                    .count();
        }
    }

    private static void setCacheFilesLastModifiedTime(Path cacheRoot, FileTime time)
            throws IOException
    {
        try (var paths = Files.walk(cacheRoot)) {
            for (Path path : paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".cache"))
                    .toList()) {
                Files.setLastModifiedTime(path, time);
            }
        }
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
