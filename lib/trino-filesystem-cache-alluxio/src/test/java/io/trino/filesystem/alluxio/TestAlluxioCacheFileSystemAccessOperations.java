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

import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.store.PageStoreOptions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.cache.CacheFileSystem;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.filesystem.tracing.TracingFileSystemCache;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingTelemetry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.filesystem.alluxio.TestingCacheKeyProvider.testingCacheKeyForLocation;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_LOCATION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_SIZE;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_SIZE;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_KEY;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_READ_POSITION;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_READ_SIZE;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(Lifecycle.PER_CLASS)
public class TestAlluxioCacheFileSystemAccessOperations
{
    private static final int CACHE_SIZE = 1024;
    private static final int PAGE_SIZE = 128;
    private final TestingTelemetry testingTelemetry = TestingTelemetry.create("alluxio-cache");
    private final TestingCacheKeyProvider cacheKeyProvider = new TestingCacheKeyProvider();
    private TracingFileSystemFactory tracingFileSystemFactory;
    private AlluxioFileSystemCache alluxioCache;
    private CacheFileSystem fileSystem;
    private Path tempDirectory;
    private PageStore pageStore;

    @BeforeAll
    public void setUp()
            throws IOException
    {
        tempDirectory = Files.createTempDirectory("test");
        Path cacheDirectory = Files.createDirectory(tempDirectory.resolve("cache"));

        AlluxioFileSystemCacheConfig configuration = new AlluxioFileSystemCacheConfig()
                .setCacheDirectories(ImmutableList.of(cacheDirectory.toAbsolutePath().toString()))
                .disableTTL()
                .setCachePageSize(DataSize.ofBytes(PAGE_SIZE))
                .setMaxCacheSizes(ImmutableList.of(DataSize.ofBytes(CACHE_SIZE)));

        tracingFileSystemFactory = new TracingFileSystemFactory(testingTelemetry.getTracer(), new MemoryFileSystemFactory());
        alluxioCache = new AlluxioFileSystemCache(testingTelemetry.getTracer(), configuration, new AlluxioCacheStats());
        fileSystem = new CacheFileSystem(tracingFileSystemFactory.create(ConnectorIdentity.ofUser("hello")), new TracingFileSystemCache(testingTelemetry.getTracer(), alluxioCache), cacheKeyProvider);
        pageStore = PageStore.create(Iterables.getOnlyElement(PageStoreOptions.create(AlluxioConfigurationFactory.create(configuration))));
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        tracingFileSystemFactory = null;
        fileSystem = null;
        deleteRecursively(tempDirectory, ALLOW_INSECURE);
        tempDirectory = null;
    }

    @AfterEach
    public void nextCacheId()
    {
        cacheKeyProvider.increaseCacheVersion();
    }

    @Test
    public void testCache()
            throws IOException
    {
        Location location = getRootLocation().appendPath("hello");
        byte[] content = "hello world".getBytes(StandardCharsets.UTF_8);
        try (OutputStream output = fileSystem.newOutputFile(location).create()) {
            output.write(content);
        }

        int readTimes = 3;
        assertCacheOperations(0, location, content, readTimes,
                ImmutableMultiset.<CacheOperationSpan>builder()
                        .addCopies(new CacheOperationSpan("Alluxio.readCached", location.toString(), 11), readTimes)
                        .addCopies(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, 11), readTimes)
                        .add(new CacheOperationSpan("AlluxioCacheManager.put", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, 11))
                        .add(new CacheOperationSpan("Alluxio.writeCache", location.toString(), 11))
                        .add(new CacheOperationSpan("Input.readFully", location.toString(), 11))
                        .build());

        byte[] modifiedContent = "modified content".getBytes(StandardCharsets.UTF_8);
        fileSystem.newOutputFile(location).createOrOverwrite(modifiedContent);

        // Clear the cache, as lastModified time might be unchanged
        cacheKeyProvider.increaseCacheVersion();

        readTimes = 7;
        assertCacheOperations(0, location, modifiedContent, readTimes,
                ImmutableMultiset.<CacheOperationSpan>builder()
                        .add(new CacheOperationSpan("Input.readFully", location.toString(), 16))
                        .add(new CacheOperationSpan("Alluxio.writeCache", location.toString(), 16))
                        .add(new CacheOperationSpan("AlluxioCacheManager.put", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, 16))
                        .addCopies(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, 16), readTimes)
                        .addCopies(new CacheOperationSpan("Alluxio.readCached", location.toString(), 16), readTimes)
                        .build());
    }

    @Test
    public void testPartialCacheHits()
            throws IOException
    {
        Location location = getRootLocation().appendPath("partial");
        byte[] content = new byte[2 * PAGE_SIZE];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) i;
        }
        try (OutputStream output = fileSystem.newOutputFile(location).create()) {
            output.write(content);
        }

        assertCacheOperations(location, Arrays.copyOf(content, PAGE_SIZE),
                ImmutableMultiset.<CacheOperationSpan>builder()
                        .add(new CacheOperationSpan("Alluxio.readCached", "memory:///partial", 0, PAGE_SIZE))
                        .add(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, PAGE_SIZE))
                        .add(new CacheOperationSpan("Input.readFully", location.toString(), 0, PAGE_SIZE))
                        .add(new CacheOperationSpan("Alluxio.writeCache", location.toString(), 0, PAGE_SIZE))
                        .add(new CacheOperationSpan("AlluxioCacheManager.put", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, PAGE_SIZE))
                        .build());

        assertCacheOperations(location, Arrays.copyOf(content, PAGE_SIZE + 10),
                ImmutableMultiset.<CacheOperationSpan>builder()
                        .add(new CacheOperationSpan("Alluxio.readCached", location.toString(), 0, PAGE_SIZE + 10))
                        .add(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, PAGE_SIZE))
                        .add(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), PAGE_SIZE, PAGE_SIZE))
                        .add(new CacheOperationSpan("Input.readFully", location.toString(), PAGE_SIZE, PAGE_SIZE))
                        .add(new CacheOperationSpan("Alluxio.writeCache", location.toString(), PAGE_SIZE, PAGE_SIZE))
                        .add(new CacheOperationSpan("AlluxioCacheManager.put", cacheKey(location, cacheKeyProvider.currentCacheVersion()), PAGE_SIZE, PAGE_SIZE))
                        .build());

        assertCacheOperations(location, Arrays.copyOf(content, PAGE_SIZE + 10),
                ImmutableMultiset.<CacheOperationSpan>builder()
                        .add(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, PAGE_SIZE))
                        .add(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), PAGE_SIZE, PAGE_SIZE))
                        .add(new CacheOperationSpan("Alluxio.readCached", location.toString(), PAGE_SIZE + 10))
                        .build());

        assertCacheOperations(location, content,
                ImmutableMultiset.<CacheOperationSpan>builder()
                        .add(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, PAGE_SIZE))
                        .add(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), PAGE_SIZE, PAGE_SIZE))
                        .add(new CacheOperationSpan("Alluxio.readCached", location.toString(), 0, PAGE_SIZE * 2))
                        .build());
    }

    @Test
    public void testMultiPageExternalsReads()
            throws IOException
    {
        Location location = getRootLocation().appendPath("multipage");
        byte[] content = new byte[2 * PAGE_SIZE];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) i;
        }
        try (OutputStream output = fileSystem.newOutputFile(location).create()) {
            output.write(content);
        }

        assertCacheOperations(location, Arrays.copyOf(content, PAGE_SIZE + 1),
                ImmutableMultiset.<CacheOperationSpan>builder()
                        .add(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, PAGE_SIZE))
                        .add(new CacheOperationSpan("AlluxioCacheManager.put", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, PAGE_SIZE))
                        .add(new CacheOperationSpan("AlluxioCacheManager.put", cacheKey(location, cacheKeyProvider.currentCacheVersion()), PAGE_SIZE, PAGE_SIZE))
                        .add(new CacheOperationSpan("Alluxio.readCached", location.toString(), PAGE_SIZE + 1))
                        .add(new CacheOperationSpan("Input.readFully", location.toString(), PAGE_SIZE * 2))
                        .add(new CacheOperationSpan("Alluxio.writeCache", location.toString(), PAGE_SIZE * 2))
                        .build());
        cacheKeyProvider.increaseCacheVersion();
        assertCacheOperations(location, Arrays.copyOf(content, 2 * PAGE_SIZE),
                ImmutableMultiset.<CacheOperationSpan>builder()
                        .add(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, PAGE_SIZE))
                        .add(new CacheOperationSpan("AlluxioCacheManager.put", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, PAGE_SIZE))
                        .add(new CacheOperationSpan("AlluxioCacheManager.put", cacheKey(location, cacheKeyProvider.currentCacheVersion()), PAGE_SIZE, PAGE_SIZE))
                        .add(new CacheOperationSpan("Input.readFully", location.toString(), 2 * PAGE_SIZE))
                        .add(new CacheOperationSpan("Alluxio.readCached", location.toString(), 2 * PAGE_SIZE))
                        .add(new CacheOperationSpan("Alluxio.writeCache", location.toString(), 2 * PAGE_SIZE))
                        .build());
    }

    @Test
    public void testCacheInvalidation()
            throws IOException
    {
        Location aLocation = createFile("a", PAGE_SIZE * 6);
        Location bLocation = createFile("b", PAGE_SIZE * 5);
        Location cLocation = createFile("c", PAGE_SIZE * 4);
        Location dLocation = createFile("d", PAGE_SIZE * 3);

        assertUnCachedRead(aLocation, PAGE_SIZE * 6);
        assertCachedRead(aLocation, PAGE_SIZE * 6);
        assertUnCachedRead(bLocation, PAGE_SIZE * 5);
        assertUnCachedRead(aLocation, PAGE_SIZE * 6);
        assertCachedRead(aLocation, PAGE_SIZE * 6);
        assertCachedRead(aLocation, PAGE_SIZE * 6);
        assertUnCachedRead(bLocation, PAGE_SIZE * 5);
        assertCachedRead(bLocation, PAGE_SIZE * 5);

        assertUnCachedRead(cLocation, PAGE_SIZE * 4);
        assertUnCachedRead(dLocation, PAGE_SIZE * 3);
        assertCachedRead(cLocation, PAGE_SIZE * 4);
        assertCachedRead(dLocation, PAGE_SIZE * 3);

        assertUnCachedRead(bLocation, PAGE_SIZE * 5);
        assertCachedRead(bLocation, PAGE_SIZE * 5);
        assertUnCachedRead(cLocation, PAGE_SIZE * 4);
        assertUnCachedRead(dLocation, PAGE_SIZE * 3);
    }

    // This mimics the behaviour when dealing with concurrent read access to Alluxio cache for new files
    @Test
    public void testCacheWithMissingPage()
            throws Exception
    {
        Location location = getRootLocation().appendPath("missing_page");
        byte[] content = "missing page".getBytes(StandardCharsets.UTF_8);
        try (OutputStream output = fileSystem.newOutputFile(location).create()) {
            output.write(content);
        }

        int readTimes = 3;
        assertCacheOperations(0, location, content, readTimes,
                ImmutableMultiset.<CacheOperationSpan>builder()
                        .addCopies(new CacheOperationSpan("Alluxio.readCached", location.toString(), 12), readTimes)
                        .addCopies(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, 12), readTimes)
                        .add(new CacheOperationSpan("AlluxioCacheManager.put", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, 12))
                        .add(new CacheOperationSpan("Alluxio.writeCache", location.toString(), 12))
                        .add(new CacheOperationSpan("Input.readFully", location.toString(), 12))
                        .build());

        TrinoInputFile inputFile = fileSystem.newInputFile(location);
        String fileId = alluxioCache.uriStatus(inputFile, cacheKeyProvider.getCacheKey(inputFile).get()).getCacheContext().getCacheIdentifier();

        // Drop this file
        pageStore.delete(new PageId(fileId, 0));

        assertCacheOperations(location, content,
                ImmutableMultiset.<CacheOperationSpan>builder()
                        .add(new CacheOperationSpan("Alluxio.readCached", location.toString(), 12))
                        .add(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 12))
                        .add(new CacheOperationSpan("Input.readFully", location.toString(), 12))
                        .add(new CacheOperationSpan("Alluxio.writeCache", location.toString(), 12))
                        .add(new CacheOperationSpan("AlluxioCacheManager.put", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, 12))
                        .build());
    }

    // This mimics the behaviour when dealing with concurrent read access to Alluxio cache for new files
    @Test
    public void testCacheWithCorruptedPage()
            throws Exception
    {
        Location location = getRootLocation().appendPath("corrupted_page");
        byte[] content = "corrupted page".getBytes(StandardCharsets.UTF_8);
        try (OutputStream output = fileSystem.newOutputFile(location).create()) {
            output.write(content);
        }

        int readTimes = 3;
        assertCacheOperations(0, location, content, readTimes,
                ImmutableMultiset.<CacheOperationSpan>builder()
                        .addCopies(new CacheOperationSpan("Alluxio.readCached", location.toString(), 14), readTimes)
                        .addCopies(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, 14), readTimes)
                        .add(new CacheOperationSpan("AlluxioCacheManager.put", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, 14))
                        .add(new CacheOperationSpan("Alluxio.writeCache", location.toString(), 14))
                        .add(new CacheOperationSpan("Input.readFully", location.toString(), 14))
                        .build());

        TrinoInputFile inputFile = fileSystem.newInputFile(location);
        String fileId = alluxioCache.uriStatus(inputFile, cacheKeyProvider.getCacheKey(inputFile).get()).getCacheContext().getCacheIdentifier();

        // Drop this file
        pageStore.put(new PageId(fileId, 0), new byte[0]);

        assertCacheOperations(location, content,
                ImmutableMultiset.<CacheOperationSpan>builder()
                        .add(new CacheOperationSpan("Alluxio.readCached", location.toString(), 14))
                        .add(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 14))
                        .add(new CacheOperationSpan("Input.readFully", location.toString(), 14))
                        .add(new CacheOperationSpan("Alluxio.writeCache", location.toString(), 14))
                        .add(new CacheOperationSpan("AlluxioCacheManager.put", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, 14))
                        .build());
    }

    @Test
    public void testCacheHitAfterReadFromNoneZeroPosition()
            throws IOException
    {
        Location location = getRootLocation().appendPath("read_from_non_zero_position");
        byte[] content = "hello world".getBytes(StandardCharsets.UTF_8);
        try (OutputStream output = fileSystem.newOutputFile(location).create()) {
            output.write(content);
        }

        byte[] readContent = "rl".getBytes(StandardCharsets.UTF_8);
        int readTimes = 5;
        assertCacheOperations(8, location, readContent, readTimes,
                ImmutableMultiset.<CacheOperationSpan>builder()
                        .addCopies(new CacheOperationSpan("Alluxio.readCached", location.toString(), 8, 2), readTimes)
                        .addCopies(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, 11), 2)
                        .add(new CacheOperationSpan("Input.readFully", location.toString(), 0, 11))
                        .add(new CacheOperationSpan("AlluxioCacheManager.put", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, 11))
                        .add(new CacheOperationSpan("Alluxio.writeCache", location.toString(), 0, 11))
                        .build());
    }

    private Location createFile(String name, int size)
            throws IOException
    {
        Location location = getRootLocation().appendPath(name);
        try (OutputStream output = fileSystem.newOutputFile(location).create()) {
            output.write("a".repeat(size).getBytes(StandardCharsets.UTF_8));
        }
        return location;
    }

    private Location getRootLocation()
    {
        return Location.of("memory://");
    }

    private void assertCachedRead(Location location, int fileSize)
            throws IOException
    {
        ImmutableMultiset.Builder<CacheOperationSpan> builder = ImmutableMultiset.<CacheOperationSpan>builder()
                .add(new CacheOperationSpan("Alluxio.readCached", location.toString(), 0, fileSize));

        for (int offset = 0; offset < fileSize; offset = offset + PAGE_SIZE) {
            builder.add(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), offset, PAGE_SIZE));
        }

        assertCacheOperations(location, builder.build());
    }

    private void assertUnCachedRead(Location location, int fileSize)
            throws IOException
    {
        ImmutableMultiset.Builder<CacheOperationSpan> builder = ImmutableMultiset.<CacheOperationSpan>builder()
                .add(new CacheOperationSpan("Alluxio.readCached", location.toString(), fileSize))
                .add(new CacheOperationSpan("Alluxio.writeCache", location.toString(), fileSize))
                .add(new CacheOperationSpan("Input.readFully", location.toString(), fileSize));

        for (int offset = 0; offset < fileSize; offset = offset + PAGE_SIZE) {
            builder.add(new CacheOperationSpan("AlluxioCacheManager.put", cacheKey(location, cacheKeyProvider.currentCacheVersion()), offset, PAGE_SIZE));
        }
        builder.add(new CacheOperationSpan("AlluxioCacheManager.get", cacheKey(location, cacheKeyProvider.currentCacheVersion()), 0, PAGE_SIZE));

        assertCacheOperations(location, builder.build());
    }

    private void assertCacheOperations(Location location, Multiset<CacheOperationSpan> cacheOperations)
            throws IOException
    {
        List<SpanData> spans = testingTelemetry.captureSpans(() -> {
            TrinoInputFile file = fileSystem.newInputFile(location);
            int length = (int) file.length();
            try (TrinoInput input = file.newInput()) {
                input.readFully(0, length);
            }
        });
        assertMultisetsEqual(getCacheOperations(spans), cacheOperations);
    }

    private void assertCacheOperations(Location location, byte[] content, Multiset<CacheOperationSpan> cacheOperations)
            throws IOException
    {
        assertCacheOperations(0, location, content, 1, cacheOperations);
    }

    private void assertCacheOperations(int position, Location location, byte[] content, int readTimes, Multiset<CacheOperationSpan> cacheOperations)
            throws IOException
    {
        List<SpanData> spans = testingTelemetry.captureSpans(() -> {
            TrinoInputFile file = fileSystem.newInputFile(location);
            int length = content.length;
            try (TrinoInput input = file.newInput()) {
                for (int i = 0; i < readTimes; i++) {
                    assertThat(input.readFully(position, length)).isEqualTo(Slices.wrappedBuffer(content));
                }
            }
        });
        assertMultisetsEqual(getCacheOperations(spans), cacheOperations);
    }

    private Multiset<CacheOperationSpan> getCacheOperations(List<SpanData> spans)
    {
        return spans.stream()
                .filter(span -> span.getName().startsWith("Input.") || span.getName().startsWith("Alluxio"))
                .map(CacheOperationSpan::create)
                .collect(toCollection(HashMultiset::create));
    }

    private record CacheOperationSpan(String spanName, String location, long position, long length)
    {
        public CacheOperationSpan(String spanName, String location, long length)
        {
            this(spanName, location, 0, length);
        }

        public static CacheOperationSpan create(SpanData span)
        {
            Attributes attributes = span.getAttributes();

            long length = switch (span.getName()) {
                case "Alluxio.readCached", "Alluxio.readExternalStream", "AlluxioCacheManager.get" -> attributes.get(CACHE_FILE_READ_SIZE);
                case "Alluxio.writeCache", "AlluxioCacheManager.put" -> attributes.get(CACHE_FILE_WRITE_SIZE);
                case "Input.readFully" -> attributes.get(FILE_READ_SIZE);
                default -> throw new IllegalArgumentException("Unrecognized span " + span.getName() + " [" + span.getAttributes() + "]");
            };

            long position = switch (span.getName()) {
                case "Alluxio.readCached", "Alluxio.readExternalStream", "AlluxioCacheManager.get" -> attributes.get(CACHE_FILE_READ_POSITION);
                case "Alluxio.writeCache", "AlluxioCacheManager.put" -> attributes.get(CACHE_FILE_WRITE_POSITION);
                case "Input.readFully" -> attributes.get(FILE_READ_POSITION);
                default -> throw new IllegalArgumentException("Unrecognized span  " + span.getName() + " [" + span.getAttributes() + "]");
            };

            return new CacheOperationSpan(span.getName(), getLocation(span), position, length);
        }

        @Override
        public String toString()
        {
            return "CacheOperationSpan(\"%s\", \"%s\", %d, %d)".formatted(spanName, location, position, length);
        }
    }

    private static String cacheKey(Location location, int cacheVersion)
    {
        return testingCacheKeyForLocation(location, cacheVersion);
    }

    private static String getLocation(SpanData span)
    {
        if (span.getName().startsWith("Input.")) {
            return requireNonNull(span.getAttributes().get(FILE_LOCATION));
        }
        return firstNonNull(span.getAttributes().get(CACHE_FILE_LOCATION), span.getAttributes().get(CACHE_KEY));
    }
}
