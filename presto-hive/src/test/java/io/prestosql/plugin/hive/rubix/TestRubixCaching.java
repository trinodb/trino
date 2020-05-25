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
package io.prestosql.plugin.hive.rubix;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import com.qubole.rubix.core.utils.DummyClusterManager;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.metadata.InternalNode;
import io.prestosql.plugin.base.CatalogName;
import io.prestosql.plugin.hive.HdfsConfig;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.orc.OrcReaderConfig;
import io.prestosql.plugin.hive.rubix.RubixConfig.ReadMode;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.testing.TestingConnectorSession;
import io.prestosql.testing.TestingNodeManager;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.qubole.rubix.spi.CacheConfig.setPrestoClusterManager;
import static com.qubole.rubix.spi.CacheConfig.setRemoteFetchProcessInterval;
import static com.qubole.rubix.spi.CacheUtil.skipCache;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.client.NodeVersion.UNKNOWN;
import static io.prestosql.plugin.hive.HiveTestUtils.getHiveSessionProperties;
import static io.prestosql.plugin.hive.rubix.RubixConfig.ReadMode.ASYNC;
import static io.prestosql.plugin.hive.rubix.RubixConfig.ReadMode.READ_THROUGH;
import static io.prestosql.plugin.hive.util.RetryDriver.retry;
import static io.prestosql.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Collections.nCopies;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestRubixCaching
{
    private static final DataSize SMALL_FILE_SIZE = DataSize.of(1, MEGABYTE);
    private static final DataSize LARGE_FILE_SIZE = DataSize.of(100, MEGABYTE);
    private static final MBeanServer BEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    private java.nio.file.Path tempDirectory;
    private Path cacheStoragePath;
    private HdfsConfig config;
    private List<PropertyMetadata<?>> hiveSessionProperties;
    private HdfsContext context;
    private RubixInitializer rubixInitializer;
    private RubixConfigurationInitializer rubixConfigInitializer;
    private FileSystem nonCachingFileSystem;
    private FileSystem cachingFileSystem;

    @BeforeClass
    public void setup()
            throws IOException
    {
        cacheStoragePath = getStoragePath("/");
        config = new HdfsConfig();
        hiveSessionProperties = getHiveSessionProperties(
                new HiveConfig(),
                new RubixEnabledConfig().setCacheEnabled(true),
                new OrcReaderConfig()).getSessionProperties();
        context = new HdfsContext(
                TestingConnectorSession.builder()
                        .setPropertyMetadata(hiveSessionProperties)
                        .build(),
                "test");

        nonCachingFileSystem = getNonCachingFileSystem();
    }

    private FileSystem getNonCachingFileSystem()
            throws IOException
    {
        HdfsConfigurationInitializer configurationInitializer = new HdfsConfigurationInitializer(config);
        HiveHdfsConfiguration configuration = new HiveHdfsConfiguration(configurationInitializer, ImmutableSet.of());
        HdfsEnvironment environment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());
        return environment.getFileSystem(context, cacheStoragePath);
    }

    private void initializeCachingFileSystem(RubixConfig rubixConfig)
            throws IOException
    {
        initializeRubix(rubixConfig);
        cachingFileSystem = getCachingFileSystem();
    }

    private void initializeRubix(RubixConfig rubixConfig)
            throws IOException
    {
        tempDirectory = createTempDirectory(getClass().getSimpleName());

        // create cache directories
        List<java.nio.file.Path> cacheDirectories = ImmutableList.of(
                tempDirectory.resolve("cache1"),
                tempDirectory.resolve("cache2"));
        for (java.nio.file.Path directory : cacheDirectories) {
            createDirectories(directory);
        }

        // initialize rubix in master-only mode
        rubixConfig.setStartServerOnCoordinator(true);
        rubixConfig.setCacheLocation(Joiner.on(",").join(
                cacheDirectories.stream()
                        .map(java.nio.file.Path::toString)
                        .collect(toImmutableList())));
        rubixConfigInitializer = new RubixConfigurationInitializer(rubixConfig);
        HdfsConfigurationInitializer configurationInitializer = new HdfsConfigurationInitializer(
                config,
                ImmutableSet.of(
                        // fetch data immediately in async mode
                        config -> setRemoteFetchProcessInterval(config, 0)));
        InternalNode coordinatorNode = new InternalNode(
                "master",
                URI.create("http://127.0.0.1:8080"),
                UNKNOWN,
                true);
        TestingNodeManager nodeManager = new TestingNodeManager(
                coordinatorNode,
                ImmutableList.of());
        rubixInitializer = new RubixInitializer(
                rubixConfig,
                nodeManager,
                new CatalogName("catalog"),
                rubixConfigInitializer,
                configurationInitializer);
        rubixInitializer.initializeRubix();
    }

    private FileSystem getCachingFileSystem()
            throws IOException
    {
        return getCachingFileSystem(context);
    }

    private FileSystem getCachingFileSystem(HdfsContext context)
            throws IOException
    {
        HdfsConfigurationInitializer configurationInitializer = new HdfsConfigurationInitializer(config, ImmutableSet.of());
        HiveHdfsConfiguration configuration = new HiveHdfsConfiguration(
                configurationInitializer,
                ImmutableSet.of(
                        rubixConfigInitializer,
                        (dynamicConfig, ignoredContext, ignoredUri) -> {
                            // make sure that dummy cluster manager is used
                            setPrestoClusterManager(dynamicConfig, DummyClusterManager.class.getName());
                            dynamicConfig.set("fs.file.impl", CachingLocalFileSystem.class.getName());
                        }));
        HdfsEnvironment environment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());
        return environment.getFileSystem(context, cacheStoragePath);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        nonCachingFileSystem.close();
    }

    @AfterMethod(alwaysRun = true)
    public void closeRubix()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(() -> {
                if (tempDirectory != null) {
                    deleteRecursively(tempDirectory, ALLOW_INSECURE);
                    tempDirectory = null;
                }
            });
            closer.register(() -> {
                if (cachingFileSystem != null) {
                    cachingFileSystem.close();
                    cachingFileSystem = null;
                }
            });
            closer.register(() -> {
                if (rubixInitializer != null) {
                    try {
                        retry().run(
                                "stopRubix",
                                () -> {
                                    rubixInitializer.stopRubix();
                                    return null;
                                });
                    }
                    catch (Exception exception) {
                        throw new RuntimeException(exception);
                    }
                    rubixInitializer = null;
                }
            });
        }
    }

    @DataProvider
    public static Object[][] readMode()
    {
        return new Object[][] {{ASYNC}, {READ_THROUGH}};
    }

    @Test
    public void testCoordinatorNotJoining()
    {
        RubixConfig rubixConfig = new RubixConfig();
        RubixConfigurationInitializer rubixConfigInitializer = new RubixConfigurationInitializer(rubixConfig);
        HdfsConfigurationInitializer configurationInitializer = new HdfsConfigurationInitializer(config, ImmutableSet.of());
        InternalNode workerNode = new InternalNode(
                "master",
                URI.create("http://127.0.0.1:8080"),
                UNKNOWN,
                false);
        RubixInitializer rubixInitializer = new RubixInitializer(
                retry().maxAttempts(1),
                true,
                new TestingNodeManager(ImmutableList.of(workerNode)),
                new CatalogName("catalog"),
                rubixConfigInitializer,
                configurationInitializer);
        assertThatThrownBy(rubixInitializer::initializeRubix)
                .hasMessage("No coordinator node available");
    }

    @Test(dataProvider = "readMode")
    public void testCacheRead(ReadMode readMode)
            throws Exception
    {
        RubixConfig rubixConfig = new RubixConfig().setReadMode(readMode);
        initializeCachingFileSystem(rubixConfig);
        byte[] randomData = new byte[(int) SMALL_FILE_SIZE.toBytes()];
        new Random().nextBytes(randomData);

        Path file = getStoragePath("some_file");
        writeFile(nonCachingFileSystem.create(file), randomData);

        long beforeRemoteReadsCount = getRemoteReadsCount();
        long beforeCachedReadsCount = getCachedReadsCount();
        long beforeAsyncDownloadedMb = getAsyncDownloadedMb(readMode);

        assertEquals(readFile(cachingFileSystem.open(file)), randomData);

        // stats are propagated asynchronously
        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    // data should be read from remote source only
                    assertGreaterThan(getRemoteReadsCount(), beforeRemoteReadsCount);
                    assertEquals(getCachedReadsCount(), beforeCachedReadsCount);
                });
        long firstRemoteReadsCount = getRemoteReadsCount();

        if (readMode == ASYNC) {
            // wait for async Rubix requests to complete
            assertEventually(
                    new Duration(10, SECONDS),
                    () -> assertEquals(getAsyncDownloadedMb(readMode), beforeAsyncDownloadedMb + 1));
        }

        assertEquals(readFile(cachingFileSystem.open(file)), randomData);

        // stats are propagated asynchronously
        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    // data should be read from cache only
                    assertGreaterThan(getCachedReadsCount(), beforeCachedReadsCount);
                    assertEquals(getRemoteReadsCount(), firstRemoteReadsCount);
                });

        long secondRemoteReadsCount = getRemoteReadsCount();
        long secondCachedReadsCount = getCachedReadsCount();

        // read data with cache disabled via session property
        HdfsContext nonCachingContext = new HdfsContext(
                TestingConnectorSession.builder()
                        .setPropertyMetadata(hiveSessionProperties)
                        .setPropertyValues(ImmutableMap.of("cache_enabled", false))
                        .build(),
                "test");
        try (FileSystem cachingFileSystemWithCacheDisabled = getCachingFileSystem(nonCachingContext)) {
            assertEquals(readFile(cachingFileSystemWithCacheDisabled.open(file)), randomData);

            // non-caching (native) file system should be used
            sleep(1000);
            assertTrue(skipCache(file.toString(), cachingFileSystemWithCacheDisabled.getConf()));
            assertEquals(secondCachedReadsCount, getCachedReadsCount());
            assertEquals(secondRemoteReadsCount, getRemoteReadsCount());
        }

        // re-initialize caching file system to flush file system cache
        cachingFileSystem.close();
        cachingFileSystem = getCachingFileSystem();

        // read data with re-enabled cache
        assertEquals(readFile(cachingFileSystem.open(file)), randomData);

        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    // data should be read from cache only
                    assertGreaterThan(getCachedReadsCount(), secondCachedReadsCount);
                    assertEquals(getRemoteReadsCount(), secondRemoteReadsCount);
                });
    }

    @Test(dataProvider = "readMode")
    public void testCacheWrite(ReadMode readMode)
            throws IOException
    {
        initializeCachingFileSystem(new RubixConfig().setReadMode(readMode));
        Path file = getStoragePath("some_file_write");

        byte[] data = "Hello world".getBytes(UTF_8);
        writeFile(cachingFileSystem.create(file), data);
        assertEquals(readFile(nonCachingFileSystem.open(file)), data);
    }

    @Test(dataProvider = "readMode")
    public void testLargeFile(ReadMode readMode)
            throws Exception
    {
        initializeCachingFileSystem(new RubixConfig().setReadMode(readMode));
        byte[] randomData = new byte[(int) LARGE_FILE_SIZE.toBytes()];
        new Random().nextBytes(randomData);

        Path file = getStoragePath("large_file");
        writeFile(nonCachingFileSystem.create(file), randomData);

        long beforeRemoteReadsCount = getRemoteReadsCount();
        long beforeCachedReadsCount = getCachedReadsCount();
        long beforeAsyncDownloadedMb = getAsyncDownloadedMb(readMode);

        assertTrue(Arrays.equals(randomData, readFile(cachingFileSystem.open(file))));

        // stats are propagated asynchronously
        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    // data should be fetched from remote source
                    assertGreaterThan(getRemoteReadsCount(), beforeRemoteReadsCount);
                });
        long firstRemoteReadsCount = getRemoteReadsCount();

        if (readMode == ASYNC) {
            // wait for async Rubix requests to complete
            assertEventually(
                    new Duration(10, SECONDS),
                    () -> assertEquals(getAsyncDownloadedMb(readMode), beforeAsyncDownloadedMb + 100));
        }

        assertTrue(Arrays.equals(randomData, readFile(cachingFileSystem.open(file))));

        // stats are propagated asynchronously
        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    // data should be read from cache only
                    assertGreaterThan(getCachedReadsCount(), beforeCachedReadsCount);
                    assertEquals(getRemoteReadsCount(), firstRemoteReadsCount);
                });
        long secondCachedReadsCount = getCachedReadsCount();
        long secondRemoteReadsCount = getRemoteReadsCount();

        // make sure parallel reading of large file works
        ExecutorService executorService = newFixedThreadPool(3);
        try {
            List<Callable<?>> reads = nCopies(
                    3,
                    () -> {
                        assertTrue(Arrays.equals(randomData, readFile(cachingFileSystem.open(file))));
                        return null;
                    });
            List<Future<?>> futures = reads.stream()
                    .map(executorService::submit)
                    .collect(toImmutableList());
            for (Future<?> future : futures) {
                future.get();
            }
        }
        finally {
            executorService.shutdownNow();
        }

        // stats are propagated asynchronously
        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    // data should be read from cache only
                    assertGreaterThan(getCachedReadsCount(), secondCachedReadsCount);
                    assertEquals(getRemoteReadsCount(), secondRemoteReadsCount);
                });
    }

    private byte[] readFile(FSDataInputStream inputStream)
            throws IOException
    {
        try {
            return ByteStreams.toByteArray(inputStream);
        }
        finally {
            inputStream.close();
        }
    }

    private void writeFile(FSDataOutputStream outputStream, byte[] content)
            throws IOException
    {
        try {
            outputStream.write(content);
        }
        finally {
            outputStream.close();
        }
    }

    private Path getStoragePath(String path)
    {
        return new Path(format("file:///%s/storage/%s", tempDirectory, path));
    }

    private long getRemoteReadsCount()
    {
        try {
            return (long) BEAN_SERVER.getAttribute(new ObjectName("rubix:name=stats,catalog=catalog"), "RemoteReads");
        }
        catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private long getCachedReadsCount()
    {
        try {
            return (long) BEAN_SERVER.getAttribute(new ObjectName("rubix:name=stats,catalog=catalog"), "CachedReads");
        }
        catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private long getAsyncDownloadedMb(ReadMode readMode)
    {
        if (readMode == READ_THROUGH) {
            return 0;
        }

        try {
            return (long) BEAN_SERVER.getAttribute(new ObjectName("metrics:name=rubix.bookkeeper.count.async_downloaded_mb"), "Count");
        }
        catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }
}
