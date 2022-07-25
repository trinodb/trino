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
package io.trino.plugin.hive.rubix;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import com.qubole.rubix.core.CachingFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoAdlFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoAzureBlobFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoDistributedFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoGoogleHadoopFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoSecureAzureBlobFileSystem;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.metadata.InternalNode;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.HdfsAuthenticationConfig;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.rubix.RubixConfig.ReadMode;
import io.trino.plugin.hive.rubix.RubixModule.DefaultRubixHdfsInitializer;
import io.trino.spi.Node;
import io.trino.spi.session.PropertyMetadata;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingNodeManager;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
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
import static com.qubole.rubix.spi.CacheConfig.setRemoteFetchProcessInterval;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.client.NodeVersion.UNKNOWN;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSessionProperties;
import static io.trino.plugin.hive.rubix.RubixConfig.ReadMode.ASYNC;
import static io.trino.plugin.hive.rubix.RubixConfig.ReadMode.READ_THROUGH;
import static io.trino.plugin.hive.util.RetryDriver.retry;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.net.InetAddress.getLocalHost;
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
        List<PropertyMetadata<?>> hiveSessionProperties = getHiveSessionProperties(
                new HiveConfig(),
                new RubixEnabledConfig().setCacheEnabled(true),
                new OrcReaderConfig()).getSessionProperties();
        context = new HdfsContext(
                TestingConnectorSession.builder()
                        .setPropertyMetadata(hiveSessionProperties)
                        .build());

        nonCachingFileSystem = getNonCachingFileSystem();
    }

    @AfterMethod
    @BeforeMethod
    public void deinitializeRubix()
    {
        // revert static rubix initialization done by other tests
        CachingFileSystem.deinitialize();
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
            throws Exception
    {
        initializeRubix(rubixConfig);
        cachingFileSystem = getCachingFileSystem();
    }

    private void initializeRubix(RubixConfig rubixConfig)
            throws Exception
    {
        InternalNode coordinatorNode = new InternalNode(
                "master",
                URI.create("http://" + getLocalHost().getHostAddress() + ":8080"),
                UNKNOWN,
                true);
        initializeRubix(rubixConfig, ImmutableList.of(coordinatorNode));
    }

    private void initializeRubix(RubixConfig rubixConfig, List<Node> nodes)
            throws Exception
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
        HdfsConfigurationInitializer configurationInitializer = new HdfsConfigurationInitializer(
                config,
                ImmutableSet.of(
                        // fetch data immediately in async mode
                        config -> setRemoteFetchProcessInterval(config, 0)));
        TestingNodeManager nodeManager = new TestingNodeManager(nodes);
        rubixInitializer = new RubixInitializer(
                rubixConfig,
                nodeManager,
                new CatalogName("catalog"),
                configurationInitializer,
                new DefaultRubixHdfsInitializer(new HdfsAuthenticationConfig()));
        rubixConfigInitializer = new RubixConfigurationInitializer(rubixInitializer);
        rubixInitializer.initializeRubix();
        retry().run("wait for rubix to startup", () -> {
            if (!rubixInitializer.isServerUp()) {
                throw new IllegalStateException("Rubix server has not started");
            }
            return null;
        });
    }

    private FileSystem getCachingFileSystem()
            throws IOException
    {
        return getCachingFileSystem(context, cacheStoragePath);
    }

    private FileSystem getCachingFileSystem(HdfsContext context, Path path)
            throws IOException
    {
        HdfsConfigurationInitializer configurationInitializer = new HdfsConfigurationInitializer(config, ImmutableSet.of());
        HiveHdfsConfiguration configuration = new HiveHdfsConfiguration(
                configurationInitializer,
                ImmutableSet.of(
                        rubixConfigInitializer,
                        (dynamicConfig, ignoredContext, ignoredUri) -> {
                            dynamicConfig.set("fs.file.impl", CachingLocalFileSystem.class.getName());
                            dynamicConfig.setBoolean("fs.gs.lazy.init.enable", true);
                            dynamicConfig.set("fs.azure.account.key", "Zm9vCg==");
                            dynamicConfig.set("fs.adl.oauth2.client.id", "test");
                            dynamicConfig.set("fs.adl.oauth2.refresh.url", "http://localhost");
                            dynamicConfig.set("fs.adl.oauth2.credential", "password");
                        }));
        HdfsEnvironment environment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());
        return environment.getFileSystem(context, path);
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
        RubixConfig rubixConfig = new RubixConfig()
                .setCacheLocation("/tmp/not/existing/dir");
        HdfsConfigurationInitializer configurationInitializer = new HdfsConfigurationInitializer(config, ImmutableSet.of());
        InternalNode workerNode = new InternalNode(
                "worker",
                URI.create("http://127.0.0.2:8080"),
                UNKNOWN,
                false);
        RubixInitializer rubixInitializer = new RubixInitializer(
                retry().maxAttempts(1),
                rubixConfig.setStartServerOnCoordinator(true),
                new TestingNodeManager(ImmutableList.of(workerNode)),
                new CatalogName("catalog"),
                configurationInitializer,
                new DefaultRubixHdfsInitializer(new HdfsAuthenticationConfig()));
        assertThatThrownBy(rubixInitializer::initializeRubix)
                .hasMessage("No coordinator node available");
    }

    @Test
    public void testGetBlockLocations()
            throws Exception
    {
        RubixConfig rubixConfig = new RubixConfig();
        InternalNode coordinatorNode = new InternalNode(
                "master",
                URI.create("http://" + getLocalHost().getHostAddress() + ":8080"),
                UNKNOWN,
                true);
        InternalNode workerNode1 = new InternalNode(
                "worker1",
                URI.create("http://127.0.0.2:8080"),
                UNKNOWN,
                false);
        InternalNode workerNode2 = new InternalNode(
                "worker2",
                URI.create("http://127.0.0.3:8080"),
                UNKNOWN,
                false);
        initializeRubix(rubixConfig, ImmutableList.of(coordinatorNode, workerNode1, workerNode2));
        cachingFileSystem = getCachingFileSystem();

        FileStatus file1 = new FileStatus(3, false, 0, 3, 0, new Path("aaa"));
        FileStatus file2 = new FileStatus(3, false, 0, 3, 0, new Path("zzzz"));

        BlockLocation[] file1Locations = cachingFileSystem.getFileBlockLocations(file1, 0, 3);
        BlockLocation[] file2Locations = cachingFileSystem.getFileBlockLocations(file2, 0, 3);

        assertEquals(file1Locations.length, 1);
        assertEquals(file2Locations.length, 1);

        assertEquals(file1Locations[0].getHosts()[0], "127.0.0.3");
        assertEquals(file2Locations[0].getHosts()[0], "127.0.0.2");
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

        assertEquals(readFile(cachingFileSystem, file), randomData);

        if (readMode == ASYNC) {
            // wait for async Rubix requests to complete
            assertEventually(
                    new Duration(10, SECONDS),
                    () -> assertEquals(getAsyncDownloadedMb(readMode), beforeAsyncDownloadedMb + 1));
        }

        // stats are propagated asynchronously
        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    // data should be read from remote source only
                    assertGreaterThan(getRemoteReadsCount(), beforeRemoteReadsCount);
                    assertEquals(getCachedReadsCount(), beforeCachedReadsCount);
                });

        // ensure that subsequent read uses cache exclusively
        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    long remoteReadsCount = getRemoteReadsCount();
                    assertEquals(readFile(cachingFileSystem, file), randomData);
                    assertGreaterThan(getCachedReadsCount(), beforeCachedReadsCount);
                    assertEquals(getRemoteReadsCount(), remoteReadsCount);
                });
    }

    @Test(dataProvider = "readMode")
    public void testCacheWrite(ReadMode readMode)
            throws Exception
    {
        initializeCachingFileSystem(new RubixConfig().setReadMode(readMode));
        Path file = getStoragePath("some_file_write");

        byte[] data = "Hello world".getBytes(UTF_8);
        writeFile(cachingFileSystem.create(file), data);
        assertEquals(readFile(nonCachingFileSystem, file), data);
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

        assertTrue(Arrays.equals(randomData, readFile(cachingFileSystem, file)));

        if (readMode == ASYNC) {
            // wait for async Rubix requests to complete
            assertEventually(
                    new Duration(10, SECONDS),
                    () -> assertEquals(getAsyncDownloadedMb(readMode), beforeAsyncDownloadedMb + 100));
        }

        // stats are propagated asynchronously
        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    // data should be fetched from remote source
                    assertGreaterThan(getRemoteReadsCount(), beforeRemoteReadsCount);
                });

        // ensure that subsequent read uses cache exclusively
        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    long remoteReadsCount = getRemoteReadsCount();
                    assertTrue(Arrays.equals(randomData, readFile(cachingFileSystem, file)));
                    assertGreaterThan(getCachedReadsCount(), beforeCachedReadsCount);
                    assertEquals(getRemoteReadsCount(), remoteReadsCount);
                });
        long secondCachedReadsCount = getCachedReadsCount();
        long secondRemoteReadsCount = getRemoteReadsCount();

        // make sure parallel reading of large file works
        ExecutorService executorService = newFixedThreadPool(3);
        try {
            List<Callable<?>> reads = nCopies(
                    3,
                    () -> {
                        assertTrue(Arrays.equals(randomData, readFile(cachingFileSystem, file)));
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

    @Test
    public void testFileSystemBindings()
            throws Exception
    {
        initializeRubix(new RubixConfig());

        try (FileSystem fileSystem = getCachingFileSystem(context, new Path("s3://bucket_name"))) {
            assertRawFileSystemInstanceOf(fileSystem, CachingTrinoS3FileSystem.class);
        }

        try (FileSystem fileSystem = getCachingFileSystem(context, new Path("s3a://bucket_name"))) {
            assertRawFileSystemInstanceOf(fileSystem, CachingTrinoS3FileSystem.class);
        }

        try (FileSystem fileSystem = getCachingFileSystem(context, new Path("s3n://bucket_name"))) {
            assertRawFileSystemInstanceOf(fileSystem, CachingTrinoS3FileSystem.class);
        }

        try (FileSystem fileSystem = getCachingFileSystem(context, new Path("abfs://fileanalysis@foo-bar.dfs.core.windows.net/tutorials"))) {
            assertRawFileSystemInstanceOf(fileSystem, CachingPrestoAzureBlobFileSystem.class);
        }

        // TODO: add check for "wasb" Azure FS.
        // Testing "wasb" Azure FS requires valid Azure credentials as NativeAzureFileSystem tries to connect during initialization
        // Fix after: https://github.com/trinodb/trino/issues/2380

        try (FileSystem fileSystem = getCachingFileSystem(context, new Path("abfss://fileanalysis@foo-bar.dfs.core.windows.net/tutorials"))) {
            assertRawFileSystemInstanceOf(fileSystem, CachingPrestoSecureAzureBlobFileSystem.class);
        }

        try (FileSystem fileSystem = getCachingFileSystem(context, new Path("adl://fileanalysis@foo-bar.dfs.core.windows.net/tutorials"))) {
            assertRawFileSystemInstanceOf(fileSystem, CachingPrestoAdlFileSystem.class);
        }

        try (FileSystem fileSystem = getCachingFileSystem(context, new Path("gs://bucket_name"))) {
            assertRawFileSystemInstanceOf(fileSystem, CachingPrestoGoogleHadoopFileSystem.class);
        }

        try (FileSystem fileSystem = getCachingFileSystem(context, new Path("hdfs://localhost:7897"))) {
            assertRawFileSystemInstanceOf(fileSystem, CachingPrestoDistributedFileSystem.class);
        }
    }

    private void assertRawFileSystemInstanceOf(FileSystem actual, Class<? extends FileSystem> expectedType)
    {
        assertInstanceOf(actual, FilterFileSystem.class);
        FileSystem rawFileSystem = ((FilterFileSystem) actual).getRawFileSystem();
        assertInstanceOf(rawFileSystem, expectedType);
    }

    private byte[] readFile(FileSystem fileSystem, Path path)
    {
        try (FSDataInputStream inputStream = fileSystem.open(path)) {
            return ByteStreams.toByteArray(inputStream);
        }
        catch (IOException exception) {
            throw new RuntimeException(exception);
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
            long directRemoteReads = (long) BEAN_SERVER.getAttribute(new ObjectName("rubix:name=stats,type=detailed,catalog=catalog"), "Direct_rrc_requests");
            long remoteReads = (long) BEAN_SERVER.getAttribute(new ObjectName("rubix:name=stats,type=detailed,catalog=catalog"), "Remote_rrc_requests");
            return directRemoteReads + remoteReads;
        }
        catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private long getCachedReadsCount()
    {
        try {
            return (long) BEAN_SERVER.getAttribute(new ObjectName("rubix:name=stats,type=detailed,catalog=catalog"), "Cached_rrc_requests");
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
