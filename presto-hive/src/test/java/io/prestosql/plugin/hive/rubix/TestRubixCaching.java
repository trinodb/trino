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
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.common.io.Closer;
import com.qubole.rubix.core.utils.DummyClusterManager;
import io.airlift.units.DataSize;
import io.prestosql.metadata.InternalNode;
import io.prestosql.plugin.base.CatalogName;
import io.prestosql.plugin.hive.HdfsConfig;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.testing.TestingNodeManager;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.IOException;
import java.io.InputStreamReader;
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
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.client.NodeVersion.UNKNOWN;
import static io.prestosql.plugin.hive.util.RetryDriver.retry;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Collections.nCopies;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestRubixCaching
{
    private static final DataSize LARGE_FILE_SIZE = DataSize.of(100, MEGABYTE);
    private static final MBeanServer BEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    private java.nio.file.Path tempDirectory;
    private Path cacheStoragePath;
    private HdfsConfig config;
    private HdfsContext context;
    private RubixInitializer rubixInitializer;
    private FileSystem nonCachingFileSystem;
    private FileSystem cachingFileSystem;

    @BeforeClass
    public void setup()
            throws IOException
    {
        tempDirectory = createTempDirectory(getClass().getSimpleName());
        cacheStoragePath = getStoragePath("/");
        config = new HdfsConfig();
        context = new HdfsContext(ConnectorIdentity.ofUser("user"));

        nonCachingFileSystem = getNonCachingFileSystem();
        cachingFileSystem = getCachingFileSystem(initializeRubix());
    }

    private FileSystem getNonCachingFileSystem()
            throws IOException
    {
        HdfsConfigurationInitializer configurationInitializer = new HdfsConfigurationInitializer(config);
        HiveHdfsConfiguration configuration = new HiveHdfsConfiguration(configurationInitializer, ImmutableSet.of());
        HdfsEnvironment environment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());
        return environment.getFileSystem(context, cacheStoragePath);
    }

    private RubixConfigurationInitializer initializeRubix()
            throws IOException
    {
        // create cache directories
        List<java.nio.file.Path> cacheDirectories = ImmutableList.of(
                tempDirectory.resolve("cache1"),
                tempDirectory.resolve("cache2"));
        for (java.nio.file.Path directory : cacheDirectories) {
            createDirectories(directory);
        }

        // initialize rubix in master-only mode
        RubixConfig rubixConfig = new RubixConfig()
                .setStartServerOnCoordinator(true);
        rubixConfig.setCacheLocation(Joiner.on(",").join(
                cacheDirectories.stream()
                        .map(java.nio.file.Path::toString)
                        .collect(toImmutableList())));
        RubixConfigurationInitializer rubixConfigInitializer = new RubixConfigurationInitializer(rubixConfig);
        HdfsConfigurationInitializer configurationInitializer = new HdfsConfigurationInitializer(config, ImmutableSet.of());
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

        return rubixConfigInitializer;
    }

    private FileSystem getCachingFileSystem(
            RubixConfigurationInitializer rubixConfigInitializer)
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
        try (Closer closer = Closer.create()) {
            closer.register(() -> deleteRecursively(tempDirectory, ALLOW_INSECURE));
            closer.register(nonCachingFileSystem);
            closer.register(cachingFileSystem);
            closer.register(rubixInitializer::stopRubix);
        }
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

    @Test
    public void testCacheRead()
            throws Exception
    {
        Path file = getStoragePath("some_file");

        writeFile(nonCachingFileSystem.create(file), "Hello world");

        long beforeRemoteReadsCount = getRemoteReadsCount();
        long beforeCachedReadsCount = getCachedReadsCount();

        assertEquals(readFile(cachingFileSystem.open(file)), "Hello world");

        // stats are propagated asynchronously, wait for them
        sleep(1000L);
        long firstRemoteReadsCount = getRemoteReadsCount();
        // data should be read from remote source only
        assertGreaterThan(firstRemoteReadsCount, beforeRemoteReadsCount);
        assertEquals(getCachedReadsCount(), beforeCachedReadsCount);

        assertEquals(readFile(cachingFileSystem.open(file)), "Hello world");

        // stats are propagated asynchronously, wait for them
        sleep(1000L);
        // data should be read from cache only
        assertGreaterThan(getCachedReadsCount(), beforeCachedReadsCount);
        assertEquals(getRemoteReadsCount(), firstRemoteReadsCount);
    }

    @Test
    public void testCacheWrite()
            throws IOException
    {
        Path file = getStoragePath("some_file_write");

        writeFile(cachingFileSystem.create(file), "Hello world");
        assertEquals(readFile(nonCachingFileSystem.open(file)), "Hello world");
    }

    @Test
    public void testLargeFile()
            throws Exception
    {
        byte[] randomData = new byte[(int) LARGE_FILE_SIZE.toBytes()];
        new Random().nextBytes(randomData);

        Path file = getStoragePath("large_file");
        try (FSDataOutputStream outputStream = nonCachingFileSystem.create(file)) {
            outputStream.write(randomData);
        }

        long beforeRemoteReadsCount = getRemoteReadsCount();
        long beforeCachedReadsCount = getCachedReadsCount();

        assertTrue(Arrays.equals(randomData, readFileBytes(cachingFileSystem.open(file))));

        // stats are propagated asynchronously, wait for them
        sleep(1000L);
        long firstRemoteReadsCount = getRemoteReadsCount();
        // data should be fetched from remote source
        assertGreaterThan(firstRemoteReadsCount, beforeRemoteReadsCount);

        assertTrue(Arrays.equals(randomData, readFileBytes(cachingFileSystem.open(file))));

        // stats are propagated asynchronously, wait for them
        sleep(1000L);
        long secondCachedReadsCount = getCachedReadsCount();
        long secondRemoteReadsCount = getRemoteReadsCount();
        // data should be read from cache only
        assertGreaterThan(secondCachedReadsCount, beforeCachedReadsCount);
        assertEquals(secondRemoteReadsCount, firstRemoteReadsCount);

        // make sure parallel reading of large file works
        ExecutorService executorService = newFixedThreadPool(3);
        try {
            List<Callable<?>> reads = nCopies(
                    3,
                    () -> {
                        assertTrue(Arrays.equals(randomData, readFileBytes(cachingFileSystem.open(file))));
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

        // stats are propagated asynchronously, wait for them
        sleep(1000L);
        // data should be read from cache only
        assertGreaterThan(getCachedReadsCount(), secondCachedReadsCount);
        assertEquals(getRemoteReadsCount(), secondRemoteReadsCount);
    }

    private String readFile(FSDataInputStream inputStream)
            throws IOException
    {
        try {
            return CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
        }
        finally {
            inputStream.close();
        }
    }

    private byte[] readFileBytes(FSDataInputStream inputStream)
            throws IOException
    {
        try {
            return ByteStreams.toByteArray(inputStream);
        }
        finally {
            inputStream.close();
        }
    }

    private void writeFile(FSDataOutputStream outputStream, String content)
            throws IOException
    {
        try {
            outputStream.writeBytes(content);
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
}
