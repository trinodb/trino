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
import com.google.common.io.CharStreams;
import com.google.common.io.Closer;
import com.qubole.rubix.bookkeeper.LocalDataTransferServer;
import com.qubole.rubix.core.CachingFileSystem;
import com.qubole.rubix.core.CachingFileSystemStats;
import com.qubole.rubix.core.utils.DummyClusterManager;
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
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.URI;
import java.time.Duration;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.qubole.rubix.spi.CacheConfig.setPrestoClusterManager;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.prestosql.client.NodeVersion.UNKNOWN;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestRubixCaching
{
    private java.nio.file.Path tempDirectory;
    private Path cacheStoragePath;
    private HdfsConfig config;
    private HdfsContext context;
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
        RubixConfig rubixConfig = new RubixConfig();
        rubixConfig.setCacheLocation(Joiner.on(",").join(
                cacheDirectories.stream()
                        .map(java.nio.file.Path::toString)
                        .collect(toImmutableList())));
        RubixConfigurationInitializer rubixConfigInitializer = new RubixConfigurationInitializer(rubixConfig);
        HdfsConfigurationInitializer configurationInitializer = new HdfsConfigurationInitializer(
                config,
                ImmutableSet.of(
                        // make sure that dummy cluster manager is used
                        initConfig -> setPrestoClusterManager(initConfig, DummyClusterManager.class.getName())));
        RubixInitializer rubixInitializer = new RubixInitializer(
                new CatalogName("catalog"),
                rubixConfigInitializer,
                configurationInitializer);
        InternalNode coordinatorNode = new InternalNode(
                "master",
                URI.create("http://127.0.0.1:8080"),
                UNKNOWN,
                true);
        TestingNodeManager nodeManager = new TestingNodeManager(
                coordinatorNode,
                ImmutableList.of());
        rubixInitializer.initializeRubix(nodeManager);
        waitForRubix(rubixConfigInitializer);

        return rubixConfigInitializer;
    }

    private void waitForRubix(RubixConfigurationInitializer rubixConfigInitializer)
    {
        // wait for rubix to start
        Failsafe.with(
                new RetryPolicy<>()
                        .withDelay(Duration.ofSeconds(1))
                        // unlimited attempts
                        .withMaxAttempts(-1)
                        .withMaxDuration(Duration.ofMinutes(1)))
                .run(() -> checkState(rubixConfigInitializer.isCacheReady()));
    }

    private FileSystem getCachingFileSystem(
            RubixConfigurationInitializer rubixConfigInitializer)
            throws IOException
    {
        HdfsConfigurationInitializer configurationInitializer = new HdfsConfigurationInitializer(config, ImmutableSet.of());
        HiveHdfsConfiguration configuration = new HiveHdfsConfiguration(
                configurationInitializer,
                ImmutableSet.of(
                        (dynamicConfig, ignoredContext, ignoredUri) -> {
                            dynamicConfig.set("fs.file.impl", CachingLocalFileSystem.class.getName());
                        },
                        rubixConfigInitializer));
        HdfsEnvironment environment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());
        return environment.getFileSystem(context, cacheStoragePath);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(() -> deleteRecursively(tempDirectory, ALLOW_INSECURE));
            closer.register(() -> nonCachingFileSystem.close());
            closer.register(() -> cachingFileSystem.close());
            closer.register(LocalDataTransferServer::stopServer);
        }
    }

    @Test
    public void testCoordinatorNotJoining()
    {
        RubixConfig rubixConfig = new RubixConfig();
        RubixConfigurationInitializer rubixConfigInitializer = new RubixConfigurationInitializer(rubixConfig);
        HdfsConfigurationInitializer configurationInitializer = new HdfsConfigurationInitializer(config, ImmutableSet.of());
        RubixInitializer rubixInitializer = new RubixInitializer(
                new RetryPolicy<>().withMaxAttempts(1),
                new CatalogName("catalog"),
                rubixConfigInitializer,
                configurationInitializer);
        InternalNode workerNode = new InternalNode(
                "master",
                URI.create("http://127.0.0.1:8080"),
                UNKNOWN,
                false);
        rubixInitializer.initializeRubix(new TestingNodeManager(ImmutableList.of(workerNode)));
        HiveHdfsConfiguration configuration = new HiveHdfsConfiguration(configurationInitializer, ImmutableSet.of(rubixConfigInitializer));
        HdfsEnvironment environment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());
        waitForRubix(rubixConfigInitializer);
        assertThatThrownBy(() -> environment.getFileSystem(context, cacheStoragePath))
                .hasRootCauseMessage("Exceeded timeout while waiting for coordinator node");
    }

    @Test
    public void testNonExistingCacheDirectory()
    {
        RubixConfig rubixConfig = new RubixConfig();
        rubixConfig.setCacheLocation("/tmp/non/existing/directory");
        RubixConfigurationInitializer rubixConfigInitializer = new RubixConfigurationInitializer(rubixConfig);
        HdfsConfigurationInitializer configurationInitializer = new HdfsConfigurationInitializer(config, ImmutableSet.of());
        RubixInitializer rubixInitializer = new RubixInitializer(new CatalogName("catalog"), rubixConfigInitializer, configurationInitializer);
        InternalNode coordinatorNode = new InternalNode(
                "master",
                URI.create("http://127.0.0.1:8080"),
                UNKNOWN,
                true);
        rubixInitializer.initializeRubix(new TestingNodeManager(ImmutableList.of(coordinatorNode)));
        HiveHdfsConfiguration configuration = new HiveHdfsConfiguration(configurationInitializer, ImmutableSet.of(rubixConfigInitializer));
        HdfsEnvironment environment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());
        waitForRubix(rubixConfigInitializer);
        assertThatThrownBy(() -> environment.getFileSystem(context, cacheStoragePath))
                .hasRootCauseMessage("None of the cache parent directories exists");
    }

    @Test
    public void testCacheRead()
            throws Exception
    {
        Path file = getStoragePath("some_file");

        writeFile(nonCachingFileSystem.create(file), "Hello world");

        long beforeRemoteReads = getCacheStats().getRemoteReads();
        long beforeCachedReads = getCacheStats().getCachedReads();

        assertEquals(readFile(cachingFileSystem.open(file)), "Hello world");

        // stats are propagated asynchronously, wait for them
        sleep(1000L);
        long intermittentRemoteReads = getCacheStats().getRemoteReads();
        assertGreaterThan(intermittentRemoteReads, beforeRemoteReads);

        assertEquals(readFile(cachingFileSystem.open(file)), "Hello world");

        // stats are propagated asynchronously, wait for them
        sleep(1000L);
        assertGreaterThan(getCacheStats().getCachedReads(), beforeCachedReads);
        assertEquals(getCacheStats().getRemoteReads(), intermittentRemoteReads);
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

    private CachingFileSystemStats getCacheStats()
            throws NoSuchFieldException, IllegalAccessException
    {
        Field field = CachingFileSystem.class.getDeclaredField("statsMBean");
        field.setAccessible(true);
        return (CachingFileSystemStats) field.get(null);
    }
}
