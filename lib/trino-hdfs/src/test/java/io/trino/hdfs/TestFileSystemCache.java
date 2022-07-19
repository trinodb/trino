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
package io.trino.hdfs;

import com.google.common.collect.ImmutableSet;
import io.trino.hdfs.authentication.ImpersonatingHdfsAuthentication;
import io.trino.hdfs.authentication.SimpleHadoopAuthentication;
import io.trino.hdfs.authentication.SimpleUserNameProvider;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestFileSystemCache
{
    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        FileSystem.closeAll();
    }

    @Test
    public void testFileSystemCache()
            throws IOException
    {
        FileSystem.closeAll();
        HdfsEnvironment environment = new HdfsEnvironment(
                new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig()), ImmutableSet.of()),
                new HdfsConfig(),
                new ImpersonatingHdfsAuthentication(new SimpleHadoopAuthentication(), new SimpleUserNameProvider()));
        ConnectorIdentity userId = ConnectorIdentity.ofUser("user");
        ConnectorIdentity otherUserId = ConnectorIdentity.ofUser("other_user");
        FileSystem fs1 = getFileSystem(environment, userId);
        FileSystem fs2 = getFileSystem(environment, userId);
        assertSame(fs1, fs2);

        FileSystem fs3 = getFileSystem(environment, otherUserId);
        assertNotSame(fs1, fs3);

        FileSystem fs4 = getFileSystem(environment, otherUserId);
        assertSame(fs3, fs4);

        FileSystem.closeAll();

        FileSystem fs5 = getFileSystem(environment, userId);
        assertNotSame(fs5, fs1);
    }

    @Test
    public void testFileSystemCacheException() throws IOException
    {
        FileSystem.closeAll();
        HdfsEnvironment environment = new HdfsEnvironment(
                new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig()), ImmutableSet.of()),
                new HdfsConfig(),
                new ImpersonatingHdfsAuthentication(new SimpleHadoopAuthentication(), new SimpleUserNameProvider()));

        int maxCacheSize = 1000;
        for (int i = 0; i < maxCacheSize; ++i) {
            assertEquals(TrinoFileSystemCache.INSTANCE.getFileSystemCacheStats().getCacheSize(), i);
            getFileSystem(environment, ConnectorIdentity.ofUser("user" + String.valueOf(i)));
        }
        assertEquals(TrinoFileSystemCache.INSTANCE.getFileSystemCacheStats().getCacheSize(), maxCacheSize);

        try {
            getFileSystem(environment, ConnectorIdentity.ofUser("user" + String.valueOf(maxCacheSize)));
            fail("Should have thrown IOException from above");
        }
        catch (IOException e) {
            assertEquals(e.getMessage(), "FileSystem max cache size has been reached: " + maxCacheSize);
        }
    }

    @Test
    public void testFileSystemCacheConcurrency() throws InterruptedException, ExecutionException, IOException
    {
        int numThreads = 20;
        List<Callable<Void>> callableTasks = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            callableTasks.add(
                    new CreateFileSystemAndConsume(
                            new SplittableRandom(i),
                            10,
                            1000,
                            new FileSystemCloser()));
        }

        FileSystem.closeAll();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        assertEquals(TrinoFileSystemCache.INSTANCE.getFileSystemCacheStats().getCacheSize(), 0);
        List<Future<Void>> futures = executor.invokeAll(callableTasks);
        for (Future<Void> fut : futures) {
            fut.get();
        }
        executor.shutdown();
        assertEquals(TrinoFileSystemCache.INSTANCE.getFileSystemCacheStats().getCacheSize(), 0, "Cache size is non zero");
    }

    private static FileSystem getFileSystem(HdfsEnvironment environment, ConnectorIdentity identity)
            throws IOException
    {
        return environment.getFileSystem(identity, new Path("/"), newEmptyConfiguration());
    }

    @FunctionalInterface
    public interface FileSystemConsumer
    {
        void consume(FileSystem fileSystem) throws IOException;
    }

    private static class FileSystemCloser
            implements FileSystemConsumer
    {
        @Override
        @SuppressModernizer
        public void consume(FileSystem fileSystem) throws IOException
        {
            fileSystem.close();  /* triggers fscache.remove() */
        }
    }

    // A callable that creates (and consumes) filesystem objects X times for Y users
    public static class CreateFileSystemAndConsume
            implements Callable<Void>
    {
        private final SplittableRandom random;
        private final int numUsers;
        private final int numGetCallsPerInvocation;
        private final FileSystemConsumer consumer;

        private HdfsEnvironment environment = new HdfsEnvironment(
                new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig()), ImmutableSet.of()),
                new HdfsConfig(),
                new ImpersonatingHdfsAuthentication(new SimpleHadoopAuthentication(), new SimpleUserNameProvider()));

        CreateFileSystemAndConsume(SplittableRandom random, int numUsers, int numGetCallsPerInvocation, FileSystemConsumer consumer)
        {
            this.random = random;
            this.numUsers = numUsers;
            this.numGetCallsPerInvocation = numGetCallsPerInvocation;
            this.consumer = consumer;
        }

        @Override
        public Void call() throws IOException
        {
            for (int i = 0; i < numGetCallsPerInvocation; ++i) {
                FileSystem fs = getFileSystem(environment, ConnectorIdentity.ofUser("user" + String.valueOf(random.nextInt(numUsers))));
                consumer.consume(fs);
            }
            return null;
        }
    }
}
