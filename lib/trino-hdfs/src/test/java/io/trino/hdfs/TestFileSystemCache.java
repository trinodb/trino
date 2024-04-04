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
import io.airlift.concurrent.MoreFutures;
import io.trino.hdfs.authentication.ImpersonatingHdfsAuthentication;
import io.trino.hdfs.authentication.SimpleHadoopAuthentication;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.trino.plugin.base.security.UserNameProvider.SIMPLE_USER_NAME_PROVIDER;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestFileSystemCache
{
    @BeforeEach
    @AfterAll
    public void cleanup()
            throws IOException
    {
        FileSystem.closeAll();
    }

    @Test
    public void testFileSystemCache()
            throws IOException
    {
        HdfsEnvironment environment = new HdfsEnvironment(
                new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig()), ImmutableSet.of()),
                new HdfsConfig(),
                new ImpersonatingHdfsAuthentication(new SimpleHadoopAuthentication(), SIMPLE_USER_NAME_PROVIDER));
        ConnectorIdentity userId = ConnectorIdentity.ofUser("user");
        ConnectorIdentity otherUserId = ConnectorIdentity.ofUser("other_user");
        FileSystem fs1 = getFileSystem(environment, userId);
        FileSystem fs2 = getFileSystem(environment, userId);
        assertThat(fs1).isSameAs(fs2);

        FileSystem fs3 = getFileSystem(environment, otherUserId);
        assertThat(fs1).isNotSameAs(fs3);

        FileSystem fs4 = getFileSystem(environment, otherUserId);
        assertThat(fs3).isSameAs(fs4);

        FileSystem.closeAll();

        FileSystem fs5 = getFileSystem(environment, userId);
        assertThat(fs5).isNotSameAs(fs1);
    }

    @Test
    public void testFileSystemCacheException()
            throws IOException
    {
        HdfsEnvironment environment = new HdfsEnvironment(
                new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig()), ImmutableSet.of()),
                new HdfsConfig(),
                new ImpersonatingHdfsAuthentication(new SimpleHadoopAuthentication(), SIMPLE_USER_NAME_PROVIDER));

        int maxCacheSize = 1000;
        for (int i = 0; i < maxCacheSize; i++) {
            assertThat(TrinoFileSystemCacheStats.instance().getCacheSize()).isEqualTo(i);
            getFileSystem(environment, ConnectorIdentity.ofUser("user" + i));
        }
        assertThat(TrinoFileSystemCacheStats.instance().getCacheSize()).isEqualTo(maxCacheSize);
        assertThatThrownBy(() -> getFileSystem(environment, ConnectorIdentity.ofUser("user" + maxCacheSize)))
                .isInstanceOf(IOException.class)
                .hasMessage("FileSystem max cache size has been reached: " + maxCacheSize);
    }

    @Test
    public void testFileSystemCacheConcurrency()
            throws InterruptedException, ExecutionException, IOException
    {
        int numThreads = 20;
        List<Callable<Void>> callableTasks = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            callableTasks.add(
                    new CreateFileSystemsAndConsume(
                            new SplittableRandom(i),
                            10,
                            1000,
                            new FileSystemCloser()));
        }
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        assertThat(TrinoFileSystemCacheStats.instance().getCacheSize()).isEqualTo(0);
        executor.invokeAll(callableTasks).forEach(MoreFutures::getFutureValue);
        executor.shutdown();
        assertThat(TrinoFileSystemCacheStats.instance().getCacheSize())
                .describedAs("Cache size is non zero")
                .isEqualTo(0);
    }

    private static FileSystem getFileSystem(HdfsEnvironment environment, ConnectorIdentity identity)
            throws IOException
    {
        return environment.getFileSystem(identity, new Path("/"), new Configuration(false));
    }

    @FunctionalInterface
    public interface FileSystemConsumer
    {
        void consume(FileSystem fileSystem)
                throws IOException;
    }

    private static class FileSystemCloser
            implements FileSystemConsumer
    {
        @Override
        @SuppressModernizer
        public void consume(FileSystem fileSystem)
                throws IOException
        {
            fileSystem.close();  /* triggers fscache.remove() */
        }
    }

    public static class CreateFileSystemsAndConsume
            implements Callable<Void>
    {
        private final SplittableRandom random;
        private final int userCount;
        private final int getCallsPerInvocation;
        private final FileSystemConsumer consumer;

        private static final HdfsEnvironment environment = new HdfsEnvironment(
                new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig()), ImmutableSet.of()),
                new HdfsConfig(),
                new ImpersonatingHdfsAuthentication(new SimpleHadoopAuthentication(), SIMPLE_USER_NAME_PROVIDER));

        CreateFileSystemsAndConsume(SplittableRandom random, int numUsers, int numGetCallsPerInvocation, FileSystemConsumer consumer)
        {
            this.random = requireNonNull(random, "random is null");
            this.userCount = numUsers;
            this.getCallsPerInvocation = numGetCallsPerInvocation;
            this.consumer = consumer;
        }

        @Override
        public Void call()
                throws IOException
        {
            for (int i = 0; i < getCallsPerInvocation; i++) {
                FileSystem fs = getFileSystem(environment, ConnectorIdentity.ofUser("user" + random.nextInt(userCount)));
                consumer.consume(fs);
            }
            return null;
        }
    }
}
