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
package io.trino.filesystem.hdfs;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import io.airlift.units.DataSize;
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.AbstractTrinoFileSystemTestingEnvironment;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.hdfs.s3.HiveS3Config;
import io.trino.hdfs.s3.TrinoS3ConfigurationInitializer;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHdfsFileSystemS3Mock
        extends AbstractTestTrinoFileSystem
{
    private HdfsFileSystemTestingEnvironmentS3Mock testingEnvironment;

    @BeforeAll
    void beforeAll()
    {
        testingEnvironment = new HdfsFileSystemTestingEnvironmentS3Mock();
    }

    @AfterAll
    void afterAll()
    {
        if (testingEnvironment != null) {
            testingEnvironment.close();
            testingEnvironment = null;
        }
    }

    @AfterEach
    void afterEach()
            throws IOException
    {
        testingEnvironment.cleanupFiles();
    }

    @Override
    protected AbstractTrinoFileSystemTestingEnvironment testingEnvironment()
    {
        return testingEnvironment;
    }

    public static class HdfsFileSystemTestingEnvironmentS3Mock
            extends AbstractTrinoFileSystemTestingEnvironment
    {
        private static final String BUCKET = "test-bucket";

        private final S3MockContainer s3Mock;

        private final HdfsEnvironment hdfsEnvironment;
        private final HdfsContext hdfsContext;
        private final TrinoFileSystem fileSystem;

        public HdfsFileSystemTestingEnvironmentS3Mock()
        {
            s3Mock = new S3MockContainer("3.0.1")
                    .withInitialBuckets(BUCKET);
            s3Mock.start();
            HiveS3Config s3Config = new HiveS3Config()
                    .setS3AwsAccessKey("accesskey")
                    .setS3AwsSecretKey("secretkey")
                    .setS3Endpoint(s3Mock.getHttpEndpoint())
                    .setS3PathStyleAccess(true)
                    .setS3StreamingPartSize(DataSize.valueOf("5.5MB"));

            HdfsConfig hdfsConfig = new HdfsConfig();
            ConfigurationInitializer s3Initializer = new TrinoS3ConfigurationInitializer(s3Config);
            HdfsConfigurationInitializer initializer = new HdfsConfigurationInitializer(hdfsConfig, Set.of(s3Initializer));
            HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(initializer, emptySet());
            hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
            hdfsContext = new HdfsContext(ConnectorIdentity.ofUser("test"));

            fileSystem = new HdfsFileSystem(hdfsEnvironment, hdfsContext, new TrinoHdfsFileSystemStats());
        }

        public void close()
        {
            s3Mock.close();
        }

        public void cleanupFiles()
                throws IOException
        {
            Path root = new Path(getRootLocation().toString());
            FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, root);
            for (FileStatus status : fs.listStatus(root)) {
                fs.delete(status.getPath(), true);
            }
        }

        @Override
        protected final boolean isHierarchical()
        {
            return false;
        }

        @Override
        public TrinoFileSystem getFileSystem()
        {
            return fileSystem;
        }

        @Override
        protected Location getRootLocation()
        {
            return Location.of("s3://%s/".formatted(BUCKET));
        }

        @Override
        protected final boolean supportsCreateWithoutOverwrite()
        {
            return false;
        }

        @Override
        protected final boolean deleteFileFailsIfNotExists()
        {
            return false;
        }

        @Override
        protected boolean normalizesListFilesResult()
        {
            return true;
        }

        @Override
        protected boolean seekPastEndOfFileFails()
        {
            return false;
        }

        @Override
        protected void verifyFileSystemIsEmpty()
        {
            try {
                Path root = new Path(getRootLocation().toString());
                FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, root);
                assertThat(fs.listStatus(root)).isEmpty();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
