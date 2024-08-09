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

import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;

import static io.trino.spi.StandardErrorCode.STORAGE_LIMIT_EXCEEDED;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHdfsFileSystemHdfs
        extends AbstractTestTrinoFileSystem
{
    private Hadoop hadoop;
    private HdfsConfiguration hdfsConfiguration;
    private HdfsEnvironment hdfsEnvironment;
    private HdfsContext hdfsContext;
    private TrinoFileSystem fileSystem;

    @BeforeAll
    void beforeAll()
    {
        hadoop = new Hadoop();
        hadoop.start();

        HdfsConfig hdfsConfig = new HdfsConfig();
        hdfsConfiguration = new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), emptySet());
        hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        hdfsContext = new HdfsContext(ConnectorIdentity.ofUser("test"));

        fileSystem = new HdfsFileSystem(hdfsEnvironment, hdfsContext, new TrinoHdfsFileSystemStats());
    }

    @AfterEach
    void afterEach()
            throws IOException
    {
        Path root = new Path(getRootLocation().toString());
        FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, root);
        for (FileStatus status : fs.listStatus(root)) {
            fs.delete(status.getPath(), true);
        }
    }

    @AfterAll
    void afterAll()
    {
        hadoop.stop();
    }

    @Override
    protected boolean isHierarchical()
    {
        return true;
    }

    @Override
    protected boolean supportsIncompleteWriteNoClobber()
    {
        return false;
    }

    @Override
    protected TrinoFileSystem getFileSystem()
    {
        return fileSystem;
    }

    @Override
    protected Location getRootLocation()
    {
        return Location.of(hadoop.getHdfsUri());
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

    @Test
    void testCreateDirectoryPermission()
            throws IOException
    {
        assertCreateDirectoryPermission(fileSystem, hdfsEnvironment, (short) 777);
    }

    @Test
    void testCreateDirectoryPermissionWithSkip()
            throws IOException
    {
        HdfsConfig configWithSkip = new HdfsConfig()
                .setNewDirectoryPermissions(HdfsConfig.SKIP_DIR_PERMISSIONS);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, configWithSkip, new NoHdfsAuthentication());
        TrinoFileSystem fileSystem = new HdfsFileSystem(hdfsEnvironment, hdfsContext, new TrinoHdfsFileSystemStats());

        assertCreateDirectoryPermission(fileSystem, hdfsEnvironment, (short) 755);
    }

    @Test
    void testNamespaceQuotaExceededOnMkdirs()
            throws Exception
    {
        testQuotaExceededException((location) -> fileSystem.createDirectory(location));
    }

    @Test
    void testNamespaceQuotaExceededOnNewFile()
            throws Exception
    {
        testQuotaExceededException((location) -> fileSystem.newOutputFile(location).create().close());
    }

    @Test
    void testSpaceQuotaExceededOnFileWrite()
            throws Exception
    {
        testQuotaExceededException((location) -> {
            try (var stream = fileSystem.newOutputFile(location).create()) {
                stream.write(42);
            }
        });
    }

    private void testQuotaExceededException(ThrowingConsumer<Location> quotaExceedingAction)
            throws Exception
    {
        Path root = new Path(getRootLocation().toString());
        FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, root);
        fs.mkdirs(new Path(root, "quota"));

        // Set tiny quotas such that any new addition will trigger the quota to be exceeded
        for (var command : new String[] {"-setQuota", "-setSpaceQuota" }) {
            hadoop.executeInContainerFailOnError("hdfs", "dfsadmin", command, "1", "/quota");
        }

        Location location = getRootLocation().appendPath("quota").appendPath("file");
        assertThatThrownBy(() -> quotaExceedingAction.acceptThrows(location))
                .isInstanceOfSatisfying(TrinoException.class,
                        e -> assertThat(e.getErrorCode()).isEqualTo(STORAGE_LIMIT_EXCEEDED.toErrorCode()))
                .hasMessageContaining("storage quota exceeded")
                .hasCauseInstanceOf(QuotaExceededException.class);
    }

    private void assertCreateDirectoryPermission(TrinoFileSystem fileSystem, HdfsEnvironment hdfsEnvironment, short permission)
            throws IOException
    {
        Location location = getRootLocation().appendPath("test");
        fileSystem.createDirectory(location);
        Path path = new Path(location.toString());
        FileStatus status = hdfsEnvironment.getFileSystem(hdfsContext, path).getFileStatus(path);
        assertThat(status.getPermission().toOctal()).isEqualTo(permission);
    }
}
