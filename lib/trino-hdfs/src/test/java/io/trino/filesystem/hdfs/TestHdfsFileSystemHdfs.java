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
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.io.UncheckedIOException;

import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHdfsFileSystemHdfs
        extends AbstractTestTrinoFileSystem
{
    private Hadoop hadoop;
    private HdfsEnvironment hdfsEnvironment;
    private HdfsContext hdfsContext;
    private TrinoFileSystem fileSystem;

    @BeforeAll
    void beforeAll()
    {
        hadoop = new Hadoop();
        hadoop.start();

        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), emptySet());
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
}
