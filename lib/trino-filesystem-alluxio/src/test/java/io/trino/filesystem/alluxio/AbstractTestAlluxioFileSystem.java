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

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.DeletePOptions;
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractTestAlluxioFileSystem
        extends AbstractTestTrinoFileSystem
{
    private TrinoFileSystem fileSystem;
    private Location rootLocation;
    private FileSystem alluxioFs;
    private AlluxioFileSystemFactory alluxioFileSystemFactory;

    protected void initialize()
            throws IOException
    {
        this.rootLocation = Location.of("alluxio:///");
        InstancedConfiguration conf = Configuration.copyGlobal();
        FileSystemContext fsContext = FileSystemContext.create(conf);
        this.alluxioFs = FileSystem.Factory.create(fsContext);
        this.alluxioFileSystemFactory = new AlluxioFileSystemFactory(conf);
        this.fileSystem = alluxioFileSystemFactory.create(ConnectorIdentity.ofUser("alluxio"));
    }

    @AfterAll
    void tearDown()
    {
        fileSystem = null;
        alluxioFs = null;
        rootLocation = null;
        alluxioFileSystemFactory = null;
    }

    @AfterEach
    void afterEach()
            throws IOException, AlluxioException
    {
        AlluxioURI root = new AlluxioURI(getRootLocation().toString());

        for (URIStatus status : alluxioFs.listStatus(root)) {
            alluxioFs.delete(new AlluxioURI(status.getPath()), DeletePOptions.newBuilder().setRecursive(true).build());
        }
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
        return rootLocation;
    }

    @Override
    protected void verifyFileSystemIsEmpty()
    {
        AlluxioURI bucket =
                AlluxioUtils.convertToAlluxioURI(rootLocation, ((AlluxioFileSystem) fileSystem).getMountRoot());
        try {
            assertThat(alluxioFs.listStatus(bucket)).isEmpty();
        }
        catch (IOException | AlluxioException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected final boolean supportsCreateExclusive()
    {
        return false;
    }

    @Override
    protected boolean supportsIncompleteWriteNoClobber()
    {
        return false;
    }
}
