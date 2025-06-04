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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestAlluxioFileSystem
        extends AbstractTestTrinoFileSystem
{
    private static final String IMAGE_NAME = "alluxio/alluxio:2.9.5";
    private static final DockerImageName ALLUXIO_IMAGE = DockerImageName.parse(IMAGE_NAME);

    @Container
    private static final GenericContainer<?> ALLUXIO_MASTER_CONTAINER = createAlluxioMasterContainer();

    @Container
    private static final GenericContainer<?> ALLUXIO_WORKER_CONTAINER = createAlluxioWorkerContainer();

    private TrinoFileSystem fileSystem;
    private Location rootLocation;
    private FileSystem alluxioFs;
    private AlluxioFileSystemFactory alluxioFileSystemFactory;

    @BeforeAll
    void setup()
    {
        this.rootLocation = Location.of("alluxio:///");
        InstancedConfiguration conf = Configuration.copyGlobal();
        FileSystemContext fsContext = FileSystemContext.create(conf);
        this.alluxioFs = FileSystem.Factory.create(fsContext);
        this.alluxioFileSystemFactory = new AlluxioFileSystemFactory(conf);
        this.fileSystem = alluxioFileSystemFactory.create(ConnectorIdentity.ofUser("alluxio"));
        // the SSHD container will be stopped by TestContainers on shutdown
        // https://github.com/trinodb/trino/discussions/21969
        System.setProperty("ReportLeakedContainers.disabled", "true");
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

    private static GenericContainer<?> createAlluxioMasterContainer()
    {
        GenericContainer<?> container = new GenericContainer<>(ALLUXIO_IMAGE);
        container.withCommand("master-only")
                .withEnv("ALLUXIO_JAVA_OPTS",
                        "-Dalluxio.security.authentication.type=NOSASL "
                                + "-Dalluxio.master.hostname=localhost "
                                + "-Dalluxio.worker.hostname=localhost "
                                + "-Dalluxio.master.mount.table.root.ufs=/opt/alluxio/underFSStorage "
                                + "-Dalluxio.master.journal.type=NOOP "
                                + "-Dalluxio.security.authorization.permission.enabled=false "
                                + "-Dalluxio.security.authorization.plugins.enabled=false ")
                .withNetworkMode("host")
                .withAccessToHost(true)
                .waitingFor(new LogMessageWaitStrategy()
                        .withRegEx(".*Primary started*\n")
                        .withStartupTimeout(Duration.ofMinutes(3)));
        return container;
    }

    private static GenericContainer<?> createAlluxioWorkerContainer()
    {
        GenericContainer<?> container = new GenericContainer<>(ALLUXIO_IMAGE);
        container.withCommand("worker-only")
                .withNetworkMode("host")
                .withEnv("ALLUXIO_JAVA_OPTS",
                        "-Dalluxio.security.authentication.type=NOSASL "
                                + "-Dalluxio.worker.ramdisk.size=128MB "
                                + "-Dalluxio.worker.hostname=localhost "
                                + "-Dalluxio.worker.tieredstore.level0.alias=HDD "
                                + "-Dalluxio.worker.tieredstore.level0.dirs.path=/tmp "
                                + "-Dalluxio.master.hostname=localhost "
                                + "-Dalluxio.security.authorization.permission.enabled=false "
                                + "-Dalluxio.security.authorization.plugins.enabled=false ")
                .withAccessToHost(true)
                .dependsOn(ALLUXIO_MASTER_CONTAINER)
                .waitingFor(Wait.forLogMessage(".*Alluxio worker started.*\n", 1));
        return container;
    }
}
