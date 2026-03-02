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
package io.trino.testing.containers;

import com.google.common.io.Closer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

public class Alluxio
        implements AutoCloseable
{
    private static final String IMAGE_NAME = "alluxio/alluxio:2.9.5";
    private static final DockerImageName ALLUXIO_IMAGE = DockerImageName.parse(IMAGE_NAME);

    private final Closer closer = Closer.create();

    private final GenericContainer<?> alluxioMaster;
    private final GenericContainer<?> alluxioWorker;

    public Alluxio()
    {
        this.alluxioMaster = createAlluxioMasterContainer();
        this.alluxioWorker = createAlluxioWorkerContainer();

        closer.register(alluxioWorker::close);
        closer.register(alluxioMaster::close);
    }

    public void start()
    {
        alluxioMaster.start();
        alluxioWorker.start();
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }

    private static GenericContainer<?> createAlluxioMasterContainer()
    {
        return new GenericContainer<>(ALLUXIO_IMAGE).withCommand("master-only")
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
                .withStartupAttempts(10)
                .withStartupTimeout(Duration.ofMinutes(10))
                .waitingFor(new LogMessageWaitStrategy()
                        .withRegEx(".*Primary started*\n")
                        .withStartupTimeout(Duration.ofMinutes(3)));
    }

    private static GenericContainer<?> createAlluxioWorkerContainer()
    {
        return new GenericContainer<>(ALLUXIO_IMAGE).withCommand("worker-only")
                .withNetworkMode("host")
                .withEnv("ALLUXIO_JAVA_OPTS",
                        "-Dalluxio.security.authentication.type=NOSASL "
                                + "-Dalluxio.worker.ramdisk.size=512MB "
                                + "-Dalluxio.worker.hostname=localhost "
                                + "-Dalluxio.worker.tieredstore.level0.alias=HDD "
                                + "-Dalluxio.worker.tieredstore.level0.dirs.path=/tmp "
                                + "-Dalluxio.master.hostname=localhost "
                                + "-Dalluxio.security.authorization.permission.enabled=false "
                                + "-Dalluxio.security.authorization.plugins.enabled=false ")
                .withAccessToHost(true)
                .withStartupAttempts(10)
                .withStartupTimeout(Duration.ofMinutes(10))
                .waitingFor(Wait.forLogMessage(".*Alluxio worker started.*\n", 1));
    }
}
