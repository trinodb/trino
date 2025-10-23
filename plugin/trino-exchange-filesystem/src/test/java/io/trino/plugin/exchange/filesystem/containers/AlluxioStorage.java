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
package io.trino.plugin.exchange.filesystem.containers;

import com.google.common.collect.ImmutableMap;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;

public class AlluxioStorage
        implements AutoCloseable
{
    private static final String IMAGE_NAME = "alluxio/alluxio:2.9.5";
    private static final DockerImageName ALLUXIO_IMAGE = DockerImageName.parse(IMAGE_NAME);

    private GenericContainer<?> alluxioMaster;
    private GenericContainer<?> alluxioWorker;

    public void start()
    {
        alluxioMaster = createAlluxioMasterContainer();
        alluxioMaster.start();
        alluxioWorker = createAlluxioWorkerContainer();
        alluxioWorker.start();
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
        container.start();
        return container;
    }

    private static GenericContainer<?> createAlluxioWorkerContainer()
    {
        GenericContainer<?> container = new GenericContainer<>(ALLUXIO_IMAGE);
        container.withCommand("worker-only")
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
                .waitingFor(Wait.forLogMessage(".*Alluxio worker started.*\n", 1));
        container.start();
        return container;
    }

    @Override
    public void close()
            throws Exception
    {
        if (alluxioMaster != null) {
            alluxioMaster.close();
        }
        if (alluxioWorker != null) {
            alluxioWorker.close();
        }
    }

    public static Map<String, String> getExchangeManagerProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("exchange.base-directories", "alluxio:///trino-exchange")
                // to trigger file split in some tests
                .put("exchange.sink-max-file-size", "16MB")
                // create more granular source handles given the fault-tolerant execution target task input size is set to lower value for testing
                .put("exchange.source-handle-target-data-size", "1MB")
                .buildOrThrow();
    }
}
