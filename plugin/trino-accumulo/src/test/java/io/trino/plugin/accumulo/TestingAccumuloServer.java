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
package io.trino.plugin.accumulo;

import io.trino.testing.TestingProperties;
import io.trino.testing.containers.junit.ReportLeakedContainers;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

import static java.lang.String.format;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

public class TestingAccumuloServer
{
    private static final int ACCUMULO_MASTER_PORT = 9999;
    private static final int ACCUMULO_TSERVER_PORT = 9997;
    private static final int ACCUMULO_MONITOR_UI_PORT = 9995;
    private static final int ZOOKEEPER_PORT = 2181;

    private static final String ACCUMULO_EXT_PATH = "/usr/local/lib/accumulo/lib";
    private static final String ITERATORS_JAR = "trino-accumulo-iterators.jar";

    private static final TestingAccumuloServer instance = new TestingAccumuloServer();

    private final FixedHostPortGenericContainer<?> accumuloContainer;

    public static TestingAccumuloServer getInstance()
    {
        return instance;
    }

    private TestingAccumuloServer()
    {
        accumuloContainer = new FixedHostPortGenericContainer<>("ghcr.io/trinodb/testing/accumulo:" + TestingProperties.getDockerImagesVersion());
        accumuloContainer.withFixedExposedPort(ACCUMULO_MASTER_PORT, ACCUMULO_MASTER_PORT);
        accumuloContainer.withFixedExposedPort(ACCUMULO_TSERVER_PORT, ACCUMULO_TSERVER_PORT);
        accumuloContainer.withFixedExposedPort(ACCUMULO_MONITOR_UI_PORT, ACCUMULO_MONITOR_UI_PORT);
        accumuloContainer.withExposedPorts(ZOOKEEPER_PORT);
        accumuloContainer.withCreateContainerCmdModifier(cmd -> cmd
                .withHostName("localhost")
                .withEnv("ADDRESS=0.0.0.0")
                .withEntrypoint("supervisord", "-c", "/etc/supervisord.conf"));
        accumuloContainer.waitingFor(Wait.forHealthcheck().withStartupTimeout(Duration.ofMinutes(10)));
        accumuloContainer.withCopyFileToContainer(forClasspathResource(ITERATORS_JAR), ACCUMULO_EXT_PATH + "/" + ITERATORS_JAR);

        // No need for an explicit stop since this server is a singleton
        // and the container will be stopped by TestContainers on shutdown
        // TODO Change this class to not be a singleton
        //  https://github.com/trinodb/trino/issues/5842
        accumuloContainer.start();
        ReportLeakedContainers.ignoreContainerId(accumuloContainer.getContainerId());
    }

    public String getInstanceName()
    {
        return "default";
    }

    public String getZooKeepers()
    {
        return format("%s:%s", accumuloContainer.getHost(), accumuloContainer.getMappedPort(ZOOKEEPER_PORT));
    }

    public String getUser()
    {
        return "root";
    }

    public String getPassword()
    {
        return "secret";
    }

    public AccumuloClient getClient()
    {
        return Accumulo.newClient().to(getInstanceName(), getZooKeepers())
                .as(getUser(), getPassword())
                .build();
    }
}
