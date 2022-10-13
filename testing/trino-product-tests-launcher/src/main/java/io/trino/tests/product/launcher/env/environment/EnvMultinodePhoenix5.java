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
package io.trino.tests.product.launcher.env.environment;

import io.trino.testing.TestingProperties;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import java.time.Duration;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isTrinoContainer;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodePhoenix5
        extends EnvironmentProvider
{
    private static final int ZOOKEEPER_PORT = 2181;

    private final ResourceProvider configDir;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodePhoenix5(StandardMultinode standardMultinode, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(standardMultinode);
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-phoenix5/");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        String dockerImageName = "ghcr.io/trinodb/testing/phoenix5:" + TestingProperties.getDockerImagesVersion();
        DockerContainer phoenix = new DockerContainer(dockerImageName, "phoenix")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(ZOOKEEPER_PORT))
                .withStartupTimeout(Duration.ofMinutes(5));

        portBinder.exposePort(phoenix, ZOOKEEPER_PORT);

        builder.addContainer(phoenix);

        builder.configureContainers(container -> {
            if (isTrinoContainer(container.getLogicalName())) {
                container.withCopyFileToContainer(forHostPath(configDir.getPath("hbase-site.xml")), CONTAINER_TRINO_ETC + "/hbase-site.xml");
                container.withCopyFileToContainer(forHostPath(configDir.getPath("phoenix.properties")), CONTAINER_TRINO_ETC + "/catalog/phoenix.properties");
            }
        });

        configureTempto(builder, configDir);
        builder.addConnector("phoenix5");
    }
}
