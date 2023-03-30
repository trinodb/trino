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

import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isTrinoContainer;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_JVM_CONFIG;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeIgnite
        extends EnvironmentProvider
{
    private static final String IGNITE = "ignite";
    private static final int IGNITE_PORT = 10800;
    private final ResourceProvider configDir;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodeIgnite(StandardMultinode standardMultinode, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(standardMultinode);
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-ignite/");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addContainers(createIgnite(1), createIgnite(2));
        builder.configureContainer(logicalName(1), container -> portBinder.exposePort(container, IGNITE_PORT));
        builder.configureContainer(logicalName(1), container -> container.withNetworkAliases(IGNITE, container.getLogicalName(), "localhost"));
        builder.configureContainer(logicalName(2), container -> container.withNetworkAliases(container.getLogicalName(), "localhost"));

        builder.addConnector("ignite", forHostPath(configDir.getPath("ignite.properties")));
        builder.configureContainers(container -> {
            if (isTrinoContainer(container.getLogicalName())) {
                container.withCopyFileToContainer(forHostPath(configDir.getPath("jvm.config")), CONTAINER_TRINO_JVM_CONFIG);
            }
        });
        configureTempto(builder, configDir);
    }

    private DockerContainer createIgnite(int number)
    {
        return new DockerContainer("apacheignite/ignite:2.8.0", logicalName(number))
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .withStartupTimeout(Duration.ofMinutes(5));
    }

    private String logicalName(int number)
    {
        return IGNITE + "-" + number;
    }
}
