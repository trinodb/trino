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

import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment.Builder;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeExasol
        extends EnvironmentProvider
{
    private static final int EXASOL_PORT = 8563;

    private final ResourceProvider configDir;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodeExasol(StandardMultinode standardMultinode, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(standardMultinode);
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-exasol/");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Builder builder)
    {
        builder.addConnector("exasol", forHostPath(configDir.getPath("exasol.properties")));
        builder.addContainer(createExasol());
        configureTempto(builder, configDir);
    }

    private DockerContainer createExasol()
    {
        DockerContainer container = new DockerContainer("exasol/docker-db:8.27.0", "exasol")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(EXASOL_PORT));
        container.setPrivilegedMode(true);
        portBinder.exposePort(container, EXASOL_PORT);
        return container;
    }
}
