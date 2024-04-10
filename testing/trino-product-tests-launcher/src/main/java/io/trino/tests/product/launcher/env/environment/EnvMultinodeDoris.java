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
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.time.Duration;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeDoris
        extends EnvironmentProvider
{
    private static final String DORIS = "doris";
    private static final int DEFAULT_PORT = 9030;
    private final ResourceProvider configDir;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodeDoris(StandardMultinode standardMultinode, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(standardMultinode);
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-starrocks/");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addConnector(DORIS, forHostPath(configDir.getPath("doris.properties")));
        builder.addContainer(createStarRocks());
        configureTempto(builder, configDir);
    }

    private DockerContainer createStarRocks()
    {
        DockerContainer container = new DockerContainer("doris/allin1-ubuntu:2.5.6", DORIS)
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .withCopyFileToContainer(forHostPath(configDir.getPath("start_fe_be.sh"), 0777), "/data/deploy/start_fe_be.sh")
                .waitingFor(new HttpWaitStrategy()
                        .forStatusCode(200)
                        .forResponsePredicate("Ok."::equals)
                        .withStartupTimeout(Duration.ofMinutes(1L)))
                .waitingFor(forSelectedPorts(DEFAULT_PORT))
                .withStartupTimeout(Duration.ofMinutes(5));

        portBinder.exposePort(container, DEFAULT_PORT);

        return container;
    }
}
