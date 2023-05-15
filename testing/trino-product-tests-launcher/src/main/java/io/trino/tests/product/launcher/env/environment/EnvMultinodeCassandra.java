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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import java.time.Duration;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeCassandra
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;
    public static final int CASSANDRA_PORT = 9042;

    @Inject
    protected EnvMultinodeCassandra(DockerFiles dockerFiles, PortBinder portBinder, StandardMultinode standardMultinode)
    {
        super(ImmutableList.of(standardMultinode));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addContainer(createCassandra());
        builder.addConnector("cassandra", forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-cassandra/cassandra.properties")));
    }

    private DockerContainer createCassandra()
    {
        DockerContainer container = new DockerContainer("cassandra:3.9", "cassandra")
                .withEnv("HEAP_NEWSIZE", "128M")
                .withEnv("MAX_HEAP_SIZE", "512M")
                .withCommand(
                        "bash",
                        "-cxeu",
                        "ln -snf /usr/share/zoneinfo/Asia/Kathmandu /etc/localtime && echo Asia/Kathmandu > /etc/timezone && /docker-entrypoint.sh cassandra -f")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(CASSANDRA_PORT))
                .withStartupTimeout(Duration.ofMinutes(5));

        portBinder.exposePort(container, CASSANDRA_PORT);

        return container;
    }
}
