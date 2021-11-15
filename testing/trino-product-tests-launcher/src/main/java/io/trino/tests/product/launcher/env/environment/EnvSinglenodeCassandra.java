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
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Standard;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import java.time.Duration;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvSinglenodeCassandra
        extends EnvironmentProvider
{
    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;

    public static final String CONTAINER_PRESTO_CASSANDRA_PROPERTIES = CONTAINER_PRESTO_ETC + "/catalog/cassandra.properties";
    public static final int CASSANDRA_PORT = 9042;

    @Inject
    protected EnvSinglenodeCassandra(DockerFiles dockerFiles, PortBinder portBinder, Standard standard)
    {
        super(ImmutableList.of(standard));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addContainer(createCassandra());

        builder.configureContainer(COORDINATOR, container -> container
                .withCopyFileToContainer(forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-cassandra/cassandra.properties")), CONTAINER_PRESTO_CASSANDRA_PROPERTIES));
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
