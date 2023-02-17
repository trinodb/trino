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
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodePostgresql
        extends EnvironmentProvider
{
    // Use non-default PostgreSQL port to avoid conflicts with locally installed PostgreSQL if any.
    public static final int POSTGRESQL_PORT = 15432;

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodePostgresql(StandardMultinode standardMultinode, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(ImmutableList.of(standardMultinode));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addConnector(
                "postgresql",
                forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-postgresql/postgresql.properties")));
        builder.addContainer(createPostgreSql());
    }

    @SuppressWarnings("resource")
    private DockerContainer createPostgreSql()
    {
        // Use the oldest supported PostgreSQL version
        DockerContainer container = new DockerContainer("postgres:10.20", "postgresql")
                .withEnv("POSTGRES_PASSWORD", "test")
                .withEnv("POSTGRES_USER", "test")
                .withEnv("POSTGRES_DB", "test")
                .withEnv("PGPORT", Integer.toString(POSTGRESQL_PORT))
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(POSTGRESQL_PORT));

        portBinder.exposePort(container, POSTGRESQL_PORT);

        return container;
    }
}
