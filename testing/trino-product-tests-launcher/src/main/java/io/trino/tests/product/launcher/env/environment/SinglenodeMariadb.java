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

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class SinglenodeMariadb
        extends EnvironmentProvider
{
    // Use non-default MySQL port to avoid conflicts with locally installed MySQL if any.
    public static final int MARIADB_PORT = 23306;

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;

    @Inject
    public SinglenodeMariadb(Standard standard, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(ImmutableList.of(standard));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(COORDINATOR, container -> container
                .withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-mariadb/mariadb.properties")),
                        CONTAINER_PRESTO_ETC + "/catalog/mariadb.properties"));

        builder.addContainer(createMariaDb());
        builder.configureContainer("mariadb", container -> container
                .withCopyFileToContainer(
                        // This custom my.cnf overrides the MariaDB port from 3306 to 23306
                        forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-mariadb/my.cnf")),
                        "/etc/mysql/my.cnf"));
    }

    @SuppressWarnings("resource")
    private DockerContainer createMariaDb()
    {
        DockerContainer container = new DockerContainer("mariadb:10.5.4", "mariadb")
                .withEnv("MYSQL_USER", "test")
                .withEnv("MYSQL_PASSWORD", "test")
                .withEnv("MYSQL_ROOT_PASSWORD", "test")
                .withEnv("MYSQL_DATABASE", "test")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(MARIADB_PORT));

        portBinder.exposePort(container, MARIADB_PORT);

        return container;
    }
}
