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
import io.trino.tests.product.launcher.env.Environment.Builder;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeMariadb
        extends EnvironmentProvider
{
    // Use non-default MariaDB port to avoid conflicts with locally installed MariaDB if any.
    public static final int MARIADB_PORT = 23306;

    private final ResourceProvider configDir;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodeMariadb(StandardMultinode standardMultinode, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(standardMultinode);
        this.configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-mariadb/");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Builder builder)
    {
        builder.addConnector("mariadb", forHostPath(configDir.getPath("mariadb.properties")));
        builder.addContainer(createMariaDb());
        configureTempto(builder, configDir);
    }

    private DockerContainer createMariaDb()
    {
        DockerContainer container = new DockerContainer("mariadb:10.7.1", "mariadb")
                .withEnv("MYSQL_USER", "test")
                .withEnv("MYSQL_PASSWORD", "test")
                .withEnv("MYSQL_ROOT_PASSWORD", "test")
                .withEnv("MYSQL_DATABASE", "test")
                .withCommand("mysqld", "--port", Integer.toString(MARIADB_PORT), "--character-set-server", "utf8mb4")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(MARIADB_PORT));

        portBinder.exposePort(container, MARIADB_PORT);

        return container;
    }
}
