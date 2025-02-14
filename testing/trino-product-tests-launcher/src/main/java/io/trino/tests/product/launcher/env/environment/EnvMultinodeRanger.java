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
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * Trino with Apache Ranger authorizer plugin
 */
@TestsEnvironment
public class EnvMultinodeRanger
        extends EnvironmentProvider
{
    public static final int MARIADB_PORT = 23306;

    private final DockerFiles dockerFiles;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodeRanger(StandardMultinode standardMultinode, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(standardMultinode);
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        DockerFiles.ResourceProvider configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-ranger/");

        builder.addConnector("mariadb", forHostPath(configDir.getPath("mariadb.properties")));
        builder.addContainer(createMariaDb());

        builder.configureContainer(COORDINATOR, container -> container.withCopyFileToContainer(forHostPath(configDir.getPath("access-control.properties")), CONTAINER_TRINO_ETC + "/access-control.properties"));
        builder.configureContainer(COORDINATOR, container -> container.withCopyFileToContainer(forHostPath(configDir.getPath("ranger-trino-security.xml")), CONTAINER_TRINO_ETC + "/ranger-trino-security.xml"));
        builder.configureContainer(COORDINATOR, container -> container.withCopyFileToContainer(forHostPath(configDir.getPath("ranger-trino-audit.xml")), CONTAINER_TRINO_ETC + "/ranger-trino-audit.xml"));
        builder.configureContainer(COORDINATOR, container -> container.withCopyFileToContainer(forHostPath(configDir.getPath("ranger-policymgr-ssl.xml")), CONTAINER_TRINO_ETC + "/ranger-policymgr-ssl.xml"));
        builder.configureContainer(COORDINATOR, container -> container.withCopyFileToContainer(forHostPath(configDir.getPath("trino-policies.json")), "/tmp/ranger-policycache/trino_dev_trino.json"));

        builder.configureContainer(WORKER, container -> container.withCopyFileToContainer(forHostPath(configDir.getPath("access-control.properties")), CONTAINER_TRINO_ETC + "/access-control.properties"));
        builder.configureContainer(WORKER, container -> container.withCopyFileToContainer(forHostPath(configDir.getPath("ranger-trino-security.xml")), CONTAINER_TRINO_ETC + "/ranger-trino-security.xml"));
        builder.configureContainer(WORKER, container -> container.withCopyFileToContainer(forHostPath(configDir.getPath("ranger-trino-audit.xml")), CONTAINER_TRINO_ETC + "/ranger-trino-audit.xml"));
        builder.configureContainer(WORKER, container -> container.withCopyFileToContainer(forHostPath(configDir.getPath("ranger-policymgr-ssl.xml")), CONTAINER_TRINO_ETC + "/ranger-policymgr-ssl.xml"));
        builder.configureContainer(WORKER, container -> container.withCopyFileToContainer(forHostPath(configDir.getPath("trino-policies.json")), "/tmp/ranger-policycache/trino_dev_trino.json"));
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
