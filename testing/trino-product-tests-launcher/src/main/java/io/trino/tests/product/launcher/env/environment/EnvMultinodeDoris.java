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
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.TESTS;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.WORKER;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public final class EnvMultinodeDoris
        extends EnvironmentProvider
{
    private static final DockerImageName DORIS_IMAGE = DockerImageName.parse("apache/doris");
    private static final DockerImageName DORIS_FE_IMAGE = DORIS_IMAGE.withTag("fe-2.1.11");
    private static final DockerImageName DORIS_BE_IMAGE = DORIS_IMAGE.withTag("be-2.1.11");
    private static final DockerImageName MYSQL_CLIENT_IMAGE = DockerImageName.parse("mysql").withTag("8.0.36");

    private static final String DORIS_FE = "doris-fe";
    private static final String DORIS_BE = "doris-be";
    private static final String DORIS_INIT = "doris-init";
    private static final String CONTAINER_CONFIG_DIR = "/docker/trino-product-tests/conf/environment/multinode-doris";
    private static final String FE_START_SCRIPT = CONTAINER_CONFIG_DIR + "/run-doris-fe.sh";
    private static final String BE_START_SCRIPT = CONTAINER_CONFIG_DIR + "/run-doris-be.sh";

    private static final int FE_HTTP_PORT = 8030;
    private static final int FE_QUERY_PORT = 9030;
    private static final int FE_FLIGHT_SQL_PORT = 9040;
    private static final int BE_WEB_PORT = 8040;
    private static final int BE_FLIGHT_SQL_PORT = 8050;
    private static final int BE_HEARTBEAT_PORT = 9050;

    private final ResourceProvider configDir;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodeDoris(StandardMultinode standardMultinode, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(standardMultinode);
        this.configDir = requireNonNull(dockerFiles, "dockerFiles is null")
                .getDockerFilesHostDirectory("conf/environment/multinode-doris/");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Builder builder)
    {
        builder.addConnector("doris", forHostPath(configDir.getPath("doris.properties")));
        builder.addContainers(
                        createFrontend(),
                        createBackend(),
                        createInitializer())
                .containerDependsOn(DORIS_BE, DORIS_FE)
                .containerDependsOn(DORIS_INIT, DORIS_FE)
                .containerDependsOn(DORIS_INIT, DORIS_BE)
                .containerDependsOn(COORDINATOR, DORIS_INIT)
                .containerDependsOn(WORKER, DORIS_INIT)
                .containerDependsOn(TESTS, DORIS_INIT);

        configureTempto(builder, configDir);
    }

    private DockerContainer createFrontend()
    {
        DockerContainer container = new DockerContainer(DORIS_FE_IMAGE.toString(), DORIS_FE)
                .withEnv("DORIS_FE_ALIAS", DORIS_FE)
                .withEnv("FE_ID", "1")
                .withEnv("TZ", "UTC")
                .withCopyFileToContainer(forHostPath(configDir.getPath("fe.conf")), "/opt/apache-doris/fe/conf/fe.conf")
                .withCopyFileToContainer(forHostPath(configDir.getPath("run-doris-fe.sh")), FE_START_SCRIPT)
                .withCreateContainerCmdModifier(command -> command.withEntrypoint("bash", FE_START_SCRIPT))
                .withExposedLogPaths("/opt/apache-doris/fe/log")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(FE_HTTP_PORT, FE_QUERY_PORT, FE_FLIGHT_SQL_PORT))
                .withStartupTimeout(Duration.ofMinutes(10));

        portBinder.exposePort(container, FE_HTTP_PORT);
        portBinder.exposePort(container, FE_QUERY_PORT);
        portBinder.exposePort(container, FE_FLIGHT_SQL_PORT);

        return container;
    }

    private DockerContainer createBackend()
    {
        DockerContainer container = new DockerContainer(DORIS_BE_IMAGE.toString(), DORIS_BE)
                .withEnv("DORIS_FE_ALIAS", DORIS_FE)
                .withEnv("TZ", "UTC")
                .withEnv("SKIP_CHECK_ULIMIT", "true")
                .withCopyFileToContainer(forHostPath(configDir.getPath("be.conf")), "/opt/apache-doris/be/conf/be.conf")
                .withCopyFileToContainer(forHostPath(configDir.getPath("run-doris-be.sh")), BE_START_SCRIPT)
                .withCreateContainerCmdModifier(command -> command.withEntrypoint("bash", BE_START_SCRIPT))
                .withExposedLogPaths("/opt/apache-doris/be/log")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(BE_WEB_PORT, BE_FLIGHT_SQL_PORT, BE_HEARTBEAT_PORT))
                .withStartupTimeout(Duration.ofMinutes(10));

        portBinder.exposePort(container, BE_WEB_PORT);
        portBinder.exposePort(container, BE_FLIGHT_SQL_PORT);
        portBinder.exposePort(container, BE_HEARTBEAT_PORT);

        return container;
    }

    private DockerContainer createInitializer()
    {
        return new DockerContainer(MYSQL_CLIENT_IMAGE.toString(), DORIS_INIT)
                .withCommand(
                        "bash",
                        "-xeuc",
                        """
                        until mysql --protocol=tcp -hdoris-fe -P9030 -uroot -e 'SHOW FRONTENDS' >/dev/null 2>&1; do
                            sleep 2
                        done

                        until mysql --protocol=tcp -hdoris-fe -P9030 -uroot -e "SHOW BACKENDS\\\\G" | grep -q 'Alive: true'; do
                            sleep 2
                        done
                        """)
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy())
                .withStartupTimeout(Duration.ofMinutes(5))
                .setTemporary(true);
    }
}
