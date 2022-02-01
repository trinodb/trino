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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Set;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

@TestsEnvironment
public class EnvMultinodeClickhouse
        extends EnvironmentProvider
{
    private static final String ZOOKEEPER = "zookeeper";

    private static final String CLICKHOUSE = "clickhouse";
    private static final String CLICKHOUSE_NTH = CLICKHOUSE + "-";
    private static final String CONTAINER_CLICKHOUSE_CONFIG_DIR = "/etc/clickhouse-server/";
    private static final String CONTAINER_CLICKHOUSE_USERS_D = CONTAINER_CLICKHOUSE_CONFIG_DIR + "users.d/";
    private static final String CONTAINER_CLICKHOUSE_CONFIG_D = CONTAINER_CLICKHOUSE_CONFIG_DIR + "config.d/";
    private static final int CLICKHOUSE_DEFAULT_HTTP_PORT = 8123;
    private static final int CLICKHOUSE_DEFAULT_NATIVE_PORT = 9000;

    private final DockerFiles dockerFiles;
    private final ResourceProvider configDir;
    private final PortBinder portBinder;

    @Inject
    public EnvMultinodeClickhouse(StandardMultinode standardMultinode, DockerFiles dockerFiles, PortBinder portBinder)
    {
        super(standardMultinode);
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        this.configDir = requireNonNull(dockerFiles, "dockerFiles is null").getDockerFilesHostDirectory("conf/environment/multinode-clickhouse/");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Builder builder)
    {
        builder.addConnector("clickhouse", forHostPath(configDir.getPath("clickhouse.properties")));

        builder.addContainers(
                        createZookeeper(portBinder),
                        createClickHouse(1, dockerFiles, portBinder),
                        createClickHouse(2, dockerFiles, portBinder),
                        createClickHouse(3, dockerFiles, portBinder))
                .containerDependsOn(logicalName(1), ZOOKEEPER)
                .containerDependsOn(logicalName(2), ZOOKEEPER)
                .containerDependsOn(logicalName(3), ZOOKEEPER);

        builder.configureContainer(ZOOKEEPER, container -> container.withNetworkAliases(container.getLogicalName(), "localhost"));
        builder.configureContainer(logicalName(1), container -> container.withNetworkAliases(CLICKHOUSE, container.getLogicalName(), "localhost"));
        builder.configureContainer(logicalName(2), container -> container.withNetworkAliases(container.getLogicalName(), "localhost"));
        builder.configureContainer(logicalName(3), container -> container.withNetworkAliases(container.getLogicalName(), "localhost"));

        configureTempto(builder, configDir);
    }

    private static DockerContainer createZookeeper(PortBinder portBinder)
    {
        DockerContainer container = new DockerContainer("zookeeper:3.7.0", ZOOKEEPER)
                .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
                .withEnv("ZOOKEEPER_TICK_TIME", "2000")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(2181))
                .withStartupTimeout(Duration.ofMinutes(5));

        portBinder.exposePort(container, 2181);

        return container;
    }

    private static DockerContainer createClickHouse(int number, DockerFiles dockerFiles, PortBinder portBinder)
    {
        int httpPort = CLICKHOUSE_DEFAULT_HTTP_PORT + number;
        int nativePort = CLICKHOUSE_DEFAULT_NATIVE_PORT + number;

        DockerContainer container = new DockerContainer("yandex/clickhouse-server:21.3.2.5", logicalName(number))
                .withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-clickhouse/test.xml")),
                        CONTAINER_CLICKHOUSE_USERS_D + "test.xml")
                .withCopyFileToContainer(
                        forHostPath(dockerFiles.getDockerFilesHostPath("conf/environment/multinode-clickhouse/metrika.xml")),
                        CONTAINER_CLICKHOUSE_CONFIG_D + "metrika.xml")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(httpPort, nativePort))
                .withStartupTimeout(Duration.ofMinutes(5));

        modifyDefaultPorts(container, httpPort, nativePort);

        portBinder.exposePort(container, httpPort);
        portBinder.exposePort(container, nativePort);

        return container;
    }

    private static String logicalName(int number)
    {
        return CLICKHOUSE_NTH + number;
    }

    private static void modifyDefaultPorts(DockerContainer container, int httpPort, int nativePort)
    {
        try {
            FileAttribute<Set<PosixFilePermission>> rwx = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrwx"));
            Path customConfig = Files.createTempFile("custom", ".xml", rwx);
            customConfig.toFile().deleteOnExit();
            Files.writeString(
                    customConfig,
                    format(
                            "<?xml version=\"1.0\"?>\n" +
                                    "<yandex>\n" +
                                    "    <http_port>%s</http_port>\n" +
                                    "    <tcp_port>%s</tcp_port>\n" +
                                    "</yandex>\n",
                            httpPort,
                            nativePort),
                    UTF_8);
            container.withCopyFileToContainer(forHostPath(customConfig), CONTAINER_CLICKHOUSE_CONFIG_D + "custom.xml");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
