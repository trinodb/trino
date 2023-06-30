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

package io.trino.tests.product.launcher.env.common;

import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;

import static io.trino.tests.product.launcher.docker.ContainerUtil.forSelectedPorts;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isTrinoContainer;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class Kafka
        implements EnvironmentExtender
{
    private static final String CONFLUENT_VERSION = "7.3.1";
    private static final int SCHEMA_REGISTRY_PORT = 8081;
    static final String KAFKA = "kafka";
    static final String SCHEMA_REGISTRY = "schema-registry";
    static final String ZOOKEEPER = "zookeeper";

    private final ResourceProvider configDir;

    private final PortBinder portBinder;

    @Inject
    public Kafka(DockerFiles dockerFiles, PortBinder portBinder)
    {
        this.configDir = requireNonNull(dockerFiles, "dockerFiles is null")
                .getDockerFilesHostDirectory("common/kafka");
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addContainers(createZookeeper(), createKafka(), createSchemaRegistry())
                .containerDependsOn(KAFKA, ZOOKEEPER)
                .containerDependsOn(SCHEMA_REGISTRY, KAFKA);

        builder.configureContainers(container -> {
            if (isTrinoContainer(container.getLogicalName())) {
                MountableFile logConfigFile = forHostPath(configDir.getPath("log.properties"));
                container
                        .withCopyFileToContainer(logConfigFile, CONTAINER_TRINO_ETC + "/log.properties");
            }
        });

        // Confluent Docker entry point script overwrites /etc/kafka/log4j.properties
        // Modify the template directly instead
        builder.configureContainer(KAFKA, container -> {
            MountableFile logConfigFile = forHostPath(configDir.getPath("log4j-kafka.properties.template"));
            container.withCopyFileToContainer(logConfigFile, "/etc/confluent/docker/log4j.properties.template");
        });

        builder.configureContainer(SCHEMA_REGISTRY, container -> {
            MountableFile logConfigFile = forHostPath(configDir.getPath("log4j-schema-registry.properties.template"));
            container.withCopyFileToContainer(logConfigFile, "/etc/confluent/docker/log4j.properties.template");
        });
    }

    @SuppressWarnings("resource")
    private DockerContainer createZookeeper()
    {
        DockerContainer container = new DockerContainer("confluentinc/cp-zookeeper:" + CONFLUENT_VERSION, ZOOKEEPER)
                .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
                .withEnv("ZOOKEEPER_TICK_TIME", "2000")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(2181))
                .withStartupTimeout(Duration.ofMinutes(5));

        portBinder.exposePort(container, 2181);

        return container;
    }

    @SuppressWarnings("resource")
    private DockerContainer createKafka()
    {
        DockerContainer container = new DockerContainer("confluentinc/cp-kafka:" + CONFLUENT_VERSION, KAFKA)
                .withEnv("KAFKA_BROKER_ID", "1")
                .withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://kafka:9092")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingForAll(forSelectedPorts(9092), forLogMessage(".*started \\(kafka.server.KafkaServer\\).*", 1))
                .withStartupTimeout(Duration.ofMinutes(5));

        portBinder.exposePort(container, 9092);

        return container;
    }

    @SuppressWarnings("resource")
    private DockerContainer createSchemaRegistry()
    {
        DockerContainer container = new DockerContainer("confluentinc/cp-schema-registry:" + CONFLUENT_VERSION, SCHEMA_REGISTRY)
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "0.0.0.0")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT)
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(forSelectedPorts(SCHEMA_REGISTRY_PORT))
                .withStartupTimeout(Duration.ofMinutes(5));

        portBinder.exposePort(container, SCHEMA_REGISTRY_PORT);

        return container;
    }
}
