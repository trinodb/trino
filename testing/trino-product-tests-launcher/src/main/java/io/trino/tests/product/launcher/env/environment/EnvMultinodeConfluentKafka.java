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
import io.trino.tests.product.launcher.docker.DockerFiles.ResourceProvider;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentProvider;
import io.trino.tests.product.launcher.env.common.Kafka;
import io.trino.tests.product.launcher.env.common.StandardMultinode;
import io.trino.tests.product.launcher.env.common.TestsEnvironment;

import java.io.File;

import static io.trino.testing.TestingProperties.getConfluentVersion;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.configureTempto;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isTrinoContainer;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forClasspathResource;
import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * {@link EnvMultinodeConfluentKafka} is intended to be the only Kafka product test environment which copies the non-free Confluent License libraries to the Kafka connector
 * classpath to test functionality which requires those classes.
 * The other {@link Kafka} environments MUST NOT copy these jars otherwise it's not possible to verify the out of box Trino setup which doesn't ship with the Confluent licensed
 * libraries.
 */
@TestsEnvironment
public final class EnvMultinodeConfluentKafka
        extends EnvironmentProvider
{
    private static final File KAFKA_PROTOBUF_PROVIDER = new File("testing/trino-product-tests-launcher/target/kafka-protobuf-provider-" + getConfluentVersion() + ".jar");
    private static final File KAFKA_PROTOBUF_TYPES = new File("testing/trino-product-tests-launcher/target/kafka-protobuf-types-" + getConfluentVersion() + ".jar");

    private final ResourceProvider configDir;

    @Inject
    public EnvMultinodeConfluentKafka(Kafka kafka, StandardMultinode standardMultinode, DockerFiles dockerFiles)
    {
        super(ImmutableList.of(standardMultinode, kafka));
        requireNonNull(dockerFiles, "dockerFiles is null");
        configDir = dockerFiles.getDockerFilesHostDirectory("conf/environment/multinode-kafka-confluent-license/");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainers(container -> {
            if (isTrinoContainer(container.getLogicalName())) {
                builder.addConnector("kafka", forHostPath(configDir.getPath("kafka.properties")), CONTAINER_TRINO_ETC + "/catalog/kafka.properties");
                builder.addConnector("kafka", forHostPath(configDir.getPath("kafka_schema_registry.properties")), CONTAINER_TRINO_ETC + "/catalog/kafka_schema_registry.properties");
                container
                        .withCopyFileToContainer(forHostPath(KAFKA_PROTOBUF_PROVIDER.getAbsolutePath()), "/docker/kafka-protobuf-provider/kafka-protobuf-provider.jar")
                        .withCopyFileToContainer(forHostPath(KAFKA_PROTOBUF_TYPES.getAbsolutePath()), "/docker/kafka-protobuf-provider/kafka-protobuf-types.jar")
                        .withCopyFileToContainer(forClasspathResource("install-kafka-protobuf-provider.sh", 0755), "/docker/presto-init.d/install-kafka-protobuf-provider.sh");
            }
        });

        configureTempto(builder, configDir);
    }
}
