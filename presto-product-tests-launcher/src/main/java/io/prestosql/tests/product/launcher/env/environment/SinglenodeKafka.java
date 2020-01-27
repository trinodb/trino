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

package io.prestosql.tests.product.launcher.env.environment;

import com.google.common.collect.ImmutableList;
import io.prestosql.tests.product.launcher.docker.DockerFiles;
import io.prestosql.tests.product.launcher.env.DockerContainer;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.common.AbstractEnvironmentProvider;
import io.prestosql.tests.product.launcher.env.common.Standard;
import io.prestosql.tests.product.launcher.testcontainers.SelectedPortWaitStrategy;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import static io.prestosql.tests.product.launcher.env.common.Standard.CONTAINER_PRESTO_ETC;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;

public final class SinglenodeKafka
        extends AbstractEnvironmentProvider
{
    private static final String CONFLUENT_VERSION = "5.2.1";

    private final DockerFiles dockerFiles;

    @Inject
    public SinglenodeKafka(Standard standard, DockerFiles dockerFiles)
    {
        super(ImmutableList.of(standard));
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
    }

    @Override
    protected void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer("presto-master", container -> container
                .withFileSystemBind(
                        dockerFiles.getDockerFilesHostPath("conf/environment/singlenode-kafka/kafka.properties"),
                        CONTAINER_PRESTO_ETC + "/catalog/kafka.properties",
                        READ_ONLY));

        builder.addContainer("zookeeper", createZookeeper());
        builder.addContainer("kafka", createKafka());
    }

    @SuppressWarnings("resource")
    private DockerContainer createZookeeper()
    {
        return new DockerContainer("confluentinc/cp-zookeeper:" + CONFLUENT_VERSION)
                .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
                .withEnv("ZOOKEEPER_TICK_TIME", "2000")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new SelectedPortWaitStrategy(2181));
    }

    @SuppressWarnings("resource")
    private DockerContainer createKafka()
    {
        return new DockerContainer("confluentinc/cp-kafka:" + CONFLUENT_VERSION)
                .withEnv("KAFKA_BROKER_ID", "1")
                .withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://kafka:9092")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new SelectedPortWaitStrategy(9092));
    }
}
