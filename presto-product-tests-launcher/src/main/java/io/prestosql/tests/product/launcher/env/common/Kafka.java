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

package io.prestosql.tests.product.launcher.env.common;

import io.prestosql.tests.product.launcher.env.DockerContainer;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.testcontainers.PortBinder;
import io.prestosql.tests.product.launcher.testcontainers.SelectedPortWaitStrategy;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class Kafka
        implements EnvironmentExtender
{
    private static final String CONFLUENT_VERSION = "5.2.1";

    private final PortBinder portBinder;

    @Inject
    public Kafka(PortBinder portBinder)
    {
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.addContainers(createZookeeper(), createKafka());
    }

    @SuppressWarnings("resource")
    private DockerContainer createZookeeper()
    {
        DockerContainer container = new DockerContainer("confluentinc/cp-zookeeper:" + CONFLUENT_VERSION, "zookeeper")
                .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
                .withEnv("ZOOKEEPER_TICK_TIME", "2000")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new SelectedPortWaitStrategy(2181));

        portBinder.exposePort(container, 2181);

        return container;
    }

    @SuppressWarnings("resource")
    private DockerContainer createKafka()
    {
        DockerContainer container = new DockerContainer("confluentinc/cp-kafka:" + CONFLUENT_VERSION, "kafka")
                .withEnv("KAFKA_BROKER_ID", "1")
                .withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://kafka:9092")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new SelectedPortWaitStrategy(9092));

        portBinder.exposePort(container, 9092);

        return container;
    }
}
