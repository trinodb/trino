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
package io.trino.testing.kafka;

import com.google.common.io.Closer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestingKafkaWithSchemaRegistry
        implements TestingKafka
{
    private static final int SCHEMA_REGISTRY_PORT = 8081;
    private final TestingKafka delegate;
    private final GenericContainer<?> schemaRegistryContainer;

    @SuppressWarnings("resource")
    private final Closer closer = Closer.create();

    public TestingKafkaWithSchemaRegistry()
    {
        this(DEFAULT_CONFLUENT_PLATFORM_VERSION);
    }

    public TestingKafkaWithSchemaRegistry(String confluentPlatformVersion)
    {
        requireNonNull(confluentPlatformVersion, "confluentPlatformVersion is null");
        this.delegate = new BasicTestingKafka(confluentPlatformVersion);
        schemaRegistryContainer = new GenericContainer<>("confluentinc/cp-schema-registry:" + confluentPlatformVersion)
                .withNetwork(Network.SHARED)
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "0.0.0.0")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", format("http://0.0.0.0:%s", SCHEMA_REGISTRY_PORT))
                .withExposedPorts(SCHEMA_REGISTRY_PORT);
        closer.register(delegate);
        closer.register(schemaRegistryContainer::stop);
    }

    @Override
    public void start()
    {
        delegate.start();
        schemaRegistryContainer.start();
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }

    @Override
    public void createTopicWithConfig(int partitions, int replication, String topic, boolean enableLogAppendTime)
    {
        delegate.createTopicWithConfig(partitions, replication, topic, enableLogAppendTime);
    }

    @Override
    public <K, V> RecordMetadata sendMessages(Stream<ProducerRecord<K, V>> recordStream, Map<String, String> extraProducerProperties)
    {
        return delegate.sendMessages(recordStream, extraProducerProperties);
    }

    @Override
    public String getConnectString()
    {
        return delegate.getConnectString();
    }

    public String getSchemaRegistryConnectString()
    {
        return "http://" + schemaRegistryContainer.getContainerIpAddress() + ":" + schemaRegistryContainer.getMappedPort(SCHEMA_REGISTRY_PORT);
    }
}
