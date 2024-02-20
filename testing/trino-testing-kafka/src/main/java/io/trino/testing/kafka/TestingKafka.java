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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.testing.ResourcePresence;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.testcontainers.containers.KafkaContainer.KAFKA_PORT;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

public final class TestingKafka
        implements Closeable
{
    private static final Logger log = Logger.get(TestingKafka.class);

    private static final String DEFAULT_CONFLUENT_PLATFORM_VERSION = "7.3.1";
    private static final int SCHEMA_REGISTRY_PORT = 8081;

    private static final DockerImageName KAFKA_IMAGE_NAME = DockerImageName.parse("confluentinc/cp-kafka");
    private static final DockerImageName SCHEMA_REGISTRY_IMAGE_NAME = DockerImageName.parse("confluentinc/cp-schema-registry");

    private final Network network;
    private final KafkaContainer kafka;
    private final GenericContainer<?> schemaRegistry;
    private final boolean withSchemaRegistry;
    private final Closer closer = Closer.create();
    private boolean stopped;

    public static TestingKafka create()
    {
        return create(DEFAULT_CONFLUENT_PLATFORM_VERSION);
    }

    public static TestingKafka create(String confluentPlatformVersions)
    {
        return new TestingKafka(confluentPlatformVersions, false);
    }

    public static TestingKafka createWithSchemaRegistry()
    {
        return new TestingKafka(DEFAULT_CONFLUENT_PLATFORM_VERSION, true);
    }

    private TestingKafka(String confluentPlatformVersion, boolean withSchemaRegistry)
    {
        this.withSchemaRegistry = withSchemaRegistry;
        network = Network.newNetwork();
        closer.register(network::close);

        // Confluent Docker entry point script overwrites /etc/kafka/log4j.properties
        // Modify the template directly instead.
        MountableFile kafkaLogTemplate = forClasspathResource("log4j-kafka.properties.template");
        MountableFile schemaRegistryLogTemplate = forClasspathResource("log4j-schema-registry.properties.template");
        kafka = new KafkaContainer(KAFKA_IMAGE_NAME.withTag(confluentPlatformVersion))
                .withStartupAttempts(3)
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withCopyFileToContainer(
                        kafkaLogTemplate,
                        "/etc/confluent/docker/log4j.properties.template");
        schemaRegistry = new GenericContainer<>(SCHEMA_REGISTRY_IMAGE_NAME.withTag(confluentPlatformVersion))
                .withStartupAttempts(3)
                .withNetwork(network)
                .withNetworkAliases("schema-registry")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "0.0.0.0")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT)
                .withEnv("SCHEMA_REGISTRY_HEAP_OPTS", "-Xmx1G")
                .withExposedPorts(SCHEMA_REGISTRY_PORT)
                .withCopyFileToContainer(
                        schemaRegistryLogTemplate,
                        "/etc/confluent/docker/log4j.properties.template")
                .dependsOn(kafka);
        closer.register(kafka::stop);
        closer.register(schemaRegistry::stop);

        try {
            // write directly to System.out, bypassing logging & io.airlift.log.Logging#rewireStdStreams
            //noinspection resource
            PrintStream out = new PrintStream(new FileOutputStream(FileDescriptor.out), true, Charset.defaultCharset().name());
            kafka.withLogConsumer(new PrintingLogConsumer(out, format("%-20s| ", "kafka")));
            schemaRegistry.withLogConsumer(new PrintingLogConsumer(out, format("%-20s| ", "schema-registry")));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void start()
    {
        checkState(!stopped, "Cannot start again");
        kafka.start();
        if (withSchemaRegistry) {
            schemaRegistry.start();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
        stopped = true;
    }

    @ResourcePresence
    public boolean isNotStopped()
    {
        return !stopped;
    }

    public void createTopic(String topic)
    {
        createTopic(2, 1, topic);
    }

    private void createTopic(int partitions, int replication, String topic)
    {
        try {
            List<String> command = new ArrayList<>();
            command.add("kafka-topics");
            command.add("--partitions");
            command.add(Integer.toString(partitions));
            command.add("--replication-factor");
            command.add(Integer.toString(replication));
            command.add("--topic");
            command.add(topic);

            kafka.execInContainer(command.toArray(new String[0]));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void createTopicWithConfig(int partitions, int replication, String topic, boolean enableLogAppendTime)
    {
        try {
            List<String> command = new ArrayList<>();
            command.add("kafka-topics");
            command.add("--create");
            command.add("--topic");
            command.add(topic);
            command.add("--partitions");
            command.add(Integer.toString(partitions));
            command.add("--replication-factor");
            command.add(Integer.toString(replication));
            command.add("--bootstrap-server");
            command.add("localhost:9092");
            if (enableLogAppendTime) {
                command.add("--config");
                command.add("message.timestamp.type=LogAppendTime");
            }

            kafka.execInContainer(command.toArray(new String[0]));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <K, V> RecordMetadata sendMessages(Stream<ProducerRecord<K, V>> recordStream)
    {
        return sendMessages(recordStream, ImmutableMap.of());
    }

    public <K, V> RecordMetadata sendMessages(Stream<ProducerRecord<K, V>> recordStream, Map<String, String> extraProducerProperties)
    {
        try (KafkaProducer<K, V> producer = createProducer(extraProducerProperties)) {
            Future<RecordMetadata> future = recordStream.map(record -> send(producer, record))
                    .reduce((first, second) -> second)
                    .orElseGet(() -> Futures.immediateFuture(null));
            producer.flush();
            return future.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private <K, V> Future<RecordMetadata> send(KafkaProducer<K, V> producer, ProducerRecord<K, V> record)
    {
        return Failsafe.with(
                RetryPolicy.builder()
                        .onRetry(event -> log.warn(event.getLastException(), "Retrying message send"))
                        .withMaxAttempts(10)
                        .withBackoff(1, 10_000, MILLIS)
                        .build())
                .get(() -> producer.send(record));
    }

    public String getConnectString()
    {
        return kafka.getHost() + ":" + kafka.getMappedPort(KAFKA_PORT);
    }

    private <K, V> KafkaProducer<K, V> createProducer(Map<String, String> extraProperties)
    {
        Map<String, String> properties = new HashMap<>(extraProperties);

        properties.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getConnectString());
        properties.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        properties.putIfAbsent(ProducerConfig.PARTITIONER_CLASS_CONFIG, NumberPartitioner.class.getName());
        properties.putIfAbsent(ProducerConfig.ACKS_CONFIG, "1");

        return new KafkaProducer<>(toProperties(properties));
    }

    private static Properties toProperties(Map<String, String> map)
    {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        return properties;
    }

    public String getSchemaRegistryConnectString()
    {
        return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(SCHEMA_REGISTRY_PORT);
    }

    public Network getNetwork()
    {
        return network;
    }
}
