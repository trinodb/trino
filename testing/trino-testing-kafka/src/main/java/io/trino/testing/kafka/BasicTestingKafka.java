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

import com.google.common.util.concurrent.Futures;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static java.time.temporal.ChronoUnit.MILLIS;
import static org.testcontainers.containers.KafkaContainer.KAFKA_PORT;

public class BasicTestingKafka
        implements TestingKafka
{
    private final KafkaContainer container;

    public BasicTestingKafka()
    {
        this(DEFAULT_CONFLUENT_PLATFORM_VERSION);
    }

    public BasicTestingKafka(String confluentPlatformVersion)
    {
        container = new KafkaContainer(confluentPlatformVersion)
                .withNetwork(Network.SHARED)
                .withNetworkAliases("kafka");
    }

    @Override
    public void start()
    {
        container.start();
    }

    @Override
    public void close()
    {
        container.close();
    }

    @Override
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
            command.add("--zookeeper");
            command.add("localhost:2181");
            if (enableLogAppendTime) {
                command.add("--config");
                command.add("message.timestamp.type=LogAppendTime");
            }

            container.execInContainer(command.toArray(new String[0]));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <K, V> RecordMetadata sendMessages(Stream<ProducerRecord<K, V>> recordStream, Map<String, String> extraProducerProperties)
    {
        try (KafkaProducer<K, V> producer = createProducer(extraProducerProperties)) {
            Future<RecordMetadata> future = recordStream.map(record -> send(producer, record))
                    .reduce((first, second) -> second)
                    .orElse(Futures.immediateFuture(null));
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
                new RetryPolicy<>()
                        .withMaxAttempts(10)
                        .withBackoff(1, 10_000, MILLIS))
                .get(() -> producer.send(record));
    }

    @Override
    public String getConnectString()
    {
        return container.getContainerIpAddress() + ":" + container.getMappedPort(KAFKA_PORT);
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
}
