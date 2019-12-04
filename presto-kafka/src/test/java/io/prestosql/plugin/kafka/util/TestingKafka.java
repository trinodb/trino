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
package io.prestosql.plugin.kafka.util;

import com.google.common.collect.ImmutableMap;
import kafka.producer.ProducerConfig;
import org.testcontainers.containers.KafkaContainer;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.prestosql.plugin.kafka.util.TestUtils.toProperties;
import static org.testcontainers.containers.KafkaContainer.KAFKA_PORT;

public class TestingKafka
        implements Closeable
{
    private final KafkaContainer container;

    public TestingKafka()
    {
        container = new KafkaContainer("5.2.1");
    }

    public void start()
    {
        container.start();
    }

    @Override
    public void close()
    {
        container.close();
    }

    public void createTopics(String topic)
    {
        createTopics(2, 1, new Properties(), topic);
    }

    public void createTopics(int partitions, int replication, Properties topicProperties, String topic)
    {
        try {
            List<String> command = new ArrayList<>();
            command.add("kafka-topics");
            command.add("--partitions");
            command.add(Integer.toString(partitions));
            command.add("--replication-factor");
            command.add(Integer.toString(replication));
            command.add("kafka-topics");
            command.add("--topic");
            command.add(topic);
            for (Map.Entry<Object, Object> property : topicProperties.entrySet()) {
                command.add("--config");
                command.add(property.getKey() + "=" + property.getValue());
            }

            container.execInContainer(command.toArray(new String[0]));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getConnectString()
    {
        return container.getContainerIpAddress() + ":" + container.getMappedPort(KAFKA_PORT);
    }

    public CloseableProducer<Long, Object> createProducer()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("metadata.broker.list", getConnectString())
                .put("serializer.class", JsonEncoder.class.getName())
                .put("key.serializer.class", NumberEncoder.class.getName())
                .put("partitioner.class", NumberPartitioner.class.getName())
                .put("request.required.acks", "1")
                .build();

        ProducerConfig producerConfig = new ProducerConfig(toProperties(properties));
        return new CloseableProducer<>(producerConfig);
    }

    public static class CloseableProducer<K, V>
            extends kafka.javaapi.producer.Producer<K, V>
            implements AutoCloseable
    {
        public CloseableProducer(ProducerConfig config)
        {
            super(config);
        }
    }
}
