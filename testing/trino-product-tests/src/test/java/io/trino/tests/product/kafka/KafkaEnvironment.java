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
package io.trino.tests.product.kafka;

import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.kafka.TestingKafka;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Map;
import java.util.stream.Stream;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/**
 * Abstract base class for Kafka product test environments.
 * <p>
 * This allows tests to be written against the common interface and run
 * in multiple Kafka environment variants (basic with table definitions,
 * schema registry, etc.).
 */
public abstract class KafkaEnvironment
        extends ProductTestEnvironment
{
    /**
     * Returns the Kafka catalog name for this environment.
     * Either "kafka" (basic) or "kafka_schema_registry".
     */
    public abstract String getCatalogName();

    /**
     * Returns the topic name suffix for this environment.
     * Used to create different topics for each catalog to avoid conflicts.
     */
    public abstract String getTopicNameSuffix();

    /**
     * Returns whether this environment supports column mapping in table definitions.
     * The basic kafka catalog with table definitions supports mapping Avro/Protobuf
     * field names to different column names. The schema registry catalog does not.
     */
    public abstract boolean supportsColumnMapping();

    /**
     * Returns the TestingKafka instance for sending messages.
     */
    public abstract TestingKafka getKafka();

    /**
     * Creates a Kafka topic with the given name.
     */
    public void createTopic(String topicName)
    {
        getKafka().createTopic(topicName);
    }

    /**
     * Sends raw byte messages to a Kafka topic.
     */
    public void sendMessages(String topic, byte[] key, byte[] value)
    {
        getKafka().sendMessages(
                Stream.of(new ProducerRecord<>(topic, key, value)),
                Map.of(
                        KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                        VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()));
    }

    /**
     * Sends raw byte messages to a specific partition of a Kafka topic.
     */
    public void sendMessages(String topic, int partition, byte[] key, byte[] value)
    {
        getKafka().sendMessages(
                Stream.of(new ProducerRecord<>(topic, partition, key, value)),
                Map.of(
                        KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                        VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()));
    }
}
