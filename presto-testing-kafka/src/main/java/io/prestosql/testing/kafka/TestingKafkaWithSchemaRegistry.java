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
package io.prestosql.testing.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestingKafkaWithSchemaRegistry
        implements Closeable
{
    public static final int SCHEMA_REGISTRY_PORT = 8081;
    private final TestingKafka testingKafka;
    private final GenericContainer<?> schemaRegistryContainer;

    @SuppressWarnings("resource")
    private final Closer closer = Closer.create();

    public TestingKafkaWithSchemaRegistry(TestingKafka testingKafka)
    {
        this.testingKafka = requireNonNull(testingKafka, "testingKafka is null");
        schemaRegistryContainer = new GenericContainer<>("confluentinc/cp-schema-registry:5.4.1")
                .withNetwork(Network.SHARED)
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "0.0.0.0")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", format("http://0.0.0.0:%s", SCHEMA_REGISTRY_PORT))
                .withExposedPorts(SCHEMA_REGISTRY_PORT);
        closer.register(testingKafka);
        closer.register(schemaRegistryContainer::stop);
    }

    public void start()
    {
        testingKafka.start();
        try {
            schemaRegistryContainer.start();
        }
        catch (Throwable e) {
            testingKafka.close();
            throw e;
        }
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }

    public void createTopic(String topic)
    {
        testingKafka.createTopic(topic);
    }

    public String getConnectString()
    {
        return testingKafka.getConnectString();
    }

    public String getSchemaRegistryConnectString()
    {
        return "http://" + schemaRegistryContainer.getContainerIpAddress() + ":" + schemaRegistryContainer.getMappedPort(SCHEMA_REGISTRY_PORT);
    }

    public KafkaProducer<Long, Object> createProducer()
    {
        return testingKafka.createProducer();
    }

    public <T> KafkaProducer<T, GenericRecord> createConfluentProducer()
    {
        return createConfluentProducer(ImmutableMap.of());
    }

    public <T> KafkaProducer<T, GenericRecord> createConfluentProducer(Map<?, ?> extraProperties)
    {
        return createConfluentProducer(KafkaAvroSerializer.class, extraProperties);
    }

    public KafkaProducer<Long, GenericRecord> createConfluentProducerWithLongKeys()
    {
        return createConfluentProducer(LongSerializer.class, ImmutableMap.of());
    }

    public <T, U> KafkaProducer<U, GenericRecord> createConfluentProducer(Class<T> serializerClass, Map<?, ?> extraProperties)
    {
        Properties properties = new Properties();
        properties.put(SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryConnectString());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testingKafka.getConnectString());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializerClass.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        properties.putAll(extraProperties);
        return new KafkaProducer<>(properties);
    }
}
