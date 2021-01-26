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
package io.trino.plugin.kafka;

import io.trino.spi.HostAddress;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import javax.inject.Inject;

import java.util.Properties;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class PlainTextKafkaProducerFactory
        implements KafkaProducerFactory
{
    private final Set<HostAddress> nodes;

    @Inject
    public PlainTextKafkaProducerFactory(KafkaConfig kafkaConfig)
    {
        requireNonNull(kafkaConfig, "kafkaConfig is null");

        nodes = kafkaConfig.getNodes();
    }

    @Override
    public Properties configure()
    {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, nodes.stream()
                .map(HostAddress::toString)
                .collect(joining(",")));
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.setProperty(ACKS_CONFIG, "all");
        properties.setProperty(LINGER_MS_CONFIG, Long.toString(5));
        return properties;
    }
}
