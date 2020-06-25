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
package io.prestosql.plugin.kafka;

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.HostAddress;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;

import javax.inject.Inject;

import java.util.Properties;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;

public class PlainTextKafkaProducerFactory
{
    private final Set<HostAddress> nodes;
    private final Properties properties;

    @Inject
    public PlainTextKafkaProducerFactory(KafkaConfig kafkaConfig, Properties properties)
    {
        requireNonNull(kafkaConfig, "kafkaConfig is null");
        this.nodes = ImmutableSet.copyOf(kafkaConfig.getNodes());
        this.properties = requireNonNull(properties, "properties is null");
    }

    public <K, V> KafkaProducer<K, V> create(Serializer<K> keySerializer, Serializer<V> messageSerializer)
    {
        properties.put(
                BOOTSTRAP_SERVERS_CONFIG,
                nodes.stream()
                        .map(HostAddress::toString)
                        .collect(joining(",")));
        properties.put(ACKS_CONFIG, "all");
        properties.put(LINGER_MS_CONFIG, 5);  // reduces number of requests sent, adds 5ms of latency in the absence of load
        return new KafkaProducer<>(properties, keySerializer, messageSerializer);
    }
}
