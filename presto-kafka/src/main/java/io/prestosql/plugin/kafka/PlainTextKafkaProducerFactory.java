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
import org.apache.kafka.common.serialization.ByteArraySerializer;

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
    private Properties properties;

    @Inject
    public PlainTextKafkaProducerFactory(KafkaConfig kafkaConfig)
    {
        requireNonNull(kafkaConfig, "kafkaConfig is null");
        Set<HostAddress> nodes = ImmutableSet.copyOf(kafkaConfig.getNodes());
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, nodes.stream()
                .map(HostAddress::toString)
                .collect(joining(",")));
        properties.put(ACKS_CONFIG, "all");
        properties.put(LINGER_MS_CONFIG, 5);
        if (kafkaConfig.getSecurityProtocol() != null) {
            properties.put("security.protocol", kafkaConfig.getSecurityProtocol());
        }
        if (kafkaConfig.getSslTruststoreLocation() != null) {
            properties.put("ssl.truststore.location", kafkaConfig.getSslTruststoreLocation());
        }
        if (kafkaConfig.getSslTruststorePassword() != null) {
            properties.put("ssl.truststore.password", kafkaConfig.getSslTruststorePassword());
        }
        if (kafkaConfig.getSslKeystoreLocation() != null) {
            properties.put("ssl.keystore.location", kafkaConfig.getSslKeystoreLocation());
        }
        if (kafkaConfig.getSslKeystorePassword() != null) {
            properties.put("ssl.keystore.password", kafkaConfig.getSslKeystorePassword());
        }
        if (kafkaConfig.getSslEndpointIdentificationAlgorithm() != null) {
            properties.put("ssl.endpoint.identification.algorithm", kafkaConfig.getSslEndpointIdentificationAlgorithm());
        }
    }

    /**
     * Creates a KafkaProducer with the properties set in the constructor.
     */
    public KafkaProducer<byte[], byte[]> create()
    {
        return new KafkaProducer<>(properties, new ByteArraySerializer(), new ByteArraySerializer());
    }
}
