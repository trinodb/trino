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

import io.airlift.units.DataSize;
import io.prestosql.spi.HostAddress;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RECEIVE_BUFFER_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class KafkaClientConsumerFactory
        implements KafkaConsumerFactory
{
    private final KafkaConfig kafkaConfig;
    private final Set<HostAddress> nodes;
    private final DataSize kafkaBufferSize;
    private final Properties securityConfig;

    @Inject
    public KafkaClientConsumerFactory(KafkaConfig kafkaConfig, Optional<KafkaSecurityConfig> securityConfig)
    {
        requireNonNull(kafkaConfig, "kafkaConfig is null");
        if (securityConfig.isPresent()) {
            this.securityConfig = securityConfig.get().getKafkaClientProperties();
        }
        else {
            this.securityConfig = new Properties();
        }
        this.kafkaConfig = kafkaConfig;
        nodes = kafkaConfig.getNodes();
        kafkaBufferSize = kafkaConfig.getKafkaBufferSize();
    }

    @Override
    public Properties configure()
    {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, nodes.stream()
                .map(HostAddress::toString)
                .collect(joining(",")));
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(RECEIVE_BUFFER_CONFIG, Long.toString(kafkaBufferSize.toBytes()));
        properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(false));
        properties.setProperty("security.protocol", kafkaConfig.getSecurityProtocol().name);
        properties.putAll(securityConfig);
        return properties;
    }
}
