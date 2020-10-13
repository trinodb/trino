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

import io.prestosql.plugin.kafka.KafkaSecurityModules.ForKafkaSecurity;
import io.prestosql.spi.HostAddress;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import javax.inject.Inject;

import java.util.Map;
import java.util.Properties;

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
    private final Properties kafkaProperties;

    @Inject
    public KafkaClientConsumerFactory(KafkaConfig kafkaConfig, @ForKafkaSecurity Map<String, Object> kafkaSecurityProperties)
    {
        requireNonNull(kafkaConfig, "kafkaConfig is null");

        kafkaProperties = new Properties();
        kafkaProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getNodes().stream()
                .map(HostAddress::toString)
                .collect(joining(",")));
        kafkaProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        kafkaProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        kafkaProperties.setProperty(RECEIVE_BUFFER_CONFIG, Long.toString(kafkaConfig.getKafkaBufferSize().toBytes()));
        kafkaProperties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(false));
        kafkaProperties.setProperty("security.protocol", kafkaConfig.getSecurityProtocol().name);
        kafkaProperties.putAll(kafkaSecurityProperties);
    }

    @Override
    public Properties configure()
    {
        return kafkaProperties;
    }
}
