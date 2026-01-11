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
package io.trino.plugin.openlineage.transport.kafka;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.openlineage.client.transports.KafkaConfig;
import io.openlineage.client.transports.KafkaTransport;
import io.trino.plugin.kafka.KafkaSecurityConfig;
import io.trino.plugin.kafka.security.KafkaSslConfig;
import io.trino.plugin.openlineage.transport.OpenLineageTransportCreator;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static io.trino.plugin.kafka.utils.PropertiesUtils.readProperties;
import static org.apache.kafka.common.security.auth.SecurityProtocol.PLAINTEXT;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SSL;

public class OpenLineageKafkaTransport
        implements OpenLineageTransportCreator
{
    private static final Logger log = Logger.get(OpenLineageKafkaTransport.class);

    private final String topicName;
    private final Properties kafkaProperties;
    private final Optional<String> messageKey;

    @Inject
    public OpenLineageKafkaTransport(
            OpenLineageKafkaTransportConfig config,
            KafkaSecurityConfig securityConfig,
            Optional<KafkaSslConfig> sslConfig)
            throws Exception
    {
        this.topicName = config.getTopicName();
        this.messageKey = config.getMessageKey();
        this.kafkaProperties = buildKafkaProperties(config, securityConfig, sslConfig);

        log.info("Initializing OpenLineage Kafka transport: brokers=%s, topic=%s, protocol=%s, messageKey=%s",
                config.getBrokerEndpoints(),
                topicName,
                securityConfig.getSecurityProtocol().orElse(PLAINTEXT),
                messageKey);
    }

    @Override
    public KafkaTransport buildTransport()
    {
        KafkaConfig kafkaConfig = new KafkaConfig(
                topicName,
                messageKey.orElse(null),
                kafkaProperties);
        KafkaTransport transport = new KafkaTransport(kafkaConfig);
        log.info("Created OpenLineage Kafka transport: topic=%s", topicName);
        return transport;
    }

    private Properties buildKafkaProperties(
            OpenLineageKafkaTransportConfig config,
            KafkaSecurityConfig securityConfig,
            Optional<KafkaSslConfig> sslConfig)
            throws Exception
    {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBrokerEndpoints());
        config.getClientId().ifPresent(id -> properties.put(ProducerConfig.CLIENT_ID_CONFIG, id));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Long.toString(config.getRequestTimeout().toMillis()));

        SecurityProtocol protocol = securityConfig.getSecurityProtocol().orElse(PLAINTEXT);
        if (protocol == SSL) {
            KafkaSslConfig kafkaSslConfig = sslConfig.orElseThrow(() ->
                    new IllegalStateException("SSL security protocol specified but SSL configuration is not provided"));
            properties.putAll(kafkaSslConfig.getKafkaClientProperties());
        }
        else {
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol.name());
        }

        Map<String, String> configOverrides = readProperties(config.getResourceConfigFiles());
        properties.putAll(configOverrides);
        return properties;
    }
}
