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

package io.trino.plugin.eventlistener.kafka.producer;

import com.google.inject.Inject;
import io.trino.plugin.eventlistener.kafka.KafkaEventListenerConfig;
import io.trino.plugin.kafka.security.KafkaSslConfig;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SSL;

public class SSLKafkaProducerFactory
        extends BaseKafkaProducerFactory
{
    private final KafkaEventListenerConfig config;
    private final KafkaSslConfig sslConfig;

    @Inject
    public SSLKafkaProducerFactory(KafkaEventListenerConfig config, KafkaSslConfig sslConfig)
    {
        this.config = requireNonNull(config, "config is null");
        this.sslConfig = sslConfig;
    }

    @Override
    public KafkaProducer<String, String> producer(Map<String, String> overrides)
    {
        return new KafkaProducer<>(createKafkaClientConfig(config, sslConfig, overrides));
    }

    private Map<String, Object> createKafkaClientConfig(KafkaEventListenerConfig config, KafkaSslConfig sslConfig, Map<String, String> kafkaClientOverrides)
    {
        Map<String, Object> kafkaClientConfig = baseConfig(config);

        sslConfig.getKeystoreLocation().ifPresent(value -> kafkaClientConfig.put(SSL_KEYSTORE_LOCATION_CONFIG, value));
        sslConfig.getKeystorePassword().ifPresent(value -> kafkaClientConfig.put(SSL_KEYSTORE_PASSWORD_CONFIG, value));
        sslConfig.getKeystoreType().ifPresent(value -> kafkaClientConfig.put(SSL_KEYSTORE_TYPE_CONFIG, value.name()));
        sslConfig.getTruststoreLocation().ifPresent(value -> kafkaClientConfig.put(SSL_TRUSTSTORE_LOCATION_CONFIG, value));
        sslConfig.getTruststorePassword().ifPresent(value -> kafkaClientConfig.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, value));
        sslConfig.getTruststoreType().ifPresent(value -> kafkaClientConfig.put(SSL_TRUSTSTORE_TYPE_CONFIG, value.name()));
        sslConfig.getKeyPassword().ifPresent(value -> kafkaClientConfig.put(SSL_KEY_PASSWORD_CONFIG, value));
        sslConfig.getEndpointIdentificationAlgorithm().ifPresent(value -> kafkaClientConfig.put(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, value.getValue()));
        kafkaClientConfig.put(SECURITY_PROTOCOL_CONFIG, SSL.name());

        kafkaClientConfig.putAll(kafkaClientOverrides);
        return kafkaClientConfig;
    }
}
