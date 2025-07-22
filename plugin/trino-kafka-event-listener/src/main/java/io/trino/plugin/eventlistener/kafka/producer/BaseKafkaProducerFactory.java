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

import io.trino.plugin.eventlistener.kafka.KafkaEventListenerConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

abstract class BaseKafkaProducerFactory
        implements KafkaProducerFactory
{
    @Override
    public Producer<String, String> producer(Map<String, String> overrides)
    {
        throw new UnsupportedOperationException("Cannot call producer() on abstract class");
    }

    protected Map<String, Object> baseConfig(KafkaEventListenerConfig config)
    {
        Map<String, Object> kafkaClientConfig = new HashMap<>();
        kafkaClientConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBrokerEndpoints());
        config.getClientId().ifPresent(clientId ->
                kafkaClientConfig.put(ProducerConfig.CLIENT_ID_CONFIG, clientId));
        kafkaClientConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaClientConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaClientConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        kafkaClientConfig.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Long.toString(config.getMaxRequestSize().toBytes()));
        kafkaClientConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, Long.toString(config.getBatchSize().toBytes()));
        kafkaClientConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Long.toString(config.getRequestTimeout().toMillis()));

        // Add SCRAM/SASL configs if present
        config.getSaslMechanism()
                .ifPresent(mechanism -> kafkaClientConfig.put(SaslConfigs.SASL_MECHANISM, mechanism));

        config.getSecurityProtocol()
                .ifPresent(protocol -> kafkaClientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol));

        config.getSaslJaasConfig()
                .ifPresent(jaasConfig -> kafkaClientConfig.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig));

        return kafkaClientConfig;
    }
}
