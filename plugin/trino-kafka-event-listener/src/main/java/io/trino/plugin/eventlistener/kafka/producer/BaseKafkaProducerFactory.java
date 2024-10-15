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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

abstract class BaseKafkaProducerFactory
        implements KafkaProducerFactory
{
    @Override
    public KafkaProducer<String, String> producer(Map<String, String> overrides)
    {
        throw new UnsupportedOperationException("Cannot call producer() on abstract class");
    }

    protected Map<String, Object> baseConfig(KafkaEventListenerConfig config)
    {
        Map<String, Object> kafkaClientConfig = new HashMap<>();
        kafkaClientConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBrokerEndpoints());
        config.getClientId().ifPresent(clientId -> kafkaClientConfig.put(ProducerConfig.CLIENT_ID_CONFIG, clientId));
        kafkaClientConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaClientConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaClientConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        kafkaClientConfig.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "5242880");
        kafkaClientConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Long.toString(config.getRequestTimeout().toMillis()));
        return kafkaClientConfig;
    }
}
