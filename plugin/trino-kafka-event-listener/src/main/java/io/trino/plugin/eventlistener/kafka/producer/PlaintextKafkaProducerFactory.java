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
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.kafkaclients.v2_6.KafkaTelemetry;
import io.trino.plugin.eventlistener.kafka.KafkaEventListenerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PlaintextKafkaProducerFactory
        extends BaseKafkaProducerFactory
{
    private final KafkaEventListenerConfig config;
    private final KafkaTelemetry kafkaTelemetry;

    @Inject
    public PlaintextKafkaProducerFactory(KafkaEventListenerConfig config, OpenTelemetry openTelemetry)
    {
        this.config = requireNonNull(config, "config is null");
        this.kafkaTelemetry = KafkaTelemetry.builder(requireNonNull(openTelemetry, "openTelemetry is null"))
                .build();
    }

    @Override
    public Producer<String, String> producer(Map<String, String> overrides)
    {
        return kafkaTelemetry.wrap(new KafkaProducer<>(createKafkaClientConfig(config, overrides)));
    }

    private Map<String, Object> createKafkaClientConfig(KafkaEventListenerConfig config, Map<String, String> kafkaClientOverrides)
    {
        Map<String, Object> kafkaClientConfig = baseConfig(config);
        kafkaClientConfig.putAll(kafkaClientOverrides);
        return kafkaClientConfig;
    }
}
