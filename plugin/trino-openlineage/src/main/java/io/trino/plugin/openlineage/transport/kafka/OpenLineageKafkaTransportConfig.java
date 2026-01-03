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

import com.google.common.base.Splitter;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotEmpty;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class OpenLineageKafkaTransportConfig
{
    private static final Splitter.MapSplitter MAP_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator("=");

    private String brokerEndpoints;
    private String topicName;
    private Optional<String> clientId = Optional.empty();
    private Duration requestTimeout = new Duration(10, TimeUnit.SECONDS);
    private Map<String, String> kafkaClientOverrides = Collections.emptyMap();
    private Optional<String> messageKey = Optional.empty();

    @NotEmpty
    public String getBrokerEndpoints()
    {
        return brokerEndpoints;
    }

    @Config("openlineage-event-listener.kafka-transport.broker-endpoints")
    @ConfigDescription("Comma-separated list of Kafka broker addresses (host:port)")
    public OpenLineageKafkaTransportConfig setBrokerEndpoints(String brokerEndpoints)
    {
        this.brokerEndpoints = brokerEndpoints;
        return this;
    }

    @NotEmpty
    public String getTopicName()
    {
        return topicName;
    }

    @Config("openlineage-event-listener.kafka-transport.topic-name")
    @ConfigDescription("Kafka topic name for OpenLineage events")
    public OpenLineageKafkaTransportConfig setTopicName(String topicName)
    {
        this.topicName = topicName;
        return this;
    }

    public Optional<String> getClientId()
    {
        return clientId;
    }

    @Config("openlineage-event-listener.kafka-transport.client-id")
    @ConfigDescription("Kafka client ID for identifying this producer")
    public OpenLineageKafkaTransportConfig setClientId(String clientId)
    {
        this.clientId = Optional.ofNullable(clientId);
        return this;
    }

    @MinDuration("1s")
    @MaxDuration("1h")
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("openlineage-event-listener.kafka-transport.request-timeout")
    @ConfigDescription("Kafka request timeout")
    public OpenLineageKafkaTransportConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    public Map<String, String> getKafkaClientOverrides()
    {
        return kafkaClientOverrides;
    }

    @Config("openlineage-event-listener.kafka-transport.client-config-overrides")
    @ConfigDescription("Comma-separated list of key-value pairs to specify kafka client config overrides. E.g.: 'buffer.memory=67108864,compression.type=zstd'")
    public OpenLineageKafkaTransportConfig setKafkaClientOverrides(String kafkaClientOverrides)
    {
        this.kafkaClientOverrides = MAP_SPLITTER.split(requireNonNull(kafkaClientOverrides, "kafkaClientOverrides is null"));
        return this;
    }

    public Optional<String> getMessageKey()
    {
        return messageKey;
    }

    @Config("openlineage-event-listener.kafka-transport.message-key")
    @ConfigDescription("Optional key for all Kafka messages produced by transport. If not specified, OpenLineage will use default value based on event type")
    public OpenLineageKafkaTransportConfig setMessageKey(String messageKey)
    {
        this.messageKey = Optional.ofNullable(messageKey);
        return this;
    }
}
