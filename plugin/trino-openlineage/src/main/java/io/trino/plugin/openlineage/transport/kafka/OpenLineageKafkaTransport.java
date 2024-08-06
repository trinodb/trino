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
import io.openlineage.client.transports.KafkaConfig;
import io.openlineage.client.transports.KafkaTransport;
import io.openlineage.client.transports.Transport;
import io.trino.plugin.openlineage.config.kafka.OpenLineageClientKafkaTransportConfig;
import io.trino.plugin.openlineage.transport.OpenLineageTransport;

import java.util.Properties;

public class OpenLineageKafkaTransport
        implements OpenLineageTransport
{
    private final String topicName;
    private final String messageKey;
    private final String brokerEndpoints;
    private final String bootstrapServers = "bootstrap.servers";
    private final String acks = "acks";
    private final String acksDefault = "all";
    private final String keySerializer = "key.serializer";
    private final String keySerializerDefault = "org.apache.kafka.common.serialization.StringSerializer";
    private final String valueSerializer = "value.serializer";
    private final String valueSerializerDefault = "org.apache.kafka.common.serialization.StringSerializer";

    @Inject
    public OpenLineageKafkaTransport(OpenLineageClientKafkaTransportConfig config)
    {
        this.topicName = config.getTopicName();
        this.messageKey = config.getMessageKey();
        this.brokerEndpoints = config.getBrokerEndpoints();
    }

    @Override
    public Transport buildTransport()
    {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(bootstrapServers, brokerEndpoints);
        kafkaProperties.put(acks, acksDefault);
        kafkaProperties.put(keySerializer, keySerializerDefault);
        kafkaProperties.put(valueSerializer, valueSerializerDefault);
        return new KafkaTransport(new KafkaConfig(topicName, messageKey, kafkaProperties));
    }
}
