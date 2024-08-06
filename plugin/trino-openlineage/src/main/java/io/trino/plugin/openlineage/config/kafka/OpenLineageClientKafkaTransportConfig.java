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
package io.trino.plugin.openlineage.config.kafka;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

public class OpenLineageClientKafkaTransportConfig
{
    private String topicName;
    private String messageKey;
    private String brokerEndpoints;

    @NotNull
    public String getTopicName()
    {
        return topicName;
    }

    @Config("openlineage-event-listener.transport.kafka.topic")
    @ConfigDescription("String specifying the topic to which events will be sent")
    public OpenLineageClientKafkaTransportConfig setTopicName(String topicName)
    {
        this.topicName = topicName;
        return this;
    }

    @NotNull
    public String getBrokerEndpoints()
    {
        return brokerEndpoints;
    }

    @Config("openlineage-event-listener.transport.kafka.broker-endpoints")
    @ConfigDescription("List of kafka brokers provided as: \"broker_host1:broker_port1,broker_host2:broker_port2, ...\"")
    public OpenLineageClientKafkaTransportConfig setBrokerEndpoints(String brokerEndpoints)
    {
        this.brokerEndpoints = brokerEndpoints;
        return this;
    }

    public String getMessageKey()
    {
        return messageKey;
    }

    @Config("openlineage-event-listener.transport.kafka.message-key")
    @ConfigDescription("String key for all kafka messages produced")
    public OpenLineageClientKafkaTransportConfig setMessageKey(String messageKey)
    {
        this.messageKey = messageKey;
        return this;
    }
}
