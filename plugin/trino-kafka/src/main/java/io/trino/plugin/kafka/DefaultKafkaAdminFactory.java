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

package io.trino.plugin.kafka;

import com.google.inject.Inject;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSession;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static io.trino.plugin.kafka.utils.PropertiesUtils.readProperties;
import static java.util.stream.Collectors.joining;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class DefaultKafkaAdminFactory
        implements KafkaAdminFactory
{
    private final Set<HostAddress> nodes;
    private final Map<String, String> configurationProperties;

    @Inject
    public DefaultKafkaAdminFactory(KafkaConfig kafkaConfig)
            throws Exception
    {
        nodes = kafkaConfig.getNodes();
        configurationProperties = readProperties(kafkaConfig.getResourceConfigFiles());
    }

    @Override
    public Properties configure(ConnectorSession session)
    {
        Properties properties = new Properties();
        properties.putAll(configurationProperties);
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, nodes.stream()
                .map(HostAddress::toString)
                .collect(joining(",")));
        return properties;
    }
}
