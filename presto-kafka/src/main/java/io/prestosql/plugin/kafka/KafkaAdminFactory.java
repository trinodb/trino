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

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.HostAddress;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;

import javax.inject.Inject;

import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class KafkaAdminFactory
{
    private final Properties kafkaProperties;

    @Inject
    public KafkaAdminFactory(KafkaConfig kafkaConfig, @KafkaSecurityModules.ForKafkaSecurity Map<String, Object> kafkaSecurityConfig)
    {
        requireNonNull(kafkaConfig, "kafkaConfig is null");
        Map<String, Object> kafkaSecurityProperties = ImmutableMap.copyOf(kafkaSecurityConfig);

        kafkaProperties = new Properties();
        kafkaProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getNodes().stream()
                .map(HostAddress::toString)
                .collect(joining(",")));
        kafkaProperties.putAll(kafkaSecurityProperties);
    }

    public AdminClient create()
    {
        return KafkaAdminClient.create(kafkaProperties);
    }
}
