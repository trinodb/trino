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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.kafka.security.ForKafkaSsl;
import io.trino.plugin.kafka.security.KafkaSslConfig;
import io.trino.spi.connector.ConnectorSession;

import javax.inject.Inject;

import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class SslKafkaProducerFactory
        implements KafkaProducerFactory
{
    private final ImmutableMap<String, Object> map;
    private final KafkaProducerFactory delegate;

    @Inject
    public SslKafkaProducerFactory(@ForKafkaSsl KafkaProducerFactory delegate, KafkaSslConfig sslConfig)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        map = ImmutableMap.copyOf(sslConfig.getKafkaClientProperties());
    }

    @Override
    public Properties configure(ConnectorSession session)
    {
        Properties properties = new Properties();
        properties.putAll(delegate.configure(session));
        properties.putAll(map);
        return properties;
    }
}
