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

package io.trino.plugin.eventlistener.kafka;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.eventlistener.kafka.producer.KafkaProducerFactory;
import io.trino.plugin.eventlistener.kafka.producer.PlaintextKafkaProducerFactory;
import io.trino.plugin.eventlistener.kafka.producer.SSLKafkaProducerFactory;
import io.trino.plugin.kafka.KafkaSecurityConfig;
import io.trino.plugin.kafka.security.KafkaSslConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class KafkaProducerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(conditionalModule(
                KafkaSecurityConfig.class,
                config -> config.getSecurityProtocol().orElse(SecurityProtocol.PLAINTEXT) == SecurityProtocol.PLAINTEXT,
                // KafkaSecurityConfig only allows PLAINTEXT or SSL for security protocol types
                plainTextBinder -> plainTextBinder.bind(KafkaProducerFactory.class).to(PlaintextKafkaProducerFactory.class).in(Scopes.SINGLETON),
                sslBinder -> {
                    sslBinder.bind(KafkaProducerFactory.class).to(SSLKafkaProducerFactory.class).in(Scopes.SINGLETON);
                    configBinder(sslBinder).bindConfig(KafkaSslConfig.class);
                }));
    }
}
