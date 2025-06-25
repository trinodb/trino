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
package io.trino.plugin.kafka.schema;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.kafka.schema.confluent.BasicAuthConfig;
import io.trino.plugin.kafka.schema.confluent.ConfluentSchemaRegistryAuth;
import io.trino.plugin.kafka.schema.confluent.ConfluentSchemaRegistryBasicAuth;
import io.trino.plugin.kafka.schema.confluent.ConfluentSchemaRegistryNoAuth;
import io.trino.plugin.kafka.schema.confluent.SchemaRegistryClientPropertiesProvider;

import java.util.Optional;

public class KafkaSchemaRegistryClientPropertiesProvider
        implements SchemaRegistryClientPropertiesProvider
{
    private final ConfluentSchemaRegistryAuth auth;

    @Inject
    public KafkaSchemaRegistryClientPropertiesProvider(
            Optional<BasicAuthConfig> basicAuthConfig)
    {
        if (basicAuthConfig.isPresent()) {
            auth = new ConfluentSchemaRegistryBasicAuth(
                   basicAuthConfig.get().getConfluentSchemaRegistryUsername(),
                   basicAuthConfig.get().getConfluentSchemaRegistryPassword());
        }
        else {
            auth = new ConfluentSchemaRegistryNoAuth();
        }
    }

    @Override
    public ImmutableMap<String, Object> getSchemaRegistryClientProperties()
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.putAll(auth.getClientProperties());
        return properties.buildOrThrow();
    }
}
