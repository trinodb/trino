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

import com.google.inject.Inject;
import io.trino.plugin.kafka.schema.confluent.ConfluentSchemaRegistryConfig;
import io.trino.plugin.kafka.schema.confluent.SchemaRegistryClientPropertiesProvider;

import java.util.HashMap;
import java.util.Map;

public class KafkaSchemaRegistryClientPropertiesProvider
        implements SchemaRegistryClientPropertiesProvider
{
    public final String username;
    public final String password;

    @Inject
    public KafkaSchemaRegistryClientPropertiesProvider(ConfluentSchemaRegistryConfig confluentConfig)
    {
        this.username = confluentConfig.getConfluentSchemaRegistryUsername();
        this.password = confluentConfig.getConfluentSchemaRegistryPassword();
    }

    @Override
    public Map<String, Object> getSchemaRegistryClientProperties()
    {
        Map<String, Object> result = new HashMap<>();
        if (username != null) {
            result.put("basic.auth.credentials.source", "USER_INFO");
            result.put("basic.auth.user.info", username + ":" + password);
        }
        return result;
    }
}
