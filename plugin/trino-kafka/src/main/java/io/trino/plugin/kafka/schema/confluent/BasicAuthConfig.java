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
package io.trino.plugin.kafka.schema.confluent;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

public class BasicAuthConfig
{
    private String username;
    private String password;

    @NotNull
    public String getConfluentSchemaRegistryUsername()
    {
        return username;
    }

    @Config("kafka.confluent-schema-registry.basic-auth.username")
    @ConfigDescription("The username for the Confluent Schema Registry")
    @ConfigSecuritySensitive
    public BasicAuthConfig setConfluentSchemaRegistryUsername(String username)
    {
        this.username = username;
        return this;
    }

    @NotNull
    public String getConfluentSchemaRegistryPassword()
    {
        return password;
    }

    @Config("kafka.confluent-schema-registry.basic-auth.password")
    @ConfigSecuritySensitive
    public BasicAuthConfig setConfluentSchemaRegistryPassword(String password)
    {
        this.password = password;
        return this;
    }
}
