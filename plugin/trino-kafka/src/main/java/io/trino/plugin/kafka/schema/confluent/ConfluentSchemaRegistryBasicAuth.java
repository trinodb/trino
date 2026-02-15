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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

public class ConfluentSchemaRegistryBasicAuth
        implements SchemaRegistryClientPropertiesProvider
{
    private final String user;
    private final String password;

    @Inject
    public ConfluentSchemaRegistryBasicAuth(BasicAuthConfig basicAuthConfig)
    {
        this.user = requireNonNull(basicAuthConfig.getConfluentSchemaRegistryUsername(), "user is null");
        this.password = requireNonNull(basicAuthConfig.getConfluentSchemaRegistryPassword(), "password is null");
    }

    @Override
    public ImmutableMap<String, Object> getSchemaRegistryClientProperties()
    {
        return ImmutableMap.<String, Object>builder()
                .put("basic.auth.credentials.source", "USER_INFO")
                .put("basic.auth.user.info", user + ":" + password)
                .buildOrThrow();
    }
}
