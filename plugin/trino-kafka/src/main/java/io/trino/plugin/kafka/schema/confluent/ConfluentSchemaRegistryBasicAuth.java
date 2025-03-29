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

import static java.util.Objects.requireNonNull;

public class ConfluentSchemaRegistryBasicAuth
        implements ConfluentSchemaRegistryAuth
{
    private final String user;
    private final String password;

    public ConfluentSchemaRegistryBasicAuth(String user, String password)
    {
        this.user = requireNonNull(user, "user is null");
        this.password = requireNonNull(password, "password is null");
    }

    public String getUser()
    {
        return user;
    }

    public String getPassword()
    {
        return password;
    }

    @Override
    public ImmutableMap<String, Object> getClientProperties()
    {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put("basic.auth.credentials.source", "USER_INFO");
        builder.put("basic.auth.user.info", user + ":" + password);
        return builder.buildOrThrow();
    }
}
