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
package io.trino.plugin.jdbc.credential;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DefaultCredentialPropertiesProvider
        implements CredentialPropertiesProvider<String, String>
{
    private final CredentialProvider provider;

    public DefaultCredentialPropertiesProvider(CredentialProvider provider)
    {
        this.provider = requireNonNull(provider, "provider is null");
    }

    @Override
    public Map<String, String> getCredentialProperties(ConnectorIdentity identity)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        provider.getConnectionUser(Optional.of(identity)).ifPresent(user -> properties.put("user", user));
        provider.getConnectionPassword(Optional.of(identity)).ifPresent(password -> properties.put("password", password));
        return properties.build();
    }
}
