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
package io.trino.testing;

import io.trino.security.credential.CredentialProviderStore;
import io.trino.spi.TrinoException;
import io.trino.spi.security.credential.CredentialProvider;
import io.trino.spi.security.credential.CredentialProviderFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;

public class TestingCredentialProviderStore
        implements CredentialProviderStore
{
    private final Map<String, CredentialProviderProperties> providers = new ConcurrentHashMap<>();

    public void addCredentialProvider(String name, String factoryName, Map<String, String> properties)
    {
        providers.put(name, new CredentialProviderProperties(factoryName, properties));
    }

    @Override
    public void loadCredentialProviders(Map<String, CredentialProviderFactory> factories, Map<String, CredentialProvider> credentialProviders)
    {
        providers.forEach((name, properties) -> {
            CredentialProviderFactory factory = factories.get(properties.factoryName());
            if (factory == null) {
                throw new TrinoException(CONFIGURATION_INVALID, "Factory not found: " + properties.factoryName());
            }
            if (!credentialProviders.containsKey(name)) {
                credentialProviders.put(name, factory.create(name, properties.config()));
            }
        });
    }

    private record CredentialProviderProperties(String factoryName, Map<String, String> config) {}
}
