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
package io.trino.security.credential;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.spi.security.credential.CredentialProvider;
import io.trino.spi.security.credential.CredentialProviderFactory;
import io.trino.spi.security.credential.CredentialResolver;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CredentialProviderRegistry
        implements CredentialResolver
{
    public static final Pattern VALID_NAME = Pattern.compile("[a-z][a-z0-9-]*");
    private static final Pattern VALID_FACTORY_NAME = Pattern.compile("[a-z][a-z0-9_]*");

    private final CredentialProviderStore credentialProviderStore;
    private final Map<String, CredentialProviderFactory> factories = new ConcurrentHashMap<>();
    private final Map<String, CredentialProvider> credentialProviders = new ConcurrentHashMap<>();

    @Inject
    public CredentialProviderRegistry(CredentialProviderStore credentialProviderStore)
    {
        this.credentialProviderStore = requireNonNull(credentialProviderStore, "credentialProviderStore is null");
    }

    public void addCredentialProviderFactory(CredentialProviderFactory credentialProviderFactory)
    {
        if (!VALID_FACTORY_NAME.matcher(credentialProviderFactory.getFactoryName()).matches()) {
            throw new IllegalArgumentException("Invalid credential provider factory name: %s, should match %s".formatted(credentialProviderFactory.getFactoryName(), VALID_FACTORY_NAME.pattern()));
        }
        if (factories.putIfAbsent(credentialProviderFactory.getFactoryName(), credentialProviderFactory) != null) {
            throw new IllegalArgumentException(format("Credential provider factory '%s' is already registered", credentialProviderFactory.getFactoryName()));
        }
    }

    public void loadCredentialProviders()
    {
        credentialProviderStore.loadCredentialProviders(factories, credentialProviders);
        for (String provider : credentialProviders.keySet()) {
            if (!VALID_NAME.matcher(provider).matches()) {
                throw new IllegalArgumentException("Invalid credential provider name: %s, should match %s".formatted(provider, VALID_NAME.pattern()));
            }
        }
    }

    @Override
    public Optional<CredentialProvider> get(String providerName)
    {
        return Optional.ofNullable(credentialProviders.get(providerName));
    }

    @Override
    public Set<String> loadedProviders()
    {
        return ImmutableSet.copyOf(credentialProviders.keySet());
    }
}
