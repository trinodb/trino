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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Credential;
import io.trino.spi.security.CredentialProvider;
import io.trino.spi.security.CredentialProviders;
import io.trino.spi.security.CredentialRequest;

import java.util.Optional;
import java.util.Set;

@Singleton
public class CredentialProviderManager
        implements CredentialProviders
{
    private final Set<CredentialProvider> providers;

    @Inject
    public CredentialProviderManager(Set<CredentialProvider> providers)
    {
        this.providers = providers;
    }

    @Override
    public Optional<Credential> getCredential(ConnectorIdentity identity, CredentialRequest request)
    {
        for (CredentialProvider provider : providers) {
            Optional<Credential> result = provider.getCredential(identity, request);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }
}
