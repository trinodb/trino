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
package io.trino.plugin.base.security.credential;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.credential.Credential;
import io.trino.spi.security.credential.CredentialProvider;
import io.trino.spi.security.credential.CredentialResolver;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.util.Objects.requireNonNull;

public class LazyCredentialProvider
        implements CredentialProvider
{
    private final CredentialResolver credentialResolver;
    private final String name;
    private CredentialProvider credentialProvider;

    @Inject
    public LazyCredentialProvider(CredentialResolver credentialResolver, String name)
    {
        this.credentialResolver = requireNonNull(credentialResolver, "credentialResolver is null");
        this.name = requireNonNull(name, "name is null");
    }

    @Override
    public <T extends Credential> T getCredential(ConnectorIdentity identity, Class<T> type)
    {
        if (credentialProvider == null) {
            credentialProvider = credentialResolver.get(name).orElseThrow(() -> new TrinoException(CONFIGURATION_INVALID, "Credential provider %s not configured".formatted(name)));
        }
        return credentialProvider.getCredential(identity, type);
    }
}
