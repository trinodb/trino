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
package io.trino.plugin.credential.apikey;

import com.google.inject.Inject;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.credential.BearerTokenCredential;
import io.trino.spi.security.credential.Credential;
import io.trino.spi.security.credential.CredentialProvider;
import io.trino.spi.security.credential.StringCredential;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

public class ApiKeyCredentialProvider
        implements CredentialProvider
{
    private final String apiKey;
    private BearerTokenCredential bearerTokenCredential;
    private StringCredential stringCredential;

    @Inject
    public ApiKeyCredentialProvider(ApiKeyCredentialProviderConfig config)
    {
        this.apiKey = requireNonNull(config.getApiKey(), "apiKey is null");
    }

    @Override
    public <T extends Credential> T getCredential(ConnectorIdentity identity, Class<T> type)
    {
        assertSupportedTypes(type, BearerTokenCredential.class, StringCredential.class);

        if (type.isAssignableFrom(BearerTokenCredential.class)) {
            if (bearerTokenCredential == null) {
                bearerTokenCredential = new BearerTokenCredential(apiKey, Instant.MAX);
            }
            return (T) bearerTokenCredential;
        }

        if (stringCredential == null) {
            stringCredential = new StringCredential(apiKey);
        }
        return (T) stringCredential;
    }
}
