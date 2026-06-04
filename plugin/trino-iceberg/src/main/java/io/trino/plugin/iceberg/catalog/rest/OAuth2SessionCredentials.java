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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.trino.spi.security.ConnectorIdentity;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.security.ExtraCredentials.authenticatedExtraCredentialName;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.rest.auth.OAuth2Properties.CREDENTIAL;
import static org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN;

final class OAuth2SessionCredentials
{
    private static final List<String> OAUTH2_SESSION_CREDENTIALS = List.of(TOKEN, CREDENTIAL);

    private OAuth2SessionCredentials() {}

    static Map<String, String> fromIdentity(ConnectorIdentity identity)
    {
        requireNonNull(identity, "identity is null");
        return fromExtraCredentials(identity.getExtraCredentials());
    }

    static Map<String, String> fromExtraCredentials(Map<String, String> extraCredentials)
    {
        requireNonNull(extraCredentials, "extraCredentials is null");

        ImmutableMap.Builder<String, String> credentials = ImmutableMap.builder();
        for (String credentialName : OAUTH2_SESSION_CREDENTIALS) {
            String authenticatedCredentialName = authenticatedExtraCredentialName(credentialName);
            if (extraCredentials.containsKey(authenticatedCredentialName)) {
                credentials.put(credentialName, extraCredentials.get(authenticatedCredentialName));
            }
        }
        return credentials.buildOrThrow();
    }

    static Optional<String> cacheKey(ConnectorIdentity identity)
    {
        requireNonNull(identity, "identity is null");

        Hasher hasher = Hashing.murmur3_128().newHasher();
        boolean hasAny = false;
        for (String credentialName : OAUTH2_SESSION_CREDENTIALS) {
            String value = identity.getExtraCredentials().get(authenticatedExtraCredentialName(credentialName));
            if (value != null) {
                hasher.putUnencodedChars(credentialName);
                hasher.putByte((byte) 0);
                hasher.putUnencodedChars(value);
                hasher.putByte((byte) 0);
                hasAny = true;
            }
        }
        return hasAny ? Optional.of(hasher.hash().toString()) : Optional.empty();
    }

    static Map<String, String> catalogPropertiesWithSessionCredentials(
            Map<String, String> catalogProperties,
            Map<String, String> sessionCredentials)
    {
        requireNonNull(catalogProperties, "catalogProperties is null");
        requireNonNull(sessionCredentials, "sessionCredentials is null");

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.putAll(catalogProperties);
        sessionCredentials.forEach((key, value) -> {
            if (!catalogProperties.containsKey(key)) {
                properties.put(key, value);
            }
        });
        return properties.buildOrThrow();
    }
}
