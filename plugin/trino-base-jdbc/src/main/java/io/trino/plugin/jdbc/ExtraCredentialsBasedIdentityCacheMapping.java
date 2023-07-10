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
package io.trino.plugin.jdbc;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.credential.ExtraCredentialConfig;
import io.trino.spi.connector.ConnectorSession;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ExtraCredentialsBasedIdentityCacheMapping
        implements IdentityCacheMapping
{
    private final MessageDigest sha256;
    private final Optional<String> userCredentialName;
    private final Optional<String> passwordCredentialName;

    @Inject
    public ExtraCredentialsBasedIdentityCacheMapping(ExtraCredentialConfig config)
    {
        try {
            sha256 = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        userCredentialName = config.getUserCredentialName();
        passwordCredentialName = config.getPasswordCredentialName();
    }

    @Override
    public IdentityCacheKey getRemoteUserCacheKey(ConnectorSession session)
    {
        Map<String, String> extraCredentials = session.getIdentity().getExtraCredentials();
        return new ExtraCredentialsBasedIdentityCacheKey(
                userCredentialName.map(extraCredentials::get)
                        .map(this::hash),
                passwordCredentialName.map(extraCredentials::get)
                        .map(this::hash));
    }

    private byte[] hash(String value)
    {
        return sha256.digest(value.getBytes(UTF_8));
    }

    private static final class ExtraCredentialsBasedIdentityCacheKey
            extends IdentityCacheKey
    {
        private static final byte[] EMPTY_BYTES = new byte[0];
        private final byte[] userHash;
        private final byte[] passwordHash;

        public ExtraCredentialsBasedIdentityCacheKey(Optional<byte[]> userHash, Optional<byte[]> passwordHash)
        {
            this.userHash = userHash.orElse(EMPTY_BYTES);
            this.passwordHash = passwordHash.orElse(EMPTY_BYTES);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ExtraCredentialsBasedIdentityCacheKey that = (ExtraCredentialsBasedIdentityCacheKey) o;
            return Arrays.equals(userHash, that.userHash) && Arrays.equals(passwordHash, that.passwordHash);
        }

        @Override
        public int hashCode()
        {
            int result = Arrays.hashCode(userHash);
            result = 31 * result + Arrays.hashCode(passwordHash);
            return result;
        }
    }
}
