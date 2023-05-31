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
package io.trino.plugin.bigquery;

import io.trino.spi.connector.ConnectorSession;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public interface IdentityCacheMapping
{
    IdentityCacheKey getRemoteUserCacheKey(ConnectorSession session);

    /**
     * This will be used as cache key for creating BigQuery service. If {@link ConnectorSession} content can influence
     * service creation we should chavereate {@link IdentityCacheKey} instance so
     * we could cache proper service for given {@link ConnectorSession}.
     */
    abstract class IdentityCacheKey
    {
        @Override
        public abstract int hashCode();

        @Override
        public abstract boolean equals(Object obj);
    }

    final class SingletonIdentityCacheMapping
            implements IdentityCacheMapping
    {
        @Override
        public IdentityCacheKey getRemoteUserCacheKey(ConnectorSession session)
        {
            return SingletonIdentityCacheKey.INSTANCE;
        }

        private static final class SingletonIdentityCacheKey
                extends IdentityCacheKey
        {
            private static final SingletonIdentityCacheKey INSTANCE = new SingletonIdentityCacheKey();

            @Override
            public int hashCode()
            {
                return getClass().hashCode();
            }

            @Override
            public boolean equals(Object obj)
            {
                return obj instanceof SingletonIdentityCacheKey;
            }
        }
    }

    final class ExtraCredentialsBasedIdentityCacheMapping
            implements IdentityCacheMapping
    {
        private final MessageDigest sha256;

        public ExtraCredentialsBasedIdentityCacheMapping()
        {
            try {
                sha256 = MessageDigest.getInstance("SHA-256");
            }
            catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public IdentityCacheKey getRemoteUserCacheKey(ConnectorSession session)
        {
            Map<String, String> extraCredentials = session.getIdentity().getExtraCredentials();
            byte[] userHash = hash(session.getUser());
            String token = extraCredentials.get(CredentialsConfig.OAUTH_TOKEN_KEY);
            byte[] tokenHash = token == null ? new byte[0] : hash(token);
            return new ExtraCredentialsBasedIdentityCacheKey(userHash, tokenHash);
        }

        private byte[] hash(String value)
        {
            return sha256.digest(value.getBytes(UTF_8));
        }

        private static final class ExtraCredentialsBasedIdentityCacheKey
                extends IdentityCacheKey
        {
            private final byte[] userHash;
            private final byte[] tokenHash;

            public ExtraCredentialsBasedIdentityCacheKey(byte[] userHash, byte[] tokenHash)
            {
                this.userHash = userHash;
                this.tokenHash = tokenHash;
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
                return Arrays.equals(userHash, that.userHash) && Arrays.equals(tokenHash, that.tokenHash);
            }

            @Override
            public int hashCode()
            {
                int result = Arrays.hashCode(userHash);
                result = 31 * result + Arrays.hashCode(tokenHash);
                return result;
            }
        }
    }
}
