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
package io.prestosql.server.security.oauth2;

import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.UrlJwkProvider;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.SigningKeyResolver;

import java.security.Key;
import java.security.PublicKey;

import static java.util.Objects.requireNonNull;

class JWKSSigningKeyResolver
        implements SigningKeyResolver
{
    private final String idpUrl;

    JWKSSigningKeyResolver(String idpUrl)
    {
        this.idpUrl = requireNonNull(idpUrl, "idpUrl is null");
    }

    @Override
    public Key resolveSigningKey(JwsHeader header, Claims claims)
    {
        return getPublicKey(header.getKeyId());
    }

    @Override
    public Key resolveSigningKey(JwsHeader header, String plaintext)
    {
        return getPublicKey(header.getKeyId());
    }

    private PublicKey getPublicKey(String keyId)
    {
        JwkProvider provider = new UrlJwkProvider(idpUrl);
        try {
            Jwk jwk = provider.get(keyId);
            return jwk.getPublicKey();
        }
        catch (JwkException e) {
            throw new UncheckedJwkException(e);
        }
    }

    static class UncheckedJwkException
            extends RuntimeException
    {
        public UncheckedJwkException(JwkException cause)
        {
            super(cause);
        }
    }
}
