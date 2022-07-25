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
package io.trino.server.security.jwt;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.SigningKeyResolver;
import io.jsonwebtoken.security.SecurityException;

import java.security.Key;

import static java.util.Objects.requireNonNull;

public class JwkSigningKeyResolver
        implements SigningKeyResolver
{
    private final JwkService keys;

    public JwkSigningKeyResolver(JwkService keys)
    {
        this.keys = requireNonNull(keys, "keys is null");
    }

    @Override
    public Key resolveSigningKey(JwsHeader header, Claims claims)
    {
        return getKey(header);
    }

    @Override
    public Key resolveSigningKey(JwsHeader header, String plaintext)
    {
        return getKey(header);
    }

    private Key getKey(JwsHeader<?> header)
    {
        String keyId = header.getKeyId();
        if (keyId == null) {
            throw new SecurityException("Key ID is required");
        }
        return keys.getKey(keyId)
                .orElseThrow(() -> new SecurityException("Unknown signing key ID"));
    }
}
