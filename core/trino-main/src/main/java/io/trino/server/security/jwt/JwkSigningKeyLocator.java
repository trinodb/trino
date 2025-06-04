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

import io.jsonwebtoken.Header;
import io.jsonwebtoken.JweHeader;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.Locator;
import io.jsonwebtoken.UnsupportedJwtException;
import io.jsonwebtoken.security.SecurityException;

import java.security.Key;

import static java.util.Objects.requireNonNull;

public class JwkSigningKeyLocator
        implements Locator<Key>
{
    private final JwkService keys;

    public JwkSigningKeyLocator(JwkService keys)
    {
        this.keys = requireNonNull(keys, "keys is null");
    }

    @Override
    public Key locate(Header header)
    {
        return switch (header) {
            case JwsHeader jwsHeader -> getKey(jwsHeader.getKeyId());
            case JweHeader jweHeader -> getKey(jweHeader.getKeyId());
            default -> throw new UnsupportedJwtException("Cannot locate key for header: %s".formatted(header.getType()));
        };
    }

    private Key getKey(String keyId)
    {
        if (keyId == null) {
            throw new SecurityException("Key ID is required");
        }
        return keys.getKey(keyId)
                .orElseThrow(() -> new SecurityException("Unknown signing key ID"));
    }
}
