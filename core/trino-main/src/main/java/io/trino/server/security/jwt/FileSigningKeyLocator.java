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

import com.google.common.base.CharMatcher;
import com.google.inject.Inject;
import io.airlift.security.pem.PemReader;
import io.jsonwebtoken.Header;
import io.jsonwebtoken.JweHeader;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.Locator;
import io.jsonwebtoken.UnsupportedJwtException;
import io.jsonwebtoken.security.MacAlgorithm;
import io.jsonwebtoken.security.SecureDigestAlgorithm;
import io.jsonwebtoken.security.SecurityException;

import javax.crypto.SecretKey;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.PublicKey;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.CharMatcher.inRange;
import static com.google.common.io.Files.asCharSource;
import static io.jsonwebtoken.security.Keys.hmacShaKeyFor;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Base64.getMimeDecoder;
import static java.util.Objects.requireNonNull;

public class FileSigningKeyLocator
        implements Locator<Key>
{
    private static final String DEFAULT_KEY = "default-key";
    private static final CharMatcher INVALID_KID_CHARS = inRange('a', 'z').or(inRange('A', 'Z')).or(inRange('0', '9')).or(CharMatcher.anyOf("_-")).negate();
    private static final String KEY_ID_VARIABLE = "${KID}";

    private final String keyFile;
    private final LoadedKey staticKey;
    private final ConcurrentMap<String, LoadedKey> keys = new ConcurrentHashMap<>();

    @Inject
    public FileSigningKeyLocator(JwtAuthenticatorConfig config)
    {
        this(config.getKeyFile());
    }

    public FileSigningKeyLocator(String keyFile)
    {
        this.keyFile = requireNonNull(keyFile, "keyFile is null");
        if (keyFile.contains(KEY_ID_VARIABLE)) {
            this.staticKey = null;
        }
        else {
            this.staticKey = loadKeyFile(new File(keyFile));
        }
    }

    @Override
    public Key locate(Header header)
    {
        return switch (header) {
            case JwsHeader jwsHeader -> getKey(jwsHeader.getKeyId(), jwsHeader.getAlgorithm());
            case JweHeader jweHeader -> getKey(jweHeader.getKeyId(), jweHeader.getAlgorithm());
            default -> throw new UnsupportedJwtException("Cannot locate key for header: %s".formatted(header.getType()));
        };
    }

    private Key getKey(String keyId, String algorithm)
    {
        SecureDigestAlgorithm<?, ?> secureDigestAlgorithm = Jwts.SIG.get().forKey(algorithm);
        if (staticKey != null) {
            return staticKey.getKey(secureDigestAlgorithm);
        }

        LoadedKey key = keys.computeIfAbsent(getKeyId(keyId), this::loadKey);
        return key.getKey(secureDigestAlgorithm);
    }

    private static String getKeyId(String keyId)
    {
        if (keyId == null) {
            // allow for migration from system not using kid
            return DEFAULT_KEY;
        }
        keyId = INVALID_KID_CHARS.replaceFrom(keyId, '_');
        return keyId;
    }

    private LoadedKey loadKey(String keyId)
    {
        return loadKeyFile(new File(keyFile.replace(KEY_ID_VARIABLE, keyId)));
    }

    private static LoadedKey loadKeyFile(File file)
    {
        if (!file.canRead()) {
            throw new SecurityException("Unknown signing key ID");
        }

        String data;
        try {
            data = asCharSource(file, US_ASCII).read();
        }
        catch (IOException e) {
            throw new SecurityException("Unable to read signing key", e);
        }

        // try to load the key as a PEM encoded public key
        if (PemReader.isPem(data)) {
            try {
                return new LoadedKey(PemReader.loadPublicKey(data));
            }
            catch (RuntimeException | GeneralSecurityException e) {
                throw new SecurityException("Unable to decode PEM signing key id", e);
            }
        }

        // try to load the key as a base64 encoded HMAC key
        try {
            SecretKey hmacKey = hmacShaKeyFor(getMimeDecoder().decode(data.getBytes(US_ASCII)));
            return new LoadedKey(hmacKey);
        }
        catch (RuntimeException e) {
            throw new SecurityException("Unable to decode HMAC signing key", e);
        }
    }

    private static class LoadedKey
    {
        private final PublicKey publicKey;
        private final SecretKey secretKey;

        public LoadedKey(PublicKey publicKey)
        {
            this.publicKey = requireNonNull(publicKey, "publicKey is null");
            this.secretKey = null;
        }

        public LoadedKey(SecretKey secretKey)
        {
            this.secretKey = requireNonNull(secretKey, "secretKey is null");
            this.publicKey = null;
        }

        public Key getKey(SecureDigestAlgorithm<?, ?> algorithm)
        {
            if (algorithm instanceof MacAlgorithm) {
                if (secretKey == null) {
                    throw new UnsupportedJwtException(format("JWT is signed with %s, but no HMAC key is configured", algorithm));
                }
                return secretKey;
            }

            if (publicKey == null) {
                throw new UnsupportedJwtException(format("JWT is signed with %s, but no key is configured", algorithm));
            }
            return publicKey;
        }
    }
}
