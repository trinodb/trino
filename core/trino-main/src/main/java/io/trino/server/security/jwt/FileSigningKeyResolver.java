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
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SigningKeyResolver;
import io.jsonwebtoken.UnsupportedJwtException;
import io.jsonwebtoken.security.SecurityException;

import javax.crypto.spec.SecretKeySpec;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.CharMatcher.inRange;
import static com.google.common.io.Files.asCharSource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Base64.getMimeDecoder;
import static java.util.Objects.requireNonNull;

public class FileSigningKeyResolver
        implements SigningKeyResolver
{
    private static final String DEFAULT_KEY = "default-key";
    private static final CharMatcher INVALID_KID_CHARS = inRange('a', 'z').or(inRange('A', 'Z')).or(inRange('0', '9')).or(CharMatcher.anyOf("_-")).negate();
    private static final String KEY_ID_VARIABLE = "${KID}";

    private final String keyFile;
    private final LoadedKey staticKey;
    private final ConcurrentMap<String, LoadedKey> keys = new ConcurrentHashMap<>();

    @Inject
    public FileSigningKeyResolver(JwtAuthenticatorConfig config)
    {
        this(config.getKeyFile());
    }

    public FileSigningKeyResolver(String keyFile)
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
        SignatureAlgorithm algorithm = SignatureAlgorithm.forName(header.getAlgorithm());

        if (staticKey != null) {
            return staticKey.getKey(algorithm);
        }

        String keyId = getKeyId(header);
        LoadedKey key = keys.computeIfAbsent(keyId, this::loadKey);
        return key.getKey(algorithm);
    }

    private static String getKeyId(JwsHeader<?> header)
    {
        String keyId = header.getKeyId();
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

    public static LoadedKey loadKeyFile(File file)
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
            byte[] rawKey = getMimeDecoder().decode(data.getBytes(US_ASCII));
            return new LoadedKey(rawKey);
        }
        catch (RuntimeException e) {
            throw new SecurityException("Unable to decode HMAC signing key", e);
        }
    }

    private static class LoadedKey
    {
        private final Key publicKey;
        private final byte[] hmacKey;

        public LoadedKey(Key publicKey)
        {
            this.publicKey = requireNonNull(publicKey, "publicKey is null");
            this.hmacKey = null;
        }

        public LoadedKey(byte[] hmacKey)
        {
            this.hmacKey = requireNonNull(hmacKey, "hmacKey is null");
            this.publicKey = null;
        }

        public Key getKey(SignatureAlgorithm algorithm)
        {
            if (algorithm.isHmac()) {
                if (hmacKey == null) {
                    throw new UnsupportedJwtException(format("JWT is signed with %s, but no HMAC key is configured", algorithm));
                }
                return new SecretKeySpec(hmacKey, algorithm.getJcaName());
            }

            if (publicKey == null) {
                throw new UnsupportedJwtException(format("JWT is signed with %s, but no key is configured", algorithm));
            }
            return publicKey;
        }
    }
}
