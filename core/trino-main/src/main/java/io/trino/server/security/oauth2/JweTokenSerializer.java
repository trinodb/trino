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
package io.trino.server.security.oauth2;

import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.KeyLengthException;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.AESDecrypter;
import com.nimbusds.jose.crypto.AESEncrypter;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.io.CompressionAlgorithm;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.time.Clock;
import java.util.Date;
import java.util.Map;

import static io.trino.server.security.jwt.JwtUtil.newJwtBuilder;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JweTokenSerializer
        implements TokenPairSerializer
{
    private static final CompressionAlgorithm COMPRESSION_ALGORITHM = new ZstdCodec();

    private static final Logger LOG = Logger.get(JweTokenSerializer.class);
    private static final String ACCESS_TOKEN_KEY = "access_token";
    private static final String EXPIRATION_TIME_KEY = "expiration_time";
    private static final String REFRESH_TOKEN_KEY = "refresh_token";
    private final JweEncryptedSerializer jweSerializer;
    private final OAuth2Client client;
    private final Clock clock;
    private final String issuer;
    private final String audience;
    private final Duration tokenExpiration;
    private final JwtParser parser;
    private final String principalField;

    public JweTokenSerializer(
            RefreshTokensConfig config,
            OAuth2Client client,
            String issuer,
            String audience,
            String principalField,
            Clock clock,
            Duration tokenExpiration)
    {
        this.jweSerializer = new JweEncryptedSerializer(getOrGenerateKey(config));
        this.client = requireNonNull(client, "client is null");
        this.issuer = requireNonNull(issuer, "issuer is null");
        this.principalField = requireNonNull(principalField, "principalField is null");
        this.audience = requireNonNull(audience, "issuer is null");
        this.clock = requireNonNull(clock, "clock is null");
        this.tokenExpiration = requireNonNull(tokenExpiration, "tokenExpiration is null");

        this.parser = newJwtParserBuilder()
                .clock(() -> Date.from(clock.instant()))
                .requireIssuer(this.issuer)
                .requireAudience(this.audience)
                .zip()
                .add(COMPRESSION_ALGORITHM)
                .and()
                .unsecuredDecompression()
                .unsecured()
                .build();
    }

    @Override
    public TokenPair deserialize(String token)
    {
        requireNonNull(token, "token is null");

        try {
            Claims claims = parser.parseUnsecuredClaims(jweSerializer.deserialize(token)).getPayload();
            return TokenPair.withAccessAndRefreshTokens(
                    claims.get(ACCESS_TOKEN_KEY, String.class),
                    claims.get(EXPIRATION_TIME_KEY, Date.class),
                    claims.get(REFRESH_TOKEN_KEY, String.class));
        }
        catch (ParseException ex) {
            return TokenPair.withAccessToken(token);
        }
    }

    @Override
    public String serialize(TokenPair tokenPair)
    {
        requireNonNull(tokenPair, "tokenPair is null");

        Map<String, Object> claims = client.getClaims(tokenPair.accessToken()).orElseThrow(() -> new IllegalArgumentException("Claims are missing"));
        if (!claims.containsKey(principalField)) {
            throw new IllegalArgumentException(format("%s field is missing", principalField));
        }
        JwtBuilder jwt = newJwtBuilder()
                .expiration(Date.from(clock.instant().plusMillis(tokenExpiration.toMillis())))
                .claim(principalField, claims.get(principalField).toString())
                .audience().add(audience).and()
                .issuer(issuer)
                .claim(ACCESS_TOKEN_KEY, tokenPair.accessToken())
                .claim(EXPIRATION_TIME_KEY, tokenPair.expiration())
                .compressWith(COMPRESSION_ALGORITHM);

        if (tokenPair.refreshToken().isPresent()) {
            jwt.claim(REFRESH_TOKEN_KEY, tokenPair.refreshToken().orElseThrow());
        }
        else {
            LOG.info("No refresh token has been issued, although coordinator expects one. Please check your IdP whether that is correct behaviour");
        }
        return jweSerializer.serialize(jwt.compact());
    }

    private static SecretKey getOrGenerateKey(RefreshTokensConfig config)
    {
        SecretKey signingKey = config.getSecretKey();
        if (signingKey == null) {
            try {
                KeyGenerator generator = KeyGenerator.getInstance("AES");
                generator.init(256);
                return generator.generateKey();
            }
            catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }
        return signingKey;
    }

    private static class JweEncryptedSerializer
    {
        private final AESEncrypter jweEncrypter;
        private final AESDecrypter jweDecrypter;
        private final JWEHeader encryptionHeader;

        private JweEncryptedSerializer(SecretKey secretKey)
        {
            try {
                this.encryptionHeader = createEncryptionHeader(secretKey);
                this.jweEncrypter = new AESEncrypter(secretKey);
                this.jweDecrypter = new AESDecrypter(secretKey);
            }
            catch (KeyLengthException e) {
                throw new RuntimeException(e);
            }
        }

        private JWEHeader createEncryptionHeader(SecretKey key)
        {
            int keyLength = key.getEncoded().length;
            return switch (keyLength) {
                case 16 -> new JWEHeader(JWEAlgorithm.A128GCMKW, EncryptionMethod.A128GCM);
                case 24 -> new JWEHeader(JWEAlgorithm.A192GCMKW, EncryptionMethod.A192GCM);
                case 32 -> new JWEHeader(JWEAlgorithm.A256GCMKW, EncryptionMethod.A256GCM);
                default -> throw new IllegalArgumentException("Secret key size must be either 16, 24 or 32 bytes but was %d".formatted(keyLength));
            };
        }

        private String serialize(String payload)
        {
            try {
                JWEObject jwe = new JWEObject(encryptionHeader, new Payload(payload));
                jwe.encrypt(jweEncrypter);
                return jwe.serialize();
            }
            catch (JOSEException e) {
                throw new RuntimeException(e);
            }
        }

        private String deserialize(String token)
                throws ParseException
        {
            try {
                JWEObject jwe = JWEObject.parse(token);
                jwe.decrypt(jweDecrypter);
                return jwe.getPayload().toString();
            }
            catch (JOSEException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
