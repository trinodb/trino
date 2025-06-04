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

import com.nimbusds.jose.KeyLengthException;
import io.airlift.units.Duration;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jwts;
import io.trino.server.security.oauth2.TokenPairSerializer.TokenPair;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static io.airlift.units.Duration.succinctDuration;
import static io.trino.server.security.oauth2.TokenPairSerializer.TokenPair.withAccessAndRefreshTokens;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJweTokenSerializer
{
    @Test
    public void testSerialization()
            throws Exception
    {
        JweTokenSerializer serializer = tokenSerializer(Clock.systemUTC(), succinctDuration(5, SECONDS), randomEncodedSecret());

        Date expiration = new Calendar.Builder().setDate(2022, 6, 22).build().getTime();
        String serializedTokenPair = serializer.serialize(withAccessAndRefreshTokens("access_token", expiration, "refresh_token"));
        TokenPair deserializedTokenPair = serializer.deserialize(serializedTokenPair);

        assertThat(deserializedTokenPair.accessToken()).isEqualTo("access_token");
        assertThat(deserializedTokenPair.expiration()).isEqualTo(expiration);
        assertThat(deserializedTokenPair.refreshToken()).isEqualTo(Optional.of("refresh_token"));
    }

    @Test
    public void testDeserializationWithWrongSecret()
    {
        assertThatThrownBy(() -> assertRoundTrip(Optional.of(randomEncodedSecret()), Optional.of(randomEncodedSecret())))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("decryption failed: Tag mismatch");

        assertThatThrownBy(() -> assertRoundTrip(Optional.of(randomEncodedSecret(16)), Optional.of(randomEncodedSecret(24))))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("decryption failed: Tag mismatch");

        // This will generate two different secret keys
        assertThatThrownBy(() -> assertRoundTrip(Optional.empty(), Optional.empty()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("decryption failed: Tag mismatch");

        assertThatThrownBy(() -> assertRoundTrip(Optional.empty(), Optional.of(randomEncodedSecret())))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("decryption failed: Tag mismatch");

        assertThatThrownBy(() -> assertRoundTrip(Optional.of(randomEncodedSecret()), Optional.empty()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("decryption failed: Tag mismatch");
    }

    @Test
    public void testSerializationDeserializationRoundTripWithDifferentKeyLengths()
            throws Exception
    {
        for (int keySize : new int[] {16, 24, 32}) {
            String secret = randomEncodedSecret(keySize);
            assertRoundTrip(secret, secret);
        }
    }

    @Test
    public void testSerializationFailsWithWrongKeySize()
    {
        for (int wrongKeySize : new int[] {8, 64, 128}) {
            String tooShortSecret = randomEncodedSecret(wrongKeySize);
            assertThatThrownBy(() -> assertRoundTrip(tooShortSecret, tooShortSecret))
                    .hasStackTraceContaining("Secret key size must be either 16, 24 or 32 bytes but was " + wrongKeySize);
        }
    }

    private void assertRoundTrip(String serializerSecret, String deserializerSecret)
            throws Exception
    {
        assertRoundTrip(Optional.of(serializerSecret), Optional.of(deserializerSecret));
    }

    private void assertRoundTrip(Optional<String> serializerSecret, Optional<String> deserializerSecret)
            throws Exception
    {
        JweTokenSerializer serializer = tokenSerializer(Clock.systemUTC(), succinctDuration(5, SECONDS), serializerSecret);
        JweTokenSerializer deserializer = tokenSerializer(Clock.systemUTC(), succinctDuration(5, SECONDS), deserializerSecret);

        Date expiration = new Calendar.Builder().setDate(2023, 6, 22).build().getTime();

        TokenPair tokenPair = withAccessAndRefreshTokens(randomEncodedSecret(), expiration, randomEncodedSecret());
        assertThat(deserializer.deserialize(serializer.serialize(tokenPair)))
                .isEqualTo(tokenPair);
    }

    @Test
    public void testTokenDeserializationAfterTimeoutButBeforeExpirationExtension()
            throws Exception
    {
        TestingClock clock = new TestingClock();
        JweTokenSerializer serializer = tokenSerializer(
                clock,
                succinctDuration(12, MINUTES),
                randomEncodedSecret());
        Date expiration = new Calendar.Builder().setDate(2022, 6, 22).build().getTime();
        String serializedTokenPair = serializer.serialize(withAccessAndRefreshTokens("access_token", expiration, "refresh_token"));
        clock.advanceBy(succinctDuration(10, MINUTES));
        TokenPair deserializedTokenPair = serializer.deserialize(serializedTokenPair);

        assertThat(deserializedTokenPair.accessToken()).isEqualTo("access_token");
        assertThat(deserializedTokenPair.expiration()).isEqualTo(expiration);
        assertThat(deserializedTokenPair.refreshToken()).isEqualTo(Optional.of("refresh_token"));
    }

    @Test
    public void testTokenDeserializationAfterTimeoutAndExpirationExtension()
            throws Exception
    {
        TestingClock clock = new TestingClock();

        JweTokenSerializer serializer = tokenSerializer(
                clock,
                succinctDuration(12, MINUTES),
                randomEncodedSecret());
        Date expiration = new Calendar.Builder().setDate(2022, 6, 22).build().getTime();
        String serializedTokenPair = serializer.serialize(withAccessAndRefreshTokens("access_token", expiration, "refresh_token"));

        clock.advanceBy(succinctDuration(20, MINUTES));
        assertThatThrownBy(() -> serializer.deserialize(serializedTokenPair))
                .isExactlyInstanceOf(ExpiredJwtException.class);
    }

    @Test
    public void testTokenDeserializationWhenNonJWETokenIsPassed()
            throws Exception
    {
        JweTokenSerializer serializer = tokenSerializer(new TestingClock(), succinctDuration(12, MINUTES), randomEncodedSecret());
        String nonJWEToken = "non_jwe_token";

        TokenPair tokenPair = serializer.deserialize(nonJWEToken);

        assertThat(tokenPair.accessToken()).isEqualTo(nonJWEToken);
        assertThat(tokenPair.refreshToken()).isEmpty();
    }

    private JweTokenSerializer tokenSerializer(Clock clock, Duration tokenExpiration, String encodedSecretKey)
            throws GeneralSecurityException, KeyLengthException
    {
        return tokenSerializer(clock, tokenExpiration, Optional.of(encodedSecretKey));
    }

    private JweTokenSerializer tokenSerializer(Clock clock, Duration tokenExpiration, Optional<String> secretKey)
            throws GeneralSecurityException, KeyLengthException
    {
        RefreshTokensConfig refreshTokensConfig = new RefreshTokensConfig();
        secretKey.ifPresent(refreshTokensConfig::setSecretKey);
        return new JweTokenSerializer(
                refreshTokensConfig,
                new Oauth2ClientStub(),
                "trino_coordinator_test_version",
                "trino_coordinator",
                "sub",
                clock,
                tokenExpiration);
    }

    static class Oauth2ClientStub
            implements OAuth2Client
    {
        private final Map<String, Object> claims = Jwts.claims()
                .subject("user")
                .build();

        @Override
        public void load() {}

        @Override
        public Request createAuthorizationRequest(String state, URI callbackUri)
        {
            throw new UnsupportedOperationException("operation is not yet supported");
        }

        @Override
        public Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
        {
            throw new UnsupportedOperationException("operation is not yet supported");
        }

        @Override
        public Optional<Map<String, Object>> getClaims(String accessToken)
        {
            return Optional.of(claims);
        }

        @Override
        public Response refreshTokens(String refreshToken)
        {
            throw new UnsupportedOperationException("operation is not yet supported");
        }

        @Override
        public Optional<URI> getLogoutEndpoint(Optional<String> idToken, URI callbackUrl)
        {
            return Optional.empty();
        }
    }

    private static class TestingClock
            extends Clock
    {
        private Instant currentTime = ZonedDateTime.of(2022, 5, 6, 10, 15, 0, 0, ZoneId.systemDefault()).toInstant();

        @Override
        public ZoneId getZone()
        {
            return ZoneId.systemDefault();
        }

        @Override
        public Clock withZone(ZoneId zone)
        {
            return this;
        }

        @Override
        public Instant instant()
        {
            return currentTime;
        }

        public void advanceBy(Duration currentTimeDelta)
        {
            this.currentTime = currentTime.plus(currentTimeDelta.toMillis(), MILLIS);
        }
    }

    private static String randomEncodedSecret()
    {
        return randomEncodedSecret(24);
    }

    private static String randomEncodedSecret(int length)
    {
        Random random = new SecureRandom();
        final byte[] buffer = new byte[length];
        random.nextBytes(buffer);
        return Base64.getEncoder().encodeToString(buffer);
    }
}
