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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.time.Instant;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TestOidcStsCredentialExchanger
{
    // --- extractClaim ---

    @Test
    void testExtractClaim_returnsValue()
    {
        String jwt = makeJwt("{\"sub\":\"user1\",\"exp\":9999999999}");
        assertThat(OidcStsCredentialExchanger.extractClaim(jwt, "sub")).isEqualTo("user1");
        assertThat(OidcStsCredentialExchanger.extractClaim(jwt, "exp")).isEqualTo("9999999999");
    }

    @Test
    void testExtractClaim_missingClaim_returnsNull()
    {
        String jwt = makeJwt("{\"sub\":\"user1\"}");
        assertThat(OidcStsCredentialExchanger.extractClaim(jwt, "exp")).isNull();
    }

    @Test
    void testExtractClaim_malformedJwt_returnsNull()
    {
        assertThat(OidcStsCredentialExchanger.extractClaim("notajwt", "sub")).isNull();
        assertThat(OidcStsCredentialExchanger.extractClaim("", "sub")).isNull();
    }

    // --- computeDuration ---

    @Test
    void testComputeDuration_usesExpClaim()
    {
        long futureExp = Instant.now().getEpochSecond() + 7200;
        String jwt = makeJwt("{\"sub\":\"u\",\"exp\":" + futureExp + "}");
        int duration = OidcStsCredentialExchanger.computeDuration(jwt);
        // Allow ±5 s for test execution time
        assertThat(duration).isBetween(7195, 7205);
    }

    @Test
    void testComputeDuration_expiredToken_fallsBackToMinimum()
    {
        long pastExp = Instant.now().getEpochSecond() - 3600;
        String jwt = makeJwt("{\"sub\":\"u\",\"exp\":" + pastExp + "}");
        assertThat(OidcStsCredentialExchanger.computeDuration(jwt)).isEqualTo(900);
    }

    @Test
    void testComputeDuration_noExpClaim_fallsBackToMinimum()
    {
        String jwt = makeJwt("{\"sub\":\"u\"}");
        assertThat(OidcStsCredentialExchanger.computeDuration(jwt)).isEqualTo(900);
    }

    @Test
    void testComputeDuration_malformedJwt_fallsBackToMinimum()
    {
        assertThat(OidcStsCredentialExchanger.computeDuration("notajwt")).isEqualTo(900);
    }

    // --- getCredentials caching ---

    @Test
    void testGetCredentials_cachesOnSub()
    {
        Instant farFuture = Instant.now().plusSeconds(7200);
        AwsSessionCredentials creds = AwsSessionCredentials.create("AK", "SK", "ST");
        CountingExchanger exchanger = new CountingExchanger(creds, farFuture);

        String jwt = makeJwt("{\"sub\":\"alice\",\"exp\":" + farFuture.getEpochSecond() + "}");

        AwsSessionCredentials first = exchanger.getCredentials(jwt);
        AwsSessionCredentials second = exchanger.getCredentials(jwt);

        assertThat(first).isSameAs(second);
        assertThat(exchanger.exchangeCallCount).isEqualTo(1);
    }

    @Test
    void testGetCredentials_differentSubsCachedSeparately()
    {
        Instant farFuture = Instant.now().plusSeconds(7200);
        AwsSessionCredentials creds = AwsSessionCredentials.create("AK", "SK", "ST");
        CountingExchanger exchanger = new CountingExchanger(creds, farFuture);

        String jwtAlice = makeJwt("{\"sub\":\"alice\",\"exp\":" + farFuture.getEpochSecond() + "}");
        String jwtBob = makeJwt("{\"sub\":\"bob\",\"exp\":" + farFuture.getEpochSecond() + "}");

        exchanger.getCredentials(jwtAlice);
        exchanger.getCredentials(jwtBob);
        exchanger.getCredentials(jwtAlice);

        assertThat(exchanger.exchangeCallCount).isEqualTo(2);
    }

    @Test
    void testGetCredentials_refreshesWhenNearExpiry()
    {
        Instant nearExpiry = Instant.now().plusSeconds(30);
        AwsSessionCredentials creds = AwsSessionCredentials.create("AK", "SK", "ST");
        CountingExchanger exchanger = new CountingExchanger(creds, nearExpiry);

        String jwt = makeJwt("{\"sub\":\"alice\",\"exp\":" + nearExpiry.getEpochSecond() + "}");

        exchanger.getCredentials(jwt);
        exchanger.getCredentials(jwt);

        assertThat(exchanger.exchangeCallCount).isEqualTo(2);
    }

    // --- helpers ---

    private static String makeJwt(String payloadJson)
    {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String header = enc.encodeToString("{\"alg\":\"none\"}".getBytes(UTF_8));
        String payload = enc.encodeToString(payloadJson.getBytes(UTF_8));
        return header + "." + payload + ".fakesig";
    }

    private static final class CountingExchanger
            extends OidcStsCredentialExchanger
    {
        int exchangeCallCount;
        private final AwsSessionCredentials credentials;
        private final Instant expiresAt;

        CountingExchanger(AwsSessionCredentials credentials, Instant expiresAt)
        {
            super(null, "test-role");
            this.credentials = credentials;
            this.expiresAt = expiresAt;
        }

        @Override
        CachedCredentials exchange(String oidcToken, String sub)
        {
            exchangeCallCount++;
            return new CachedCredentials(credentials, expiresAt);
        }
    }
}
