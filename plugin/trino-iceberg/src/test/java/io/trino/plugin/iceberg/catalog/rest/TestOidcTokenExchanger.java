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

import java.net.URI;
import java.time.Instant;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TestOidcTokenExchanger
{
    // --- getToken caching ---

    @Test
    void testGetToken_cachesOnSub()
    {
        Instant farFuture = Instant.now().plusSeconds(7200);
        FakeExchanger exchanger = new FakeExchanger("exchanged-token", farFuture);

        String jwt = makeJwt("{\"sub\":\"alice\",\"exp\":" + farFuture.getEpochSecond() + "}");

        String first = exchanger.getToken(jwt);
        String second = exchanger.getToken(jwt);

        assertThat(first).isEqualTo("exchanged-token");
        assertThat(first).isSameAs(second);
        assertThat(exchanger.callCount).isEqualTo(1);
    }

    @Test
    void testGetToken_differentSubsCachedSeparately()
    {
        Instant farFuture = Instant.now().plusSeconds(7200);
        FakeExchanger exchanger = new FakeExchanger("exchanged-token", farFuture);

        String jwtAlice = makeJwt("{\"sub\":\"alice\",\"exp\":" + farFuture.getEpochSecond() + "}");
        String jwtBob = makeJwt("{\"sub\":\"bob\",\"exp\":" + farFuture.getEpochSecond() + "}");

        exchanger.getToken(jwtAlice);
        exchanger.getToken(jwtBob);
        exchanger.getToken(jwtAlice);

        assertThat(exchanger.callCount).isEqualTo(2);
    }

    @Test
    void testGetToken_refreshesWhenNearExpiry()
    {
        Instant nearExpiry = Instant.now().plusSeconds(30);
        FakeExchanger exchanger = new FakeExchanger("exchanged-token", nearExpiry);

        String jwt = makeJwt("{\"sub\":\"alice\",\"exp\":" + nearExpiry.getEpochSecond() + "}");

        exchanger.getToken(jwt);
        exchanger.getToken(jwt);

        assertThat(exchanger.callCount).isEqualTo(2);
    }

    // --- helpers ---

    private static String makeJwt(String payloadJson)
    {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String header = enc.encodeToString("{\"alg\":\"none\"}".getBytes(UTF_8));
        String payload = enc.encodeToString(payloadJson.getBytes(UTF_8));
        return header + "." + payload + ".fakesig";
    }

    private static final class FakeExchanger
            extends OidcTokenExchanger
    {
        int callCount;
        private final String token;
        private final Instant expiresAt;

        FakeExchanger(String token, Instant expiresAt)
        {
            super(URI.create("http://fake-endpoint"), "client-id", "client-secret", "api://minio/storage.access");
            this.token = token;
            this.expiresAt = expiresAt;
        }

        @Override
        CachedToken exchange(String oidcToken)
        {
            callCount++;
            return new CachedToken(token, expiresAt);
        }
    }
}
