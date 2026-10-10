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
package io.trino.server;

import io.airlift.http.client.HeaderName;
import io.airlift.http.client.Request;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.JwtException;
import io.trino.server.security.InternalPrincipal;
import io.trino.spi.security.Identity;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;

import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestInternalAuthenticationManager
{
    private static final String SHARED_SECRET = "shared-secret";
    private static final String NODE_ID = "node-id";

    @Test
    void testValidTokenIsAccepted()
    {
        InternalAuthenticationManager manager = createManager(SHARED_SECRET, NODE_ID);

        Identity identity = manager.authenticate(generateToken(manager));

        assertThat(identity.getUser()).isEqualTo("<internal>");
        assertThat(identity.getPrincipal()).contains(new InternalPrincipal(NODE_ID));
    }

    @Test
    void testRepeatedVerificationOfSameTokenReusesIdentity()
    {
        InternalAuthenticationManager manager = createManager(SHARED_SECRET, NODE_ID);
        String token = generateToken(manager);

        // the second call is served from the cache and must return the very same identity
        assertThat(manager.authenticate(token)).isSameAs(manager.authenticate(token));
    }

    @Test
    void testTokenFromDifferentSharedSecretIsRejected()
    {
        String token = generateToken(createManager("other-shared-secret", NODE_ID));
        InternalAuthenticationManager manager = createManager(SHARED_SECRET, NODE_ID);

        assertThatThrownBy(() -> manager.authenticate(token))
                .isInstanceOf(JwtException.class);
    }

    @Test
    void testMalformedTokenIsRejected()
    {
        InternalAuthenticationManager manager = createManager(SHARED_SECRET, NODE_ID);

        // repeated to verify that a rejected token is not cached and keeps being rejected
        assertThatThrownBy(() -> manager.authenticate("not-a-token"))
                .isInstanceOf(JwtException.class);
        assertThatThrownBy(() -> manager.authenticate("not-a-token"))
                .isInstanceOf(JwtException.class);
    }

    @Test
    void testAlreadyExpiredTokenIsRejected()
    {
        InternalAuthenticationManager manager = createManager(
                SHARED_SECRET,
                NODE_ID,
                () -> Instant.now().minus(1, ChronoUnit.MINUTES));

        assertThatThrownBy(() -> manager.authenticate(generateToken(manager)))
                .isInstanceOf(ExpiredJwtException.class);
    }

    @Test
    void testCachedTokenIsRejectedOnceItExpires()
    {
        InternalAuthenticationManager manager = createManager(
                SHARED_SECRET,
                NODE_ID,
                () -> Instant.now().plusSeconds(1));
        String token = generateToken(manager);

        // populate the cache while the token is still valid
        assertThat(manager.authenticate(token).getPrincipal()).contains(new InternalPrincipal(NODE_ID));

        assertEventually(() -> assertThatThrownBy(() -> manager.authenticate(token))
                .isInstanceOf(ExpiredJwtException.class));
    }

    private static InternalAuthenticationManager createManager(String sharedSecret, String nodeId)
    {
        return createManager(sharedSecret, nodeId, () -> Instant.now().plusSeconds(60));
    }

    private static InternalAuthenticationManager createManager(String sharedSecret, String nodeId, Supplier<Instant> expirationSupplier)
    {
        StartupStatus startupStatus = new StartupStatus();
        startupStatus.startupComplete();
        return new InternalAuthenticationManager(sharedSecret, nodeId, startupStatus, expirationSupplier);
    }

    private static String generateToken(InternalAuthenticationManager manager)
    {
        Request request = manager.filterRequest(Request.builder()
                .setMethod("GET")
                .setUri(URI.create("http://example.com"))
                .build());
        return request.getHeaders().get(HeaderName.of("X-Trino-Internal-Bearer")).getFirst();
    }
}
