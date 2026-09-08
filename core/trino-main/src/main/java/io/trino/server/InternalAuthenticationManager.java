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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.trino.cache.NonEvictableCache;
import io.trino.server.security.InternalPrincipal;
import io.trino.server.security.SecurityConfig;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Response;

import javax.crypto.KDF;
import javax.crypto.SecretKey;
import javax.crypto.spec.HKDFParameterSpec;

import java.security.spec.AlgorithmParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.airlift.http.client.Request.Builder.fromRequest;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static io.trino.server.security.jwt.JwtUtil.newJwtBuilder;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static jakarta.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static jakarta.ws.rs.core.Response.Status.UNAUTHORIZED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Objects.requireNonNull;

public class InternalAuthenticationManager
        implements HttpRequestFilter
{
    private static final Logger log = Logger.get(InternalAuthenticationManager.class);
    private static final Supplier<Instant> DEFAULT_EXPIRATION_SUPPLIER = () -> ZonedDateTime.now().plusMinutes(6).toInstant();
    // Leave a 5 minute buffer to allow for clock skew and GC pauses
    private static final Function<Instant, Instant> TOKEN_REUSE_THRESHOLD = instant -> instant.minus(5, MINUTES);

    private static final HeaderName TRINO_INTERNAL_BEARER = HeaderName.of("X-Trino-Internal-Bearer");
    private static final String INTERNAL_USER = "<internal>";

    // A node reuses the same token for minutes at a time, so the number of distinct tokens in
    // flight is proportional to the cluster size. The bound only guards against pathological cases.
    private static final int MAX_VERIFIED_TOKENS = 1000;
    // Entries are only retained to avoid re-verifying a token that is still being sent. Correctness
    // does not depend on this, because the expiration of every entry is checked on each use.
    private static final Duration VERIFIED_TOKEN_RETENTION = Duration.ofMinutes(10);

    private final SecretKey hmac;
    private final String nodeId;
    private final JwtParser jwtParser;
    private final AtomicReference<InternalToken> currentToken;
    private final StartupStatus startupStatus;
    private final Supplier<Instant> expirationSupplier;
    private final NonEvictableCache<String, VerifiedToken> verifiedTokens = buildNonEvictableCache(
            CacheBuilder.newBuilder()
                    .maximumSize(MAX_VERIFIED_TOKENS)
                    .expireAfterWrite(VERIFIED_TOKEN_RETENTION));

    @Inject
    public InternalAuthenticationManager(InternalCommunicationConfig internalCommunicationConfig, SecurityConfig securityConfig, NodeInfo nodeInfo, StartupStatus startupStatus)
    {
        this(getSharedSecret(internalCommunicationConfig, nodeInfo, !securityConfig.getAuthenticationTypes().equals(ImmutableList.of("insecure"))), nodeInfo.getNodeId(), startupStatus);
    }

    private static String getSharedSecret(InternalCommunicationConfig internalCommunicationConfig, NodeInfo nodeInfo, boolean authenticationEnabled)
    {
        requireNonNull(nodeInfo, "nodeInfo is null");

        // This check should not be required (as bean validation already checked it),
        // but be extra careful to not use a known secret for authentication.
        if (!internalCommunicationConfig.isRequiredSharedSecretSet()) {
            throw new IllegalArgumentException("Shared secret (internal-communication.shared-secret) is required when internal communications uses HTTPS");
        }

        if (internalCommunicationConfig.getSharedSecret().isEmpty() && authenticationEnabled) {
            throw new IllegalArgumentException("Shared secret (internal-communication.shared-secret) is required when authentication is enabled");
        }

        return internalCommunicationConfig.getSharedSecret().orElseGet(nodeInfo::getEnvironment);
    }

    public InternalAuthenticationManager(String sharedSecret, String nodeId, StartupStatus startupStatus)
    {
        this(sharedSecret, nodeId, startupStatus, DEFAULT_EXPIRATION_SUPPLIER);
    }

    @VisibleForTesting
    InternalAuthenticationManager(String sharedSecret, String nodeId, StartupStatus startupStatus, Supplier<Instant> expirationSupplier)
    {
        requireNonNull(sharedSecret, "sharedSecret is null");
        requireNonNull(nodeId, "nodeId is null");
        this.startupStatus = requireNonNull(startupStatus, "startupStatus is null");
        this.expirationSupplier = requireNonNull(expirationSupplier, "expirationSupplier is null");
        this.hmac = expandKey(sharedSecret);
        this.nodeId = nodeId;
        this.jwtParser = newJwtParserBuilder().verifyWith(hmac).build();
        this.currentToken = new AtomicReference<>(createJwt());
    }

    public static boolean isInternalRequest(ContainerRequestContext request)
    {
        return request.getHeaders().getFirst(TRINO_INTERNAL_BEARER.toString()) != null;
    }

    public void handleInternalRequest(ContainerRequestContext request)
    {
        Identity identity;
        try {
            identity = authenticate(request.getHeaders().getFirst(TRINO_INTERNAL_BEARER.toString()));
        }
        catch (JwtException e) {
            log.error(e, "Internal authentication failed");
            request.abortWith(Response.status(UNAUTHORIZED)
                    .type(TEXT_PLAIN_TYPE.toString())
                    .build());
            return;
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Authentication error", e);
        }

        if (!startupStatus.isStartupComplete()) {
            request.abortWith(Response.status(SERVICE_UNAVAILABLE)
                    .type(TEXT_PLAIN_TYPE.toString())
                    .entity("Trino server is still initializing")
                    .build());
            return;
        }

        setAuthenticatedIdentity(request, identity);
    }

    @Override
    public Request filterRequest(Request request)
    {
        return fromRequest(request)
                .addHeader(TRINO_INTERNAL_BEARER, getOrGenerateJwt())
                .build();
    }

    private String getOrGenerateJwt()
    {
        InternalToken token = currentToken.get();
        if (token.isExpired()) {
            InternalToken newToken = createJwt();
            if (currentToken.compareAndSet(token, newToken)) {
                token = newToken;
            }
            else {
                // Another thread already generated a new token
                token = currentToken.get();
            }
        }
        return token.token();
    }

    private InternalToken createJwt()
    {
        Instant expiration = expirationSupplier.get();
        return new InternalToken(expiration, newJwtBuilder()
                .signWith(hmac)
                .subject(nodeId)
                .expiration(Date.from(expiration))
                .compact());
    }

    /**
     * Verifying a token is a pure function of the token and the HMAC key, which is fixed for the
     * lifetime of the process, so the result can be cached. The sending node reuses a token for
     * minutes at a time, which means the same token is otherwise re-verified on every request.
     * <p>
     * Only successfully verified tokens are cached, so an entry cannot be created without knowing
     * the shared secret. Expiration is checked against the claim on every use rather than being
     * left to the cache, so a cached token stops being accepted the moment it expires.
     */
    Identity authenticate(String jwt)
    {
        VerifiedToken verified = verifiedTokens.getIfPresent(jwt);
        if (verified != null && Instant.now().isBefore(verified.expiration())) {
            return verified.identity();
        }

        Claims claims = jwtParser.parseSignedClaims(jwt).getPayload();
        // Identity is immutable, so a single instance can be shared by every request carrying this token
        Identity identity = Identity.forUser(INTERNAL_USER)
                .withPrincipal(new InternalPrincipal(claims.getSubject()))
                .build();

        Date expiration = claims.getExpiration();
        if (expiration != null) {
            verifiedTokens.put(jwt, new VerifiedToken(identity, expiration.toInstant()));
        }
        return identity;
    }

    private record InternalToken(Instant expiration, String token)
    {
        public InternalToken
        {
            expiration = TOKEN_REUSE_THRESHOLD.apply(requireNonNull(expiration, "expiration is null"));
            requireNonNull(token, "token is null");
        }

        public boolean isExpired()
        {
            return Instant.now().isAfter(expiration);
        }
    }

    private record VerifiedToken(Identity identity, Instant expiration)
    {
        private VerifiedToken
        {
            requireNonNull(identity, "identity is null");
            requireNonNull(expiration, "expiration is null");
        }
    }

    private static SecretKey expandKey(String sharedSecret)
    {
        try {
            KDF hkdf = KDF.getInstance("HKDF-SHA256");

            AlgorithmParameterSpec params =
                    HKDFParameterSpec.ofExtract()
                            .addIKM(sharedSecret.getBytes(UTF_8))
                            .thenExpand("internal-communication".getBytes(UTF_8), 32);

            return hkdf.deriveKey("HmacSHA256", params);
        }
        catch (Exception e) {
            throw new RuntimeException("Could not expand internal communication shared key using HKDF-SHA256", e);
        }
    }
}
