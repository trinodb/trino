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

import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.trino.server.security.InternalPrincipal;
import io.trino.server.security.SecurityConfig;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.Arrays;
import java.util.Base64;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.airlift.http.client.Request.Builder.fromRequest;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static jakarta.ws.rs.core.Response.Status.UNAUTHORIZED;
import static java.lang.Long.parseLong;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class InternalAuthenticationManager
        implements HttpRequestFilter
{
    private static final Logger log = Logger.get(InternalAuthenticationManager.class);

    private final HashFunction hashing;
    private final long maxRequestAgeMillis;
    private final String nodeId;

    @Inject
    public InternalAuthenticationManager(InternalCommunicationConfig internalCommunicationConfig, SecurityConfig securityConfig, NodeInfo nodeInfo)
    {
        this(getSharedSecret(internalCommunicationConfig, nodeInfo, !securityConfig.getAuthenticationTypes().equals(ImmutableList.of("insecure"))), internalCommunicationConfig.getMaxRequestAge(), nodeInfo.getNodeId());
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

    public InternalAuthenticationManager(String sharedSecret, Duration maxRequestAge, String nodeId)
    {
        requireNonNull(sharedSecret, "sharedSecret is null");
        requireNonNull(nodeId, "nodeId is null");
        requireNonNull(maxRequestAge, "maxRequestAge is null");
        this.hashing = Hashing.hmacSha256(sharedSecret.getBytes(UTF_8));
        this.maxRequestAgeMillis = maxRequestAge.toMillis();
        this.nodeId = nodeId;
    }

    public static boolean isInternalRequest(ContainerRequestContext request)
    {
        return request.getHeaders().getFirst(TRINO_HEADERS.requestInternalSignature()) != null;
    }

    public void handleInternalRequest(ContainerRequestContext request)
    {
        String nodeId = getRequiredHeader(request, TRINO_HEADERS.requestInternalNodeId());
        byte[] signature = Base64.getUrlDecoder().decode(getRequiredHeader(request, TRINO_HEADERS.requestInternalSignature()));
        long validUntil = parseLong(getRequiredHeader(request, TRINO_HEADERS.requestInternalRequestValidUntil()));

        if (!Arrays.equals(signature, signature(nodeId, request.getMethod().toUpperCase(ENGLISH), validUntil, request.getUriInfo().getRequestUri()))) {
            request.abortWith(unauthorizedResponse(request, "request signature mismatch"));
            return;
        }

        if (validUntil < currentTimeMillis()) {
            request.abortWith(unauthorizedResponse(request, "request expired %s seconds ago".formatted(Duration.succinctDuration(currentTimeMillis() - validUntil, MILLISECONDS))));
            return;
        }

        Identity identity = Identity.forUser("<internal>")
                .withPrincipal(new InternalPrincipal(nodeId))
                .build();
        setAuthenticatedIdentity(request, identity);
    }

    private byte[] signature(String method, String nodeId, long requestTimestampMillis, URI uri)
    {
        return hashing
                .newHasher()
                .putUnencodedChars(nodeId)
                .putUnencodedChars(method)
                .putUnencodedChars(uri.getPath())
                .putUnencodedChars(firstNonNull(uri.getQuery(), ""))
                .putLong(requestTimestampMillis)
                .hash()
                .asBytes();
    }

    @Override
    public Request filterRequest(Request request)
    {
        long validUntil = currentTimeMillis() + maxRequestAgeMillis;
        byte[] signature = signature(nodeId, request.getMethod().toUpperCase(ENGLISH), validUntil, request.getUri());
        return fromRequest(request)
                .addHeader(TRINO_HEADERS.requestInternalNodeId(), nodeId)
                .addHeader(TRINO_HEADERS.requestInternalRequestValidUntil(), Long.toString(validUntil))
                .addHeader(TRINO_HEADERS.requestInternalSignature(), Base64.getUrlEncoder().encodeToString(signature))
                .build();
    }

    private static Response unauthorizedResponse(ContainerRequestContext requestContext, String error)
    {
        String errorMessage = "%s %s authentication failed: %s".formatted(requestContext.getMethod(), requestContext.getUriInfo().getPath(), error);
        log.error(errorMessage);
        return Response.status(UNAUTHORIZED)
                .entity(errorMessage)
                .type(TEXT_PLAIN_TYPE.toString())
                .build();
    }

    private static String getRequiredHeader(ContainerRequestContext request, String headerName)
    {
        String headerValue = request.getHeaderString(headerName);
        if (headerValue == null || headerValue.isEmpty()) {
            request.abortWith(unauthorizedResponse(request, "missing required signature header: " + headerName));
        }
        return headerValue;
    }
}
