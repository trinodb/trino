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
import io.trino.server.security.InternalPrincipal;
import io.trino.server.security.SecurityConfig;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.Base64;

import static io.airlift.http.client.Request.Builder.fromRequest;
import static io.trino.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static jakarta.ws.rs.core.Response.Status.UNAUTHORIZED;
import static java.lang.Long.parseLong;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class InternalAuthenticationManager
        implements HttpRequestFilter
{
    private static final Logger log = Logger.get(InternalAuthenticationManager.class);

    private static final int MAX_REQUEST_AGE_SECONDS = 300; // 5 minutes

    private static final String TRINO_INTERNAL_SIGNATURE = "X-Trino-Internal-Signature";
    private static final String TRINO_INTERNAL_NODE_ID = "X-Trino-Internal-Node-Id";
    private static final String TRINO_INTERNAL_REQUEST_TIMESTAMP = "X-Trino-Internal-Timestamp";

    private final HashFunction hashing;
    private final String nodeId;

    @Inject
    public InternalAuthenticationManager(InternalCommunicationConfig internalCommunicationConfig, SecurityConfig securityConfig, NodeInfo nodeInfo)
    {
        this(getSharedSecret(internalCommunicationConfig, nodeInfo, !securityConfig.getAuthenticationTypes().equals(ImmutableList.of("insecure"))), nodeInfo.getNodeId());
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

    public InternalAuthenticationManager(String sharedSecret, String nodeId)
    {
        requireNonNull(sharedSecret, "sharedSecret is null");
        requireNonNull(nodeId, "nodeId is null");
        this.hashing = Hashing.hmacSha256(sharedSecret.getBytes(UTF_8));
        this.nodeId = nodeId;
    }

    public static boolean isInternalRequest(ContainerRequestContext request)
    {
        return request.getHeaders().getFirst(TRINO_INTERNAL_SIGNATURE) != null;
    }

    public void handleInternalRequest(ContainerRequestContext request)
    {
        String nodeId = getRequiredHeader(request, TRINO_INTERNAL_NODE_ID);
        String signature = getRequiredHeader(request, TRINO_INTERNAL_SIGNATURE);

        long requestTimestampMillis = parseLong(getRequiredHeader(request, TRINO_INTERNAL_REQUEST_TIMESTAMP));

        if (!signature.equals(signature(nodeId, requestTimestampMillis, request.getUriInfo().getRequestUri()))) {
            log.error("Internal authentication failed: request signature mismatch");
            request.abortWith(Response.status(UNAUTHORIZED)
                    .type(TEXT_PLAIN_TYPE.toString())
                    .build());
            return;
        }

        if (requestTimestampMillis < 0 || requestTimestampMillis + MAX_REQUEST_AGE_SECONDS * 1000 < currentTimeMillis()) {
            log.error("Internal authentication failed: request expired at %s", requestTimestampMillis);
            request.abortWith(Response.status(UNAUTHORIZED)
                    .type(TEXT_PLAIN_TYPE.toString())
                    .build());
            return;
        }

        Identity identity = Identity.forUser("<internal>")
                .withPrincipal(new InternalPrincipal(nodeId))
                .build();
        setAuthenticatedIdentity(request, identity);
    }

    private String signature(String nodeId, long requestTimestampMillis, URI uri)
    {
        return Base64.getEncoder()
                .encodeToString(hashing.newHasher()
                        .putUnencodedChars(nodeId)
                        .putUnencodedChars(uri.toString())
                        .putLong(requestTimestampMillis)
                        .hash()
                        .asBytes());
    }

    @Override
    public Request filterRequest(Request request)
    {
        long now = currentTimeMillis();
        return fromRequest(request)
                .addHeader(TRINO_INTERNAL_NODE_ID, nodeId)
                .addHeader(TRINO_INTERNAL_REQUEST_TIMESTAMP, Long.toString(now))
                .addHeader(TRINO_INTERNAL_SIGNATURE, signature(nodeId, now, request.getUri()))
                .build();
    }

    private static String getRequiredHeader(ContainerRequestContext request, String headerName)
    {
        String headerValue = request.getHeaderString(headerName);
        if (headerValue == null || headerValue.isEmpty()) {
            throw new ForbiddenException("Missing required authentication header: " + headerName);
        }
        return headerValue;
    }
}
