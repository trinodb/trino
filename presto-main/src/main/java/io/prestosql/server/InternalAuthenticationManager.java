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
package io.prestosql.server;

import com.google.common.hash.Hashing;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.prestosql.server.security.InternalPrincipal;
import io.prestosql.spi.security.Identity;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;

import java.time.ZonedDateTime;
import java.util.Date;

import static io.airlift.http.client.Request.Builder.fromRequest;
import static io.prestosql.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

public class InternalAuthenticationManager
        implements HttpRequestFilter
{
    private static final Logger log = Logger.get(InternalAuthenticationManager.class);

    private static final String PRESTO_INTERNAL_BEARER = "X-Presto-Internal-Bearer";

    private final byte[] hmac;
    private final String nodeId;

    @Inject
    public InternalAuthenticationManager(InternalCommunicationConfig internalCommunicationConfig, NodeInfo nodeInfo)
    {
        this(getSharedSecret(internalCommunicationConfig, nodeInfo), nodeInfo.getNodeId());
    }

    private static String getSharedSecret(InternalCommunicationConfig internalCommunicationConfig, NodeInfo nodeInfo)
    {
        requireNonNull(internalCommunicationConfig, "internalCommunicationConfig is null");
        requireNonNull(nodeInfo, "nodeInfo is null");

        // This check should not be required (as bean validation already checked it),
        // but be extra careful to not use a known secret for authentication.
        if (!internalCommunicationConfig.isRequiredSharedSecretSet()) {
            throw new IllegalArgumentException("Shared secret is required when internal communications uses https");
        }

        return internalCommunicationConfig.getSharedSecret().orElseGet(nodeInfo::getEnvironment);
    }

    public InternalAuthenticationManager(String sharedSecret, String nodeId)
    {
        requireNonNull(sharedSecret, "sharedSecret is null");
        requireNonNull(nodeId, "nodeId is null");
        this.hmac = Hashing.sha256().hashString(sharedSecret, UTF_8).asBytes();
        this.nodeId = nodeId;
    }

    public static boolean isInternalRequest(ContainerRequestContext request)
    {
        return request.getHeaders().getFirst(PRESTO_INTERNAL_BEARER) != null;
    }

    public void handleInternalRequest(ContainerRequestContext request)
    {
        String subject;
        try {
            subject = parseJwt(request.getHeaders().getFirst(PRESTO_INTERNAL_BEARER));
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

        Identity identity = Identity.forUser("<internal>")
                .withPrincipal(new InternalPrincipal(subject))
                .build();
        setAuthenticatedIdentity(request, identity);
    }

    @Override
    public Request filterRequest(Request request)
    {
        return fromRequest(request)
                .addHeader(PRESTO_INTERNAL_BEARER, generateJwt())
                .build();
    }

    private String generateJwt()
    {
        return Jwts.builder()
                .signWith(SignatureAlgorithm.HS256, hmac)
                .setSubject(nodeId)
                .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                .compact();
    }

    private String parseJwt(String jwt)
    {
        return Jwts.parser()
                .setSigningKey(hmac)
                .parseClaimsJws(jwt)
                .getBody()
                .getSubject();
    }
}
