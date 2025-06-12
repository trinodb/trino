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
import com.google.inject.Inject;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.trino.server.security.InternalPrincipal;
import io.trino.server.security.SecurityConfig;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.container.ContainerRequestContext;

import javax.crypto.KDF;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.HKDFParameterSpec;

import java.security.GeneralSecurityException;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Optional;

import static io.airlift.http.client.Request.Builder.fromRequest;
import static io.airlift.units.Duration.succinctDuration;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class InternalAuthenticationManager
        implements HttpRequestFilter
{
    private static final String REALM_NAME = "trino-internal";
    private static final String HKDF_ALGORITHM = "HKDF-SHA256";
    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final String CHALLENGE = "Trino-Cluster-Sign realm=\"%s\", algorithm=\"%s\"".formatted(REALM_NAME, HMAC_ALGORITHM);

    private static final Logger log = Logger.get(InternalAuthenticationManager.class);

    private final ThreadLocal<Mac> hmac = ThreadLocal.withInitial(this::initializeMac);
    private final long maxRequestAgeMillis;
    private final String nodeId;
    private final StartupStatus startupStatus;
    private final SecretKey key;

    @Inject
    public InternalAuthenticationManager(InternalCommunicationConfig internalCommunicationConfig, SecurityConfig securityConfig, NodeInfo nodeInfo, StartupStatus startupStatus)
    {
        this(getSharedSecret(internalCommunicationConfig, nodeInfo, !securityConfig.getAuthenticationTypes().equals(ImmutableList.of("insecure"))), internalCommunicationConfig.getMaxRequestAge(), nodeInfo.getNodeId(), startupStatus);
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

    public InternalAuthenticationManager(String sharedSecret, Duration maxRequestAge, String nodeId, StartupStatus startupStatus)
    {
        requireNonNull(sharedSecret, "sharedSecret is null");
        requireNonNull(nodeId, "nodeId is null");
        requireNonNull(maxRequestAge, "maxRequestAge is null");
        requireNonNull(startupStatus, "startupStatus is null");
        this.startupStatus = startupStatus;
        this.key = expandKey(sharedSecret);
        this.maxRequestAgeMillis = maxRequestAge.toMillis();
        this.nodeId = nodeId;
    }

    /**
     * Returns true if the request was handled as an internal request.
     */
    public boolean handleInternalRequest(ContainerRequestContext request)
    {
        if (!startupStatus.isStartupComplete()) {
            throw new ServiceUnavailableException("Trino server is still initializing");
        }

        Optional<InternalAuthorization> maybeSignature = getSignature(request);
        // Not an internal request
        if (maybeSignature.isEmpty()) {
            return false;
        }

        InternalAuthorization signature = maybeSignature.orElseThrow();
        if (!signature.isValid(hmac.get())) {
            throw unauthorizedException(request, "signature invalid");
        }

        if (signature.ttl() < currentTimeMillis()) {
            throw unauthorizedException(request, "signature expired %s seconds ago".formatted(succinctDuration(currentTimeMillis() - signature.ttl(), MILLISECONDS)));
        }

        Identity identity = Identity.forUser("<internal>")
                .withPrincipal(new InternalPrincipal(signature.nodeId()))
                .build();
        setAuthenticatedIdentity(request, identity);
        return true;
    }

    @Override
    public Request filterRequest(Request request)
    {
        InternalAuthorization signature = InternalAuthorization.signRequest(hmac.get(), request.getMethod(), request.getUri().getPath(), nodeId, currentTimeMillis() + maxRequestAgeMillis);
        return fromRequest(request)
                .addHeader(TRINO_HEADERS.requestInternalAuthorization(), signature.toHeader())
                .build();
    }

    public static NotAuthorizedException unauthorizedException(ContainerRequestContext requestContext, String error)
    {
        String errorMessage = "%s /%s: %s".formatted(requestContext.getMethod(), requestContext.getUriInfo().getPath(), error);
        log.error(errorMessage);
        return new NotAuthorizedException(errorMessage, CHALLENGE);
    }

    private Optional<InternalAuthorization> getSignature(ContainerRequestContext request)
    {
        return Optional.ofNullable(request.getHeaderString(TRINO_HEADERS.requestInternalAuthorization()))
                .filter(value -> !value.isEmpty())
                .map(header -> InternalAuthorization.fromHeader(header, request.getMethod(), request.getUriInfo().getRequestUri().getPath()));
    }

    private Mac initializeMac()
    {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(key);
            return mac;
        }
        catch (GeneralSecurityException e) {
            throw new IllegalStateException("Could not initialize %s".formatted(HMAC_ALGORITHM), e);
        }
    }

    private static SecretKey expandKey(String sharedSecret)
    {
        try {
            KDF hkdf = KDF.getInstance(HKDF_ALGORITHM);

            AlgorithmParameterSpec params =
                    HKDFParameterSpec.ofExtract()
                            .addIKM(sharedSecret.getBytes(UTF_8))
                            .thenExpand(REALM_NAME.getBytes(UTF_8), 32);

            return hkdf.deriveKey(HMAC_ALGORITHM, params);
        }
        catch (Exception e) {
            throw new RuntimeException("Could not expand internal communication shared key using %s".formatted(HKDF_ALGORITHM), e);
        }
    }
}
