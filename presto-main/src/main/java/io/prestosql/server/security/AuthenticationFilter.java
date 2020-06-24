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
package io.prestosql.server.security;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.prestosql.server.InternalAuthenticationManager;
import io.prestosql.server.ui.WebUiAuthenticationManager;
import io.prestosql.spi.security.Identity;

import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Strings.emptyToNull;
import static io.prestosql.client.PrestoHeaders.PRESTO_USER;
import static io.prestosql.server.HttpRequestSessionContext.AUTHENTICATED_IDENTITY;
import static io.prestosql.server.ServletSecurityUtils.sendErrorMessage;
import static io.prestosql.server.ServletSecurityUtils.sendWwwAuthenticate;
import static io.prestosql.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static io.prestosql.server.security.BasicAuthCredentials.extractBasicAuthCredentials;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.Priorities.AUTHENTICATION;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;

@Priority(AUTHENTICATION)
public class AuthenticationFilter
        implements ContainerRequestFilter, ContainerResponseFilter
{
    private final List<Authenticator> authenticators;
    private final InternalAuthenticationManager internalAuthenticationManager;
    private final WebUiAuthenticationManager uiAuthenticationManager;

    @Inject
    public AuthenticationFilter(List<Authenticator> authenticators,
            SecurityConfig securityConfig,
            InternalAuthenticationManager internalAuthenticationManager,
            WebUiAuthenticationManager uiAuthenticationManager)
    {
        this.authenticators = ImmutableList.copyOf(requireNonNull(authenticators, "authenticators is null"));
        this.internalAuthenticationManager = requireNonNull(internalAuthenticationManager, "internalAuthenticationManager is null");
        this.uiAuthenticationManager = requireNonNull(uiAuthenticationManager, "uiAuthenticationManager is null");
    }

    @Override
    public void filter(ContainerRequestContext request)
    {
        if (InternalAuthenticationManager.isInternalRequest(request)) {
            internalAuthenticationManager.handleInternalRequest(request);
            return;
        }

        if (WebUiAuthenticationManager.isUiRequest(request)) {
            uiAuthenticationManager.handleUiRequest(request);
            return;
        }

        // skip authentication if non-secure or not configured
        if (!doesRequestSupportAuthentication(request)) {
            handleInsecureRequest(request);
            return;
        }

        // try to authenticate, collecting errors and authentication headers
        Set<String> messages = new LinkedHashSet<>();
        Set<String> authenticateHeaders = new LinkedHashSet<>();

        for (Authenticator authenticator : authenticators) {
            Identity authenticatedIdentity;
            try {
                authenticatedIdentity = authenticator.authenticate(request);
            }
            catch (AuthenticationException e) {
                if (e.getMessage() != null) {
                    messages.add(e.getMessage());
                }
                e.getAuthenticateHeader().ifPresent(authenticateHeaders::add);
                continue;
            }

            // authentication succeeded
            setAuthenticatedIdentity(request, authenticatedIdentity);
            return;
        }

        // authentication failed
        if (messages.isEmpty()) {
            messages.add("Unauthorized");
        }
        // The error string is used by clients for exception messages and
        // is presented to the end user, thus it should be a single line.
        String error = Joiner.on(" | ").join(messages);

        sendWwwAuthenticate(request, error, authenticateHeaders);
    }

    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response)
    {
        // destroy identity if identity is still attached to the request
        Optional.ofNullable(request.getProperty(AUTHENTICATED_IDENTITY))
                .map(Identity.class::cast)
                .ifPresent(Identity::destroy);
    }

    private static void handleInsecureRequest(ContainerRequestContext request)
    {
        Optional<BasicAuthCredentials> basicAuthCredentials;
        try {
            basicAuthCredentials = extractBasicAuthCredentials(request);
        }
        catch (AuthenticationException e) {
            sendErrorMessage(request, FORBIDDEN, e.getMessage());
            return;
        }

        String user;
        if (basicAuthCredentials.isPresent()) {
            if (basicAuthCredentials.get().getPassword().isPresent()) {
                sendErrorMessage(request, FORBIDDEN, "Password not allowed for insecure request");
                return;
            }
            user = basicAuthCredentials.get().getUser();
        }
        else {
            user = emptyToNull(request.getHeaders().getFirst(PRESTO_USER));
        }

        if (user == null) {
            sendWwwAuthenticate(request, "Basic authentication or " + PRESTO_USER + " must be sent", ImmutableList.of("Basic realm=\"Presto\""));
            return;
        }

        setAuthenticatedIdentity(request, user);
    }

    private boolean doesRequestSupportAuthentication(ContainerRequestContext request)
    {
        return !authenticators.isEmpty() && request.getSecurityContext().isSecure();
    }
}
