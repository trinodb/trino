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
import io.prestosql.spi.security.Identity;

import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.server.ServletSecurityUtils.sendWwwAuthenticate;
import static io.prestosql.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.Priorities.AUTHENTICATION;

@Priority(AUTHENTICATION)
public class AuthenticationFilter
        implements ContainerRequestFilter
{
    private final List<Authenticator> authenticators;
    private final InternalAuthenticationManager internalAuthenticationManager;
    private final boolean insecureAuthenticationOverHttpAllowed;
    private final InsecureAuthenticator insecureAuthenticator;

    @Inject
    public AuthenticationFilter(
            List<Authenticator> authenticators,
            InternalAuthenticationManager internalAuthenticationManager,
            SecurityConfig securityConfig,
            InsecureAuthenticator insecureAuthenticator)
    {
        this.authenticators = ImmutableList.copyOf(requireNonNull(authenticators, "authenticators is null"));
        checkArgument(!authenticators.isEmpty(), "authenticators is empty");
        this.internalAuthenticationManager = requireNonNull(internalAuthenticationManager, "internalAuthenticationManager is null");
        insecureAuthenticationOverHttpAllowed = requireNonNull(securityConfig, "securityConfig is null").isInsecureAuthenticationOverHttpAllowed();
        this.insecureAuthenticator = requireNonNull(insecureAuthenticator, "insecureAuthenticator is null");
    }

    @Override
    public void filter(ContainerRequestContext request)
    {
        if (InternalAuthenticationManager.isInternalRequest(request)) {
            internalAuthenticationManager.handleInternalRequest(request);
            return;
        }

        List<Authenticator> authenticators;
        if (request.getSecurityContext().isSecure()) {
            authenticators = this.authenticators;
        }
        else if (insecureAuthenticationOverHttpAllowed) {
            authenticators = ImmutableList.of(insecureAuthenticator);
        }
        else {
            throw new ForbiddenException("Authentication over HTTP is not enabled");
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
}
