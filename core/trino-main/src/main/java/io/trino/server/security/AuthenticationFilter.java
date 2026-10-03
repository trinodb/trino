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
package io.trino.server.security;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.server.InternalAuthenticationManager;
import io.trino.server.security.AuthenticatorChain.Authenticated;
import io.trino.server.security.AuthenticatorChain.Failed;
import io.trino.spi.security.Identity;
import jakarta.annotation.Priority;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.server.ServletSecurityUtils.sendWwwAuthenticate;
import static io.trino.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static jakarta.ws.rs.Priorities.AUTHENTICATION;
import static java.util.Objects.requireNonNull;

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
        insecureAuthenticationOverHttpAllowed = securityConfig.isInsecureAuthenticationOverHttpAllowed();
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

        switch (AuthenticatorChain.authenticate(authenticators, authenticator -> authenticator.authenticate(request))) {
            case Authenticated(Identity identity) -> setAuthenticatedIdentity(request, identity);
            case Failed(Set<String> authenticateHeaders, String error) -> sendWwwAuthenticate(request, error, authenticateHeaders);
        }
    }
}
