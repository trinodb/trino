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

import io.prestosql.security.AccessControl;
import io.prestosql.server.InternalAuthenticationManager;
import io.prestosql.server.security.ResourceSecurity.AccessType;
import io.prestosql.server.ui.WebUiAuthenticationFilter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.GroupProvider;
import io.prestosql.spi.security.Identity;

import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.Priorities;
import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.DynamicFeature;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.FeatureContext;

import java.util.Optional;

import static io.prestosql.server.HttpRequestSessionContext.AUTHENTICATED_IDENTITY;
import static io.prestosql.server.HttpRequestSessionContext.extractAuthorizedIdentity;
import static io.prestosql.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static io.prestosql.server.security.ResourceSecurity.AccessType.MANAGEMENT_READ;
import static io.prestosql.spi.StandardErrorCode.SERVER_STARTING_UP;
import static java.util.Objects.requireNonNull;

public class ResourceSecurityDynamicFeature
        implements DynamicFeature
{
    private final ResourceAccessType resourceAccessType;
    private final AuthenticationFilter authenticationFilter;
    private final WebUiAuthenticationFilter webUiAuthenticationFilter;
    private final InternalAuthenticationManager internalAuthenticationManager;
    private final AccessControl accessControl;
    private final GroupProvider groupProvider;
    private final Optional<String> fixedManagementUser;
    private final boolean fixedManagementUserForHttps;

    @Inject
    public ResourceSecurityDynamicFeature(
            ResourceAccessType resourceAccessType,
            AuthenticationFilter authenticationFilter,
            WebUiAuthenticationFilter webUiAuthenticationFilter,
            InternalAuthenticationManager internalAuthenticationManager,
            AccessControl accessControl,
            GroupProvider groupProvider,
            SecurityConfig securityConfig)
    {
        this.resourceAccessType = requireNonNull(resourceAccessType, "resourceAccessType is null");
        this.authenticationFilter = requireNonNull(authenticationFilter, "authenticationFilter is null");
        this.webUiAuthenticationFilter = requireNonNull(webUiAuthenticationFilter, "webUiAuthenticationFilter is null");
        this.internalAuthenticationManager = requireNonNull(internalAuthenticationManager, "internalAuthenticationManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        requireNonNull(securityConfig, "securityConfig is null");
        this.fixedManagementUser = securityConfig.getFixedManagementUser();
        this.fixedManagementUserForHttps = securityConfig.isFixedManagementUserForHttps();
    }

    @Override
    public void configure(ResourceInfo resourceInfo, FeatureContext context)
    {
        AccessType accessType = resourceAccessType.getAccessType(resourceInfo);
        switch (accessType) {
            case PUBLIC:
                // no authentication or authorization
                break;
            case WEB_UI:
                context.register(webUiAuthenticationFilter);
                context.register(new DisposeIdentityResponseFilter());
                break;
            case AUTHENTICATED_USER:
                context.register(authenticationFilter);
                context.register(new DisposeIdentityResponseFilter());
                break;
            case MANAGEMENT_READ:
            case MANAGEMENT_WRITE:
                context.register(new ManagementAuthenticationFilter(fixedManagementUser, fixedManagementUserForHttps, authenticationFilter));
                context.register(new ManagementAuthorizationFilter(accessControl, groupProvider, accessType == MANAGEMENT_READ));
                context.register(new DisposeIdentityResponseFilter());
                break;
            case INTERNAL_ONLY:
                context.register(new InternalOnlyRequestFilter(internalAuthenticationManager));
                break;
            default:
                throw new IllegalArgumentException("Unknown mode: " + accessType);
        }
    }

    @Priority(Priorities.AUTHENTICATION)
    private static class ManagementAuthenticationFilter
            implements ContainerRequestFilter
    {
        private final AuthenticationFilter fallbackAuthenticationFilter;
        private final Optional<String> fixedManagementUser;
        private final boolean fixedManagementUserForHttps;

        public ManagementAuthenticationFilter(Optional<String> fixedManagementUser, boolean fixedManagementUserForHttps, AuthenticationFilter fallbackAuthenticationFilter)
        {
            this.fixedManagementUser = requireNonNull(fixedManagementUser, "fixedManagementUser is null");
            this.fixedManagementUserForHttps = fixedManagementUserForHttps;
            this.fallbackAuthenticationFilter = requireNonNull(fallbackAuthenticationFilter, "fallbackAuthenticationFilter is null");
        }

        @Override
        public void filter(ContainerRequestContext request)
        {
            if (fixedManagementUser.isPresent() && (fixedManagementUserForHttps || !request.getSecurityContext().isSecure())) {
                setAuthenticatedIdentity(request, fixedManagementUser.get());
            }
            else {
                fallbackAuthenticationFilter.filter(request);
            }
        }
    }

    @Priority(Priorities.AUTHORIZATION)
    private static class ManagementAuthorizationFilter
            implements ContainerRequestFilter
    {
        private final AccessControl accessControl;
        private final GroupProvider groupProvider;
        private final boolean read;

        public ManagementAuthorizationFilter(AccessControl accessControl, GroupProvider groupProvider, boolean read)
        {
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
            this.read = read;
        }

        @Override
        public void filter(ContainerRequestContext request)
        {
            if (request.getSecurityContext().getUserPrincipal() instanceof InternalPrincipal) {
                return;
            }

            try {
                Identity identity = extractAuthorizedIdentity(
                        Optional.ofNullable((Identity) request.getProperty(AUTHENTICATED_IDENTITY)),
                        request.getHeaders(),
                        accessControl,
                        groupProvider);
                if (read) {
                    accessControl.checkCanReadSystemInformation(identity);
                }
                else {
                    accessControl.checkCanWriteSystemInformation(identity);
                }
            }
            catch (AccessDeniedException e) {
                throw new ForbiddenException("Management only resource");
            }
            catch (PrestoException e) {
                if (SERVER_STARTING_UP.toErrorCode().equals(e.getErrorCode())) {
                    throw new ServiceUnavailableException(e.getMessage());
                }
                throw e;
            }
        }
    }

    @Priority(Priorities.AUTHENTICATION)
    private static class InternalOnlyRequestFilter
            implements ContainerRequestFilter
    {
        private final InternalAuthenticationManager internalAuthenticationManager;

        @Inject
        public InternalOnlyRequestFilter(InternalAuthenticationManager internalAuthenticationManager)
        {
            this.internalAuthenticationManager = requireNonNull(internalAuthenticationManager, "internalAuthenticationManager is null");
        }

        @Override
        public void filter(ContainerRequestContext request)
        {
            if (InternalAuthenticationManager.isInternalRequest(request)) {
                internalAuthenticationManager.handleInternalRequest(request);
                return;
            }

            throw new ForbiddenException("Internal only resource");
        }
    }

    @Priority(Priorities.AUTHENTICATION)
    private static class DisposeIdentityResponseFilter
            implements ContainerResponseFilter
    {
        @Override
        public void filter(ContainerRequestContext request, ContainerResponseContext response)
        {
            // destroy identity if identity is still attached to the request
            Optional.ofNullable(request.getProperty(AUTHENTICATED_IDENTITY))
                    .map(Identity.class::cast)
                    .ifPresent(Identity::destroy);
        }
    }
}
