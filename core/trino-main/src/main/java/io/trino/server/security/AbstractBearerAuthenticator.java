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

import io.jsonwebtoken.JwtException;
import io.trino.spi.security.Identity;

import javax.ws.rs.container.ContainerRequestContext;

import java.security.Principal;
import java.util.List;
import java.util.Optional;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class AbstractBearerAuthenticator
        implements Authenticator
{
    private final UserMapping userMapping;

    protected AbstractBearerAuthenticator(UserMapping userMapping)
    {
        this.userMapping = requireNonNull(userMapping, "userMapping is null");
    }

    @Override
    public Identity authenticate(ContainerRequestContext request)
            throws AuthenticationException
    {
        return authenticate(request, extractToken(request));
    }

    public Identity authenticate(ContainerRequestContext request, String token)
            throws AuthenticationException
    {
        try {
            Optional<Principal> principal = extractPrincipalFromToken(token);
            if (principal.isEmpty()) {
                throw needAuthentication(request, "Invalid credentials");
            }

            String authenticatedUser = userMapping.mapUser(principal.get().getName());
            return Identity.forUser(authenticatedUser)
                    .withPrincipal(principal.get())
                    .build();
        }
        catch (JwtException | UserMappingException e) {
            throw needAuthentication(request, e.getMessage());
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Authentication error", e);
        }
    }

    public String extractToken(ContainerRequestContext request)
            throws AuthenticationException
    {
        List<String> headers = request.getHeaders().get(AUTHORIZATION);
        if (headers == null || headers.size() == 0) {
            throw needAuthentication(request, null);
        }
        if (headers.size() > 1) {
            throw new IllegalArgumentException(format("Multiple %s headers detected: %s, where only single %s header is supported", AUTHORIZATION, headers, AUTHORIZATION));
        }

        String header = headers.get(0);
        int space = header.indexOf(' ');
        if ((space < 0) || !header.substring(0, space).equalsIgnoreCase("bearer")) {
            throw needAuthentication(request, null);
        }
        String token = header.substring(space + 1).trim();
        if (token.isEmpty()) {
            throw needAuthentication(request, null);
        }
        return token;
    }

    protected abstract Optional<Principal> extractPrincipalFromToken(String token);

    protected abstract AuthenticationException needAuthentication(ContainerRequestContext request, String message);
}
