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

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;

import javax.ws.rs.container.ContainerRequestContext;

import java.util.List;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class AbstractBearerAuthenticator
        implements Authenticator
{
    private final String principalField;
    private final UserMapping userMapping;

    protected AbstractBearerAuthenticator(String principalField, UserMapping userMapping)
    {
        this.principalField = requireNonNull(principalField, "principalField is null");
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
            Jws<Claims> claimsJws = parseClaimsJws(token);
            String principal = claimsJws.getBody().get(principalField, String.class);
            if (principal == null) {
                throw needAuthentication(request, "Invalid credentials");
            }
            String authenticatedUser = userMapping.mapUser(principal);
            return Identity.forUser(authenticatedUser)
                    .withPrincipal(new BasicPrincipal(principal))
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

    protected abstract Jws<Claims> parseClaimsJws(String jws);

    protected abstract AuthenticationException needAuthentication(ContainerRequestContext request, String message);
}
