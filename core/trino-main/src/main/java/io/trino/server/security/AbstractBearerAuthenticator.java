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
import jakarta.ws.rs.container.ContainerRequestContext;

import java.util.List;
import java.util.Optional;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.lang.String.format;

public abstract class AbstractBearerAuthenticator
        implements Authenticator
{
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
            return createIdentity(token).orElseThrow(() -> needAuthentication(request, Optional.of(token), "Invalid credentials"));
        }
        catch (JwtException | UserMappingException e) {
            throw needAuthentication(request, Optional.empty(), e.getMessage());
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
            throw needAuthentication(request, Optional.empty(), null);
        }
        if (headers.size() > 1) {
            throw new IllegalArgumentException(format("Multiple %s headers detected: %s, where only single %s header is supported", AUTHORIZATION, headers, AUTHORIZATION));
        }

        String header = headers.get(0);
        int space = header.indexOf(' ');
        if ((space < 0) || !header.substring(0, space).equalsIgnoreCase("bearer")) {
            throw needAuthentication(request, Optional.empty(), null);
        }
        String token = header.substring(space + 1).trim();
        if (token.isEmpty()) {
            throw needAuthentication(request, Optional.empty(), null);
        }
        return token;
    }

    protected abstract Optional<Identity> createIdentity(String token)
            throws UserMappingException;

    protected abstract AuthenticationException needAuthentication(ContainerRequestContext request, Optional<String> currentToken, String message);
}
