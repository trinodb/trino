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
package io.trino.server.security.oauth2;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.Authenticator;
import io.trino.server.security.UserMapping;
import io.trino.server.security.UserMappingException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import java.net.URI;
import java.util.UUID;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static io.trino.server.security.oauth2.OAuth2CallbackResource.CALLBACK_ENDPOINT;
import static io.trino.server.security.oauth2.OAuth2TokenExchangeResource.getTokenUri;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OAuth2Authenticator
        implements Authenticator
{
    private final OAuth2Service service;
    private final UserMapping userMapping;

    @Inject
    public OAuth2Authenticator(OAuth2Service service, OAuth2Config oauth2Config)
    {
        this.service = requireNonNull(service, "service is null");
        requireNonNull(oauth2Config, "oauth2Config is null");
        this.userMapping = UserMapping.createUserMapping(oauth2Config.getUserMappingPattern(), oauth2Config.getUserMappingFile());
    }

    @Override
    public Identity authenticate(ContainerRequestContext request)
            throws AuthenticationException
    {
        String header = nullToEmpty(request.getHeaders().getFirst(AUTHORIZATION));

        int space = header.indexOf(' ');
        if ((space < 0) || !header.substring(0, space).equalsIgnoreCase("bearer")) {
            throw needAuthentication(request, null);
        }
        String token = header.substring(space + 1).trim();
        if (token.isEmpty()) {
            throw needAuthentication(request, null);
        }

        try {
            Jws<Claims> claimsJws = service.parseClaimsJws(token);
            String subject = claimsJws.getBody().getSubject();
            String authenticatedUser = userMapping.mapUser(subject);
            return Identity.forUser(authenticatedUser)
                    .withPrincipal(new BasicPrincipal(subject))
                    .build();
        }
        catch (JwtException | UserMappingException e) {
            throw needAuthentication(request, e.getMessage());
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Authentication error", e);
        }
    }

    private AuthenticationException needAuthentication(ContainerRequestContext request, String message)
    {
        UUID authId = UUID.randomUUID();
        URI redirectUri = service.startRestChallenge(request.getUriInfo().getBaseUri().resolve(CALLBACK_ENDPOINT), authId);
        URI tokenUri = request.getUriInfo().getBaseUri().resolve(getTokenUri(authId));
        return new AuthenticationException(message, format("Bearer x_redirect_server=\"%s\", x_token_server=\"%s\"", redirectUri, tokenUri));
    }
}
