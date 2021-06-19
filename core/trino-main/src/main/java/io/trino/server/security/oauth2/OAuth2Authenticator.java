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
import io.trino.server.security.AbstractBearerAuthenticator;
import io.trino.server.security.AuthenticationException;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import java.net.URI;
import java.util.UUID;

import static io.trino.server.security.UserMapping.createUserMapping;
import static io.trino.server.security.oauth2.OAuth2CallbackResource.CALLBACK_ENDPOINT;
import static io.trino.server.security.oauth2.OAuth2TokenExchangeResource.getTokenUri;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OAuth2Authenticator
        extends AbstractBearerAuthenticator
{
    private final OAuth2Service service;

    @Inject
    public OAuth2Authenticator(OAuth2Service service, OAuth2Config config)
    {
        super(config.getPrincipalField(), createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile()));
        this.service = requireNonNull(service, "service is null");
    }

    @Override
    protected Jws<Claims> parseClaimsJws(String jws)
    {
        return service.parseClaimsJws(jws);
    }

    @Override
    protected AuthenticationException needAuthentication(ContainerRequestContext request, String message)
    {
        UUID authId = UUID.randomUUID();
        URI redirectUri = service.startRestChallenge(request.getUriInfo().getBaseUri().resolve(CALLBACK_ENDPOINT), authId);
        URI tokenUri = request.getUriInfo().getBaseUri().resolve(getTokenUri(authId));
        return new AuthenticationException(message, format("Bearer x_redirect_server=\"%s\", x_token_server=\"%s\"", redirectUri, tokenUri));
    }
}
