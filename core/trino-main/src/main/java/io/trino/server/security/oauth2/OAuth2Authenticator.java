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

import io.trino.server.security.AbstractBearerAuthenticator;
import io.trino.server.security.AuthenticationException;
import io.trino.spi.security.BasicPrincipal;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import java.net.URI;
import java.security.Principal;
import java.util.Optional;
import java.util.UUID;

import static io.trino.server.security.UserMapping.createUserMapping;
import static io.trino.server.security.oauth2.OAuth2TokenExchangeResource.getInitiateUri;
import static io.trino.server.security.oauth2.OAuth2TokenExchangeResource.getTokenUri;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OAuth2Authenticator
        extends AbstractBearerAuthenticator
{
    public static final String NAME = "oauth2";

    private final OAuth2Service service;
    private final String principalField;

    @Inject
    public OAuth2Authenticator(OAuth2Service service, OAuth2Config config)
    {
        super(createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile()));
        this.service = requireNonNull(service, "service is null");
        this.principalField = config.getPrincipalField();
    }

    @Override
    protected Optional<Principal> extractPrincipalFromToken(String token)
    {
        try {
            return service.convertTokenToClaims(token)
                    .map(claims -> claims.get(principalField))
                    .map(String.class::cast)
                    .map(BasicPrincipal::new);
        }
        catch (ChallengeFailedException e) {
            return Optional.empty();
        }
    }

    @Override
    protected AuthenticationException needAuthentication(ContainerRequestContext request, String message)
    {
        UUID authId = UUID.randomUUID();
        URI initiateUri = request.getUriInfo().getBaseUri().resolve(getInitiateUri(authId));
        URI tokenUri = request.getUriInfo().getBaseUri().resolve(getTokenUri(authId));
        return new AuthenticationException(message, format("Bearer x_redirect_server=\"%s\", x_token_server=\"%s\"", initiateUri, tokenUri));
    }
}
