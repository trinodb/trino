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

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.server.security.AbstractBearerAuthenticator;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.UserMapping;
import io.trino.server.security.UserMappingException;
import io.trino.server.security.oauth2.TokenPairSerializer.TokenPair;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import java.net.URI;
import java.sql.Date;
import java.time.Instant;
import java.util.List;
import java.util.Map;
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
    private static final Logger log = Logger.get(OAuth2Authenticator.class);
    private final OAuth2Client client;
    private final String principalField;
    private final Optional<String> groupsField;
    private final UserMapping userMapping;
    private final TokenPairSerializer tokenPairSerializer;
    private final TokenRefresher tokenRefresher;

    @Inject
    public OAuth2Authenticator(OAuth2Client client, OAuth2Config config, TokenRefresher tokenRefresher, TokenPairSerializer tokenPairSerializer)
    {
        this.client = requireNonNull(client, "service is null");
        this.principalField = config.getPrincipalField();
        this.tokenRefresher = requireNonNull(tokenRefresher, "tokenRefresher is null");
        this.tokenPairSerializer = requireNonNull(tokenPairSerializer, "tokenPairSerializer is null");
        groupsField = requireNonNull(config.getGroupsField(), "groupsField is null");
        userMapping = createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile());
    }

    @Override
    protected Optional<Identity> createIdentity(String token)
            throws UserMappingException
    {
        Optional<TokenPair> deserializeToken = deserializeToken(token);
        if (deserializeToken.isEmpty()) {
            return Optional.empty();
        }

        TokenPair tokenPair = deserializeToken.get();
        if (tokenPair.getExpiration().before(Date.from(Instant.now()))) {
            return Optional.empty();
        }
        Optional<Map<String, Object>> claims = client.getClaims(tokenPair.getAccessToken());
        if (claims.isEmpty()) {
            return Optional.empty();
        }
        String principal = (String) claims.get().get(principalField);
        Identity.Builder builder = Identity.forUser(userMapping.mapUser(principal));
        builder.withPrincipal(new BasicPrincipal(principal));
        groupsField.flatMap(field -> Optional.ofNullable((List<String>) claims.get().get(field)))
                .ifPresent(groups -> builder.withGroups(ImmutableSet.copyOf(groups)));
        return Optional.of(builder.build());
    }

    private Optional<TokenPair> deserializeToken(String token)
    {
        try {
            return Optional.of(tokenPairSerializer.deserialize(token));
        }
        catch (RuntimeException ex) {
            log.debug(ex, "Failed to deserialize token");
            return Optional.empty();
        }
    }

    @Override
    protected AuthenticationException needAuthentication(ContainerRequestContext request, Optional<String> currentToken, String message)
    {
        return currentToken
                .flatMap(this::deserializeToken)
                .flatMap(tokenRefresher::refreshToken)
                .map(refreshId -> request.getUriInfo().getBaseUri().resolve(getTokenUri(refreshId)))
                .map(tokenUri -> new AuthenticationException(message, format("Bearer x_token_server=\"%s\"", tokenUri)))
                .orElseGet(() -> needAuthentication(request, message));
    }

    private AuthenticationException needAuthentication(ContainerRequestContext request, String message)
    {
        UUID authId = UUID.randomUUID();
        URI initiateUri = request.getUriInfo().getBaseUri().resolve(getInitiateUri(authId));
        URI tokenUri = request.getUriInfo().getBaseUri().resolve(getTokenUri(authId));
        return new AuthenticationException(message, format("Bearer x_redirect_server=\"%s\", x_token_server=\"%s\"", initiateUri, tokenUri));
    }
}
