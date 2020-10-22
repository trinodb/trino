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
package io.prestosql.server.security.oauth2;

import io.airlift.log.Logger;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import io.prestosql.server.security.AuthenticationException;
import io.prestosql.server.security.Authenticator;
import io.prestosql.server.security.RedirectAuthenticationException;
import io.prestosql.server.security.UserMapping;
import io.prestosql.server.security.UserMappingException;
import io.prestosql.server.security.oauth2.JWKSSigningKeyResolver.UncheckedJwkException;
import io.prestosql.spi.security.BasicPrincipal;
import io.prestosql.spi.security.Identity;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Cookie;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.prestosql.server.security.oauth2.OAuth2Resource.OAUTH2_API_PREFIX;
import static io.prestosql.server.security.oauth2.OAuth2Resource.OAUTH2_COOKIE;
import static io.prestosql.server.security.oauth2.OAuth2Resource.TOKENS_ENDPOINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;
import static javax.ws.rs.core.HttpHeaders.AUTHORIZATION;

public class OAuth2Authenticator
        implements Authenticator
{
    private static final Logger LOG = Logger.get(OAuth2Authenticator.class);

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
        String principal = getAccessToken(request)
                .map(token -> token.getBody().getSubject())
                .orElseThrow(() -> {
                    Challenge.Started challenge = service.startChallenge(request.getUriInfo().getBaseUri());
                    return new RedirectAuthenticationException(
                            "Unauthorized",
                            format("External-Bearer redirectUrl=\"%s\", tokenUrl=\"%s?state=%s\"",
                                    challenge.getAuthorizationUrl(),
                                    OAUTH2_API_PREFIX + TOKENS_ENDPOINT,
                                    challenge.getState()),
                            challenge.getAuthorizationUrl());
                });
        try {
            return Identity.forUser(userMapping.mapUser(principal))
                    .withPrincipal(new BasicPrincipal(principal))
                    .build();
        }
        catch (UserMappingException e) {
            throw new AuthenticationException(e.getMessage());
        }
    }

    private Optional<Jws<Claims>> getAccessToken(ContainerRequestContext request)
    {
        Stream<String> accessTokenSources = Stream.concat(Stream.concat(
                getTokenFromCookie(request),
                getTokenFromHeader(request)),
                getTokenFromQueryParam(request));
        return accessTokenSources
                .filter(not(String::isBlank))
                .map(token -> {
                    try {
                        return Optional.ofNullable(service.parseClaimsJws(token));
                    }
                    catch (JwtException | IllegalArgumentException | UncheckedJwkException e) {
                        LOG.debug("Unable to parse JWT token: " + e.getMessage(), e);
                        return Optional.<Jws<Claims>>empty();
                    }
                })
                .findFirst()
                .flatMap(Function.identity());
    }

    private Stream<String> getTokenFromCookie(ContainerRequestContext request)
    {
        return Stream.ofNullable(request.getCookies().get(OAUTH2_COOKIE))
                .map(Cookie::getValue);
    }

    private Stream<String> getTokenFromHeader(ContainerRequestContext request)
    {
        return Stream.ofNullable(request.getHeaders().get(AUTHORIZATION))
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .filter(header -> header.startsWith("Bearer "))
                .map(header -> header.substring("Bearer ".length()));
    }

    private Stream<String> getTokenFromQueryParam(ContainerRequestContext request)
    {
        return Stream.ofNullable(request.getUriInfo().getQueryParameters().get("access_token"))
                .flatMap(Collection::stream)
                .filter(Objects::nonNull);
    }
}
