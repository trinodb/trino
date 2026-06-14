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
package io.trino.server.security.jwt;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.JwtParserBuilder;
import io.jsonwebtoken.Locator;
import io.trino.server.security.AbstractBearerAuthenticator;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.UserMapping;
import io.trino.server.security.UserMappingException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;

import java.security.Key;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static io.jsonwebtoken.Claims.AUDIENCE;
import static io.trino.server.security.UserMapping.createUserMapping;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static java.lang.String.format;

public class JwtAuthenticator
        extends AbstractBearerAuthenticator
{
    private final JwtParser jwtParser;
    private final List<String> principalFields;
    private final UserMapping userMapping;
    private final Optional<String> requiredAudience;

    @Inject
    public JwtAuthenticator(JwtAuthenticatorConfig config, @ForJwt Locator<Key> signingKeyLocator)
    {
        principalFields = ImmutableList.copyOf(config.getPrincipalFields());
        requiredAudience = Optional.ofNullable(config.getRequiredAudience());

        JwtParserBuilder jwtParser = newJwtParserBuilder()
                .keyLocator(signingKeyLocator);

        if (config.getRequiredIssuer() != null) {
            jwtParser.requireIssuer(config.getRequiredIssuer());
        }
        this.jwtParser = jwtParser.build();
        userMapping = createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile());
    }

    @Override
    protected Optional<Identity> createIdentity(String token)
            throws UserMappingException
    {
        Claims claims = jwtParser.parseSignedClaims(token).getPayload();
        validateAudience(claims);

        Optional<String> principal = firstNonEmpty(claims, principalFields);
        if (principal.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(Identity.forUser(userMapping.mapUser(principal.get()))
                .withPrincipal(new BasicPrincipal(principal.get()))
                .build());
    }

    private static Optional<String> firstNonEmpty(Claims claims, List<String> fields)
    {
        for (String field : fields) {
            String value = claims.get(field, String.class);
            if (value != null && !value.isEmpty()) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }

    private void validateAudience(Claims claims)
    {
        if (requiredAudience.isEmpty()) {
            return;
        }

        Object tokenAudience = claims.get(AUDIENCE);
        switch (tokenAudience) {
            case String value -> {
                if (!requiredAudience.get().equals(value)) {
                    throw new InvalidClaimException(format("Invalid Audience: %s. Allowed audiences: %s", tokenAudience, requiredAudience.get()));
                }
            }
            case Collection<?> collection -> {
                if (collection.stream().noneMatch(aud -> requiredAudience.get().equals(aud))) {
                    throw new InvalidClaimException(format("Invalid Audience: %s. Allowed audiences: %s", tokenAudience, requiredAudience.get()));
                }
            }
            case null -> throw new InvalidClaimException(format("Expected %s claim to be: %s, but was not present in the JWT claims.", AUDIENCE, requiredAudience.get()));
            default -> throw new InvalidClaimException(format("Invalid Audience: %s", tokenAudience));
        }
    }

    @Override
    protected AuthenticationException needAuthentication(ContainerRequestContext request, Optional<String> currentToken, String message)
    {
        return new AuthenticationException(message, "Bearer realm=\"Trino\", token_type=\"JWT\"");
    }
}
