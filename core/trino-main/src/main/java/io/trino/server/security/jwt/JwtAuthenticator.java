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

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.JwtParserBuilder;
import io.jsonwebtoken.SigningKeyResolver;
import io.trino.server.security.AbstractBearerAuthenticator;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.UserMapping;
import io.trino.server.security.UserMappingException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import java.util.Collection;
import java.util.Optional;

import static io.jsonwebtoken.Claims.AUDIENCE;
import static io.trino.server.security.UserMapping.createUserMapping;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static java.lang.String.format;

public class JwtAuthenticator
        extends AbstractBearerAuthenticator
{
    private final JwtParser jwtParser;
    private final String principalField;
    private final UserMapping userMapping;
    private final Optional<String> requiredAudience;

    @Inject
    public JwtAuthenticator(JwtAuthenticatorConfig config, @ForJwt SigningKeyResolver signingKeyResolver)
    {
        principalField = config.getPrincipalField();
        requiredAudience = Optional.ofNullable(config.getRequiredAudience());

        JwtParserBuilder jwtParser = newJwtParserBuilder()
                .setSigningKeyResolver(signingKeyResolver);

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
        Claims claims = jwtParser.parseClaimsJws(token).getBody();
        validateAudience(claims);

        Optional<String> principal = Optional.ofNullable(claims.get(principalField, String.class));
        if (principal.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(Identity.forUser(userMapping.mapUser(principal.get()))
                .withPrincipal(new BasicPrincipal(principal.get()))
                .build());
    }

    private void validateAudience(Claims claims)
    {
        if (requiredAudience.isEmpty()) {
            return;
        }

        Object tokenAudience = claims.get(AUDIENCE);
        if (tokenAudience == null) {
            throw new InvalidClaimException(format("Expected %s claim to be: %s, but was not present in the JWT claims.", AUDIENCE, requiredAudience.get()));
        }

        if (tokenAudience instanceof String) {
            if (!requiredAudience.get().equals((String) tokenAudience)) {
                throw new InvalidClaimException(format("Invalid Audience: %s. Allowed audiences: %s", tokenAudience, requiredAudience.get()));
            }
        }
        else if (tokenAudience instanceof Collection) {
            if (((Collection<?>) tokenAudience).stream().map(String.class::cast).noneMatch(aud -> requiredAudience.get().equals(aud))) {
                throw new InvalidClaimException(format("Invalid Audience: %s. Allowed audiences: %s", tokenAudience, requiredAudience.get()));
            }
        }
        else {
            throw new InvalidClaimException(format("Invalid Audience: %s", tokenAudience));
        }
    }

    @Override
    protected AuthenticationException needAuthentication(ContainerRequestContext request, Optional<String> currentToken, String message)
    {
        return new AuthenticationException(message, "Bearer realm=\"Trino\", token_type=\"JWT\"");
    }
}
