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

import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.JwtParserBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SigningKeyResolver;
import io.trino.server.security.AbstractBearerAuthenticator;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.UserMapping;
import io.trino.server.security.UserMappingException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import java.util.Optional;

import static io.trino.server.security.UserMapping.createUserMapping;

public class JwtAuthenticator
        extends AbstractBearerAuthenticator
{
    private final JwtParser jwtParser;
    private final String principalField;
    private final UserMapping userMapping;

    @Inject
    public JwtAuthenticator(JwtAuthenticatorConfig config, SigningKeyResolver signingKeyResolver)
    {
        principalField = config.getPrincipalField();

        JwtParserBuilder jwtParser = Jwts.parserBuilder()
                .setSigningKeyResolver(signingKeyResolver);

        if (config.getRequiredIssuer() != null) {
            jwtParser.requireIssuer(config.getRequiredIssuer());
        }
        if (config.getRequiredAudience() != null) {
            jwtParser.requireAudience(config.getRequiredAudience());
        }
        this.jwtParser = jwtParser.build();
        userMapping = createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile());
    }

    @Override
    protected Optional<Identity> createIdentity(String token)
            throws UserMappingException
    {
        Optional<String> principal = Optional.ofNullable(jwtParser.parseClaimsJws(token)
                .getBody()
                .get(principalField, String.class));
        if (principal.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(Identity.forUser(userMapping.mapUser(principal.get()))
                .withPrincipal(new BasicPrincipal(principal.get()))
                .build());
    }

    @Override
    protected AuthenticationException needAuthentication(ContainerRequestContext request, String message)
    {
        return new AuthenticationException(message, "Bearer realm=\"Trino\", token_type=\"JWT\"");
    }
}
