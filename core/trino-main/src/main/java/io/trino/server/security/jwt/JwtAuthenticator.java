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
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SigningKeyResolver;
import io.trino.server.security.AbstractBearerAuthenticator;
import io.trino.server.security.AuthenticationException;
import io.trino.spi.security.BasicPrincipal;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import java.security.Principal;
import java.util.Optional;

import static io.trino.server.security.UserMapping.createUserMapping;

public class JwtAuthenticator
        extends AbstractBearerAuthenticator
{
    private final JwtParser jwtParser;
    private final String principalField;

    @Inject
    public JwtAuthenticator(JwtAuthenticatorConfig config, SigningKeyResolver signingKeyResolver)
    {
        super(createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile()));
        principalField = config.getPrincipalField();

        JwtParser jwtParser = Jwts.parser()
                .setSigningKeyResolver(signingKeyResolver);

        if (config.getRequiredIssuer() != null) {
            jwtParser.requireIssuer(config.getRequiredIssuer());
        }
        if (config.getRequiredAudience() != null) {
            jwtParser.requireAudience(config.getRequiredAudience());
        }
        this.jwtParser = jwtParser;
    }

    @Override
    protected Optional<Principal> extractPrincipalFromToken(String token)
    {
        return Optional.ofNullable(jwtParser.parseClaimsJws(token)
                .getBody()
                .get(principalField, String.class))
                .map(BasicPrincipal::new);
    }

    @Override
    protected AuthenticationException needAuthentication(ContainerRequestContext request, String message)
    {
        return new AuthenticationException(message, "Bearer realm=\"Trino\", token_type=\"JWT\"");
    }
}
