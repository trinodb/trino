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
package io.prestosql.server.security.jwt;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SigningKeyResolver;
import io.prestosql.server.security.AuthenticationException;
import io.prestosql.server.security.Authenticator;
import io.prestosql.server.security.UserMapping;
import io.prestosql.server.security.UserMappingException;
import io.prestosql.spi.security.BasicPrincipal;
import io.prestosql.spi.security.Identity;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static io.prestosql.server.security.UserMapping.createUserMapping;
import static java.util.Objects.requireNonNull;

public class JwtAuthenticator
        implements Authenticator
{
    private final JwtParser jwtParser;
    private final UserMapping userMapping;

    @Inject
    public JwtAuthenticator(JwtAuthenticatorConfig config, SigningKeyResolver signingKeyResolver)
    {
        requireNonNull(config, "config is null");
        this.userMapping = createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile());

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
    public Identity authenticate(ContainerRequestContext request)
            throws AuthenticationException
    {
        String header = nullToEmpty(request.getHeaders().getFirst(AUTHORIZATION));

        int space = header.indexOf(' ');
        if ((space < 0) || !header.substring(0, space).equalsIgnoreCase("bearer")) {
            throw needAuthentication(null);
        }
        String token = header.substring(space + 1).trim();
        if (token.isEmpty()) {
            throw needAuthentication(null);
        }

        try {
            Jws<Claims> claimsJws = jwtParser.parseClaimsJws(token);
            String subject = claimsJws.getBody().getSubject();
            String authenticatedUser = userMapping.mapUser(subject);
            return Identity.forUser(authenticatedUser)
                    .withPrincipal(new BasicPrincipal(subject))
                    .build();
        }
        catch (JwtException | UserMappingException e) {
            throw needAuthentication(e.getMessage());
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Authentication error", e);
        }
    }

    private static AuthenticationException needAuthentication(String message)
    {
        return new AuthenticationException(message, "Bearer realm=\"Presto\", token_type=\"JWT\"");
    }
}
