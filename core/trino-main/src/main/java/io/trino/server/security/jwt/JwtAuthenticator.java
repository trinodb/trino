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
import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.JwtParserBuilder;
import io.jsonwebtoken.Locator;
import io.trino.server.security.AbstractBearerAuthenticator;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.UserMapping;
import io.trino.server.security.UserMappingException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import jakarta.annotation.PreDestroy;
import jakarta.ws.rs.container.ContainerRequestContext;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.security.Key;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.jsonwebtoken.Claims.AUDIENCE;
import static io.trino.server.security.UserMapping.createUserMapping;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static java.lang.String.format;

public class JwtAuthenticator
        extends AbstractBearerAuthenticator
{
    private static final Logger log = Logger.get(JwtAuthenticator.class);

    private final List<JwtIssuer> issuers;
    private final List<JwkService> jwkServices;

    @Inject
    public JwtAuthenticator(JwtAuthenticatorConfig config, @ForJwt HttpClient httpClient)
    {
        ImmutableList.Builder<JwkService> jwkServicesBuilder = ImmutableList.builder();

        List<File> configFiles = config.getConfigFiles();
        if (configFiles.isEmpty()) {
            checkState(config.getKeyFile() != null,
                    "http-server.authentication.jwt.key-file or http-server.authentication.jwt.config-files must be set");
            Locator<Key> keyLocator = createKeyLocator(config.getKeyFile(), httpClient, jwkServicesBuilder);
            JwtParser parser = createJwtParser(keyLocator, config.getRequiredIssuer());
            issuers = ImmutableList.of(new JwtIssuer(
                    parser,
                    config.getPrincipalField(),
                    Optional.ofNullable(config.getRequiredAudience()),
                    createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile())));
        }
        else {
            ImmutableList.Builder<JwtIssuer> issuersBuilder = ImmutableList.builder();
            for (File configFile : configFiles) {
                Map<String, String> properties;
                try {
                    properties = loadPropertiesFrom(configFile.getPath());
                }
                catch (IOException e) {
                    throw new UncheckedIOException(format("Error reading JWT issuer configuration file %s", configFile), e);
                }

                String keyFile = properties.get("http-server.authentication.jwt.key-file");
                checkState(keyFile != null,
                        "http-server.authentication.jwt.key-file must be set in %s", configFile);

                Locator<Key> keyLocator = createKeyLocator(keyFile, httpClient, jwkServicesBuilder);
                String issuer = properties.get("http-server.authentication.jwt.required-issuer");
                JwtParser parser = createJwtParser(keyLocator, issuer);

                String principalField = properties.getOrDefault("http-server.authentication.jwt.principal-field", "sub");
                String audience = properties.get("http-server.authentication.jwt.required-audience");
                UserMapping userMapping = createUserMapping(
                        Optional.ofNullable(properties.get("http-server.authentication.jwt.user-mapping.pattern")),
                        Optional.ofNullable(properties.get("http-server.authentication.jwt.user-mapping.file")).map(File::new));

                issuersBuilder.add(new JwtIssuer(
                        parser,
                        principalField,
                        Optional.ofNullable(audience),
                        userMapping));
            }
            issuers = issuersBuilder.build();
        }

        jwkServices = jwkServicesBuilder.build();
        jwkServices.forEach(JwkService::start);
    }

    @PreDestroy
    public void stop()
    {
        jwkServices.forEach(JwkService::stop);
    }

    @Override
    protected Optional<Identity> createIdentity(String token)
            throws UserMappingException
    {
        JwtException lastException = null;
        for (JwtIssuer issuer : issuers) {
            try {
                Claims claims = issuer.parser().parseSignedClaims(token).getPayload();
                validateAudience(claims, issuer.requiredAudience());

                Optional<String> principal = Optional.ofNullable(claims.get(issuer.principalField(), String.class));
                if (principal.isEmpty()) {
                    return Optional.empty();
                }
                return Optional.of(Identity.forUser(issuer.userMapping().mapUser(principal.get()))
                        .withPrincipal(new BasicPrincipal(principal.get()))
                        .build());
            }
            catch (JwtException e) {
                log.debug(e, "JWT validation failed for issuer, trying next");
                lastException = e;
            }
        }
        if (lastException != null) {
            throw lastException;
        }
        return Optional.empty();
    }

    private static void validateAudience(Claims claims, Optional<String> requiredAudience)
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

    private static JwtParser createJwtParser(Locator<Key> keyLocator, String requiredIssuer)
    {
        JwtParserBuilder builder = newJwtParserBuilder()
                .keyLocator(keyLocator);
        if (requiredIssuer != null) {
            builder.requireIssuer(requiredIssuer);
        }
        return builder.build();
    }

    private static Locator<Key> createKeyLocator(String keyFile, HttpClient httpClient, ImmutableList.Builder<JwkService> jwkServices)
    {
        if (keyFile.startsWith("https://") || keyFile.startsWith("http://")) {
            JwkService jwkService = new JwkService(URI.create(keyFile), httpClient);
            jwkServices.add(jwkService);
            return new JwkSigningKeyLocator(jwkService);
        }
        return new FileSigningKeyLocator(keyFile);
    }

    @Override
    protected AuthenticationException needAuthentication(ContainerRequestContext request, Optional<String> currentToken, String message)
    {
        return new AuthenticationException(message, "Bearer realm=\"Trino\", token_type=\"JWT\"");
    }

    private record JwtIssuer(
            JwtParser parser,
            String principalField,
            Optional<String> requiredAudience,
            UserMapping userMapping) {}
}
