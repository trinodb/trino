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
import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;
import io.jsonwebtoken.JwtException;
import io.trino.server.security.AbstractBearerAuthenticator;
import io.trino.server.security.AuthenticationException;
import io.trino.server.security.UserMapping;
import io.trino.server.security.UserMappingException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;

public class JwtAuthenticator
        extends AbstractBearerAuthenticator
{

    public static final String JWT_KEY_FILE = "http-server.authentication.jwt.key-file";
    public static final String JWT_REQUIRED_ISSUER = "http-server.authentication.jwt.required-issuer";
    public static final String JWT_REQUIRED_AUDIENCE = "http-server.authentication.jwt.required-audience";
    public static final String JWT_PRINCIPAL_FIELD = "http-server.authentication.jwt.principal-field";
    public static final String JWT_USER_MAPPING_PATTERN = "http-server.authentication.jwt.user-mapping.pattern";
    public static final String JWT_USER_MAPPING_FILE = "http-server.authentication.jwt.user-mapping.file";
    private static final String TOKEN_TYPE = "http-server.token.type";
    private static final Logger log = Logger.get(JwtAuthenticatorConfig.class);

    private final List<File> configFiles;
    private List<JwtTokenValidator> tokenValidators;
    private final HttpClient httpClient;

    @Inject
    public JwtAuthenticator(JwtAuthenticatorConfig config, @ForJwt HttpClient httpClient)
    {
        this.configFiles = ImmutableList.copyOf(config.getJwtIdpConfigFiles());
        this.httpClient = httpClient;
        setTokenIssuers(config);
    }

    private void setTokenIssuers(JwtAuthenticatorConfig config)
    {
        List<File> configFiles = this.configFiles;
        tokenValidators = new ArrayList<>();
        //TODO: remove the setLegacyConfigurations once deprecated properties are removed
        if (configFiles.isEmpty()) {
            JwtTokenValidator jwtTokenValidator = TokenFactory.getJwtToken(TokenTypeEnum.JWT);
            jwtTokenValidator.setLegacyConfigurations(config);
            tokenValidators.add(jwtTokenValidator);
        }
        configFiles.forEach(this::createIdp);
    }

    private void createIdp(File configFile)
    {
        Map<String, Object> properties = loadPropertyFile(configFile);
        String tokenType = (String) properties.get(TOKEN_TYPE);
        TokenTypeEnum tokenTypeEnum = TokenTypeEnum.fromTokenType(tokenType);
        JwtTokenValidator jwtTokenValidator = TokenFactory.getJwtToken(tokenTypeEnum);
        jwtTokenValidator.setRequiredProperties(properties, configFile);
        jwtTokenValidator.setOptionalProperties(properties);
        tokenValidators.add(jwtTokenValidator);
    }

    @Override
    protected Optional<Identity> createIdentity(String givenToken)
            throws UserMappingException, JwtException
    {
        Optional<Identity> identity = Optional.empty();
        PrincipalUserMapping validatedPrincipalUserMapping = tokenValidators.stream()
                .map(tokenValidator -> new PrincipalUserMapping(tokenValidator.validateAndCreatePrincipal(givenToken, httpClient), Optional.of(tokenValidator.getUserMapping())))
                .filter(principalUserMapping -> principalUserMapping.getPrincipal().isPresent())
                .findFirst().orElse(new PrincipalUserMapping(Optional.empty(), Optional.empty()));

        Optional<String> principal = validatedPrincipalUserMapping.getPrincipal();
        if (principal.isPresent()) {
            UserMapping userMapping = validatedPrincipalUserMapping.getUserMapping().get();
            identity = Optional.of(Identity.forUser(userMapping.mapUser(principal.get()))
                    .withPrincipal(new BasicPrincipal(principal.get()))
                    .build());
        }
        return identity;
    }

    @Override
    protected AuthenticationException needAuthentication(ContainerRequestContext request, Optional<String> currentToken, String message)
    {
        return new AuthenticationException(message, "Bearer realm=\"Trino\", token_type=\"JWT\"");
    }

    private Map<String, Object> loadPropertyFile(File configFile)
    {
        log.info("-- Loading IDP properties %s --", configFile);
        configFile = configFile.getAbsoluteFile();
        Map<String, Object> properties;
        try {
            properties = new HashMap<>(loadPropertiesFrom(configFile.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read configuration file: " + configFile, e);
        }
        return properties;
    }
}
