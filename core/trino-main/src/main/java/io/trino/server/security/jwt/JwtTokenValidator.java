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

import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.JwtParserBuilder;
import io.jsonwebtoken.SigningKeyResolver;
import io.trino.server.security.UserMapping;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.server.security.UserMapping.createUserMapping;
import static io.trino.server.security.jwt.JwtAuthenticator.JWT_KEY_FILE;
import static io.trino.server.security.jwt.JwtAuthenticator.JWT_PRINCIPAL_FIELD;
import static io.trino.server.security.jwt.JwtAuthenticator.JWT_REQUIRED_AUDIENCE;
import static io.trino.server.security.jwt.JwtAuthenticator.JWT_REQUIRED_ISSUER;
import static io.trino.server.security.jwt.JwtAuthenticator.JWT_USER_MAPPING_FILE;
import static io.trino.server.security.jwt.JwtAuthenticator.JWT_USER_MAPPING_PATTERN;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;

public class JwtTokenValidator
{
    private static final int JWK_CLOCK_SKEW_SECONDS = 2 * 60; // 2 minutes
    private static final Logger log = Logger.get(JwtTokenValidator.class);

    protected String keyFile;
    protected String requiredIssuer;
    protected String requiredAudience;
    protected String principalField;
    protected UserMapping userMapping;

    public UserMapping getUserMapping()
    {
        return userMapping;
    }

    public Optional<String> validateAndCreatePrincipal(String token, HttpClient httpClient)
    {
        Optional<String> principal = Optional.empty();
        JwtParser jwtParser = setJwtParser(httpClient);
        String principalField = this.principalField != null ? this.principalField : "sub";
        try {
            principal = Optional.ofNullable(jwtParser.parseClaimsJws(token)
                    .getBody()
                    .get(principalField, String.class));
        }
        catch (JwtException e) {
            log.info("Error occurred validating claim. Verified for the idp " + requiredIssuer + " configured." + e);
        }
        return principal;
    }

    private JwtParser setJwtParser(HttpClient httpClient)
    {
        SigningKeyResolver signingKeyResolver;
        if (keyFile.startsWith("https://") || keyFile.startsWith("http://")) {
            JwkService jwkService = new JwkService((URI.create(keyFile)), httpClient);
            signingKeyResolver = new JwkSigningKeyResolver(jwkService);
        }
        else {
            signingKeyResolver = new FileSigningKeyResolver(keyFile);
        }
        JwtParserBuilder jwtParserBuilder = newJwtParserBuilder()
                .setAllowedClockSkewSeconds(JWK_CLOCK_SKEW_SECONDS)
                .setSigningKeyResolver(signingKeyResolver);

        Optional.ofNullable(requiredIssuer).ifPresent(jwtParserBuilder::requireIssuer);
        Optional.ofNullable(requiredAudience).ifPresent(jwtParserBuilder::requireAudience);
        return jwtParserBuilder.build();
    }

    protected void setRequiredProperties(Map<String, Object> properties, File configFile)
    {
        String requiredIssuer = (String) properties.get(JWT_REQUIRED_ISSUER);
        checkState(!isNullOrEmpty(requiredIssuer), "Configuration does not contain '%s' property: %s", JWT_REQUIRED_ISSUER, configFile);
        this.requiredIssuer = requiredIssuer;

        String jwtKeyFile = (String) properties.get(JWT_KEY_FILE);
        checkState(!isNullOrEmpty(jwtKeyFile), "Configuration does not contain '%s' property: %s", JWT_KEY_FILE, configFile);
        this.keyFile = jwtKeyFile;
    }

    protected void setOptionalProperties(Map<String, Object> properties)
    {
        if (properties.get(JWT_PRINCIPAL_FIELD) != null) {
            this.principalField = (String) properties.get(JWT_PRINCIPAL_FIELD);
        }
        if (properties.get(JWT_REQUIRED_AUDIENCE) != null) {
            this.requiredAudience = (String) properties.get(JWT_REQUIRED_AUDIENCE);
        }

        Optional<String> jwtUserMappingPattern = Optional.ofNullable((String) properties.get(JWT_USER_MAPPING_PATTERN));
        Optional<String> jwtUserMappingFilePath = Optional.ofNullable((String) properties.get(JWT_USER_MAPPING_FILE));
        Optional<File> jwtUserMappingFile = jwtUserMappingFilePath.isPresent() ? Optional.ofNullable(new File(jwtUserMappingFilePath.get())) : Optional.empty();
        this.userMapping = createUserMapping(jwtUserMappingPattern, jwtUserMappingFile);
    }

    //TODO: Remove this method once legacy configs are deprecated & removed.
    protected void setLegacyConfigurations(JwtAuthenticatorConfig config)
    {
        checkState(!isNullOrEmpty(config.getKeyFile()), "http-server.authentication.jwt.key-file must not be null");
        this.requiredIssuer = config.getRequiredIssuer();
        this.keyFile = config.getKeyFile();
        if (config.getPrincipalField() != null) {
            this.principalField = config.getPrincipalField();
        }
        if (config.getRequiredAudience() != null) {
            this.requiredAudience = config.getRequiredAudience();
        }
        this.userMapping = createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile());
    }
}
