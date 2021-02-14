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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.server.security.oauth2.OAuth2Service.OPENID_SCOPE;

public class OAuth2Config
{
    private Optional<String> stateKey = Optional.empty();
    private String authUrl;
    private String tokenUrl;
    private String jwksUrl;
    private String clientId;
    private String clientSecret;
    private Optional<String> audience = Optional.empty();
    private Set<String> scopes = ImmutableSet.of(OPENID_SCOPE);
    private String principalField = "sub";
    private Duration challengeTimeout = new Duration(15, TimeUnit.MINUTES);
    private Optional<String> userMappingPattern = Optional.empty();
    private Optional<File> userMappingFile = Optional.empty();

    public Optional<String> getStateKey()
    {
        return stateKey;
    }

    @Config("http-server.authentication.oauth2.state-key")
    @ConfigDescription("A secret key used by HMAC algorithm to sign the state parameter")
    public OAuth2Config setStateKey(String stateKey)
    {
        this.stateKey = Optional.ofNullable(stateKey);
        return this;
    }

    @NotNull
    public String getAuthUrl()
    {
        return authUrl;
    }

    @Config("http-server.authentication.oauth2.auth-url")
    @ConfigDescription("URL of the authorization server's authorization endpoint")
    public OAuth2Config setAuthUrl(String authUrl)
    {
        this.authUrl = authUrl;
        return this;
    }

    @NotNull
    public String getTokenUrl()
    {
        return tokenUrl;
    }

    @Config("http-server.authentication.oauth2.token-url")
    @ConfigDescription("URL of the authorization server's token endpoint")
    public OAuth2Config setTokenUrl(String tokenUrl)
    {
        this.tokenUrl = tokenUrl;
        return this;
    }

    @NotNull
    public String getJwksUrl()
    {
        return jwksUrl;
    }

    @Config("http-server.authentication.oauth2.jwks-url")
    @ConfigDescription("URL of the authorization server's JWKS (JSON Web Key Set) endpoint")
    public OAuth2Config setJwksUrl(String jwksUrl)
    {
        this.jwksUrl = jwksUrl;
        return this;
    }

    @NotNull
    public String getClientId()
    {
        return clientId;
    }

    @Config("http-server.authentication.oauth2.client-id")
    @ConfigDescription("Client ID")
    public OAuth2Config setClientId(String clientId)
    {
        this.clientId = clientId;
        return this;
    }

    @NotNull
    public String getClientSecret()
    {
        return clientSecret;
    }

    @Config("http-server.authentication.oauth2.client-secret")
    @ConfigSecuritySensitive
    @ConfigDescription("Client secret")
    public OAuth2Config setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }

    public Optional<String> getAudience()
    {
        return audience;
    }

    @Config("http-server.authentication.oauth2.audience")
    @ConfigDescription("The required audience of a token")
    public OAuth2Config setAudience(String audience)
    {
        this.audience = Optional.ofNullable(audience);
        return this;
    }

    @NotNull
    public Set<String> getScopes()
    {
        return scopes;
    }

    @Config("http-server.authentication.oauth2.scopes")
    @ConfigDescription("Scopes requested by the server during OAuth2 authorization challenge")
    public OAuth2Config setScopes(String scopes)
    {
        this.scopes = Splitter.on(',').trimResults().omitEmptyStrings().splitToStream(scopes).collect(toImmutableSet());
        return this;
    }

    @NotNull
    public String getPrincipalField()
    {
        return principalField;
    }

    @Config("http-server.authentication.oauth2.principal-field")
    public OAuth2Config setPrincipalField(String principalField)
    {
        this.principalField = principalField;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getChallengeTimeout()
    {
        return challengeTimeout;
    }

    @Config("http-server.authentication.oauth2.challenge-timeout")
    @ConfigDescription("Maximum duration of OAuth2 authorization challenge")
    public OAuth2Config setChallengeTimeout(Duration challengeTimeout)
    {
        this.challengeTimeout = challengeTimeout;
        return this;
    }

    public Optional<String> getUserMappingPattern()
    {
        return userMappingPattern;
    }

    @Config("http-server.authentication.oauth2.user-mapping.pattern")
    @ConfigDescription("Regex to match against user name")
    public OAuth2Config setUserMappingPattern(String userMappingPattern)
    {
        this.userMappingPattern = Optional.ofNullable(userMappingPattern);
        return this;
    }

    public Optional<@FileExists File> getUserMappingFile()
    {
        return userMappingFile;
    }

    @Config("http-server.authentication.oauth2.user-mapping.file")
    @ConfigDescription("File containing rules for mapping user")
    public OAuth2Config setUserMappingFile(File userMappingFile)
    {
        this.userMappingFile = Optional.ofNullable(userMappingFile);
        return this;
    }
}
