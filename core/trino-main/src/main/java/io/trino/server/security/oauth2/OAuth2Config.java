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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class OAuth2Config
{
    private Optional<String> stateKey = Optional.empty();
    private String authUrl;
    private String tokenUrl;
    private String jwksUrl;
    private String clientId;
    private String clientSecret;
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
    @ConfigDescription("Client secret")
    public OAuth2Config setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getChallengeTimeout()
    {
        return challengeTimeout;
    }

    @Config("http-server.authentication.oauth2.challenge-timeout")
    @ConfigDescription("Maximum duration of OAuth2 login challenge")
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
