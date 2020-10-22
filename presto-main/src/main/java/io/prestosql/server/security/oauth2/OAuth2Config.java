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
package io.prestosql.server.security.oauth2;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OAuth2Config
{
    private String serverUrl;
    private String authUrl;
    private String tokenUrl;
    private String clientId;
    private String clientSecret;
    private Duration challengeTimeout = new Duration(15, TimeUnit.MINUTES);
    private Optional<String> userMappingPattern = Optional.empty();
    private Optional<File> userMappingFile = Optional.empty();
    private Duration maxSinglePollDuration = Duration.succinctDuration(3, SECONDS);

    @NotNull
    public String getServerUrl()
    {
        return serverUrl;
    }

    @Config("http-server.authentication.oauth2.server-url")
    @ConfigDescription("URL of the authorization server")
    public OAuth2Config setServerUrl(String serverUrl)
    {
        this.serverUrl = serverUrl;
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
    public String getClientId()
    {
        return clientId;
    }

    @Config("http-server.authentication.oauth2.client-id")
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
    public OAuth2Config setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }

    @MinDuration("1ms")
    public Duration getChallengeTimeout()
    {
        return challengeTimeout;
    }

    @Config("http-server.authentication.oauth2.challenge-timeout")
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
    public OAuth2Config setUserMappingFile(File userMappingFile)
    {
        this.userMappingFile = Optional.ofNullable(userMappingFile);
        return this;
    }

    @NotNull
    @MinDuration("3s")
    public Duration getMaxSinglePollDuration()
    {
        return maxSinglePollDuration;
    }

    public long getMaxSinglePollMs()
    {
        return maxSinglePollDuration.roundTo(MILLISECONDS);
    }

    @Config("http-server.authentication.oauth2.token.polling.max-single-duration")
    @ConfigDescription("Maximum duration of a single poll token request - when token is not ready yet")
    public OAuth2Config setMaxSinglePollDuration(Duration duration)
    {
        this.maxSinglePollDuration = duration;
        return this;
    }
}
