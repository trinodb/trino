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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.LegacyConfig;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.server.security.oauth2.OAuth2Service.OPENID_SCOPE;

public class OAuth2Config
{
    private Optional<String> stateKey = Optional.empty();
    private String issuer;
    private Optional<String> accessTokenIssuer = Optional.empty();
    private String authUrl;
    private String tokenUrl;
    private String jwksUrl;
    private Optional<String> userinfoUrl = Optional.empty();
    private String clientId;
    private String clientSecret;
    private Set<String> scopes = ImmutableSet.of(OPENID_SCOPE);
    private String principalField = "sub";
    private Optional<String> groupsField = Optional.empty();
    private List<String> additionalAudiences = Collections.emptyList();
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
    public String getIssuer()
    {
        return issuer;
    }

    @Config("http-server.authentication.oauth2.issuer")
    @ConfigDescription("The required issuer of a token")
    public OAuth2Config setIssuer(String issuer)
    {
        this.issuer = issuer;
        return this;
    }

    @NotNull
    public Optional<String> getAccessTokenIssuer()
    {
        return accessTokenIssuer;
    }

    @Config("http-server.authentication.oauth2.access-token-issuer")
    @ConfigDescription("The required issuer for access tokens")
    public OAuth2Config setAccessTokenIssuer(String accessTokenIssuer)
    {
        this.accessTokenIssuer = Optional.ofNullable(accessTokenIssuer);
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

    public Optional<String> getUserinfoUrl()
    {
        return userinfoUrl;
    }

    @Config("http-server.authentication.oauth2.userinfo-url")
    @ConfigDescription("URL of the userinfo endpoint")
    public OAuth2Config setUserinfoUrl(String userinfoUrl)
    {
        this.userinfoUrl = Optional.ofNullable(userinfoUrl);
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

    @NotNull
    public List<String> getAdditionalAudiences()
    {
        return additionalAudiences;
    }

    @LegacyConfig("http-server.authentication.oauth2.audience")
    @Config("http-server.authentication.oauth2.additional-audiences")
    @ConfigDescription("Additional audiences to trust in addition to the Client ID")
    public OAuth2Config setAdditionalAudiences(List<String> additionalAudiences)
    {
        this.additionalAudiences = ImmutableList.copyOf(additionalAudiences);
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
    @ConfigDescription("The claim to use as the principal")
    public OAuth2Config setPrincipalField(String principalField)
    {
        this.principalField = principalField;
        return this;
    }

    public Optional<String> getGroupsField()
    {
        return groupsField;
    }

    @Config("http-server.authentication.oauth2.groups-field")
    @ConfigDescription("Groups field in the claim")
    public OAuth2Config setGroupsField(String groupsField)
    {
        this.groupsField = Optional.ofNullable(groupsField);
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
