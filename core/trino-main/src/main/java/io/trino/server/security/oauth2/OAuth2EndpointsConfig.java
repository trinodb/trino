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

import javax.validation.constraints.NotNull;

import java.util.Optional;

public class OAuth2EndpointsConfig
{
    private Optional<String> accessTokenIssuer = Optional.empty();
    private String authUrl;
    private String tokenUrl;
    private String jwksUrl;
    private Optional<String> userinfoUrl = Optional.empty();

    public Optional<String> getAccessTokenIssuer()
    {
        return accessTokenIssuer;
    }

    @Config("http-server.authentication.oauth2.access-token-issuer")
    @ConfigDescription("The required issuer for access tokens")
    public OAuth2EndpointsConfig setAccessTokenIssuer(String accessTokenIssuer)
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
    public OAuth2EndpointsConfig setAuthUrl(String authUrl)
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
    public OAuth2EndpointsConfig setTokenUrl(String tokenUrl)
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
    public OAuth2EndpointsConfig setJwksUrl(String jwksUrl)
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
    public OAuth2EndpointsConfig setUserinfoUrl(String userinfoUrl)
    {
        this.userinfoUrl = Optional.ofNullable(userinfoUrl);
        return this;
    }
}
