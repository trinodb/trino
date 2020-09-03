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

import javax.validation.constraints.NotNull;

public class OAuth2Config
{
    private String authUrl;
    private String tokenUrl;
    private String clientId;
    private String clientSecret;

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
}
