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
package io.trino.plugin.iceberg.catalog.rest;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.AssertTrue;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.net.URI;
import java.util.Optional;

public class OAuth2SecurityConfig
{
    private String credential;
    private String scope;
    private String token;
    private URI serverUri;
    private boolean tokenRefreshEnabled = OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT;

    public Optional<String> getCredential()
    {
        return Optional.ofNullable(credential);
    }

    @Config("iceberg.rest-catalog.oauth2.credential")
    @ConfigDescription("The credential to exchange for a token in the OAuth2 client credentials flow with the server")
    @ConfigSecuritySensitive
    public OAuth2SecurityConfig setCredential(String credential)
    {
        this.credential = credential;
        return this;
    }

    public Optional<String> getScope()
    {
        return Optional.ofNullable(scope);
    }

    @Config("iceberg.rest-catalog.oauth2.scope")
    @ConfigDescription("The scope which will be used for interactions with the server")
    public OAuth2SecurityConfig setScope(String scope)
    {
        this.scope = scope;
        return this;
    }

    public Optional<String> getToken()
    {
        return Optional.ofNullable(token);
    }

    @Config("iceberg.rest-catalog.oauth2.token")
    @ConfigDescription("The Bearer token which will be used for interactions with the server")
    @ConfigSecuritySensitive
    public OAuth2SecurityConfig setToken(String token)
    {
        this.token = token;
        return this;
    }

    public Optional<URI> getServerUri()
    {
        return Optional.ofNullable(serverUri);
    }

    @Config("iceberg.rest-catalog.oauth2.server-uri")
    @ConfigDescription("The endpoint to retrieve access token from OAuth2 Server")
    public OAuth2SecurityConfig setServerUri(URI serverUri)
    {
        this.serverUri = serverUri;
        return this;
    }

    public boolean isTokenRefreshEnabled()
    {
        return tokenRefreshEnabled;
    }

    @Config("iceberg.rest-catalog.oauth2.token-refresh-enabled")
    @ConfigDescription("Controls whether a token should be refreshed if information about its expiration time is available")
    public OAuth2SecurityConfig setTokenRefreshEnabled(boolean tokenRefreshEnabled)
    {
        this.tokenRefreshEnabled = tokenRefreshEnabled;
        return this;
    }

    @AssertTrue(message = "OAuth2 requires a credential or token")
    public boolean credentialOrTokenPresent()
    {
        return credential != null || token != null;
    }

    @AssertTrue(message = "Scope is applicable only when using credential")
    public boolean scopePresentOnlyWithCredential()
    {
        return !(token != null && scope != null);
    }
}
