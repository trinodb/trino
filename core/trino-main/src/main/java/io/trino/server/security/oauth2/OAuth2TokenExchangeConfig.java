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
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class OAuth2TokenExchangeConfig
{
    private boolean enabled;
    private URI tokenEndpoint;
    private String clientId;
    private Optional<String> clientSecret = Optional.empty();
    private Duration cacheRefreshMargin = new Duration(1, TimeUnit.MINUTES);

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("oauth2-token-exchange.enabled")
    @ConfigDescription("Enable the OAuth2 token exchange service for connectors")
    public OAuth2TokenExchangeConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public URI getTokenEndpoint()
    {
        return tokenEndpoint;
    }

    @Config("oauth2-token-exchange.token-endpoint")
    @ConfigDescription("Authorization server token endpoint used for RFC 8693 token exchange")
    public OAuth2TokenExchangeConfig setTokenEndpoint(URI tokenEndpoint)
    {
        this.tokenEndpoint = tokenEndpoint;
        return this;
    }

    public String getClientId()
    {
        return clientId;
    }

    @Config("oauth2-token-exchange.client-id")
    @ConfigDescription("OAuth2 client ID used to perform the token exchange")
    public OAuth2TokenExchangeConfig setClientId(String clientId)
    {
        this.clientId = clientId;
        return this;
    }

    public Optional<String> getClientSecret()
    {
        return clientSecret;
    }

    @Config("oauth2-token-exchange.client-secret")
    @ConfigSecuritySensitive
    @ConfigDescription("OAuth2 client secret used to perform the token exchange")
    public OAuth2TokenExchangeConfig setClientSecret(String clientSecret)
    {
        this.clientSecret = Optional.ofNullable(clientSecret);
        return this;
    }

    @MinDuration("0s")
    public Duration getCacheRefreshMargin()
    {
        return cacheRefreshMargin;
    }

    @Config("oauth2-token-exchange.cache-refresh-margin")
    @ConfigDescription("How long before expiration an exchanged token is refreshed from cache")
    public OAuth2TokenExchangeConfig setCacheRefreshMargin(Duration cacheRefreshMargin)
    {
        this.cacheRefreshMargin = cacheRefreshMargin;
        return this;
    }
}
