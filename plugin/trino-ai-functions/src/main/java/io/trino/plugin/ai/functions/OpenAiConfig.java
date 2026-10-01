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
package io.trino.plugin.ai.functions;

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.List;

public class OpenAiConfig
{
    private URI endpoint = URI.create("https://api.openai.com");
    private String apiKey;
    private boolean useOauth2TokenExchange;
    private List<String> oauth2TokenExchangeAudience = ImmutableList.of();
    private List<String> oauth2TokenExchangeScope = ImmutableList.of();

    @NotNull
    public URI getEndpoint()
    {
        return endpoint;
    }

    @Config("ai.openai.endpoint")
    public OpenAiConfig setEndpoint(URI endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public String getApiKey()
    {
        return apiKey;
    }

    @Config("ai.openai.api-key")
    @ConfigDescription("API key used to access the OpenAI API. Not required when oauth2 token exchange is enabled")
    public OpenAiConfig setApiKey(String apiKey)
    {
        this.apiKey = apiKey;
        return this;
    }

    public boolean isUseOauth2TokenExchange()
    {
        return useOauth2TokenExchange;
    }

    @Config("ai.openai.oauth2-token-exchange")
    @ConfigDescription("Resolve the API key per user by exchanging the user's OAuth2 access token. Requires oauth2-token-exchange.enabled in config.properties and http-server.authentication.oauth2.capture-access-token=true")
    public OpenAiConfig setUseOauth2TokenExchange(boolean useOauth2TokenExchange)
    {
        this.useOauth2TokenExchange = useOauth2TokenExchange;
        return this;
    }

    public List<String> getOauth2TokenExchangeAudience()
    {
        return oauth2TokenExchangeAudience;
    }

    @Config("ai.openai.oauth2-token-exchange.audience")
    @ConfigDescription("Audience of the exchanged token. Falls back to the default audience from oauth2-token-exchange.audience when not set")
    public OpenAiConfig setOauth2TokenExchangeAudience(List<String> oauth2TokenExchangeAudience)
    {
        this.oauth2TokenExchangeAudience = ImmutableList.copyOf(oauth2TokenExchangeAudience);
        return this;
    }

    public List<String> getOauth2TokenExchangeScope()
    {
        return oauth2TokenExchangeScope;
    }

    @Config("ai.openai.oauth2-token-exchange.scope")
    @ConfigDescription("Scope of the exchanged token. Falls back to the default scope from oauth2-token-exchange.scope when not set")
    public OpenAiConfig setOauth2TokenExchangeScope(List<String> oauth2TokenExchangeScope)
    {
        this.oauth2TokenExchangeScope = ImmutableList.copyOf(oauth2TokenExchangeScope);
        return this;
    }
}
