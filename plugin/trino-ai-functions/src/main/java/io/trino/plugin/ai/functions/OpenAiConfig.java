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

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

import java.net.URI;

public class OpenAiConfig
{
    private URI endpoint = URI.create("https://api.openai.com");
    private String apiKey;
    private String oauth2Audience;
    private String oauth2Scope;

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
    public OpenAiConfig setApiKey(String apiKey)
    {
        this.apiKey = apiKey;
        return this;
    }

    public String getOAuth2Audience()
    {
        return oauth2Audience;
    }

    @Config("ai.openai.oauth2.audience")
    public OpenAiConfig setOAuth2Audience(String oauth2Audience)
    {
        this.oauth2Audience = oauth2Audience;
        return this;
    }

    public String getOAuth2Scope()
    {
        return oauth2Scope;
    }

    @Config("ai.openai.oauth2.scope")
    public OpenAiConfig setOAuth2Scope(String oauth2Scope)
    {
        this.oauth2Scope = oauth2Scope;
        return this;
    }
}
