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
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.net.URI;

public class AnthropicConfig
{
    private URI endpoint = URI.create("https://api.anthropic.com");
    private String apiKey;

    @NotNull
    public URI getEndpoint()
    {
        return endpoint;
    }

    @Config("ai.anthropic.endpoint")
    public AnthropicConfig setEndpoint(URI endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    @NotEmpty
    public String getApiKey()
    {
        return apiKey;
    }

    @Config("ai.anthropic.api-key")
    public AnthropicConfig setApiKey(String apiKey)
    {
        this.apiKey = apiKey;
        return this;
    }
}
