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
package io.trino.filesystem.azure;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotEmpty;

public class AzureAuthOAuthConfig
{
    private String clientEndpoint;
    private String clientId;
    private String clientSecret;

    @NotEmpty
    public String getClientEndpoint()
    {
        return clientEndpoint;
    }

    @ConfigSecuritySensitive
    @Config("azure.oauth.endpoint")
    public AzureAuthOAuthConfig setClientEndpoint(String clientEndpoint)
    {
        this.clientEndpoint = clientEndpoint;
        return this;
    }

    @NotEmpty
    public String getClientId()
    {
        return clientId;
    }

    @ConfigSecuritySensitive
    @Config("azure.oauth.client-id")
    public AzureAuthOAuthConfig setClientId(String clientId)
    {
        this.clientId = clientId;
        return this;
    }

    @NotEmpty
    public String getClientSecret()
    {
        return clientSecret;
    }

    @ConfigSecuritySensitive
    @Config("azure.oauth.secret")
    public AzureAuthOAuthConfig setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }
}
