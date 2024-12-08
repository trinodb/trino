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
import jakarta.validation.constraints.AssertTrue;

import java.util.Optional;

public class AzureAuthManagedIdentityConfig
{
    private String clientId;
    private String resourceId;

    public Optional<String> getClientId()
    {
        return Optional.ofNullable(clientId);
    }

    @ConfigSecuritySensitive
    @Config("azure.managed-identity.client-id")
    public AzureAuthManagedIdentityConfig setClientId(String clientId)
    {
        this.clientId = clientId;
        return this;
    }

    public Optional<String> getResourceId()
    {
        return Optional.ofNullable(resourceId);
    }

    @ConfigSecuritySensitive
    @Config("azure.managed-identity.resource-id")
    public AzureAuthManagedIdentityConfig setResourceId(String resourceId)
    {
        this.resourceId = resourceId;
        return this;
    }

    @AssertTrue(message = "Both azure.managed-identity.client-id and azure.managed-identity.resource-id cannot be set")
    public boolean isConfigValid()
    {
        return getClientId().isEmpty() || getResourceId().isEmpty();
    }
}
