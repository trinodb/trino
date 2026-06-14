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

import java.net.URI;
import java.util.Optional;

public class IcebergRestCatalogTokenExchangeConfig
{
    private boolean enabled;
    private URI endpoint;
    private String clientId;
    private String clientSecret;
    private String scope;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("iceberg.rest-catalog.token-exchange-enabled")
    @ConfigDescription("Exchange the user's OIDC token for a catalog-audience token via RFC 8693 before forwarding to the catalog or STS")
    public IcebergRestCatalogTokenExchangeConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public Optional<URI> getEndpoint()
    {
        return Optional.ofNullable(endpoint);
    }

    @Config("iceberg.rest-catalog.token-exchange.endpoint")
    @ConfigDescription("OAuth2 token endpoint for RFC 8693 token exchange")
    public IcebergRestCatalogTokenExchangeConfig setEndpoint(URI endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public Optional<String> getClientId()
    {
        return Optional.ofNullable(clientId);
    }

    @Config("iceberg.rest-catalog.token-exchange.client-id")
    @ConfigDescription("Client ID used to authenticate the token exchange request")
    public IcebergRestCatalogTokenExchangeConfig setClientId(String clientId)
    {
        this.clientId = clientId;
        return this;
    }

    public Optional<String> getClientSecret()
    {
        return Optional.ofNullable(clientSecret);
    }

    @Config("iceberg.rest-catalog.token-exchange.client-secret")
    @ConfigDescription("Client secret used to authenticate the token exchange request")
    @ConfigSecuritySensitive
    public IcebergRestCatalogTokenExchangeConfig setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }

    public Optional<String> getScope()
    {
        return Optional.ofNullable(scope);
    }

    @Config("iceberg.rest-catalog.token-exchange.scope")
    @ConfigDescription("Target scope for the token exchange request (e.g. api://minio/storage.access)")
    public IcebergRestCatalogTokenExchangeConfig setScope(String scope)
    {
        this.scope = scope;
        return this;
    }

    @AssertTrue(message = "iceberg.rest-catalog.token-exchange.endpoint is required when token-exchange-enabled=true")
    public boolean isEndpointPresentWhenEnabled()
    {
        return !enabled || endpoint != null;
    }

    @AssertTrue(message = "iceberg.rest-catalog.token-exchange.client-id is required when token-exchange-enabled=true")
    public boolean isClientIdPresentWhenEnabled()
    {
        return !enabled || clientId != null;
    }

    @AssertTrue(message = "iceberg.rest-catalog.token-exchange.client-secret is required when token-exchange-enabled=true")
    public boolean isClientSecretPresentWhenEnabled()
    {
        return !enabled || clientSecret != null;
    }

    @AssertTrue(message = "iceberg.rest-catalog.token-exchange.scope is required when token-exchange-enabled=true")
    public boolean isScopePresentWhenEnabled()
    {
        return !enabled || scope != null;
    }
}
