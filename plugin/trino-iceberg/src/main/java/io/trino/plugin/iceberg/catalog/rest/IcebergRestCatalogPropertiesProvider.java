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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.NodeVersion;
import org.apache.iceberg.CatalogProperties;

import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogProperties.AUTH_SESSION_TIMEOUT_MS;

public class IcebergRestCatalogPropertiesProvider
{
    private final Map<String, String> catalogProperties;

    @Inject
    public IcebergRestCatalogPropertiesProvider(
            IcebergRestCatalogConfig restConfig,
            SecurityProperties securityProperties,
            NodeVersion nodeVersion)
    {
        requireNonNull(restConfig, "restConfig is null");
        requireNonNull(securityProperties, "securityProperties is null");
        requireNonNull(nodeVersion, "nodeVersion is null");

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put(CatalogProperties.URI, restConfig.getBaseUri().toString());
        restConfig.getWarehouse().ifPresent(location -> properties.put(CatalogProperties.WAREHOUSE_LOCATION, location));
        restConfig.getPrefix().ifPresent(aPrefix -> properties.put("prefix", aPrefix));
        properties.put("view-endpoints-supported", Boolean.toString(restConfig.isViewEndpointsEnabled()));
        properties.put("trino-version", nodeVersion.toString());
        properties.put(AUTH_SESSION_TIMEOUT_MS, String.valueOf(restConfig.getSessionTimeout().toMillis()));
        restConfig.getConnectionTimeout().ifPresent(duration -> properties.put("rest.client.connection-timeout-ms", String.valueOf(duration.toMillis())));
        restConfig.getSocketTimeout().ifPresent(timeout -> properties.put("rest.client.socket-timeout-ms", String.valueOf(timeout.toMillis())));
        properties.putAll(securityProperties.get());
        if (restConfig.isVendedCredentialsEnabled()) {
            properties.put("header.X-Iceberg-Access-Delegation", "vended-credentials");
        }
        catalogProperties = properties.buildOrThrow();
    }

    public Map<String, String> catalogProperties()
    {
        return catalogProperties;
    }
}
