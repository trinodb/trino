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
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig("iceberg.rest-catalog.parent-namespace")
public class IcebergRestCatalogConfig
{
    public enum Security
    {
        NONE,
        OAUTH2,
    }

    public enum SessionType
    {
        NONE,
        USER
    }

    private URI restUri;
    private Optional<String> prefix = Optional.empty();
    private Optional<String> warehouse = Optional.empty();
    private boolean nestedNamespaceEnabled;
    private Security security = Security.NONE;
    private SessionType sessionType = SessionType.NONE;
    private boolean vendedCredentialsEnabled;
    private boolean viewEndpointsEnabled = true;
    private boolean sigV4Enabled;
    private boolean caseInsensitiveNameMatching;
    private Duration caseInsensitiveNameMatchingCacheTtl = new Duration(1, MINUTES);

    @NotNull
    public URI getBaseUri()
    {
        return this.restUri;
    }

    @Config("iceberg.rest-catalog.uri")
    @ConfigDescription("The URI to the REST server")
    public IcebergRestCatalogConfig setBaseUri(String uri)
    {
        if (uri != null) {
            this.restUri = URI.create(uri);
        }
        return this;
    }

    public Optional<String> getPrefix()
    {
        return prefix;
    }

    @Config("iceberg.rest-catalog.prefix")
    @ConfigDescription("The prefix for the resource path to use with the REST catalog server")
    public IcebergRestCatalogConfig setPrefix(String prefix)
    {
        this.prefix = Optional.ofNullable(prefix);
        return this;
    }

    public Optional<String> getWarehouse()
    {
        return warehouse;
    }

    @Config("iceberg.rest-catalog.warehouse")
    @ConfigDescription("The warehouse location/identifier to use with the REST catalog server")
    public IcebergRestCatalogConfig setWarehouse(String warehouse)
    {
        this.warehouse = Optional.ofNullable(warehouse);
        return this;
    }

    public boolean isNestedNamespaceEnabled()
    {
        return nestedNamespaceEnabled;
    }

    @Config("iceberg.rest-catalog.nested-namespace-enabled")
    @ConfigDescription("Support querying objects under nested namespace")
    public IcebergRestCatalogConfig setNestedNamespaceEnabled(boolean nestedNamespaceEnabled)
    {
        this.nestedNamespaceEnabled = nestedNamespaceEnabled;
        return this;
    }

    @NotNull
    public Security getSecurity()
    {
        return security;
    }

    @Config("iceberg.rest-catalog.security")
    @ConfigDescription("Authorization protocol to use when communicating with the REST catalog server")
    public IcebergRestCatalogConfig setSecurity(Security security)
    {
        this.security = security;
        return this;
    }

    @NotNull
    public IcebergRestCatalogConfig.SessionType getSessionType()
    {
        return sessionType;
    }

    @Config("iceberg.rest-catalog.session")
    @ConfigDescription("Type of REST catalog sessionType to use when communicating with REST catalog Server")
    public IcebergRestCatalogConfig setSessionType(SessionType sessionType)
    {
        this.sessionType = sessionType;
        return this;
    }

    public boolean isVendedCredentialsEnabled()
    {
        return vendedCredentialsEnabled;
    }

    @Config("iceberg.rest-catalog.vended-credentials-enabled")
    @ConfigDescription("Use credentials provided by the REST backend for file system access")
    public IcebergRestCatalogConfig setVendedCredentialsEnabled(boolean vendedCredentialsEnabled)
    {
        this.vendedCredentialsEnabled = vendedCredentialsEnabled;
        return this;
    }

    public boolean isViewEndpointsEnabled()
    {
        return viewEndpointsEnabled;
    }

    @Config("iceberg.rest-catalog.view-endpoints-enabled")
    @ConfigDescription("Enable view endpoints")
    public IcebergRestCatalogConfig setViewEndpointsEnabled(boolean viewEndpointsEnabled)
    {
        this.viewEndpointsEnabled = viewEndpointsEnabled;
        return this;
    }

    public boolean isSigV4Enabled()
    {
        return sigV4Enabled;
    }

    @Config("iceberg.rest-catalog.sigv4-enabled")
    @ConfigDescription("Enable AWS Signature version 4 (SigV4)")
    public IcebergRestCatalogConfig setSigV4Enabled(boolean sigV4Enabled)
    {
        this.sigV4Enabled = sigV4Enabled;
        return this;
    }

    public boolean isCaseInsensitiveNameMatching()
    {
        return caseInsensitiveNameMatching;
    }

    @Config("iceberg.rest-catalog.case-insensitive-name-matching")
    @ConfigDescription("Match object names case-insensitively")
    public IcebergRestCatalogConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching)
    {
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getCaseInsensitiveNameMatchingCacheTtl()
    {
        return caseInsensitiveNameMatchingCacheTtl;
    }

    @Config("iceberg.rest-catalog.case-insensitive-name-matching.cache-ttl")
    @ConfigDescription("Duration to keep case insensitive object mapping prior to eviction")
    public IcebergRestCatalogConfig setCaseInsensitiveNameMatchingCacheTtl(Duration caseInsensitiveNameMatchingCacheTtl)
    {
        this.caseInsensitiveNameMatchingCacheTtl = caseInsensitiveNameMatchingCacheTtl;
        return this;
    }
}
