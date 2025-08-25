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
package io.trino.metastore.polaris;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for Apache Polaris metastore backend.
 */
public class PolarisMetastoreConfig
{
    public enum Security
    {
        NONE,
        OAUTH2
    }

    private URI uri;
    private String prefix = "";
    private String warehouse;
    private Security security = Security.OAUTH2;

    private String oauth2ClientId;
    private String oauth2ClientSecret;
    private String oauth2Scope = "PRINCIPAL_ROLE:ALL";
    private URI oauth2ServerUri;
    private Duration oauth2TokenCacheTtl = new Duration(50, TimeUnit.MINUTES);

    private Duration connectTimeout = new Duration(30, TimeUnit.SECONDS);
    private Duration requestTimeout = new Duration(60, TimeUnit.SECONDS);
    private int maxRetries = 3;
    private Duration retryDelay = new Duration(1, TimeUnit.SECONDS);

    private boolean createNamespaceDirectories = true;
    private boolean cacheEnabled = true;
    private Duration cacheTtl = new Duration(5, TimeUnit.MINUTES);
    private long cacheMaxSize = 1000;

    private String realm = "POLARIS";

    @NotNull
    public URI getUri()
    {
        return uri;
    }

    @Config("hive.metastore.polaris.uri")
    @ConfigDescription("URI of the Polaris catalog server (e.g., http://localhost:8181/api/catalog)")
    public PolarisMetastoreConfig setUri(URI uri)
    {
        this.uri = uri;
        return this;
    }

    public String getPrefix()
    {
        return prefix;
    }

    @Config("hive.metastore.polaris.prefix")
    @ConfigDescription("Catalog prefix for API requests (usually the catalog name)")
    public PolarisMetastoreConfig setPrefix(String prefix)
    {
        this.prefix = prefix;
        return this;
    }

    public Optional<String> getWarehouse()
    {
        return Optional.ofNullable(warehouse);
    }

    @Config("hive.metastore.polaris.warehouse")
    @ConfigDescription("Default warehouse location for tables (e.g., s3://bucket/warehouse/)")
    public PolarisMetastoreConfig setWarehouse(String warehouse)
    {
        this.warehouse = warehouse;
        return this;
    }

    public Security getSecurity()
    {
        return security;
    }

    @Config("hive.metastore.polaris.security")
    @ConfigDescription("Security type: NONE or OAUTH2")
    public PolarisMetastoreConfig setSecurity(Security security)
    {
        this.security = security;
        return this;
    }

    public Optional<String> getOauth2ClientId()
    {
        return Optional.ofNullable(oauth2ClientId);
    }

    @Config("hive.metastore.polaris.oauth2.client-id")
    @ConfigDescription("OAuth2 client ID for authentication")
    public PolarisMetastoreConfig setOauth2ClientId(String oauth2ClientId)
    {
        this.oauth2ClientId = oauth2ClientId;
        return this;
    }

    public Optional<String> getOauth2ClientSecret()
    {
        return Optional.ofNullable(oauth2ClientSecret);
    }

    @Config("hive.metastore.polaris.oauth2.client-secret")
    @ConfigDescription("OAuth2 client secret for authentication")
    @ConfigSecuritySensitive
    public PolarisMetastoreConfig setOauth2ClientSecret(String oauth2ClientSecret)
    {
        this.oauth2ClientSecret = oauth2ClientSecret;
        return this;
    }

    public String getOauth2Scope()
    {
        return oauth2Scope;
    }

    @Config("hive.metastore.polaris.oauth2.scope")
    @ConfigDescription("OAuth2 scope for token requests")
    public PolarisMetastoreConfig setOauth2Scope(String oauth2Scope)
    {
        this.oauth2Scope = oauth2Scope;
        return this;
    }

    public Optional<URI> getOauth2ServerUri()
    {
        return Optional.ofNullable(oauth2ServerUri);
    }

    @Config("hive.metastore.polaris.oauth2.server-uri")
    @ConfigDescription("OAuth2 token server URI (defaults to {uri}/v1/oauth/tokens)")
    public PolarisMetastoreConfig setOauth2ServerUri(URI oauth2ServerUri)
    {
        this.oauth2ServerUri = oauth2ServerUri;
        return this;
    }

    public Duration getOauth2TokenCacheTtl()
    {
        return oauth2TokenCacheTtl;
    }

    @Config("hive.metastore.polaris.oauth2.token-cache-ttl")
    @ConfigDescription("How long to cache OAuth2 tokens")
    public PolarisMetastoreConfig setOauth2TokenCacheTtl(Duration oauth2TokenCacheTtl)
    {
        this.oauth2TokenCacheTtl = oauth2TokenCacheTtl;
        return this;
    }

    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("hive.metastore.polaris.connect-timeout")
    @ConfigDescription("HTTP connection timeout")
    public PolarisMetastoreConfig setConnectTimeout(Duration connectTimeout)
    {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("hive.metastore.polaris.request-timeout")
    @ConfigDescription("HTTP request timeout")
    public PolarisMetastoreConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    public int getMaxRetries()
    {
        return maxRetries;
    }

    @Config("hive.metastore.polaris.max-retries")
    @ConfigDescription("Maximum number of request retries")
    public PolarisMetastoreConfig setMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
        return this;
    }

    public Duration getRetryDelay()
    {
        return retryDelay;
    }

    @Config("hive.metastore.polaris.retry-delay")
    @ConfigDescription("Delay between retries")
    public PolarisMetastoreConfig setRetryDelay(Duration retryDelay)
    {
        this.retryDelay = retryDelay;
        return this;
    }

    public boolean isCreateNamespaceDirectories()
    {
        return createNamespaceDirectories;
    }

    @Config("hive.metastore.polaris.create-namespace-directories")
    @ConfigDescription("Whether to create directories when creating namespaces")
    public PolarisMetastoreConfig setCreateNamespaceDirectories(boolean createNamespaceDirectories)
    {
        this.createNamespaceDirectories = createNamespaceDirectories;
        return this;
    }

    public boolean isCacheEnabled()
    {
        return cacheEnabled;
    }

    @Config("hive.metastore.polaris.cache.enabled")
    @ConfigDescription("Whether to enable metadata caching")
    public PolarisMetastoreConfig setCacheEnabled(boolean cacheEnabled)
    {
        this.cacheEnabled = cacheEnabled;
        return this;
    }

    public Duration getCacheTtl()
    {
        return cacheTtl;
    }

    @Config("hive.metastore.polaris.cache.ttl")
    @ConfigDescription("Time-to-live for cached metadata")
    public PolarisMetastoreConfig setCacheTtl(Duration cacheTtl)
    {
        this.cacheTtl = cacheTtl;
        return this;
    }

    public long getCacheMaxSize()
    {
        return cacheMaxSize;
    }

    @Config("hive.metastore.polaris.cache.max-size")
    @ConfigDescription("Maximum number of entries in metadata cache")
    public PolarisMetastoreConfig setCacheMaxSize(long cacheMaxSize)
    {
        this.cacheMaxSize = cacheMaxSize;
        return this;
    }

    public String getRealm()
    {
        return realm;
    }

    @Config("hive.metastore.polaris.realm")
    @ConfigDescription("Polaris realm for multi-tenant deployments")
    public PolarisMetastoreConfig setRealm(String realm)
    {
        this.realm = realm;
        return this;
    }

    public Optional<String> getOauth2Credential()
    {
        if (oauth2ClientId != null && oauth2ClientSecret != null) {
            return Optional.of(oauth2ClientId + ":" + oauth2ClientSecret);
        }
        return Optional.empty();
    }

    public URI getOauth2ServerUriWithFallback()
    {
        if (oauth2ServerUri != null) {
            return oauth2ServerUri;
        }
        return URI.create(uri.toString() + "/v1/oauth/tokens");
    }
}
