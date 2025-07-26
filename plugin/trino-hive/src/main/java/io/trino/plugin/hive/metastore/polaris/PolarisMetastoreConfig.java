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
package io.trino.plugin.hive.metastore.polaris;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
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
    private Duration requestTimeout = new Duration(30, TimeUnit.SECONDS);
    private Duration connectTimeout = new Duration(10, TimeUnit.SECONDS);
    private int maxRetries = 3;
    private Duration retryDelay = new Duration(1, TimeUnit.SECONDS);

    // Security configuration
    private Security security = Security.NONE;

    // SSL configuration
    private boolean verifySSL = true;
    private String trustStorePath;
    private String trustStorePassword;
    private String keyStorePath;
    private String keyStorePassword;

    // Generic table support
    private boolean enableGenericTables = true;
    private String defaultGenericTableFormat = "delta";

    // Policy integration
    private boolean enablePolicyIntegration = true;
    private boolean enforcePolicies;

    @NotNull
    public URI getUri()
    {
        return uri;
    }

    @Config("polaris.uri")
    @ConfigDescription("URI of the Polaris catalog server")
    public PolarisMetastoreConfig setUri(URI uri)
    {
        this.uri = uri;
        return this;
    }

    public String getPrefix()
    {
        return prefix;
    }

    @Config("polaris.prefix")
    @ConfigDescription("Optional prefix for all API requests")
    public PolarisMetastoreConfig setPrefix(String prefix)
    {
        this.prefix = prefix;
        return this;
    }

    public Optional<String> getWarehouse()
    {
        return Optional.ofNullable(warehouse);
    }

    @Config("polaris.warehouse")
    @ConfigDescription("Default warehouse location for tables")
    public PolarisMetastoreConfig setWarehouse(String warehouse)
    {
        this.warehouse = warehouse;
        return this;
    }

    @MinDuration("1s")
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("polaris.request-timeout")
    @ConfigDescription("Timeout for HTTP requests to Polaris")
    public PolarisMetastoreConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    @MinDuration("1s")
    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("polaris.connect-timeout")
    @ConfigDescription("Timeout for HTTP connections to Polaris")
    public PolarisMetastoreConfig setConnectTimeout(Duration connectTimeout)
    {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public int getMaxRetries()
    {
        return maxRetries;
    }

    @Config("polaris.max-retries")
    @ConfigDescription("Maximum number of retry attempts for failed requests")
    public PolarisMetastoreConfig setMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
        return this;
    }

    public Duration getRetryDelay()
    {
        return retryDelay;
    }

    @Config("polaris.retry-delay")
    @ConfigDescription("Initial delay between retry attempts")
    public PolarisMetastoreConfig setRetryDelay(Duration retryDelay)
    {
        this.retryDelay = retryDelay;
        return this;
    }

    public Security getSecurity()
    {
        return security;
    }

    @Config("polaris.security")
    @ConfigDescription("Security type: NONE or OAUTH2")
    public PolarisMetastoreConfig setSecurity(Security security)
    {
        this.security = security;
        return this;
    }

    public boolean isVerifySSL()
    {
        return verifySSL;
    }

    @Config("polaris.ssl.verify")
    @ConfigDescription("Whether to verify SSL certificates")
    public PolarisMetastoreConfig setVerifySSL(boolean verifySSL)
    {
        this.verifySSL = verifySSL;
        return this;
    }

    public Optional<String> getTrustStorePath()
    {
        return Optional.ofNullable(trustStorePath);
    }

    @Config("polaris.ssl.trust-store-path")
    @ConfigDescription("Path to SSL trust store")
    public PolarisMetastoreConfig setTrustStorePath(String trustStorePath)
    {
        this.trustStorePath = trustStorePath;
        return this;
    }

    public Optional<String> getTrustStorePassword()
    {
        return Optional.ofNullable(trustStorePassword);
    }

    @Config("polaris.ssl.trust-store-password")
    @ConfigDescription("Password for SSL trust store")
    public PolarisMetastoreConfig setTrustStorePassword(String trustStorePassword)
    {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    public Optional<String> getKeyStorePath()
    {
        return Optional.ofNullable(keyStorePath);
    }

    @Config("polaris.ssl.key-store-path")
    @ConfigDescription("Path to SSL key store")
    public PolarisMetastoreConfig setKeyStorePath(String keyStorePath)
    {
        this.keyStorePath = keyStorePath;
        return this;
    }

    public Optional<String> getKeyStorePassword()
    {
        return Optional.ofNullable(keyStorePassword);
    }

    @Config("polaris.ssl.key-store-password")
    @ConfigDescription("Password for SSL key store")
    public PolarisMetastoreConfig setKeyStorePassword(String keyStorePassword)
    {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    public boolean isEnableGenericTables()
    {
        return enableGenericTables;
    }

    @Config("polaris.generic-tables.enabled")
    @ConfigDescription("Enable support for generic tables (Delta Lake, etc.)")
    public PolarisMetastoreConfig setEnableGenericTables(boolean enableGenericTables)
    {
        this.enableGenericTables = enableGenericTables;
        return this;
    }

    public String getDefaultGenericTableFormat()
    {
        return defaultGenericTableFormat;
    }

    @Config("polaris.generic-tables.default-format")
    @ConfigDescription("Default format for generic tables when not specified")
    public PolarisMetastoreConfig setDefaultGenericTableFormat(String defaultGenericTableFormat)
    {
        this.defaultGenericTableFormat = defaultGenericTableFormat;
        return this;
    }

    public boolean isEnablePolicyIntegration()
    {
        return enablePolicyIntegration;
    }

    @Config("polaris.policies.enabled")
    @ConfigDescription("Enable integration with Polaris policy system")
    public PolarisMetastoreConfig setEnablePolicyIntegration(boolean enablePolicyIntegration)
    {
        this.enablePolicyIntegration = enablePolicyIntegration;
        return this;
    }

    public boolean isEnforcePolicies()
    {
        return enforcePolicies;
    }

    @Config("polaris.policies.enforce")
    @ConfigDescription("Whether to enforce policies at the metastore level")
    public PolarisMetastoreConfig setEnforcePolicies(boolean enforcePolicies)
    {
        this.enforcePolicies = enforcePolicies;
        return this;
    }
}
