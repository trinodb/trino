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
package io.trino.plugin.opensearch;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.List;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OpenSearchConfig
{
    public enum Security
    {
        AWS,
        PASSWORD,
    }

    private List<String> hosts;
    private int port = 9200;
    private String defaultSchema = "default";
    private int scrollSize = 1_000;
    private Duration scrollTimeout = new Duration(1, MINUTES);
    private Duration requestTimeout = new Duration(10, SECONDS);
    private Duration connectTimeout = new Duration(1, SECONDS);
    private Duration backoffInitDelay = new Duration(500, MILLISECONDS);
    private Duration backoffMaxDelay = new Duration(20, SECONDS);
    private Duration maxRetryTime = new Duration(30, SECONDS);
    private Duration nodeRefreshInterval = new Duration(1, MINUTES);
    private int maxHttpConnections = 25;
    private int httpThreadCount = Runtime.getRuntime().availableProcessors();

    private boolean tlsEnabled;
    private File keystorePath;
    private File trustStorePath;
    private String keystorePassword;
    private String truststorePassword;
    private boolean ignorePublishAddress;
    private boolean verifyHostnames = true;
    private boolean projectionPushDownEnabled = true;

    private Security security;

    @NotNull
    public List<String> getHosts()
    {
        return hosts;
    }

    @Config("opensearch.host")
    public OpenSearchConfig setHosts(List<String> hosts)
    {
        this.hosts = hosts;
        return this;
    }

    public int getPort()
    {
        return port;
    }

    @Config("opensearch.port")
    public OpenSearchConfig setPort(int port)
    {
        this.port = port;
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("opensearch.default-schema-name")
    @ConfigDescription("Default schema name to use")
    public OpenSearchConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @Min(1)
    public int getScrollSize()
    {
        return scrollSize;
    }

    @Config("opensearch.scroll-size")
    @ConfigDescription("Scroll batch size")
    public OpenSearchConfig setScrollSize(int scrollSize)
    {
        this.scrollSize = scrollSize;
        return this;
    }

    @NotNull
    public Duration getScrollTimeout()
    {
        return scrollTimeout;
    }

    @Config("opensearch.scroll-timeout")
    @ConfigDescription("Scroll timeout")
    public OpenSearchConfig setScrollTimeout(Duration scrollTimeout)
    {
        this.scrollTimeout = scrollTimeout;
        return this;
    }

    @NotNull
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("opensearch.request-timeout")
    @ConfigDescription("OpenSearch request timeout")
    public OpenSearchConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    @NotNull
    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("opensearch.connect-timeout")
    @ConfigDescription("OpenSearch connect timeout")
    public OpenSearchConfig setConnectTimeout(Duration timeout)
    {
        this.connectTimeout = timeout;
        return this;
    }

    @NotNull
    public Duration getBackoffInitDelay()
    {
        return backoffInitDelay;
    }

    @Config("opensearch.backoff-init-delay")
    @ConfigDescription("Initial delay to wait between backpressure retries")
    public OpenSearchConfig setBackoffInitDelay(Duration backoffInitDelay)
    {
        this.backoffInitDelay = backoffInitDelay;
        return this;
    }

    @NotNull
    public Duration getBackoffMaxDelay()
    {
        return backoffMaxDelay;
    }

    @Config("opensearch.backoff-max-delay")
    @ConfigDescription("Maximum delay to wait between backpressure retries")
    public OpenSearchConfig setBackoffMaxDelay(Duration backoffMaxDelay)
    {
        this.backoffMaxDelay = backoffMaxDelay;
        return this;
    }

    @NotNull
    public Duration getMaxRetryTime()
    {
        return maxRetryTime;
    }

    @Config("opensearch.max-retry-time")
    @ConfigDescription("Maximum timeout in case of multiple retries")
    public OpenSearchConfig setMaxRetryTime(Duration maxRetryTime)
    {
        this.maxRetryTime = maxRetryTime;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getNodeRefreshInterval()
    {
        return nodeRefreshInterval;
    }

    @Config("opensearch.node-refresh-interval")
    @ConfigDescription("How often to refresh the list of available nodes in the OpenSearch cluster")
    public OpenSearchConfig setNodeRefreshInterval(Duration nodeRefreshInterval)
    {
        this.nodeRefreshInterval = nodeRefreshInterval;
        return this;
    }

    @Config("opensearch.max-http-connections")
    @ConfigDescription("Maximum number of persistent HTTP connections to OpenSearch cluster")
    public OpenSearchConfig setMaxHttpConnections(int size)
    {
        this.maxHttpConnections = size;
        return this;
    }

    public int getMaxHttpConnections()
    {
        return maxHttpConnections;
    }

    @Config("opensearch.http-thread-count")
    @ConfigDescription("Number of threads handling HTTP connections to OpenSearch cluster")
    public OpenSearchConfig setHttpThreadCount(int count)
    {
        this.httpThreadCount = count;
        return this;
    }

    public int getHttpThreadCount()
    {
        return httpThreadCount;
    }

    public boolean isTlsEnabled()
    {
        return tlsEnabled;
    }

    @Config("opensearch.tls.enabled")
    public OpenSearchConfig setTlsEnabled(boolean tlsEnabled)
    {
        this.tlsEnabled = tlsEnabled;
        return this;
    }

    public Optional<@FileExists File> getKeystorePath()
    {
        return Optional.ofNullable(keystorePath);
    }

    @Config("opensearch.tls.keystore-path")
    public OpenSearchConfig setKeystorePath(File path)
    {
        this.keystorePath = path;
        return this;
    }

    public Optional<String> getKeystorePassword()
    {
        return Optional.ofNullable(keystorePassword);
    }

    @Config("opensearch.tls.keystore-password")
    @ConfigSecuritySensitive
    public OpenSearchConfig setKeystorePassword(String password)
    {
        this.keystorePassword = password;
        return this;
    }

    public Optional<@FileExists File> getTrustStorePath()
    {
        return Optional.ofNullable(trustStorePath);
    }

    @Config("opensearch.tls.truststore-path")
    public OpenSearchConfig setTrustStorePath(File path)
    {
        this.trustStorePath = path;
        return this;
    }

    public Optional<String> getTruststorePassword()
    {
        return Optional.ofNullable(truststorePassword);
    }

    @Config("opensearch.tls.truststore-password")
    @ConfigSecuritySensitive
    public OpenSearchConfig setTruststorePassword(String password)
    {
        this.truststorePassword = password;
        return this;
    }

    public boolean isVerifyHostnames()
    {
        return verifyHostnames;
    }

    @Config("opensearch.tls.verify-hostnames")
    public OpenSearchConfig setVerifyHostnames(boolean verify)
    {
        this.verifyHostnames = verify;
        return this;
    }

    public boolean isIgnorePublishAddress()
    {
        return ignorePublishAddress;
    }

    @Config("opensearch.ignore-publish-address")
    public OpenSearchConfig setIgnorePublishAddress(boolean ignorePublishAddress)
    {
        this.ignorePublishAddress = ignorePublishAddress;
        return this;
    }

    public boolean isProjectionPushdownEnabled()
    {
        return projectionPushDownEnabled;
    }

    @Config("opensearch.projection-pushdown-enabled")
    @ConfigDescription("Read only required fields from a row type")
    public OpenSearchConfig setProjectionPushdownEnabled(boolean projectionPushDownEnabled)
    {
        this.projectionPushDownEnabled = projectionPushDownEnabled;
        return this;
    }

    @NotNull
    public Optional<Security> getSecurity()
    {
        return Optional.ofNullable(security);
    }

    @Config("opensearch.security")
    public OpenSearchConfig setSecurity(Security security)
    {
        this.security = security;
        return this;
    }
}
