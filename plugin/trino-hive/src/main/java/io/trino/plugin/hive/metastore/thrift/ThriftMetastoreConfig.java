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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.trino.plugin.hive.util.RetryDriver;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class ThriftMetastoreConfig
{
    private Duration metastoreTimeout = new Duration(10, TimeUnit.SECONDS);
    private HostAndPort socksProxy;
    private int maxRetries = RetryDriver.DEFAULT_MAX_ATTEMPTS - 1;
    private double backoffScaleFactor = RetryDriver.DEFAULT_SCALE_FACTOR;
    private Duration minBackoffDelay = RetryDriver.DEFAULT_SLEEP_TIME;
    private Duration maxBackoffDelay = RetryDriver.DEFAULT_SLEEP_TIME;
    private Duration maxRetryTime = RetryDriver.DEFAULT_MAX_RETRY_TIME;
    private boolean impersonationEnabled;
    private boolean useSparkTableStatisticsFallback = true;
    private Duration delegationTokenCacheTtl = new Duration(1, TimeUnit.HOURS); // The default lifetime in Hive is 7 days (metastore.cluster.delegation.token.max-lifetime)
    private long delegationTokenCacheMaximumSize = 1000;
    private boolean deleteFilesOnDrop;
    private Duration maxWaitForTransactionLock = new Duration(10, TimeUnit.MINUTES);

    private boolean tlsEnabled;
    private File keystorePath;
    private String keystorePassword;
    private File truststorePath;
    private String trustStorePassword;
    private boolean assumeCanonicalPartitionKeys;
    private int writeStatisticsThreads = 20;

    @NotNull
    public Duration getMetastoreTimeout()
    {
        return metastoreTimeout;
    }

    @Config("hive.metastore-timeout")
    public ThriftMetastoreConfig setMetastoreTimeout(Duration metastoreTimeout)
    {
        this.metastoreTimeout = metastoreTimeout;
        return this;
    }

    public HostAndPort getSocksProxy()
    {
        return socksProxy;
    }

    @Config("hive.metastore.thrift.client.socks-proxy")
    public ThriftMetastoreConfig setSocksProxy(HostAndPort socksProxy)
    {
        this.socksProxy = socksProxy;
        return this;
    }

    @Min(0)
    public int getMaxRetries()
    {
        return maxRetries;
    }

    @Config("hive.metastore.thrift.client.max-retries")
    @ConfigDescription("Maximum number of retry attempts for metastore requests")
    public ThriftMetastoreConfig setMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
        return this;
    }

    public double getBackoffScaleFactor()
    {
        return backoffScaleFactor;
    }

    @Config("hive.metastore.thrift.client.backoff-scale-factor")
    @ConfigDescription("Scale factor for metastore request retry delay")
    public ThriftMetastoreConfig setBackoffScaleFactor(double backoffScaleFactor)
    {
        this.backoffScaleFactor = backoffScaleFactor;
        return this;
    }

    @NotNull
    public Duration getMaxRetryTime()
    {
        return maxRetryTime;
    }

    @Config("hive.metastore.thrift.client.max-retry-time")
    @ConfigDescription("Total time limit for a metastore request to be retried")
    public ThriftMetastoreConfig setMaxRetryTime(Duration maxRetryTime)
    {
        this.maxRetryTime = maxRetryTime;
        return this;
    }

    public Duration getMinBackoffDelay()
    {
        return minBackoffDelay;
    }

    @Config("hive.metastore.thrift.client.min-backoff-delay")
    @ConfigDescription("Minimum delay between metastore request retries")
    public ThriftMetastoreConfig setMinBackoffDelay(Duration minBackoffDelay)
    {
        this.minBackoffDelay = minBackoffDelay;
        return this;
    }

    public Duration getMaxBackoffDelay()
    {
        return maxBackoffDelay;
    }

    @Config("hive.metastore.thrift.client.max-backoff-delay")
    @ConfigDescription("Maximum delay between metastore request retries")
    public ThriftMetastoreConfig setMaxBackoffDelay(Duration maxBackoffDelay)
    {
        this.maxBackoffDelay = maxBackoffDelay;
        return this;
    }

    public boolean isImpersonationEnabled()
    {
        return impersonationEnabled;
    }

    @Config("hive.metastore.thrift.impersonation.enabled")
    @LegacyConfig("hive.metastore.impersonation-enabled")
    @ConfigDescription("Should end user be impersonated when communicating with metastore")
    public ThriftMetastoreConfig setImpersonationEnabled(boolean impersonationEnabled)
    {
        this.impersonationEnabled = impersonationEnabled;
        return this;
    }

    public boolean isUseSparkTableStatisticsFallback()
    {
        return useSparkTableStatisticsFallback;
    }

    @Config("hive.metastore.thrift.use-spark-table-statistics-fallback")
    @ConfigDescription("Enable usage of table statistics generated by Apache Spark when hive table statistics are not available")
    public ThriftMetastoreConfig setUseSparkTableStatisticsFallback(boolean useSparkTableStatisticsFallback)
    {
        this.useSparkTableStatisticsFallback = useSparkTableStatisticsFallback;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getDelegationTokenCacheTtl()
    {
        return delegationTokenCacheTtl;
    }

    @Config("hive.metastore.thrift.delegation-token.cache-ttl")
    @ConfigDescription("Time to live delegation token cache for metastore")
    public ThriftMetastoreConfig setDelegationTokenCacheTtl(Duration delegationTokenCacheTtl)
    {
        this.delegationTokenCacheTtl = delegationTokenCacheTtl;
        return this;
    }

    @NotNull
    @Min(0)
    public long getDelegationTokenCacheMaximumSize()
    {
        return delegationTokenCacheMaximumSize;
    }

    @Config("hive.metastore.thrift.delegation-token.cache-maximum-size")
    @ConfigDescription("Delegation token cache maximum size")
    public ThriftMetastoreConfig setDelegationTokenCacheMaximumSize(long delegationTokenCacheMaximumSize)
    {
        this.delegationTokenCacheMaximumSize = delegationTokenCacheMaximumSize;
        return this;
    }

    public boolean isDeleteFilesOnDrop()
    {
        return deleteFilesOnDrop;
    }

    @Config("hive.metastore.thrift.delete-files-on-drop")
    @ConfigDescription("Delete files on drop in case the metastore doesn't do it")
    public ThriftMetastoreConfig setDeleteFilesOnDrop(boolean deleteFilesOnDrop)
    {
        this.deleteFilesOnDrop = deleteFilesOnDrop;
        return this;
    }

    public Duration getMaxWaitForTransactionLock()
    {
        return maxWaitForTransactionLock;
    }

    @Config("hive.metastore.thrift.txn-lock-max-wait")
    @ConfigDescription("Maximum time to wait to acquire hive transaction lock")
    public ThriftMetastoreConfig setMaxWaitForTransactionLock(Duration maxWaitForTransactionLock)
    {
        this.maxWaitForTransactionLock = maxWaitForTransactionLock;
        return this;
    }

    public boolean isTlsEnabled()
    {
        return tlsEnabled;
    }

    @Config("hive.metastore.thrift.client.ssl.enabled")
    @ConfigDescription("Whether TLS security is enabled")
    public ThriftMetastoreConfig setTlsEnabled(boolean tlsEnabled)
    {
        this.tlsEnabled = tlsEnabled;
        return this;
    }

    @FileExists
    public File getKeystorePath()
    {
        return keystorePath;
    }

    @Config("hive.metastore.thrift.client.ssl.key")
    @ConfigDescription("Path to the key store")
    public ThriftMetastoreConfig setKeystorePath(File keystorePath)
    {
        this.keystorePath = keystorePath;
        return this;
    }

    public String getKeystorePassword()
    {
        return keystorePassword;
    }

    @Config("hive.metastore.thrift.client.ssl.key-password")
    @ConfigDescription("Password for the key store")
    public ThriftMetastoreConfig setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
        return this;
    }

    @FileExists
    public File getTruststorePath()
    {
        return truststorePath;
    }

    @Config("hive.metastore.thrift.client.ssl.trust-certificate")
    @ConfigDescription("Path to the trust store")
    public ThriftMetastoreConfig setTruststorePath(File truststorePath)
    {
        this.truststorePath = truststorePath;
        return this;
    }

    public String getTruststorePassword()
    {
        return trustStorePassword;
    }

    @Config("hive.metastore.thrift.client.ssl.trust-certificate-password")
    @ConfigDescription("Password for the trust store")
    public ThriftMetastoreConfig setTruststorePassword(String trustStorePassword)
    {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    @AssertTrue(message = "Trust store must be provided when TLS is enabled")
    public boolean isTruststorePathValid()
    {
        return !tlsEnabled || getTruststorePath() != null;
    }

    public boolean isAssumeCanonicalPartitionKeys()
    {
        return assumeCanonicalPartitionKeys;
    }

    @Config("hive.metastore.thrift.assume-canonical-partition-keys")
    public ThriftMetastoreConfig setAssumeCanonicalPartitionKeys(boolean assumeCanonicalPartitionKeys)
    {
        this.assumeCanonicalPartitionKeys = assumeCanonicalPartitionKeys;
        return this;
    }

    @Min(1)
    public int getWriteStatisticsThreads()
    {
        return writeStatisticsThreads;
    }

    @Config("hive.metastore.thrift.write-statistics-threads")
    @ConfigDescription("Number of threads for parallel statistics writes")
    public ThriftMetastoreConfig setWriteStatisticsThreads(int writeStatisticsThreads)
    {
        this.writeStatisticsThreads = writeStatisticsThreads;
        return this;
    }
}
