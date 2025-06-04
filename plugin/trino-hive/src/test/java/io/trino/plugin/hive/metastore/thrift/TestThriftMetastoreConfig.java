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

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertDeprecatedEquivalence;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestThriftMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ThriftMetastoreConfig.class)
                .setConnectTimeout(new Duration(10, SECONDS))
                .setReadTimeout(new Duration(10, SECONDS))
                .setSocksProxy(null)
                .setMaxRetries(9)
                .setBackoffScaleFactor(2.0)
                .setMinBackoffDelay(new Duration(1, SECONDS))
                .setMaxBackoffDelay(new Duration(2, SECONDS))
                .setMaxRetryTime(new Duration(30, SECONDS))
                .setTlsEnabled(false)
                .setKeystorePath(null)
                .setKeystorePassword(null)
                .setTruststorePath(null)
                .setTruststorePassword(null)
                .setImpersonationEnabled(false)
                .setUseSparkTableStatisticsFallback(true)
                .setDelegationTokenCacheTtl(new Duration(1, HOURS))
                .setDelegationTokenCacheMaximumSize(1000)
                .setDeleteFilesOnDrop(false)
                .setMaxWaitForTransactionLock(new Duration(10, MINUTES))
                .setAssumeCanonicalPartitionKeys(false)
                .setWriteStatisticsThreads(20)
                .setCatalogName(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path keystoreFile = Files.createTempFile(null, null);
        Path truststoreFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore.thrift.client.connect-timeout", "22s")
                .put("hive.metastore.thrift.client.read-timeout", "44s")
                .put("hive.metastore.thrift.client.socks-proxy", "localhost:1234")
                .put("hive.metastore.thrift.client.max-retries", "15")
                .put("hive.metastore.thrift.client.backoff-scale-factor", "3.0")
                .put("hive.metastore.thrift.client.min-backoff-delay", "2s")
                .put("hive.metastore.thrift.client.max-backoff-delay", "4s")
                .put("hive.metastore.thrift.client.max-retry-time", "60s")
                .put("hive.metastore.thrift.client.ssl.enabled", "true")
                .put("hive.metastore.thrift.client.ssl.key", keystoreFile.toString())
                .put("hive.metastore.thrift.client.ssl.key-password", "keystore-password")
                .put("hive.metastore.thrift.client.ssl.trust-certificate", truststoreFile.toString())
                .put("hive.metastore.thrift.client.ssl.trust-certificate-password", "truststore-password")
                .put("hive.metastore.thrift.impersonation.enabled", "true")
                .put("hive.metastore.thrift.delegation-token.cache-ttl", "1d")
                .put("hive.metastore.thrift.delegation-token.cache-maximum-size", "9999")
                .put("hive.metastore.thrift.delete-files-on-drop", "true")
                .put("hive.metastore.thrift.txn-lock-max-wait", "5m")
                .put("hive.metastore.thrift.write-statistics-threads", "10")
                .put("hive.metastore.thrift.assume-canonical-partition-keys", "true")
                .put("hive.metastore.thrift.use-spark-table-statistics-fallback", "false")
                .put("hive.metastore.thrift.catalog-name", "custom_catalog_name")
                .buildOrThrow();

        ThriftMetastoreConfig expected = new ThriftMetastoreConfig()
                .setConnectTimeout(new Duration(22, SECONDS))
                .setReadTimeout(new Duration(44, SECONDS))
                .setSocksProxy(HostAndPort.fromParts("localhost", 1234))
                .setMaxRetries(15)
                .setBackoffScaleFactor(3.0)
                .setMinBackoffDelay(new Duration(2, SECONDS))
                .setMaxBackoffDelay(new Duration(4, SECONDS))
                .setMaxRetryTime(new Duration(60, SECONDS))
                .setTlsEnabled(true)
                .setKeystorePath(keystoreFile.toFile())
                .setKeystorePassword("keystore-password")
                .setTruststorePath(truststoreFile.toFile())
                .setTruststorePassword("truststore-password")
                .setImpersonationEnabled(true)
                .setDelegationTokenCacheTtl(new Duration(1, DAYS))
                .setDelegationTokenCacheMaximumSize(9999)
                .setDeleteFilesOnDrop(true)
                .setMaxWaitForTransactionLock(new Duration(5, MINUTES))
                .setAssumeCanonicalPartitionKeys(true)
                .setWriteStatisticsThreads(10)
                .setUseSparkTableStatisticsFallback(false)
                .setCatalogName("custom_catalog_name");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testLegacyPropertyMappings()
    {
        assertDeprecatedEquivalence(
                ThriftMetastoreConfig.class,
                Map.of(
                        "hive.metastore.thrift.client.connect-timeout", "42s",
                        "hive.metastore.thrift.client.read-timeout", "42s",
                        "hive.metastore.thrift.impersonation.enabled", "true"),
                Map.of(
                        "hive.metastore-timeout", "42s",
                        "hive.metastore.impersonation-enabled", "true"));
    }
}
