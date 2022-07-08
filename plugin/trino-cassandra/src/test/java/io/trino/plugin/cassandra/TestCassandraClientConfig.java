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
package io.trino.plugin.cassandra;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestCassandraClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(CassandraClientConfig.class)
                .setFetchSize(5_000)
                .setConsistencyLevel(DefaultConsistencyLevel.ONE)
                .setContactPoints("")
                .setNativeProtocolPort(9042)
                .setPartitionSizeForBatchSelect(100)
                .setSplitSize(1_024)
                .setBatchSize(100)
                .setSplitsPerNode(null)
                .setAllowDropTable(false)
                .setUsername(null)
                .setPassword(null)
                .setClientReadTimeout(new Duration(12_000, MILLISECONDS))
                .setClientConnectTimeout(new Duration(5_000, MILLISECONDS))
                .setClientSoLinger(null)
                .setRetryPolicy(RetryPolicyType.DEFAULT)
                .setUseDCAware(true)
                .setDcAwareLocalDC(null)
                .setDcAwareUsedHostsPerRemoteDc(0)
                .setDcAwareAllowRemoteDCsForLocal(false)
                .setNoHostAvailableRetryTimeout(new Duration(1, MINUTES))
                .setSpeculativeExecutionLimit(null)
                .setSpeculativeExecutionDelay(new Duration(500, MILLISECONDS))
                .setProtocolVersion(null)
                .setTlsEnabled(false)
                .setKeystorePath(null)
                .setKeystorePassword(null)
                .setTruststorePath(null)
                .setTruststorePassword(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path keystoreFile = Files.createTempFile(null, null);
        Path truststoreFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("cassandra.contact-points", "host1,host2")
                .put("cassandra.native-protocol-port", "9999")
                .put("cassandra.fetch-size", "10000")
                .put("cassandra.consistency-level", "TWO")
                .put("cassandra.partition-size-for-batch-select", "77")
                .put("cassandra.split-size", "1025")
                .put("cassandra.batch-size", "999")
                .put("cassandra.splits-per-node", "10000")
                .put("cassandra.allow-drop-table", "true")
                .put("cassandra.username", "my_username")
                .put("cassandra.password", "my_password")
                .put("cassandra.client.read-timeout", "11ms")
                .put("cassandra.client.connect-timeout", "22ms")
                .put("cassandra.client.so-linger", "33")
                .put("cassandra.retry-policy", "DOWNGRADING_CONSISTENCY")
                .put("cassandra.load-policy.use-dc-aware", "false")
                .put("cassandra.load-policy.dc-aware.local-dc", "dc1")
                .put("cassandra.load-policy.dc-aware.used-hosts-per-remote-dc", "1")
                .put("cassandra.load-policy.dc-aware.allow-remote-dc-for-local", "true")
                .put("cassandra.no-host-available-retry-timeout", "3m")
                .put("cassandra.speculative-execution.limit", "10")
                .put("cassandra.speculative-execution.delay", "101s")
                .put("cassandra.protocol-version", "V3")
                .put("cassandra.tls.enabled", "true")
                .put("cassandra.tls.keystore-path", keystoreFile.toString())
                .put("cassandra.tls.keystore-password", "keystore-password")
                .put("cassandra.tls.truststore-path", truststoreFile.toString())
                .put("cassandra.tls.truststore-password", "truststore-password")
                .buildOrThrow();

        CassandraClientConfig expected = new CassandraClientConfig()
                .setContactPoints("host1", "host2")
                .setNativeProtocolPort(9999)
                .setFetchSize(10_000)
                .setConsistencyLevel(DefaultConsistencyLevel.TWO)
                .setPartitionSizeForBatchSelect(77)
                .setSplitSize(1_025)
                .setBatchSize(999)
                .setSplitsPerNode(10_000L)
                .setAllowDropTable(true)
                .setUsername("my_username")
                .setPassword("my_password")
                .setClientReadTimeout(new Duration(11, MILLISECONDS))
                .setClientConnectTimeout(new Duration(22, MILLISECONDS))
                .setClientSoLinger(33)
                .setRetryPolicy(RetryPolicyType.DOWNGRADING_CONSISTENCY)
                .setUseDCAware(false)
                .setDcAwareLocalDC("dc1")
                .setDcAwareUsedHostsPerRemoteDc(1)
                .setDcAwareAllowRemoteDCsForLocal(true)
                .setNoHostAvailableRetryTimeout(new Duration(3, MINUTES))
                .setSpeculativeExecutionLimit(10)
                .setSpeculativeExecutionDelay(new Duration(101, SECONDS))
                .setProtocolVersion(DefaultProtocolVersion.V3)
                .setTlsEnabled(true)
                .setKeystorePath(keystoreFile.toFile())
                .setKeystorePassword("keystore-password")
                .setTruststorePath(truststoreFile.toFile())
                .setTruststorePassword("truststore-password");

        assertFullMapping(properties, expected);
    }
}
