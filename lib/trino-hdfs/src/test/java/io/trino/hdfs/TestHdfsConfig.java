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
package io.trino.hdfs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.testng.Assert.assertEquals;

public class TestHdfsConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HdfsConfig.class)
                .setResourceConfigFiles("")
                .setNewDirectoryPermissions("0777")
                .setNewFileInheritOwnership(false)
                .setVerifyChecksum(true)
                .setIpcPingInterval(new Duration(10, TimeUnit.SECONDS))
                .setDfsTimeout(new Duration(60, TimeUnit.SECONDS))
                .setDfsConnectTimeout(new Duration(500, TimeUnit.MILLISECONDS))
                .setDfsConnectMaxRetries(5)
                .setDfsKeyProviderCacheTtl(new Duration(30, TimeUnit.MINUTES))
                .setDomainSocketPath(null)
                .setSocksProxy(null)
                .setWireEncryptionEnabled(false)
                .setFileSystemMaxCacheSize(1000)
                .setDfsReplication(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path resource1 = Files.createTempFile(null, null);
        Path resource2 = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.config.resources", resource1.toString() + "," + resource2.toString())
                .put("hive.fs.new-directory-permissions", "0700")
                .put("hive.fs.new-file-inherit-ownership", "true")
                .put("hive.dfs.verify-checksum", "false")
                .put("hive.dfs.ipc-ping-interval", "34s")
                .put("hive.dfs-timeout", "33s")
                .put("hive.dfs.connect.timeout", "20s")
                .put("hive.dfs.connect.max-retries", "10")
                .put("hive.dfs.key-provider.cache-ttl", "42s")
                .put("hive.dfs.domain-socket-path", "/foo")
                .put("hive.hdfs.socks-proxy", "localhost:4567")
                .put("hive.hdfs.wire-encryption.enabled", "true")
                .put("hive.fs.cache.max-size", "1010")
                .put("hive.dfs.replication", "1")
                .buildOrThrow();

        HdfsConfig expected = new HdfsConfig()
                .setResourceConfigFiles(ImmutableList.of(resource1.toFile(), resource2.toFile()))
                .setNewDirectoryPermissions("0700")
                .setNewFileInheritOwnership(true)
                .setVerifyChecksum(false)
                .setIpcPingInterval(new Duration(34, TimeUnit.SECONDS))
                .setDfsTimeout(new Duration(33, TimeUnit.SECONDS))
                .setDfsConnectTimeout(new Duration(20, TimeUnit.SECONDS))
                .setDfsConnectMaxRetries(10)
                .setDfsKeyProviderCacheTtl(new Duration(42, TimeUnit.SECONDS))
                .setDomainSocketPath("/foo")
                .setSocksProxy(HostAndPort.fromParts("localhost", 4567))
                .setWireEncryptionEnabled(true)
                .setFileSystemMaxCacheSize(1010)
                .setDfsReplication(1);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testNewDirectoryPermissionsMapping()
    {
        Map<String, String> properties = ImmutableMap.of("hive.fs.new-directory-permissions", "skip");

        HdfsConfig expected = new HdfsConfig()
                .setNewDirectoryPermissions("skip");

        assertEquals(properties.get("hive.fs.new-directory-permissions"), expected.getNewDirectoryPermissions());
        assertEquals(Optional.empty(), expected.getNewDirectoryFsPermissions());
    }
}
