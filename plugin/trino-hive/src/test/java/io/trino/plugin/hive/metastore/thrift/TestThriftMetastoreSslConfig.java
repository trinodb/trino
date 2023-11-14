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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestThriftMetastoreSslConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ThriftMetastoreSslConfig.class)
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
                .put("hive.metastore.thrift.client.ssl.key", keystoreFile.toString())
                .put("hive.metastore.thrift.client.ssl.key-password", "keystore-password")
                .put("hive.metastore.thrift.client.ssl.trust-certificate", truststoreFile.toString())
                .put("hive.metastore.thrift.client.ssl.trust-certificate-password", "truststore-password")
                .buildOrThrow();

        ThriftMetastoreSslConfig expected = new ThriftMetastoreSslConfig()
                .setKeystorePath(keystoreFile.toFile())
                .setKeystorePassword("keystore-password")
                .setTruststorePath(truststoreFile.toFile())
                .setTruststorePassword("truststore-password");

        assertFullMapping(properties, expected);
    }
}
