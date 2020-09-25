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
package io.prestosql.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestKafkaConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(KafkaConfig.class)
                .setNodes("")
                .setKafkaBufferSize("64kB")
                .setDefaultSchema("default")
                .setTableNames("")
                .setTableDescriptionDir(new File("etc/kafka/"))
                .setHideInternalColumns(true)
                .setMessagesPerSplit(100_000)
                .setSecurityProtocol(null)
                .setSslEndpointIdentificationAlgorithm(null)
                .setSslTruststoreLocation(null)
                .setSslTruststorePassword(null)
                .setSslKeystoreLocation(null)
                .setSslKeystorePassword(null)
                .setSslKeyPassword(null)
                .setSslProvider(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kafka.table-description-dir", "/var/lib/kafka")
                .put("kafka.table-names", "table1, table2, table3")
                .put("kafka.default-schema", "kafka")
                .put("kafka.nodes", "localhost:12345,localhost:23456")
                .put("kafka.buffer-size", "1MB")
                .put("kafka.hide-internal-columns", "false")
                .put("kafka.messages-per-split", "1")
                .put("kafka.security.protocol", "SSL")
                .put("kafka.ssl.endpoint.identification.algorithm", "https")
                .put("kafka.ssl.truststore.location", "/some/path")
                .put("kafka.ssl.truststore.password", "password")
                .put("kafka.ssl.truststore.type", "JKS")
                .put("kafka.ssl.keystore.location", "/another/path")
                .put("kafka.ssl.keystore.password", "anotherPassword")
                .put("kafka.ssl.keystore.type", "JKS")
                .put("kafka.ssl.key.password", "keyPassword")
                .put("kafka.ssl.provider", "sslProviderName")
                .build();
        KafkaConfig expected = new KafkaConfig()
                .setTableDescriptionDir(new File("/var/lib/kafka"))
                .setTableNames("table1, table2, table3")
                .setDefaultSchema("kafka")
                .setNodes("localhost:12345, localhost:23456")
                .setKafkaBufferSize("1MB")
                .setHideInternalColumns(false)
                .setMessagesPerSplit(1)
                .setSecurityProtocol("SSL")
                .setSslEndpointIdentificationAlgorithm("https")
                .setSslTruststoreLocation("/some/path")
                .setSslTruststorePassword("password")
                .setSslTruststoreType("JKS")
                .setSslKeystoreLocation("/another/path")
                .setSslKeystorePassword("anotherPassword")
                .setSslKeystoreType("JKS")
                .setSslKeyPassword("keyPassword")
                .setSslProvider("sslProviderName");
        assertFullMapping(properties, expected);
    }
}
