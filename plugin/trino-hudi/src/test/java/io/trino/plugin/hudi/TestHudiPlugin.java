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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;

final class TestHudiPlugin
{
    @Test
    void testCreateConnector()
    {
        ConnectorFactory factory = getOnlyElement(new HudiPlugin().getConnectorFactories());
        factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("hive.metastore.uri", "thrift://foo:1234")
                                .put("bootstrap.quiet", "true")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    void testGetSecuritySensitivePropertyNames()
    {
        ConnectorFactory factory = getOnlyElement(new HudiPlugin().getConnectorFactories());
        Map<String, String> config = ImmutableMap.of(
                "non-existent-property", "value",
                "fs.hadoop.enabled", "true",
                "hive.azure.abfs.oauth.client-id", "test-client-id", // security-sensitive property from trino-hdfs
                "hive.azure.adl-proxy-host", "proxy-host:9800", // non-sensitive property from trino-hdfs
                "hive.dfs-timeout", "invalidValue", // property from trino-hdfs with invalid value
                "hive.metastore.uri", "thrift://foo:1234",
                "hive.metastore.thrift.client.ssl.key-password", "password",
                "hudi.size-based-split-weights-enabled", "shouldBeBoolean");

        Set<String> sensitiveProperties = factory.getSecuritySensitivePropertyNames("catalog", config, new TestingConnectorContext());

        assertThat(sensitiveProperties)
                .containsExactlyInAnyOrder("non-existent-property", "hive.azure.abfs.oauth.client-id", "hive.metastore.thrift.client.ssl.key-password");
    }
}
