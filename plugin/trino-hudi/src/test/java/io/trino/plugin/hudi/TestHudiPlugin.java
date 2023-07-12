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
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.trino.plugin.hive.HiveConfig;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHudiPlugin
{
    @Test
    public void testCreateConnector()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create("test", Map.of("hive.metastore.uri", "thrift://foo:1234"), new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testCreateTestingConnector()
    {
        Plugin plugin = new TestingHudiPlugin(Optional.empty());
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", Map.of("hive.metastore.uri", "thrift://foo:1234"), new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testTestingFileMetastore()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create(
                        "test",
                        ImmutableMap.of(
                                "hive.metastore", "file",
                                "hive.metastore.catalog.dir", "/tmp"),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testThriftMetastore()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create(
                        "test",
                        Map.of(
                                "hive.metastore", "thrift",
                                "hive.metastore.uri", "thrift://foo:1234"),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testGlueMetastore()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create(
                        "test",
                        Map.of(
                                "hive.metastore", "glue",
                                "hive.metastore.glue.region", "us-east-2"),
                        new TestingConnectorContext())
                .shutdown();

        assertThatThrownBy(() -> factory.create(
                "test",
                Map.of(
                        "hive.metastore", "glue",
                        "hive.metastore.uri", "thrift://foo:1234"),
                new TestingConnectorContext()))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Error: Configuration property 'hive.metastore.uri' was not used");
    }

    @Test
    public void testHiveConfigIsNotBound()
    {
        ConnectorFactory factory = getConnectorFactory();
        assertThatThrownBy(() -> factory.create("test",
                Map.of(
                        "hive.metastore.uri", "thrift://foo:1234",
                        // Try setting any property provided by HiveConfig class
                        HiveConfig.CONFIGURATION_HIVE_PARTITION_PROJECTION_ENABLED, "true"),
                new TestingConnectorContext()))
                .hasMessageContaining("Error: Configuration property 'hive.partition-projection-enabled' was not used");
    }

    private static ConnectorFactory getConnectorFactory()
    {
        return getOnlyElement(new HudiPlugin().getConnectorFactories());
    }
}
