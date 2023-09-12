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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.trino.plugin.hive.HiveConfig;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDeltaLakePlugin
{
    @Test
    public void testCreateConnector()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create("test", ImmutableMap.of("hive.metastore.uri", "thrift://foo:1234"), new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testCreateTestingConnector()
    {
        Plugin plugin = new TestingDeltaLakePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of("hive.metastore.uri", "thrift://foo:1234"), new TestingConnectorContext())
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
                        ImmutableMap.of(
                                "hive.metastore", "thrift",
                                "hive.metastore.uri", "thrift://foo:1234"),
                        new TestingConnectorContext())
                .shutdown();

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "hive.metastore", "thrift",
                        "hive.metastore.uri", "thrift://foo:1234",
                        "delta.hide-non-delta-lake-tables", "true"),
                new TestingConnectorContext()))
                .isInstanceOf(ApplicationConfigurationException.class)
                // TODO support delta.hide-non-delta-lake-tables with thrift metastore
                .hasMessageContaining("Error: Configuration property 'delta.hide-non-delta-lake-tables' was not used");
    }

    @Test
    public void testGlueMetastore()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create(
                        "test",
                        ImmutableMap.of(
                                "hive.metastore", "glue",
                                "hive.metastore.glue.region", "us-east-2"),
                        new TestingConnectorContext())
                .shutdown();

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "hive.metastore", "glue",
                        "hive.metastore.uri", "thrift://foo:1234"),
                new TestingConnectorContext()))
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Error: Configuration property 'hive.metastore.uri' was not used");
    }

    @Test
    public void testNoCaching()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create("test",
                        ImmutableMap.of(
                                "hive.metastore.uri", "thrift://foo:1234",
                                "delta.metadata.cache-ttl", "0s"),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testNoActiveDataFilesCaching()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create("test",
                        ImmutableMap.of(
                                "hive.metastore.uri", "thrift://foo:1234",
                                "delta.metadata.live-files.cache-ttl", "0s"),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testHiveConfigIsNotBound()
    {
        ConnectorFactory factory = getConnectorFactory();
        assertThatThrownBy(() -> factory.create("test",
                ImmutableMap.of(
                        "hive.metastore.uri", "thrift://foo:1234",
                        // Try setting any property provided by HiveConfig class
                        HiveConfig.CONFIGURATION_HIVE_PARTITION_PROJECTION_ENABLED, "true"),
                new TestingConnectorContext()))
                .hasMessageContaining("Error: Configuration property 'hive.partition-projection-enabled' was not used");
    }

    @Test
    public void testReadOnlyAllAccessControl()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("hive.metastore.uri", "thrift://foo:1234")
                                .put("delta.security", "read-only")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testSystemAccessControl()
    {
        ConnectorFactory factory = getConnectorFactory();
        Connector connector = factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("delta.security", "system")
                        .buildOrThrow(),
                new TestingConnectorContext());
        assertThatThrownBy(connector::getAccessControl).isInstanceOf(UnsupportedOperationException.class);
        connector.shutdown();
    }

    @Test
    public void testFileBasedAccessControl()
            throws Exception
    {
        ConnectorFactory factory = getConnectorFactory();
        File tempFile = File.createTempFile("test-delta-lake-plugin-access-control", ".json");
        tempFile.deleteOnExit();
        Files.writeString(tempFile.toPath(), "{}");

        factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("hive.metastore.uri", "thrift://foo:1234")
                                .put("delta.security", "file")
                                .put("security.config-file", tempFile.getAbsolutePath())
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    private static ConnectorFactory getConnectorFactory()
    {
        return getOnlyElement(new DeltaLakePlugin().getConnectorFactories());
    }
}
