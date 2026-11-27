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
package io.trino.plugin.lakehouse;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestLakehousePlugin
{
    @Test
    void testCreateConnector()
    {
        assertCreateConnector(ImmutableMap.of());
    }

    @Test
    void testCreateConnectorWithIcebergObjectStoreLayout()
    {
        assertCreateConnector(ImmutableMap.of(
                "iceberg.object-store-layout.enabled", "true",
                "delta.object-store-layout.enabled", "false"));
    }

    @Test
    void testCreateConnectorWithDeltaObjectStoreLayout()
    {
        assertCreateConnector(ImmutableMap.of(
                "iceberg.object-store-layout.enabled", "false",
                "delta.object-store-layout.enabled", "true"));
    }

    @Test
    void testAllowAllAccessControl()
    {
        assertAccessControl(ImmutableMap.of("hive.security", "allow-all"));
    }

    @Test
    void testReadOnlyAccessControl()
    {
        assertAccessControl(ImmutableMap.of("hive.security", "read-only"));
    }

    @Test
    void testFileBasedAccessControl()
            throws Exception
    {
        File tempFile = Files.createTempFile("test-lakehouse-plugin-access-control", ".json").toFile();
        tempFile.deleteOnExit();
        Files.writeString(tempFile.toPath(), "{}");

        assertAccessControl(ImmutableMap.of(
                "hive.security", "file",
                "security.config-file", tempFile.getAbsolutePath()));
    }

    @Test
    void testSqlStandardAccessControl()
    {
        assertAccessControl(ImmutableMap.of("hive.security", "sql-standard"));
    }

    @Test
    void testSystemAccessControl()
    {
        Connector connector = createConnector(ImmutableMap.of("hive.security", "system"));
        try {
            assertThatThrownBy(connector::getAccessControl).isInstanceOf(UnsupportedOperationException.class);
        }
        finally {
            connector.shutdown();
        }
    }

    private static void assertAccessControl(Map<String, String> config)
    {
        Connector connector = createConnector(config);
        try {
            assertThat(connector.getAccessControl()).isNotNull();
        }
        finally {
            connector.shutdown();
        }
    }

    private static void assertCreateConnector(Map<String, String> config)
    {
        Connector connector = createConnector(config);
        try {
            assertThat(connector.getTableProperties())
                    .filteredOn(property -> property.getName().equals("object_store_layout_enabled"))
                    .singleElement()
                    .satisfies(property -> {
                        assertThat(property.getDescription()).isEqualTo("Set to true to enable object store file layout");
                        assertThat(property.getDefaultValue()).isNull();
                    });
        }
        finally {
            connector.shutdown();
        }
    }

    private static Connector createConnector(Map<String, String> config)
    {
        Plugin plugin = new LakehousePlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        return factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("bootstrap.quiet", "true")
                        .putAll(config)
                        .buildOrThrow(),
                new TestingConnectorContext());
    }
}
