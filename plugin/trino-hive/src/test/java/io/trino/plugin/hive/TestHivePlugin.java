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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.hive.HiveSessionProperties.InsertExistingPartitionsBehavior.APPEND;
import static io.trino.plugin.hive.HiveSessionProperties.InsertExistingPartitionsBehavior.ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHivePlugin
{
    @Test
    public void testCreateConnector()
    {
        ConnectorFactory factory = getHiveConnectorFactory();

        // simplest possible configuration
        factory.create(
                "test",
                ImmutableMap.of(
                        "hive.metastore.uri", "thrift://foo:1234",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext()).shutdown();
    }

    @Test
    public void testTestingFileMetastore()
    {
        ConnectorFactory factory = getHiveConnectorFactory();
        factory.create(
                        "test",
                        ImmutableMap.of(
                                "hive.metastore", "file",
                                "hive.metastore.catalog.dir", "/tmp",
                                "bootstrap.quiet", "true"),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testThriftMetastore()
    {
        ConnectorFactory factory = getHiveConnectorFactory();

        factory.create(
                "test",
                ImmutableMap.of(
                        "hive.metastore", "thrift",
                        "hive.metastore.uri", "thrift://foo:1234",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testGlueMetastore()
    {
        ConnectorFactory factory = getHiveConnectorFactory();

        factory.create(
                "test",
                ImmutableMap.of(
                        "hive.metastore", "glue",
                        "hive.metastore.glue.region", "us-east-2",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext())
                .shutdown();

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "hive.metastore", "glue",
                        "hive.metastore.uri", "thrift://foo:1234",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext()))
                .hasMessageContaining("Error: Configuration property 'hive.metastore.uri' was not used");
    }

    @Test
    public void testImmutablePartitionsAndInsertOverwriteMutuallyExclusive()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.insert-existing-partitions-behavior", "APPEND")
                        .put("hive.immutable-partitions", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining("insert-existing-partitions-behavior cannot be APPEND when immutable-partitions is true");
    }

    @Test
    public void testInsertOverwriteIsSetToErrorWhenImmutablePartitionsIsTrue()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        Connector connector = connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.immutable-partitions", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext());
        assertThat(getDefaultValueInsertExistingPartitionsBehavior(connector)).isEqualTo(ERROR);
        connector.shutdown();
    }

    @Test
    public void testInsertOverwriteIsSetToAppendWhenImmutablePartitionsIsFalseByDefault()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        Connector connector = connectorFactory.create(
                "test",
                ImmutableMap.of(
                        "hive.metastore.uri", "thrift://foo:1234",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext());
        assertThat(getDefaultValueInsertExistingPartitionsBehavior(connector)).isEqualTo(APPEND);
        connector.shutdown();
    }

    private Object getDefaultValueInsertExistingPartitionsBehavior(Connector connector)
    {
        return connector.getSessionProperties().stream()
                .filter(propertyMetadata -> "insert_existing_partitions_behavior".equals(propertyMetadata.getName()))
                .collect(onlyElement())
                .getDefaultValue();
    }

    @Test
    public void testAllowAllAccessControl()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.security", "allow-all")
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testReadOnlyAllAccessControl()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.security", "read-only")
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testFileBasedAccessControl()
            throws Exception
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();
        File tempFile = File.createTempFile("test-hive-plugin-access-control", ".json");
        tempFile.deleteOnExit();
        Files.write(tempFile.toPath(), "{}".getBytes(UTF_8));

        connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.security", "file")
                        .put("security.config-file", tempFile.getAbsolutePath())
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testSystemAccessControl()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        Connector connector = connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.security", "system")
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext());
        assertThatThrownBy(connector::getAccessControl).isInstanceOf(UnsupportedOperationException.class);
        connector.shutdown();
    }

    @Test
    public void testHttpMetastoreConfigs()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "https://localhost:443")
                        .put("hive.metastore.http.client.bearer-token", "token")
                        .put("hive.metastore.http.client.additional-headers", "key:value")
                        .put("hive.metastore.http.client.authentication.type", "BEARER")
                        .buildOrThrow(),
                new TestingConnectorContext())
                .shutdown();

        connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "http://localhost:443")
                        .put("hive.metastore.http.client.additional-headers", "key:value")
                        .put("hive.metastore.http.client.authentication.type", "BEARER")
                        .buildOrThrow(),
                new TestingConnectorContext())
                .shutdown();

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "http://localhost:443")
                        .put("hive.metastore.http.client.bearer-token", "token")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining("'hive.metastore.http.client.bearer-token' must not be set while using http metastore URIs in 'hive.metastore.uri'");

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "http://localhost:443, https://localhost:443")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining("'hive.metastore.uri' cannot contain both http and https URI schemes");

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "http://localhost:443, thrift://localhost:443")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining("'hive.metastore.uri' cannot contain both http(s) and thrift URI schemes");

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "https://localhost:443")
                        .put("hive.metastore.http.client.bearer-token", "token")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining("'hive.metastore.http.client.authentication.type' must be set while using http/https metastore URIs in 'hive.metastore.uri'");

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "https://localhost:443")
                        .put("hive.metastore.http.client.authentication.type", "BEARER")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining("'hive.metastore.http.client.bearer-token' must be set while using https metastore URIs in 'hive.metastore.uri'");
    }

    private static ConnectorFactory getHiveConnectorFactory()
    {
        Plugin plugin = new HivePlugin();
        return stream(plugin.getConnectorFactories())
                .filter(factory -> factory.getName().equals("hive"))
                .collect(toOptional())
                .orElseThrow();
    }
}
