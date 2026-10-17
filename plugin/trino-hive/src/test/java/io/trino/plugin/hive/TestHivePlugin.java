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
import java.util.Map;

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
        File tempFile = Files.createTempFile("test-hive-plugin-access-control", ".json").toFile();
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
            throws Exception
    {
        File truststore = Files.createTempFile("test-hive-plugin-truststore", ".jks").toFile();
        truststore.deleteOnExit();

        assertConnectorStarts(ImmutableMap.of(
                "hive.metastore.uri", "http://localhost:9083/metastore",
                "hive.metastore.http.client.additional-headers", "x-actor-username:hive"));
        assertConnectorStarts(ImmutableMap.of(
                "hive.metastore.uri", "https://localhost/metastore",
                "hive.metastore.http.client.bearer-token", "token",
                "hive.metastore.http.client.additional-headers", "key:value"));
        // bearer token is optional for https
        assertConnectorStarts(ImmutableMap.of("hive.metastore.uri", "https://localhost/metastore"));
        // hive.metastore.thrift.* properties apply to http(s) URIs as well
        assertConnectorStarts(ImmutableMap.of(
                "hive.metastore.uri", "https://localhost/metastore",
                "hive.metastore.thrift.client.connect-timeout", "5s",
                "hive.metastore.thrift.client.read-timeout", "1m",
                "hive.metastore.thrift.client.max-retries", "3",
                "hive.metastore.thrift.catalog-name", "custom"));

        assertConnectorFails(
                ImmutableMap.of(
                        "hive.metastore.uri", "http://localhost:9083",
                        "hive.metastore.http.client.bearer-token", "token"),
                "'hive.metastore.http.client.bearer-token' must not be set for http:// metastore URIs, use https://");
        assertConnectorFails(
                ImmutableMap.of("hive.metastore.uri", "http://localhost:9083,https://localhost:9083"),
                "'hive.metastore.uri' cannot contain both http and https URI schemes");
        assertConnectorFails(
                ImmutableMap.of("hive.metastore.uri", "http://localhost:9083,thrift://localhost:9083"),
                "'hive.metastore.uri' cannot contain both http(s) and thrift URI schemes");
        assertConnectorFails(
                ImmutableMap.of(
                        "hive.metastore.uri", "https://localhost:9083",
                        "hive.metastore.authentication.type", "KERBEROS"),
                "Kerberos metastore authentication is not supported for http(s) metastore URIs");
        assertConnectorFails(
                ImmutableMap.of(
                        "hive.metastore.uri", "https://localhost:9083",
                        "hive.metastore.thrift.impersonation.enabled", "true"),
                "Metastore impersonation is not supported for http(s) metastore URIs");
        assertConnectorFails(
                ImmutableMap.of(
                        "hive.metastore.uri", "https://localhost:9083",
                        "hive.metastore.thrift.client.socks-proxy", "localhost:1080"),
                "SOCKS proxy is not supported for http(s) metastore URIs");
        assertConnectorFails(
                ImmutableMap.of(
                        "hive.metastore.uri", "http://localhost:9083",
                        "hive.metastore.thrift.client.ssl.enabled", "true",
                        "hive.metastore.thrift.client.ssl.trust-certificate", truststore.getPath()),
                "'hive.metastore.thrift.client.ssl.enabled' requires an https:// metastore URI");
        assertConnectorFails(
                ImmutableMap.of(
                        "hive.metastore.uri", "https://localhost:9083",
                        "hive.metastore.http.client.authentication.type", "BEARER"),
                "Defunct property 'hive.metastore.http.client.authentication.type'");
        assertConnectorFails(
                ImmutableMap.of(
                        "hive.metastore.uri", "https://localhost:9083",
                        "hive.metastore.http.client.read-timeout", "10s"),
                "Defunct property 'hive.metastore.http.client.read-timeout'");
        // HTTP transport properties are rejected for thrift:// URIs
        assertConnectorFails(
                ImmutableMap.of(
                        "hive.metastore.uri", "thrift://localhost:9083",
                        "hive.metastore.http.client.bearer-token", "token"),
                "Configuration property 'hive.metastore.http.client.bearer-token' was not used");
    }

    private static void assertConnectorStarts(Map<String, String> config)
    {
        getHiveConnectorFactory().create("test", config, new TestingConnectorContext()).shutdown();
    }

    private static void assertConnectorFails(Map<String, String> config, String expectedMessage)
    {
        assertThatThrownBy(() -> getHiveConnectorFactory().create("test", config, new TestingConnectorContext()))
                .hasMessageContaining(expectedMessage);
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
