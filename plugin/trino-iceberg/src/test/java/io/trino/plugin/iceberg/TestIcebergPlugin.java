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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergPlugin
{
    @Test
    public void testCreateConnector()
    {
        ConnectorFactory factory = getConnectorFactory();
        // simplest possible configuration
        factory.create(
                "test",
                Map.of(
                        "hive.metastore.uri", "thrift://foo:1234",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext()).shutdown();
    }

    @Test
    public void testTestingFileMetastore()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create(
                        "test",
                        Map.of(
                                "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                                "hive.metastore.catalog.dir", "/tmp",
                                "bootstrap.quiet", "true"),
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
                                "iceberg.catalog.type", "HIVE_METASTORE",
                                "hive.metastore.uri", "thrift://foo:1234",
                                "bootstrap.quiet", "true"),
                        new TestingConnectorContext())
                .shutdown();

        // Ensure Glue configuration isn't bound when Glue not in use
        assertThatThrownBy(() -> factory.create(
                "test",
                Map.of(
                        "hive.metastore.uri", "thrift://foo:1234",
                        "hive.metastore.glue.region", "us-east",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext()))
                .hasMessageContaining("Configuration property 'hive.metastore.glue.region' was not used");
    }

    @Test
    public void testHiveMetastoreRejected()
    {
        ConnectorFactory factory = getConnectorFactory();

        assertThatThrownBy(() -> factory.create(
                "test",
                Map.of(
                        "hive.metastore", "thrift",
                        "hive.metastore.uri", "thrift://foo:1234",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext()))
                .hasMessageContaining("Error: Configuration property 'hive.metastore' was not used");
    }

    @Test
    public void testGlueMetastore()
    {
        ConnectorFactory factory = getConnectorFactory();

        factory.create(
                        "test",
                        Map.of(
                                "iceberg.catalog.type", "glue",
                                "hive.metastore.glue.region", "us-east-1",
                                "bootstrap.quiet", "true"),
                        new TestingConnectorContext())
                .shutdown();

        assertThatThrownBy(() -> factory.create(
                "test",
                Map.of(
                        "iceberg.catalog.type", "glue",
                        "hive.metastore.uri", "thrift://foo:1234",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext()))
                .hasMessageContaining("Error: Configuration property 'hive.metastore.uri' was not used");

        factory.create(
                        "test",
                        Map.of(
                                "iceberg.catalog.type", "glue",
                                "hive.metastore.glue.catalogid", "123",
                                "hive.metastore.glue.region", "us-east-1",
                                "bootstrap.quiet", "true"),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testAllowAllAccessControl()
    {
        ConnectorFactory connectorFactory = getConnectorFactory();

        connectorFactory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "HIVE_METASTORE")
                                .put("hive.metastore.uri", "thrift://foo:1234")
                                .put("iceberg.security", "allow-all")
                                .put("bootstrap.quiet", "true")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testReadOnlyAllAccessControl()
    {
        ConnectorFactory connectorFactory = getConnectorFactory();

        connectorFactory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "HIVE_METASTORE")
                                .put("hive.metastore.uri", "thrift://foo:1234")
                                .put("iceberg.security", "read-only")
                                .put("bootstrap.quiet", "true")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testSystemAccessControl()
    {
        ConnectorFactory connectorFactory = getConnectorFactory();

        Connector connector = connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "HIVE_METASTORE")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("iceberg.security", "system")
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext());
        assertThatThrownBy(connector::getAccessControl).isInstanceOf(UnsupportedOperationException.class);
        connector.shutdown();
    }

    @Test
    public void testFileBasedAccessControl()
            throws Exception
    {
        ConnectorFactory connectorFactory = getConnectorFactory();
        File tempFile = File.createTempFile("test-iceberg-plugin-access-control", ".json");
        tempFile.deleteOnExit();
        Files.writeString(tempFile.toPath(), "{}");

        connectorFactory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "HIVE_METASTORE")
                                .put("hive.metastore.uri", "thrift://foo:1234")
                                .put("iceberg.security", "file")
                                .put("security.config-file", tempFile.getAbsolutePath())
                                .put("bootstrap.quiet", "true")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testIcebergPluginFailsWhenIncorrectPropertyProvided()
    {
        ConnectorFactory factory = getConnectorFactory();

        assertThatThrownBy(() -> factory.create(
                        "test",
                        Map.of(
                                "iceberg.catalog.type", "HIVE_METASTORE",
                                "hive.hive-views.enabled", "true",
                                "hive.metastore.uri", "thrift://foo:1234",
                                "bootstrap.quiet", "true"),
                        new TestingConnectorContext())
                .shutdown())
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Configuration property 'hive.hive-views.enabled' was not used");
    }

    @Test
    public void testRestCatalog()
    {
        ConnectorFactory factory = getConnectorFactory();

        factory.create(
                        "test",
                        Map.of(
                                "iceberg.catalog.type", "rest",
                                "iceberg.rest-catalog.uri", "https://foo:1234",
                                "bootstrap.quiet", "true"),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testRestCatalogValidations()
    {
        ConnectorFactory factory = getConnectorFactory();

        assertThatThrownBy(() -> factory.create(
                        "test",
                        Map.of(
                                "iceberg.catalog.type", "rest",
                                "iceberg.register-table-procedure.enabled", "true",
                                "iceberg.rest-catalog.uri", "https://foo:1234",
                                "iceberg.rest-catalog.vended-credentials-enabled", "true",
                                "bootstrap.quiet", "true"),
                        new TestingConnectorContext())
                .shutdown())
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("Using the `register_table` procedure with vended credentials is currently not supported");
    }

    @Test
    public void testJdbcCatalog()
    {
        ConnectorFactory factory = getConnectorFactory();

        factory.create(
                        "test",
                        Map.of(
                                "iceberg.catalog.type", "jdbc",
                                "iceberg.jdbc-catalog.driver-class", "org.postgresql.Driver",
                                "iceberg.jdbc-catalog.connection-url", "jdbc:postgresql://localhost:5432/test",
                                "iceberg.jdbc-catalog.catalog-name", "test",
                                "iceberg.jdbc-catalog.default-warehouse-dir", "s3://bucket",
                                "bootstrap.quiet", "true"),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testNessieCatalog()
    {
        ConnectorFactory factory = getConnectorFactory();

        factory.create(
                        "test",
                        Map.of(
                                "iceberg.catalog.type", "nessie",
                                "iceberg.nessie-catalog.default-warehouse-dir", "/tmp",
                                "iceberg.nessie-catalog.uri", "http://foo:1234",
                                "bootstrap.quiet", "true"),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testNessieCatalogWithBearerAuth()
    {
        ConnectorFactory factory = getConnectorFactory();

        factory.create(
                        "test",
                        Map.of(
                                "iceberg.catalog.type", "nessie",
                                "iceberg.nessie-catalog.default-warehouse-dir", "/tmp",
                                "iceberg.nessie-catalog.uri", "http://foo:1234",
                                "iceberg.nessie-catalog.authentication.type", "BEARER",
                                "iceberg.nessie-catalog.authentication.token", "someToken"),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testNessieCatalogWithNoAuthAndAccessToken()
    {
        ConnectorFactory factory = getConnectorFactory();

        assertThatThrownBy(() -> factory.create(
                        "test",
                        Map.of(
                                "iceberg.catalog.type", "nessie",
                                "iceberg.nessie-catalog.uri", "nessieUri",
                                "iceberg.nessie-catalog.default-warehouse-dir", "/tmp",
                                "iceberg.nessie-catalog.authentication.token", "someToken"),
                        new TestingConnectorContext())
                .shutdown())
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("'iceberg.nessie-catalog.authentication.token' must be configured only with 'iceberg.nessie-catalog.authentication.type' BEARER");
    }

    @Test
    public void testNessieCatalogWithNoAccessToken()
    {
        ConnectorFactory factory = getConnectorFactory();

        assertThatThrownBy(() -> factory.create(
                        "test",
                        Map.of(
                                "iceberg.catalog.type", "nessie",
                                "iceberg.nessie-catalog.uri", "nessieUri",
                                "iceberg.nessie-catalog.default-warehouse-dir", "/tmp",
                                "iceberg.nessie-catalog.authentication.type", "BEARER"),
                        new TestingConnectorContext())
                .shutdown())
                .isInstanceOf(ApplicationConfigurationException.class)
                .hasMessageContaining("'iceberg.nessie-catalog.authentication.token' must be configured with 'iceberg.nessie-catalog.authentication.type' BEARER");
    }

    @Test
    public void testSnowflakeCatalog()
    {
        ConnectorFactory factory = getConnectorFactory();

        factory.create(
                        "test",
                        Map.of(
                                "iceberg.catalog.type", "snowflake",
                                "iceberg.snowflake-catalog.account-uri", "jdbc:snowflake://sample.url",
                                "iceberg.snowflake-catalog.user", "user",
                                "iceberg.snowflake-catalog.password", "password",
                                "iceberg.snowflake-catalog.database", "database"),
                        new TestingConnectorContext())
                .shutdown();
    }

    private static ConnectorFactory getConnectorFactory()
    {
        return getOnlyElement(new IcebergPlugin().getConnectorFactories());
    }
}
