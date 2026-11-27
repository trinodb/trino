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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.util.Base64;

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
    public void testLakehouseMetastore()
    {
        ConnectorFactory factory = getHiveConnectorFactory();

        factory.create(
                        "test",
                        ImmutableMap.of(
                                "hive.metastore", "lakehouse",
                                "hive.metastore.lakehouse.project-id", "example-project",
                                "hive.metastore.lakehouse.catalog-id", "example-catalog",
                                "bootstrap.quiet", "true"),
                        new TestingConnectorContext())
                .shutdown();

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "hive.metastore", "lakehouse",
                        "hive.metastore.lakehouse.project-id", "example-project",
                        "hive.metastore.lakehouse.catalog-id", "example-catalog",
                        "hive.metastore.uri", "thrift://foo:1234",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext()))
                .hasMessageContaining("Error: Configuration property 'hive.metastore.uri' was not used");

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "hive.metastore", "lakehouse",
                        "hive.metastore.lakehouse.project-id", "example-project",
                        "hive.metastore.lakehouse.catalog-id", "example-catalog",
                        "hive.hide-delta-lake-tables", "true",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext()))
                .hasMessageContaining("Hiding Delta Lake tables is not supported");
    }

    @Test
    public void testLakehouseMetastoreWithSeparateCredentials(@TempDir Path tempDir)
            throws Exception
    {
        ConnectorFactory factory = getHiveConnectorFactory();
        // GCS file system access eagerly parses the service account key, so it must point to an actual, well-formed key
        Path gcsKeyFilePath = createFakeServiceAccountKeyFile(tempDir);

        // The metastore key is separate from the file system key
        factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("hive.metastore", "lakehouse")
                                .put("hive.metastore.lakehouse.project-id", "example-project")
                                .put("hive.metastore.lakehouse.catalog-id", "example-catalog")
                                .put("hive.metastore.lakehouse.google-json-key", Files.readString(gcsKeyFilePath))
                                .put("fs.gcs.enabled", "true")
                                .put("gcs.json-key-file-path", gcsKeyFilePath.toString())
                                .put("bootstrap.quiet", "true")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();

        // The metastore key does not require credentials for the file system
        factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("hive.metastore", "lakehouse")
                                .put("hive.metastore.lakehouse.project-id", "example-project")
                                .put("hive.metastore.lakehouse.catalog-id", "example-catalog")
                                .put("hive.metastore.lakehouse.google-json-key-file-path", gcsKeyFilePath.toString())
                                .put("fs.gcs.enabled", "true")
                                .put("gcs.auth-type", "APPLICATION_DEFAULT")
                                .put("bootstrap.quiet", "true")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    private static Path createFakeServiceAccountKeyFile(Path directory)
            throws Exception
    {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        byte[] privateKey = keyPairGenerator.generateKeyPair().getPrivate().getEncoded();
        String privateKeyPem = "-----BEGIN PRIVATE KEY-----\\n" +
                Base64.getMimeEncoder(64, "\n".getBytes(UTF_8)).encodeToString(privateKey).replace("\n", "\\n") +
                "\\n-----END PRIVATE KEY-----\\n";
        String json =
                """
                {
                  "type": "service_account",
                  "client_id": "123456789",
                  "client_email": "test@test-project.iam.gserviceaccount.com",
                  "private_key_id": "test-key-id",
                  "private_key": "%s"
                }
                """.formatted(privateKeyPem);

        Path keyFile = directory.resolve("gcs-service-account.json");
        Files.writeString(keyFile, json);
        return keyFile;
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

    private static ConnectorFactory getHiveConnectorFactory()
    {
        Plugin plugin = new HivePlugin();
        return stream(plugin.getConnectorFactories())
                .filter(factory -> factory.getName().equals("hive"))
                .collect(toOptional())
                .orElseThrow();
    }
}
