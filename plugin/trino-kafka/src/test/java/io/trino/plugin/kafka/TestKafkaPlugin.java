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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SSL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestKafkaPlugin
{
    @Test
    public void testSpinup()
            throws IOException
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThat(factory).isInstanceOf(KafkaConnectorFactory.class);
        Path resource = Files.createTempFile("kafka", ".properties");

        Connector connector = factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kafka.table-names", "test")
                        .put("kafka.nodes", "localhost:9092")
                        .put("kafka.config.resources", resource.toString())
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext());
        assertThat(connector).isNotNull();
        connector.shutdown();
    }

    @Test
    public void testSslSpinup()
            throws IOException
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThat(factory).isInstanceOf(KafkaConnectorFactory.class);

        String secret = "confluent";
        Path keystorePath = Files.createTempFile("keystore", ".jks");
        Path truststorePath = Files.createTempFile("truststore", ".jks");

        writeToFile(keystorePath, secret);
        writeToFile(truststorePath, secret);

        Connector connector = factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kafka.table-names", "test")
                        .put("kafka.nodes", "localhost:9092")
                        .put("kafka.security-protocol", "SSL")
                        .put("kafka.ssl.keystore.type", "JKS")
                        .put("kafka.ssl.keystore.location", keystorePath.toString())
                        .put("kafka.ssl.keystore.password", "keystore-password")
                        .put("kafka.ssl.key.password", "key-password")
                        .put("kafka.ssl.truststore.type", "JKS")
                        .put("kafka.ssl.truststore.location", truststorePath.toString())
                        .put("kafka.ssl.truststore.password", "truststore-password")
                        .put("kafka.ssl.endpoint-identification-algorithm", "https")
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext());
        assertThat(connector).isNotNull();
        connector.shutdown();
    }

    @Test
    public void testSslKeystoreMissingFileSpindown()
            throws IOException
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThat(factory).isInstanceOf(KafkaConnectorFactory.class);

        Path truststorePath = Files.createTempFile("test", ".jks");

        assertThatThrownBy(() -> factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kafka.table-names", "test")
                        .put("kafka.nodes", "localhost:9092")
                        .put("kafka.security-protocol", "SSL")
                        .put("kafka.ssl.keystore.type", "JKS")
                        .put("kafka.ssl.keystore.location", "/not/a/real/path")
                        .put("kafka.ssl.keystore.password", "keystore-password")
                        .put("kafka.ssl.key.password", "key-password")
                        .put("kafka.ssl.truststore.type", "JKS")
                        .put("kafka.ssl.truststore.location", truststorePath.toString())
                        .put("kafka.ssl.truststore.password", "truststore-password")
                        .put("kafka.ssl.endpoint-identification-algorithm", "https")
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining("Error: Invalid configuration property kafka.ssl.keystore.location: file does not exist: /not/a/real/path");
    }

    @Test
    public void testSslTruststoreMissingFileSpindown()
            throws IOException
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThat(factory).isInstanceOf(KafkaConnectorFactory.class);

        Path keystorePath = Files.createTempFile("test", ".jks");

        assertThatThrownBy(() -> factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kafka.table-names", "test")
                        .put("kafka.nodes", "localhost:9092")
                        .put("kafka.security-protocol", "SSL")
                        .put("kafka.ssl.keystore.type", "JKS")
                        .put("kafka.ssl.keystore.location", keystorePath.toString())
                        .put("kafka.ssl.keystore.password", "keystore-password")
                        .put("kafka.ssl.key.password", "key-password")
                        .put("kafka.ssl.truststore.type", "JKS")
                        .put("kafka.ssl.truststore.location", "/not/a/real/path")
                        .put("kafka.ssl.truststore.password", "truststore-password")
                        .put("kafka.ssl.endpoint-identification-algorithm", "https")
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining("Error: Invalid configuration property kafka.ssl.truststore.location: file does not exist: /not/a/real/path");
    }

    @Test
    public void testResourceConfigMissingFileSpindown()
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThat(factory).isInstanceOf(KafkaConnectorFactory.class);

        assertThatThrownBy(() -> factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kafka.table-names", "test")
                        .put("kafka.nodes", "localhost:9092")
                        .put("kafka.security-protocol", "PLAINTEXT")
                        .put("kafka.config.resources", "/not/a/real/path/1,/not/a/real/path/2")
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContainingAll("Error: Invalid configuration property", ": file does not exist: /not/a/real/path/1", ": file does not exist: /not/a/real/path/2");
    }

    @Test
    public void testConfigResourceSpinup()
            throws IOException
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThat(factory).isInstanceOf(KafkaConnectorFactory.class);

        String nativeContent = "security.protocol=" + SSL;
        Path nativeKafkaResourcePath = Files.createTempFile("native_kafka", ".properties");
        writeToFile(nativeKafkaResourcePath, nativeContent);

        Connector connector = factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kafka.table-names", "test")
                        .put("kafka.nodes", "localhost:9092")
                        .put("kafka.config.resources", nativeKafkaResourcePath.toString())
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext());
        assertThat(connector).isNotNull();
        connector.shutdown();
    }

    @Test
    public void testConfluentSchemaRegistrySecuritySpinup()
            throws Exception
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        // the schema registry client loads the trust store when the connector is created
        Path truststorePath = Path.of(Resources.getResource("confluent/registry-truststore.p12").toURI());

        Connector connector = factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kafka.nodes", "localhost:9092")
                        .put("kafka.table-description-supplier", "CONFLUENT")
                        .put("kafka.confluent-schema-registry-url", "https://localhost:8081")
                        .put("kafka.confluent-schema-registry.basic-auth.user", "trino")
                        .put("kafka.confluent-schema-registry.basic-auth.password", "secret")
                        .put("kafka.confluent-schema-registry.ssl.truststore.location", truststorePath.toString())
                        .put("kafka.confluent-schema-registry.ssl.truststore.password", "secret")
                        .put("kafka.confluent-schema-registry.ssl.truststore.type", "PKCS12")
                        .put("kafka.confluent-schema-registry.ssl.endpoint-identification-algorithm", "disabled")
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext());
        assertThat(connector).isNotNull();
        connector.shutdown();
    }

    @Test
    public void testConfluentSchemaRegistryTruststoreMissingFileSpindown()
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kafka.nodes", "localhost:9092")
                        .put("kafka.table-description-supplier", "CONFLUENT")
                        .put("kafka.confluent-schema-registry-url", "https://localhost:8081")
                        .put("kafka.confluent-schema-registry.ssl.truststore.location", "/not/a/real/path")
                        .put("kafka.confluent-schema-registry.ssl.truststore.password", "secret")
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining("Error: Invalid configuration property kafka.confluent-schema-registry.ssl.truststore.location: file does not exist: /not/a/real/path");
    }

    @Test
    public void testConfluentSchemaRegistryUnreadableTruststoreSpindown()
            throws Exception
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        // the trust store is loaded at startup even though the registry URL uses http
        Path truststorePath = Files.createTempFile("truststore", ".p12");
        try {
            Files.writeString(truststorePath, "not a trust store");
            assertThatThrownBy(() -> factory.create(
                            "test-connector",
                            ImmutableMap.<String, String>builder()
                                    .put("kafka.nodes", "localhost:9092")
                                    .put("kafka.table-description-supplier", "CONFLUENT")
                                    .put("kafka.confluent-schema-registry-url", "http://localhost:8081")
                                    .put("kafka.confluent-schema-registry.ssl.truststore.location", truststorePath.toString())
                                    .put("kafka.confluent-schema-registry.ssl.truststore.password", "secret")
                                    .put("kafka.confluent-schema-registry.ssl.truststore.type", "PKCS12")
                                    .put("bootstrap.quiet", "true")
                                    .buildOrThrow(),
                            new TestingConnectorContext())
                    .shutdown())
                    .hasMessageContaining("Error initializing the ssl context for RestService");
        }
        finally {
            Files.delete(truststorePath);
        }
    }

    @Test
    public void testConfluentSchemaRegistryKeystorePasswordWithoutKeystoreSpindown()
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kafka.nodes", "localhost:9092")
                        .put("kafka.table-description-supplier", "CONFLUENT")
                        .put("kafka.confluent-schema-registry-url", "https://localhost:8081")
                        .put("kafka.confluent-schema-registry.ssl.keystore.password", "secret")
                        .put("bootstrap.quiet", "true")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining("kafka.confluent-schema-registry.ssl.keystore.location must be set when kafka.confluent-schema-registry.ssl.keystore.password or kafka.confluent-schema-registry.ssl.key.password is given");
    }

    private void writeToFile(Path filepath, String content)
            throws IOException
    {
        try (Writer writer = Files.newBufferedWriter(filepath, StandardCharsets.UTF_8)) {
            writer.write(content);
        }
    }
}
