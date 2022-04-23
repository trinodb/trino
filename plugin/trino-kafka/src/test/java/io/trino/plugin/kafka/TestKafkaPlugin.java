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
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SSL;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertNotNull;

public class TestKafkaPlugin
{
    @Test
    public void testSpinup()
            throws IOException
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, KafkaConnectorFactory.class);
        Path resource = Files.createTempFile("kafka", ".properties");

        Connector connector = factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kafka.table-names", "test")
                        .put("kafka.nodes", "localhost:9092")
                        .put("kafka.config.resources", resource.toString())
                        .buildOrThrow(),
                new TestingConnectorContext());
        assertNotNull(connector);
        connector.shutdown();
    }

    @Test
    public void testSslSpinup()
            throws IOException
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, KafkaConnectorFactory.class);

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
                        .buildOrThrow(),
                new TestingConnectorContext());
        assertNotNull(connector);
        connector.shutdown();
    }

    @Test
    public void testSslKeystoreMissingFileSpindown()
            throws IOException
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, KafkaConnectorFactory.class);

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
        assertInstanceOf(factory, KafkaConnectorFactory.class);

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
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining("Error: Invalid configuration property kafka.ssl.truststore.location: file does not exist: /not/a/real/path");
    }

    @Test
    public void testResourceConfigMissingFileSpindown()
    {
        KafkaPlugin plugin = new KafkaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, KafkaConnectorFactory.class);

        assertThatThrownBy(() -> factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kafka.table-names", "test")
                        .put("kafka.nodes", "localhost:9092")
                        .put("kafka.security-protocol", "PLAINTEXT")
                        .put("kafka.config.resources", "/not/a/real/path/1,/not/a/real/path/2")
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
        assertInstanceOf(factory, KafkaConnectorFactory.class);

        String nativeContent = "security.protocol=" + SSL;
        Path nativeKafkaResourcePath = Files.createTempFile("native_kafka", ".properties");
        writeToFile(nativeKafkaResourcePath, nativeContent);

        Connector connector = factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kafka.table-names", "test")
                        .put("kafka.nodes", "localhost:9092")
                        .put("kafka.config.resources", nativeKafkaResourcePath.toString())
                        .buildOrThrow(),
                new TestingConnectorContext());
        assertNotNull(connector);
        connector.shutdown();
    }

    private void writeToFile(Path filepath, String content)
            throws IOException
    {
        try (FileWriter writer = new FileWriter(filepath.toFile(), StandardCharsets.UTF_8)) {
            writer.write(content);
        }
    }
}
