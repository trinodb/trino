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
package io.trino.tests.product.kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.kafka.TestingKafka;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.trino.TrinoContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Kafka product test environment with the "kafka_schema_registry" catalog.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Kafka and Schema Registry containers</li>
 *   <li>Trino container with "kafka_schema_registry" catalog configured to use Schema Registry</li>
 *   <li>Access to Schema Registry client for registering schemas in tests</li>
 *   <li>No column mapping support (uses schema field names directly)</li>
 * </ul>
 */
public class KafkaSchemaRegistryEnvironment
        extends KafkaEnvironment
{
    private TestingKafka kafka;
    private TrinoContainer trino;
    private SchemaRegistryClient schemaRegistryClient;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return;
        }

        // Start Kafka with Schema Registry
        kafka = TestingKafka.createWithSchemaRegistry();
        kafka.start();

        // Build Trino container with kafka_schema_registry catalog
        trino = TrinoProductTestContainer.builder()
                .withNetwork(kafka.getNetwork())
                .withNetworkAlias("trino")
                .withCatalog("kafka_schema_registry", Map.of(
                        "connector.name", "kafka",
                        "kafka.nodes", "kafka:9093",
                        "kafka.table-description-supplier", "confluent",
                        "kafka.confluent-schema-registry-url", "http://schema-registry:8081",
                        "kafka.default-schema", "product_tests"))
                .build();

        // Copy Confluent protobuf provider jars into the Kafka plugin directory.
        // These are not included in the standard Trino Docker image because they are
        // under the Confluent Community License (provided scope in the Kafka connector).
        copyClasspathJarToPlugin(trino, "kafka-protobuf-provider", "io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider");
        copyClasspathJarToPlugin(trino, "kafka-protobuf-types", "io.confluent.protobuf.type.DecimalProto");

        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }

        // Create Schema Registry client for tests to register schemas
        schemaRegistryClient = new CachedSchemaRegistryClient(
                kafka.getSchemaRegistryConnectString(),
                100);
    }

    @Override
    public String getCatalogName()
    {
        return "kafka_schema_registry";
    }

    @Override
    public String getTopicNameSuffix()
    {
        return "_schema_registry";
    }

    @Override
    public boolean supportsColumnMapping()
    {
        return false;
    }

    @Override
    public TestingKafka getKafka()
    {
        return kafka;
    }

    /**
     * Returns the Schema Registry client for registering schemas in tests.
     */
    public SchemaRegistryClient getSchemaRegistryClient()
    {
        return schemaRegistryClient;
    }

    /**
     * Returns the Schema Registry URL (external) for tests that need direct access.
     */
    public String getSchemaRegistryConnectString()
    {
        return kafka.getSchemaRegistryConnectString();
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino);
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, user);
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trino != null ? trino.getJdbcUrl() : null;
    }

    @Override
    public boolean isRunning()
    {
        return trino != null && trino.isRunning();
    }

    /**
     * Finds a JAR on the classpath by locating a class within it, reads the JAR bytes,
     * and copies them into the Trino container's Kafka plugin directory.
     */
    static void copyClasspathJarToPlugin(TrinoContainer container, String jarName, String classInJar)
    {
        try {
            Class<?> clazz = Class.forName(classInJar);
            URL location = clazz.getProtectionDomain().getCodeSource().getLocation();
            Path jarPath = Path.of(location.toURI());
            byte[] jarBytes = Files.readAllBytes(jarPath);
            container.withCopyToContainer(
                    Transferable.of(jarBytes),
                    "/usr/lib/trino/plugin/kafka/" + jarName + ".jar");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Cannot find " + jarName + " on classpath (class " + classInJar + " not found)", e);
        }
        catch (URISyntaxException | IOException e) {
            throw new RuntimeException("Failed to copy " + jarName + " to container", e);
        }
    }

    @Override
    protected void doClose()
    {
        if (trino != null) {
            trino.close();
            trino = null;
        }
        if (kafka != null) {
            try {
                kafka.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            kafka = null;
        }
        schemaRegistryClient = null;
    }
}
