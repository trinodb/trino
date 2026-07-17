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
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Kafka product test environment with both the table-definition and Schema Registry catalogs.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Kafka and Schema Registry containers</li>
 *   <li>Trino container with "kafka" and "kafka_schema_registry" catalogs</li>
 *   <li>Access to Schema Registry client for registering schemas in tests</li>
 * </ul>
 */
public class KafkaSchemaRegistryEnvironment
        extends KafkaBasicEnvironment
{
    private SchemaRegistryClient schemaRegistryClient;

    @Override
    public void start()
    {
        if (isRunning()) {
            return;
        }

        super.start();

        // Create Schema Registry client for tests to register schemas
        schemaRegistryClient = new CachedSchemaRegistryClient(
                getKafka().getSchemaRegistryConnectString(),
                100);
    }

    @Override
    protected TestingKafka createKafka()
    {
        return TestingKafka.createWithSchemaRegistry();
    }

    @Override
    protected void customizeTrinoBuilder(TrinoProductTestContainer.Builder builder)
    {
        builder.withCatalog("kafka_schema_registry", getSchemaRegistryCatalogProperties());
    }

    protected Map<String, String> getSchemaRegistryCatalogProperties()
    {
        return new HashMap<>(Map.of(
                "connector.name", "kafka",
                "kafka.nodes", "kafka:9093",
                "kafka.table-description-supplier", "confluent",
                "kafka.confluent-schema-registry-url", "http://schema-registry:8081",
                "kafka.default-schema", "product_tests"));
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
        return getKafka().getSchemaRegistryConnectString();
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
        schemaRegistryClient = null;
        super.doClose();
    }
}
