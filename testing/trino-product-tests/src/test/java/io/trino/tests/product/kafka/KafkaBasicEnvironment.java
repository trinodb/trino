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

import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.kafka.TestingKafka;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.trino.TrinoContainer;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Basic Kafka product test environment with the "kafka" catalog using table definition files.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Kafka and Schema Registry containers</li>
 *   <li>Trino container with "kafka" catalog configured with table definitions</li>
 *   <li>Table definitions for CSV, JSON, raw, Avro, and Protobuf formats</li>
 *   <li>Column mapping support (Avro/Protobuf field names mapped to different column names)</li>
 * </ul>
 */
public class KafkaBasicEnvironment
        extends KafkaEnvironment
{
    protected static final String KAFKA_TABLE_DIR = "/etc/trino/kafka-tables";

    // Table definition files to copy (JSON files define the tables)
    private static final String[] TABLE_DEFINITION_FILES = {
            // Basic read/write tests
            "read_simple_key_and_value.json",
            "read_all_datatypes_csv.json",
            "read_all_datatypes_json.json",
            "read_all_datatypes_raw.json",
            "write_simple_key_and_value.json",
            "write_all_datatypes_csv.json",
            "write_all_datatypes_json.json",
            "write_all_datatypes_raw.json",
            // Pushdown tests
            "pushdown_partition.json",
            "pushdown_offset.json",
            "pushdown_create_time.json",
            // Avro tests
            "read_all_datatypes_avro.json",
            "read_all_null_avro.json",
            "read_structural_datatype_avro.json",
            "write_all_datatypes_avro.json",
            "write_structural_datatype_avro.json",
            // Protobuf tests
            "read_basic_datatypes_protobuf.json",
            "read_basic_structural_datatypes_protobuf.json",
            "all_datatypes_protobuf.json",
            "structural_datatype_protobuf.json",
    };

    // Schema files referenced by table definitions (copied but not registered as tables)
    private static final String[] SCHEMA_FILES = {
            "all_datatypes_avro_schema.avsc",
            "structural_datatype_avro_schema.avsc",
            "schema_with_references.avsc",
            "basic_datatypes.proto",
            "basic_structural_datatypes.proto",
            "all_datatypes.proto",
            "structural_datatype.proto",
    };

    protected TestingKafka kafka;
    protected TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return;
        }

        kafka = createKafka();
        kafka.start();

        TrinoProductTestContainer.Builder builder = TrinoProductTestContainer.builder()
                .withNetwork(kafka.getNetwork())
                .withNetworkAlias("trino")
                .withCatalog("kafka", getKafkaCatalogProperties());
        customizeTrinoBuilder(builder);
        trino = builder.build();

        copyTableDefinitions(trino);
        customizeTrinoContainer(trino);

        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }
    }

    private String buildTableNames()
    {
        StringBuilder names = new StringBuilder();
        for (String file : TABLE_DEFINITION_FILES) {
            if (names.length() > 0) {
                names.append(",");
            }
            // Table name is product_tests.<filename without .json>
            String tableName = file.replace(".json", "");
            names.append("product_tests.").append(tableName);
        }
        return names.toString();
    }

    private void copyTableDefinitions(TrinoContainer container)
    {
        // Copy table definition files
        for (String file : TABLE_DEFINITION_FILES) {
            String content = loadResource("kafka-tables/" + file);
            container.withCopyToContainer(
                    Transferable.of(content),
                    KAFKA_TABLE_DIR + "/" + file);
        }
        // Copy schema files (referenced by Avro/Protobuf table definitions)
        for (String file : SCHEMA_FILES) {
            String content = loadResource("kafka-tables/" + file);
            container.withCopyToContainer(
                    Transferable.of(content),
                    KAFKA_TABLE_DIR + "/" + file);
        }
    }

    private String loadResource(String path)
    {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalArgumentException("Resource not found: " + path);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String getCatalogName()
    {
        return "kafka";
    }

    @Override
    public String getTopicNameSuffix()
    {
        return "";
    }

    @Override
    public boolean supportsColumnMapping()
    {
        return true;
    }

    @Override
    public TestingKafka getKafka()
    {
        return kafka;
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
    }

    protected TestingKafka createKafka()
    {
        return TestingKafka.createWithSchemaRegistry();
    }

    protected Map<String, String> getKafkaCatalogProperties()
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector.name", "kafka");
        properties.put("kafka.nodes", "kafka:9093");
        properties.put("kafka.table-names", buildTableNames());
        properties.put("kafka.table-description-dir", KAFKA_TABLE_DIR);
        return properties;
    }

    protected void customizeTrinoBuilder(TrinoProductTestContainer.Builder builder)
    {
    }

    protected void customizeTrinoContainer(TrinoContainer container)
    {
    }
}
