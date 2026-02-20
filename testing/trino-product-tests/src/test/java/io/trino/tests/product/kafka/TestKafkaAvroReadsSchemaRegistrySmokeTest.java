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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Avro read tests using the kafka_schema_registry catalog with Schema Registry.
 * <p>
 * These tests use Schema Registry wire format: magic byte (0) + schema ID (4 bytes) + binary Avro data.
 * Column names use the original Avro field names (no column mapping).
 */
@ProductTest
@RequiresEnvironment(KafkaSchemaRegistryEnvironment.class)
@TestGroup.KafkaSchemaRegistry
@TestGroup.Kafka
@TestGroup.ProfileSpecificTests
class TestKafkaAvroReadsSchemaRegistrySmokeTest
{
    private static final String KAFKA_CATALOG = "kafka_schema_registry";
    private static final String KAFKA_SCHEMA = "default";

    @Test
    void testSelectPrimitiveDataType(KafkaSchemaRegistryEnvironment env)
            throws Exception
    {
        String topicName = "read_all_datatypes_avro_schema_registry";
        env.createTopic(topicName);

        // Send Avro message with primitive data types using Schema Registry format
        Schema schema = loadAvroSchema("kafka-tables/all_datatypes_avro_schema.avsc");
        GenericRecord record = new GenericData.Record(schema);
        record.put("a_varchar", "foobar");
        record.put("a_bigint", 127L);
        record.put("a_double", 234.567);
        record.put("a_boolean", true);

        byte[] avroData = serializeAvroSchemaRegistry(env, topicName, schema, record);
        sendMessage(env, topicName, avroData);

        Thread.sleep(2000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM " + KAFKA_CATALOG + "." + KAFKA_SCHEMA + ".\"" + topicName + "\"")) {
            assertThat(rs.next()).isTrue();
            // Schema Registry uses original field names (no column mapping)
            assertThat(rs.getString("a_varchar")).isEqualTo("foobar");
            assertThat(rs.getLong("a_bigint")).isEqualTo(127);
            assertThat(rs.getDouble("a_double")).isEqualTo(234.567);
            assertThat(rs.getBoolean("a_boolean")).isTrue();
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    void testNullType(KafkaSchemaRegistryEnvironment env)
            throws Exception
    {
        String topicName = "read_all_null_avro_schema_registry";
        env.createTopic(topicName);

        // Send Avro message with all null values using Schema Registry format
        Schema schema = loadAvroSchema("kafka-tables/all_datatypes_avro_schema.avsc");
        GenericRecord record = new GenericData.Record(schema);
        // All fields default to null

        byte[] avroData = serializeAvroSchemaRegistry(env, topicName, schema, record);
        sendMessage(env, topicName, avroData);

        Thread.sleep(2000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM " + KAFKA_CATALOG + "." + KAFKA_SCHEMA + ".\"" + topicName + "\"")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getObject("a_varchar")).isNull();
            assertThat(rs.getObject("a_bigint")).isNull();
            assertThat(rs.getObject("a_double")).isNull();
            assertThat(rs.getObject("a_boolean")).isNull();
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    void testSelectStructuralDataType(KafkaSchemaRegistryEnvironment env)
            throws Exception
    {
        String topicName = "read_structural_datatype_avro_schema_registry";
        env.createTopic(topicName);

        // Send Avro message with array and map using Schema Registry format
        Schema schema = loadAvroSchema("kafka-tables/structural_datatype_avro_schema.avsc");
        GenericRecord record = new GenericData.Record(schema);
        record.put("a_array", List.of(100L, 102L));
        record.put("a_map", Map.of("key1", "value1"));

        byte[] avroData = serializeAvroSchemaRegistry(env, topicName, schema, record);
        sendMessage(env, topicName, avroData);

        Thread.sleep(2000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT a[1], a[2], m['key1'] FROM (" +
                                // Schema Registry uses original field names
                                "SELECT a_array as a, a_map as m FROM " + KAFKA_CATALOG + "." + KAFKA_SCHEMA + ".\"" + topicName + "\"" +
                                ") t")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isEqualTo(100);
            assertThat(rs.getLong(2)).isEqualTo(102);
            assertThat(rs.getString(3)).isEqualTo("value1");
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    void testAvroWithSchemaReferences(KafkaSchemaRegistryEnvironment env)
            throws Exception
    {
        String topicName = "schema_with_references_avro";
        env.createTopic(topicName);

        // First, register the referred schema
        Schema referredSchema = loadAvroSchema("kafka-tables/all_datatypes_avro_schema.avsc");
        AvroSchema referredAvroSchema = new AvroSchema(referredSchema);
        String referredSubject = "all_datatypes_avro-value";
        env.getSchemaRegistryClient().register(referredSubject, referredAvroSchema);

        // Create a record for the referred type
        GenericRecord innerRecord = new GenericData.Record(referredSchema);
        innerRecord.put("a_varchar", "foobar");
        innerRecord.put("a_bigint", 127L);
        innerRecord.put("a_double", 234.567);
        innerRecord.put("a_boolean", true);

        // Load the schema with references
        Schema schemaWithReferences = loadAvroSchemaWithReferences(
                "kafka-tables/schema_with_references.avsc",
                referredSchema);

        // Create the outer record with the nested reference
        GenericRecord outerRecord = new GenericData.Record(schemaWithReferences);
        outerRecord.put("reference", innerRecord);

        // Register the schema with references
        SchemaReference reference = new SchemaReference(
                referredSchema.getName(),
                referredSubject,
                1);
        AvroSchema schemaWithRefs = new AvroSchema(
                schemaWithReferences.toString(),
                ImmutableList.of(reference),
                ImmutableMap.of(),
                null);

        byte[] avroData = serializeAvroSchemaRegistryWithRefs(env, topicName, schemaWithRefs, outerRecord);
        sendMessage(env, topicName, avroData);

        Thread.sleep(2000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT reference.a_varchar, reference.a_double FROM " +
                                KAFKA_CATALOG + "." + KAFKA_SCHEMA + ".\"" + topicName + "\"")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("foobar");
            assertThat(rs.getDouble(2)).isEqualTo(234.567);
            assertThat(rs.next()).isFalse();
        }
    }

    private Schema loadAvroSchema(String resourcePath)
            throws IOException
    {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalArgumentException("Resource not found: " + resourcePath);
            }
            String schemaJson = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            return new Schema.Parser().parse(schemaJson);
        }
    }

    private Schema loadAvroSchemaWithReferences(String resourcePath, Schema... types)
            throws IOException
    {
        Schema.Parser parser = new Schema.Parser();
        // Add the referenced types to the parser first
        for (Schema type : types) {
            parser.addTypes(Collections.singletonMap(type.getFullName(), type));
        }
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalArgumentException("Resource not found: " + resourcePath);
            }
            String schemaJson = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            return parser.parse(schemaJson);
        }
    }

    /**
     * Serializes an Avro record using Schema Registry wire format:
     * - Magic byte (0)
     * - Schema ID (4 bytes, big endian)
     * - Binary encoded Avro record
     */
    private byte[] serializeAvroSchemaRegistry(KafkaSchemaRegistryEnvironment env, String topic, Schema schema, GenericRecord record)
            throws IOException
    {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            // Magic byte
            out.write((byte) 0);

            // Register schema and get ID
            SchemaRegistryClient client = env.getSchemaRegistryClient();
            int schemaId = client.register(topic + "-value", new AvroSchema(schema));
            out.write(Ints.toByteArray(schemaId));

            // Binary encoded Avro record
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            writer.write(record, encoder);
            encoder.flush();

            return out.toByteArray();
        }
        catch (RestClientException e) {
            throw new RuntimeException("Failed to register schema", e);
        }
    }

    /**
     * Serializes an Avro record using Schema Registry wire format with schema references.
     */
    private byte[] serializeAvroSchemaRegistryWithRefs(KafkaSchemaRegistryEnvironment env, String topic, AvroSchema avroSchema, GenericRecord record)
            throws IOException
    {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            // Magic byte
            out.write((byte) 0);

            // Register schema and get ID
            SchemaRegistryClient client = env.getSchemaRegistryClient();
            int schemaId = client.register(topic + "-value", avroSchema);
            out.write(Ints.toByteArray(schemaId));

            // Binary encoded Avro record
            Schema schema = (Schema) avroSchema.rawSchema();
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            writer.write(record, encoder);
            encoder.flush();

            return out.toByteArray();
        }
        catch (RestClientException e) {
            throw new RuntimeException("Failed to register schema", e);
        }
    }

    private void sendMessage(KafkaEnvironment env, String topic, byte[] value)
    {
        env.getKafka().sendMessages(
                Stream.of(new ProducerRecord<>(topic, null, value)),
                Map.of(
                        KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                        VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()));
    }
}
