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
import com.google.common.io.Resources;
import com.google.common.primitives.Ints;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Protobuf read tests using the kafka_schema_registry catalog with Schema Registry.
 * <p>
 * These tests use Schema Registry wire format: magic byte (0) + schema ID (4 bytes) + message index (varint) + protobuf data.
 * Column names use the original Protobuf field names (no column mapping).
 */
@ProductTest
@RequiresEnvironment(KafkaSchemaRegistryEnvironment.class)
@TestGroup.KafkaSchemaRegistry
@TestGroup.KafkaConfluentLicense
@TestGroup.ProfileSpecificTests
class TestKafkaProtobufReadsSchemaRegistrySmokeTest
{
    private static final String KAFKA_CATALOG = "kafka_schema_registry";
    private static final String KAFKA_SCHEMA = "product_tests";

    @Test
    void testSelectPrimitiveDataType(KafkaSchemaRegistryEnvironment env)
            throws Exception
    {
        String topicName = "read_basic_datatypes_protobuf_schema_registry";
        env.createTopic(topicName);

        // Send Protobuf message with primitive data types using Schema Registry format
        ProtobufSchema protobufSchema = loadProtobufSchema("kafka-tables/basic_datatypes.proto");
        Map<String, Object> record = ImmutableMap.<String, Object>builder()
                .put("a_varchar", "foobar")
                .put("b_integer", 314)
                .put("c_bigint", 9223372036854775807L)
                .put("d_double", 1234567890.123456789)
                .put("e_float", 3.14f)
                .put("f_boolean", true)
                .buildOrThrow();

        byte[] protobufData = serializeProtobufSchemaRegistry(env, topicName, protobufSchema, record);
        sendMessage(env, topicName, protobufData);

        Thread.sleep(2000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM " + KAFKA_CATALOG + "." + KAFKA_SCHEMA + ".\"" + topicName + "\"")) {
            assertThat(rs.next()).isTrue();
            // Schema Registry uses original field names (no column mapping)
            assertThat(rs.getString("a_varchar")).isEqualTo("foobar");
            assertThat(rs.getInt("b_integer")).isEqualTo(314);
            assertThat(rs.getLong("c_bigint")).isEqualTo(9223372036854775807L);
            assertThat(rs.getDouble("d_double")).isEqualTo(1234567890.123456789);
            assertThat(rs.getFloat("e_float")).isEqualTo(3.14f);
            assertThat(rs.getBoolean("f_boolean")).isTrue();
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    void testSelectStructuralDataType(KafkaSchemaRegistryEnvironment env)
            throws Exception
    {
        String topicName = "read_basic_structural_datatypes_protobuf_schema_registry";
        env.createTopic(topicName);

        // Send Protobuf message with array and map using Schema Registry format
        ProtobufSchema protobufSchema = loadProtobufSchema("kafka-tables/basic_structural_datatypes.proto");
        Map<String, Object> record = ImmutableMap.of(
                "a_array", ImmutableList.of(100L, 101L),
                "a_map", ImmutableMap.of(
                        "key", "key1",
                        "value", 1234567890.123456789));

        byte[] protobufData = serializeProtobufSchemaRegistry(env, topicName, protobufSchema, record);
        sendMessage(env, topicName, protobufData);

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
            assertThat(rs.getLong(2)).isEqualTo(101);
            assertThat(rs.getDouble(3)).isEqualTo(1234567890.123456789);
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    void testProtobufWithSchemaReferences(KafkaSchemaRegistryEnvironment env)
            throws Exception
    {
        String topicName = "all_datatypes_protobuf_schema_registry";
        env.createTopic(topicName);

        // First, register the timestamp reference schema
        String timestampTopic = "timestamp";
        String timestampProtoFile = "google/protobuf/timestamp.proto";
        ProtobufSchema timestampSchema = new ProtobufSchema(
                Resources.toString(Resources.getResource(TestKafkaProtobufReadsSchemaRegistrySmokeTest.class, "/" + timestampProtoFile), StandardCharsets.UTF_8),
                ImmutableList.of(),
                ImmutableMap.of(),
                null,
                timestampProtoFile);
        env.getSchemaRegistryClient().register(timestampTopic, timestampSchema);

        // Load the all_datatypes proto that references timestamp
        ProtobufSchema allDatatypesSchema = new ProtobufSchema(
                loadResourceString("kafka-tables/all_datatypes.proto"),
                ImmutableList.of(new SchemaReference(timestampSchema.name(), timestampTopic, 1)),
                ImmutableMap.of(timestampProtoFile, timestampSchema.canonicalString()),
                null,
                null);

        // Create timestamp value
        LocalDateTime timestamp = LocalDateTime.parse("2020-12-12T15:35:45.923");
        Timestamp timestampProto = Timestamp.newBuilder()
                .setSeconds(timestamp.toEpochSecond(ZoneOffset.UTC))
                .setNanos(timestamp.getNano())
                .build();

        Map<String, Object> record = ImmutableMap.<String, Object>builder()
                .put("a_varchar", "foobar")
                .put("b_integer", 2)
                .put("c_bigint", 9223372036854775807L)
                .put("d_double", 1234567890.123456789)
                .put("e_float", 3.14f)
                .put("f_boolean", true)
                .put("h_timestamp", timestampProto)
                .buildOrThrow();

        byte[] protobufData = serializeProtobufSchemaRegistryWithRefs(env, topicName, allDatatypesSchema, record);
        sendMessage(env, topicName, protobufData);

        Thread.sleep(2000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM " + KAFKA_CATALOG + "." + KAFKA_SCHEMA + ".\"" + topicName + "\"")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("a_varchar")).isEqualTo("foobar");
            assertThat(rs.getInt("b_integer")).isEqualTo(2);
            assertThat(rs.getLong("c_bigint")).isEqualTo(9223372036854775807L);
            assertThat(rs.getDouble("d_double")).isEqualTo(1234567890.123456789);
            assertThat(rs.getFloat("e_float")).isEqualTo(3.14f);
            assertThat(rs.getBoolean("f_boolean")).isTrue();
            assertThat(rs.getString("g_enum")).isEqualTo("ZERO");
            assertThat(rs.getTimestamp("h_timestamp")).isEqualTo(java.sql.Timestamp.valueOf(timestamp));
            assertThat(rs.next()).isFalse();
        }
    }

    private String loadResourceString(String resourcePath)
            throws IOException
    {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalArgumentException("Resource not found: " + resourcePath);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private ProtobufSchema loadProtobufSchema(String resourcePath)
            throws IOException
    {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalArgumentException("Resource not found: " + resourcePath);
            }
            String protoContent = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            return new ProtobufSchema(protoContent);
        }
    }

    /**
     * Serializes a Protobuf message using Schema Registry wire format:
     * - Magic byte (0)
     * - Schema ID (4 bytes, big endian)
     * - Message index (varint, 0 for single message type)
     * - Protobuf encoded message
     */
    private byte[] serializeProtobufSchemaRegistry(KafkaSchemaRegistryEnvironment env, String topic, ProtobufSchema protobufSchema, Map<String, Object> values)
            throws Exception
    {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            // Magic byte
            out.write((byte) 0);

            // Register schema and get ID
            SchemaRegistryClient client = env.getSchemaRegistryClient();
            int schemaId = client.register(topic + "-value", protobufSchema);
            out.write(Ints.toByteArray(schemaId));

            // Message index (0 for single message type)
            out.write((byte) 0);

            // Protobuf encoded message
            out.write(buildDynamicMessage(protobufSchema.toDescriptor(), values).toByteArray());

            return out.toByteArray();
        }
    }

    /**
     * Serializes a Protobuf message using Schema Registry wire format with schema references.
     */
    private byte[] serializeProtobufSchemaRegistryWithRefs(KafkaSchemaRegistryEnvironment env, String topic, ProtobufSchema protobufSchema, Map<String, Object> values)
            throws Exception
    {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            // Magic byte
            out.write((byte) 0);

            // Register schema and get ID
            SchemaRegistryClient client = env.getSchemaRegistryClient();
            int schemaId = client.register(topic + "-value", protobufSchema);
            out.write(Ints.toByteArray(schemaId));

            // Message index (0 for single message type)
            out.write((byte) 0);

            // Protobuf encoded message
            out.write(buildDynamicMessage(protobufSchema.toDescriptor(), values).toByteArray());

            return out.toByteArray();
        }
    }

    @SuppressWarnings("unchecked")
    private DynamicMessage buildDynamicMessage(Descriptor descriptor, Map<String, Object> data)
    {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            FieldDescriptor fieldDescriptor = descriptor.findFieldByName(entry.getKey());
            if (entry.getValue() instanceof Map<?, ?>) {
                builder.setField(
                        fieldDescriptor,
                        ImmutableList.of(
                                buildDynamicMessage(fieldDescriptor.getMessageType(), (Map<String, Object>) entry.getValue())));
            }
            else {
                builder.setField(fieldDescriptor, entry.getValue());
            }
        }
        return builder.build();
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
