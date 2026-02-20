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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

@ProductTest
@RequiresEnvironment(KafkaBasicEnvironment.class)
@TestGroup.Kafka
@TestGroup.ProfileSpecificTests
class TestKafkaAvroReadsSmokeTest
{
    private static final String KAFKA_CATALOG = "kafka";
    private static final String KAFKA_SCHEMA = "product_tests";

    @Test
    void testSelectPrimitiveDataType(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "read_all_datatypes_avro";
        env.createTopic(topicName);

        // Send Avro message with primitive data types
        Schema schema = loadAvroSchema("kafka-tables/all_datatypes_avro_schema.avsc");
        GenericRecord record = new GenericData.Record(schema);
        record.put("a_varchar", "foobar");
        record.put("a_bigint", 127L);
        record.put("a_double", 234.567);
        record.put("a_boolean", true);

        byte[] avroData = serializeAvro(schema, record);
        sendMessage(env, topicName, avroData);

        Thread.sleep(2000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM " + KAFKA_CATALOG + "." + KAFKA_SCHEMA + "." + topicName)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("c_varchar")).isEqualTo("foobar");
            assertThat(rs.getLong("c_bigint")).isEqualTo(127);
            assertThat(rs.getDouble("c_double")).isEqualTo(234.567);
            assertThat(rs.getBoolean("c_boolean")).isTrue();
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    void testNullType(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "read_all_null_avro";
        env.createTopic(topicName);

        // Send Avro message with all null values
        Schema schema = loadAvroSchema("kafka-tables/all_datatypes_avro_schema.avsc");
        GenericRecord record = new GenericData.Record(schema);
        // All fields default to null

        byte[] avroData = serializeAvro(schema, record);
        sendMessage(env, topicName, avroData);

        Thread.sleep(2000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM " + KAFKA_CATALOG + "." + KAFKA_SCHEMA + "." + topicName)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getObject("c_varchar")).isNull();
            assertThat(rs.getObject("c_bigint")).isNull();
            assertThat(rs.getObject("c_double")).isNull();
            assertThat(rs.getObject("c_boolean")).isNull();
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    void testSelectStructuralDataType(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "read_structural_datatype_avro";
        env.createTopic(topicName);

        // Send Avro message with array and map
        Schema schema = loadAvroSchema("kafka-tables/structural_datatype_avro_schema.avsc");
        GenericRecord record = new GenericData.Record(schema);
        record.put("a_array", List.of(100L, 102L));
        record.put("a_map", Map.of("key1", "value1"));

        byte[] avroData = serializeAvro(schema, record);
        sendMessage(env, topicName, avroData);

        Thread.sleep(2000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT a[1], a[2], m['key1'] FROM (" +
                                "SELECT c_array as a, c_map as m FROM " + KAFKA_CATALOG + "." + KAFKA_SCHEMA + "." + topicName +
                                ") t")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isEqualTo(100);
            assertThat(rs.getLong(2)).isEqualTo(102);
            assertThat(rs.getString(3)).isEqualTo("value1");
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

    private byte[] serializeAvro(Schema schema, GenericRecord record)
            throws IOException
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            dataFileWriter.create(schema, outputStream);
            dataFileWriter.append(record);
        }
        return outputStream.toByteArray();
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
