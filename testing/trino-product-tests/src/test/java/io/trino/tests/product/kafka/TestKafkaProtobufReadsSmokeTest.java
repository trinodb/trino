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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

@ProductTest
@RequiresEnvironment(KafkaBasicEnvironment.class)
@TestGroup.Kafka
@TestGroup.ProfileSpecificTests
class TestKafkaProtobufReadsSmokeTest
{
    private static final String KAFKA_CATALOG = "kafka";
    private static final String KAFKA_SCHEMA = "product_tests";

    @Test
    void testSelectPrimitiveDataType(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "read_basic_datatypes_protobuf";
        env.createTopic(topicName);

        // Send Protobuf message with primitive data types
        ProtobufSchema protobufSchema = loadProtobufSchema("kafka-tables/basic_datatypes.proto");
        Map<String, Object> record = ImmutableMap.<String, Object>builder()
                .put("a_varchar", "foobar")
                .put("b_integer", 314)
                .put("c_bigint", 9223372036854775807L)
                .put("d_double", 1234567890.123456789)
                .put("e_float", 3.14f)
                .put("f_boolean", true)
                .buildOrThrow();

        byte[] protobufData = serializeProtobuf(protobufSchema, record);
        sendMessage(env, topicName, protobufData);

        Thread.sleep(2000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM " + KAFKA_CATALOG + "." + KAFKA_SCHEMA + "." + topicName)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("foobar");
            assertThat(rs.getInt(2)).isEqualTo(314);
            assertThat(rs.getLong(3)).isEqualTo(9223372036854775807L);
            assertThat(rs.getDouble(4)).isEqualTo(1234567890.123456789);
            assertThat(rs.getFloat(5)).isEqualTo(3.14f);
            assertThat(rs.getBoolean(6)).isTrue();
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    void testSelectStructuralDataType(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "read_basic_structural_datatypes_protobuf";
        env.createTopic(topicName);

        // Send Protobuf message with array and map
        ProtobufSchema protobufSchema = loadProtobufSchema("kafka-tables/basic_structural_datatypes.proto");
        Map<String, Object> record = ImmutableMap.of(
                "a_array", ImmutableList.of(100L, 101L),
                "a_map", ImmutableMap.of(
                        "key", "key1",
                        "value", 1234567890.123456789));

        byte[] protobufData = serializeProtobuf(protobufSchema, record);
        sendMessage(env, topicName, protobufData);

        Thread.sleep(2000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT a[1], a[2], m['key1'] FROM (" +
                                "SELECT c_array as a, c_map as m FROM " + KAFKA_CATALOG + "." + KAFKA_SCHEMA + "." + topicName +
                                ") t")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isEqualTo(100);
            assertThat(rs.getLong(2)).isEqualTo(101);
            assertThat(rs.getDouble(3)).isEqualTo(1234567890.123456789);
            assertThat(rs.next()).isFalse();
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

    private byte[] serializeProtobuf(ProtobufSchema protobufSchema, Map<String, Object> values)
    {
        return buildDynamicMessage(protobufSchema.toDescriptor(), values).toByteArray();
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
