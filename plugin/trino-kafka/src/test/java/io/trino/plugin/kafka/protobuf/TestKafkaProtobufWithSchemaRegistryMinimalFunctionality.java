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
package io.trino.plugin.kafka.protobuf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.trino.plugin.kafka.schema.confluent.KafkaWithConfluentSchemaRegistryQueryRunner;
import io.trino.spi.type.SqlTimestamp;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static com.google.common.io.Resources.getResource;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.ENUM;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.STRING;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static io.trino.decoder.protobuf.ProtobufRowDecoderFactory.DEFAULT_MESSAGE;
import static io.trino.decoder.protobuf.ProtobufUtils.getFileDescriptor;
import static io.trino.decoder.protobuf.ProtobufUtils.getProtoFile;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static java.lang.Math.floorDiv;
import static java.lang.Math.multiplyExact;
import static java.lang.StrictMath.floorMod;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestKafkaProtobufWithSchemaRegistryMinimalFunctionality
        extends AbstractTestQueryFramework
{
    private static final String RECORD_NAME = "schema";
    private static final int MESSAGE_COUNT = 100;

    private TestingKafka testingKafka;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(TestingKafka.createWithSchemaRegistry());
        return KafkaWithConfluentSchemaRegistryQueryRunner.builder(testingKafka).build();
    }

    @Test
    public void testBasicTopic()
            throws Exception
    {
        String topic = "topic-basic-MixedCase";
        assertTopic(
                topic,
                format("SELECT col_1, col_2 FROM %s", toDoubleQuoted(topic)),
                format("SELECT col_1, col_2, col_3 FROM %s", toDoubleQuoted(topic)),
                false,
                producerProperties());
    }

    @Test
    public void testTopicWithKeySubject()
            throws Exception
    {
        String topic = "topic-Key-Subject";
        assertTopic(
                topic,
                format("SELECT key, col_1, col_2 FROM %s", toDoubleQuoted(topic)),
                format("SELECT key, col_1, col_2, col_3 FROM %s", toDoubleQuoted(topic)),
                true,
                producerProperties());
    }

    @Test
    public void testTopicWithRecordNameStrategy()
            throws Exception
    {
        String topic = "topic-Record-Name-Strategy";
        assertTopic(
                topic,
                format("SELECT key, col_1, col_2 FROM \"%1$s&value-subject=%2$s\"", topic, RECORD_NAME),
                format("SELECT key, col_1, col_2, col_3 FROM \"%1$s&value-subject=%2$s\"", topic, RECORD_NAME),
                true,
                ImmutableMap.<String, String>builder()
                        .putAll(producerProperties())
                        .put(VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName())
                        .buildOrThrow());
    }

    @Test
    public void testTopicWithTopicRecordNameStrategy()
            throws Exception
    {
        String topic = "topic-Topic-Record-Name-Strategy";
        assertTopic(
                topic,
                format("SELECT key, col_1, col_2 FROM \"%1$s&value-subject=%1$s-%2$s\"", topic, RECORD_NAME),
                format("SELECT key, col_1, col_2, col_3 FROM \"%1$s&value-subject=%1$s-%2$s\"", topic, RECORD_NAME),
                true,
                ImmutableMap.<String, String>builder()
                        .putAll(producerProperties())
                        .put(VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
                        .buildOrThrow());
    }

    @Test
    public void testBasicTopicForInsert()
            throws Exception
    {
        String topic = "topic-basic-inserts";
        assertTopic(
                topic,
                format("SELECT col_1, col_2 FROM %s", toDoubleQuoted(topic)),
                format("SELECT col_1, col_2, col_3 FROM %s", toDoubleQuoted(topic)),
                false,
                producerProperties());
        assertQueryFails(
                format("INSERT INTO %s (col_1, col_2, col_3) VALUES ('Trino', 14, 1.4)", toDoubleQuoted(topic)),
                "Insert is not supported for schema registry based tables");
    }

    @Test
    public void testUnsupportedRecursiveDataTypes()
            throws Exception
    {
        String topic = "topic-unsupported-recursive";
        assertNotExists(topic);

        UnsupportedRecursiveTypes.schema message = UnsupportedRecursiveTypes.schema.newBuilder()
                .setRecursiveValueOne(UnsupportedRecursiveTypes.RecursiveValue.newBuilder().setStringValue("Value1").build())
                .build();

        ImmutableList.Builder<ProducerRecord<DynamicMessage, UnsupportedRecursiveTypes.schema>> producerRecordBuilder = ImmutableList.builder();
        producerRecordBuilder.add(new ProducerRecord<>(topic, createKeySchema(0, getKeySchema()), message));
        List<ProducerRecord<DynamicMessage, UnsupportedRecursiveTypes.schema>> messages = producerRecordBuilder.build();
        testingKafka.sendMessages(
                messages.stream(),
                producerProperties());

        waitUntilTableExists(topic);
        assertQueryFails("SELECT * FROM " + toDoubleQuoted(topic),
                "Protobuf schema containing fields with self-reference are not supported because they cannot be mapped to a Trino type: " +
                        "io.trino.protobuf.schema.recursive_value_one: io.trino.protobuf.RecursiveValue > " +
                        "io.trino.protobuf.RecursiveValue.struct_value: io.trino.protobuf.RecursiveStruct > " +
                        "io.trino.protobuf.RecursiveStruct.fields: io.trino.protobuf.RecursiveStruct.FieldsEntry > " +
                        "io.trino.protobuf.RecursiveStruct.FieldsEntry.value: io.trino.protobuf.RecursiveValue");
    }

    @Test
    public void testSchemaWithImportDataTypes()
            throws Exception
    {
        String topic = "topic-schema-with-import";
        assertNotExists(topic);

        Descriptor descriptor = getDescriptor("structural_datatypes.proto");

        Timestamp timestamp = getTimestamp(sqlTimestampOf(3, LocalDateTime.parse("2020-12-12T15:35:45.923")));
        DynamicMessage message = buildDynamicMessage(
                descriptor,
                ImmutableMap.<String, Object>builder()
                        .put("list", ImmutableList.of("Search"))
                        .put("map", ImmutableList.of(buildDynamicMessage(
                                descriptor.findFieldByName("map").getMessageType(),
                                ImmutableMap.of("key", "Key1", "value", "Value1"))))
                        .put("row", ImmutableMap.<String, Object>builder()
                                .put("string_column", "Trino")
                                .put("integer_column", 1)
                                .put("long_column", 493857959588286460L)
                                .put("double_column", 3.14159265358979323846)
                                .put("float_column", 3.14f)
                                .put("boolean_column", true)
                                .put("number_column", descriptor.findEnumTypeByName("Number").findValueByName("ONE"))
                                .put("timestamp_column", timestamp)
                                .put("bytes_column", "Trino".getBytes(UTF_8))
                                .buildOrThrow())
                        .buildOrThrow());

        ImmutableList.Builder<ProducerRecord<DynamicMessage, DynamicMessage>> producerRecordBuilder = ImmutableList.builder();
        producerRecordBuilder.add(new ProducerRecord<>(topic, createKeySchema(0, getKeySchema()), message));
        List<ProducerRecord<DynamicMessage, DynamicMessage>> messages = producerRecordBuilder.build();
        testingKafka.sendMessages(
                messages.stream(),
                producerProperties());
        waitUntilTableExists(topic);

        assertThat(query(format("SELECT list, map, row FROM %s", toDoubleQuoted(topic))))
                .matches("""
                        VALUES (
                            ARRAY[CAST('Search' AS VARCHAR)],
                            MAP(CAST(ARRAY['Key1'] AS ARRAY(VARCHAR)), CAST(ARRAY['Value1'] AS ARRAY(VARCHAR))),
                            CAST(ROW('Trino', 1, 493857959588286460, 3.14159265358979323846, 3.14, True, 'ONE', TIMESTAMP '2020-12-12 15:35:45.923', to_utf8('Trino'))
                                AS ROW(
                                    string_column VARCHAR,
                                    integer_column INTEGER,
                                    long_column BIGINT,
                                    double_column DOUBLE,
                                    float_column REAL,
                                    boolean_column BOOLEAN,
                                    number_column VARCHAR,
                                    timestamp_column TIMESTAMP(6),
                                    bytes_column VARBINARY)))""");
    }

    @Test
    public void testOneof()
            throws Exception
    {
        String topic = "topic-schema-with-oneof";
        assertNotExists(topic);

        String stringData = "stringColumnValue1";

        ProtobufSchema schema = (ProtobufSchema) new ProtobufSchemaProvider().parseSchema(Resources.toString(getResource("protobuf/test_oneof.proto"), UTF_8), List.of(), true).get();

        Descriptor descriptor = schema.toDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("stringColumn"), stringData)
                .build();

        ImmutableList.Builder<ProducerRecord<DynamicMessage, DynamicMessage>> producerRecordBuilder = ImmutableList.builder();
        producerRecordBuilder.add(new ProducerRecord<>(topic, createKeySchema(0, getKeySchema()), message));
        List<ProducerRecord<DynamicMessage, DynamicMessage>> messages = producerRecordBuilder.build();
        testingKafka.sendMessages(messages.stream(), producerProperties());
        waitUntilTableExists(topic);

        assertThat(query(format("SELECT testOneOfColumn FROM %s", toDoubleQuoted(topic))))
                .matches("""
                        VALUES (JSON '{"stringColumn":"%s"}')
                        """.formatted(stringData));
    }

    private DynamicMessage buildDynamicMessage(Descriptor descriptor, Map<String, Object> data)
    {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            FieldDescriptor fieldDescriptor = descriptor.findFieldByName(entry.getKey());
            if (entry.getValue() instanceof Map<?, ?>) {
                builder.setField(fieldDescriptor, buildDynamicMessage(fieldDescriptor.getMessageType(), (Map<String, Object>) entry.getValue()));
            }
            else {
                builder.setField(fieldDescriptor, entry.getValue());
            }
        }

        return builder.build();
    }

    protected static Timestamp getTimestamp(SqlTimestamp sqlTimestamp)
    {
        return Timestamp.newBuilder()
                .setSeconds(floorDiv(sqlTimestamp.getEpochMicros(), MICROSECONDS_PER_SECOND))
                .setNanos(floorMod(sqlTimestamp.getEpochMicros(), MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND)
                .build();
    }

    private Map<String, String> producerProperties()
    {
        return ImmutableMap.of(
                SCHEMA_REGISTRY_URL_CONFIG, testingKafka.getSchemaRegistryConnectString(),
                KEY_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName());
    }

    private void assertTopic(String topicName, String initialQuery, String evolvedQuery, boolean isKeyIncluded, Map<String, String> producerConfig)
            throws Exception
    {
        testingKafka.createTopic(topicName);

        assertNotExists(topicName);

        List<ProducerRecord<DynamicMessage, DynamicMessage>> messages = createMessages(topicName, MESSAGE_COUNT, true, getInitialSchema(), getKeySchema());
        testingKafka.sendMessages(messages.stream(), producerConfig);

        waitUntilTableExists(topicName);
        assertCount(topicName, MESSAGE_COUNT);

        assertQuery(initialQuery, getExpectedValues(messages, getInitialSchema(), isKeyIncluded));

        List<ProducerRecord<DynamicMessage, DynamicMessage>> newMessages = createMessages(topicName, MESSAGE_COUNT, false, getEvolvedSchema(), getKeySchema());
        testingKafka.sendMessages(newMessages.stream(), producerConfig);

        List<ProducerRecord<DynamicMessage, DynamicMessage>> allMessages = ImmutableList.<ProducerRecord<DynamicMessage, DynamicMessage>>builder()
                .addAll(messages)
                .addAll(newMessages)
                .build();
        assertCount(topicName, allMessages.size());
        assertQuery(evolvedQuery, getExpectedValues(allMessages, getEvolvedSchema(), isKeyIncluded));
    }

    private static String getExpectedValues(List<ProducerRecord<DynamicMessage, DynamicMessage>> messages, Descriptor descriptor, boolean isKeyIncluded)
    {
        StringBuilder valuesBuilder = new StringBuilder("VALUES ");
        ImmutableList.Builder<String> rowsBuilder = ImmutableList.builder();
        for (ProducerRecord<DynamicMessage, DynamicMessage> message : messages) {
            ImmutableList.Builder<String> columnsBuilder = ImmutableList.builder();

            if (isKeyIncluded) {
                addExpectedColumns(message.key().getDescriptorForType(), message.key(), columnsBuilder);
            }

            addExpectedColumns(descriptor, message.value(), columnsBuilder);

            rowsBuilder.add(format("(%s)", String.join(", ", columnsBuilder.build())));
        }
        valuesBuilder.append(String.join(", ", rowsBuilder.build()));
        return valuesBuilder.toString();
    }

    private static void addExpectedColumns(Descriptor descriptor, DynamicMessage message, ImmutableList.Builder<String> columnsBuilder)
    {
        for (FieldDescriptor field : descriptor.getFields()) {
            FieldDescriptor fieldDescriptor = message.getDescriptorForType().findFieldByName(field.getName());
            if (fieldDescriptor == null) {
                columnsBuilder.add("null");
                continue;
            }
            Object value = message.getField(message.getDescriptorForType().findFieldByName(field.getName()));
            if (field.getJavaType() == STRING || field.getJavaType() == ENUM) {
                columnsBuilder.add(toSingleQuoted(value));
            }
            else {
                columnsBuilder.add(String.valueOf(value));
            }
        }
    }

    private void assertNotExists(String tableName)
    {
        if (schemaExists()) {
            assertQueryReturnsEmptyResult(format("SHOW TABLES LIKE '%s'", tableName));
        }
    }

    private void waitUntilTableExists(String tableName)
    {
        Failsafe.with(
                        RetryPolicy.builder()
                                .withMaxAttempts(10)
                                .withDelay(Duration.ofMillis(100))
                                .build())
                .run(() -> assertTrue(schemaExists()));
        Failsafe.with(
                        RetryPolicy.builder()
                                .withMaxAttempts(10)
                                .withDelay(Duration.ofMillis(100))
                                .build())
                .run(() -> assertTrue(tableExists(tableName)));
    }

    private boolean schemaExists()
    {
        return computeActual(format("SHOW SCHEMAS FROM %s LIKE '%s'", getSession().getCatalog().get(), getSession().getSchema().get())).getRowCount() == 1;
    }

    private boolean tableExists(String tableName)
    {
        return computeActual(format("SHOW TABLES LIKE '%s'", tableName.toLowerCase(ENGLISH))).getRowCount() == 1;
    }

    private void assertCount(String tableName, int count)
    {
        assertQuery(format("SELECT count(*) FROM %s", toDoubleQuoted(tableName)), format("VALUES (%s)", count));
    }

    private static Descriptor getInitialSchema()
            throws Exception
    {
        return getDescriptor("initial_schema.proto");
    }

    private static Descriptor getEvolvedSchema()
            throws Exception
    {
        return getDescriptor("evolved_schema.proto");
    }

    private static Descriptor getKeySchema()
            throws Exception
    {
        return getDescriptor("key_schema.proto");
    }

    public static Descriptor getDescriptor(String fileName)
            throws Exception
    {
        return getFileDescriptor(getProtoFile("protobuf/" + fileName)).findMessageTypeByName(DEFAULT_MESSAGE);
    }

    private static String toDoubleQuoted(String tableName)
    {
        return format("\"%s\"", tableName);
    }

    private static String toSingleQuoted(Object value)
    {
        requireNonNull(value, "value is null");
        return format("'%s'", value);
    }

    private static List<ProducerRecord<DynamicMessage, DynamicMessage>> createMessages(String topicName, int messageCount, boolean useInitialSchema, Descriptor descriptor, Descriptor keyDescriptor)
    {
        ImmutableList.Builder<ProducerRecord<DynamicMessage, DynamicMessage>> producerRecordBuilder = ImmutableList.builder();
        if (useInitialSchema) {
            for (long key = 0; key < messageCount; key++) {
                producerRecordBuilder.add(new ProducerRecord<>(topicName, createKeySchema(key, keyDescriptor), createRecordWithInitialSchema(key, descriptor)));
            }
        }
        else {
            for (long key = 0; key < messageCount; key++) {
                producerRecordBuilder.add(new ProducerRecord<>(topicName, createKeySchema(key, keyDescriptor), createRecordWithEvolvedSchema(key, descriptor)));
            }
        }
        return producerRecordBuilder.build();
    }

    private static DynamicMessage createKeySchema(long key, Descriptor descriptor)
    {
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("key"), key)
                .build();
    }

    private static DynamicMessage createRecordWithInitialSchema(long key, Descriptor descriptor)
    {
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("col_1"), format("string-%s", key))
                .setField(descriptor.findFieldByName("col_2"), multiplyExact(key, 100))
                .build();
    }

    private static DynamicMessage createRecordWithEvolvedSchema(long key, Descriptor descriptor)
    {
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("col_1"), format("string-%s", key))
                .setField(descriptor.findFieldByName("col_2"), multiplyExact(key, 100))
                .setField(descriptor.findFieldByName("col_3"), (key + 10.1D) / 10.0D)
                .build();
    }
}
