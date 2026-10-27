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
import com.google.protobuf.Any;
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
import io.trino.plugin.kafka.KafkaQueryRunner;
import io.trino.spi.type.SqlTimestamp;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.io.Resources.getResource;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.ENUM;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.STRING;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static io.trino.decoder.protobuf.ProtoFileDescriptorBuilder.buildFileDescriptor;
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
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
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
        testingKafka.start();
        return KafkaQueryRunner.builderForConfluentSchemaRegistry(testingKafka)
                .build();
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
    public void testNonFirstMessageWithRecordNameStrategy()
            throws Exception
    {
        String topic = "topic-non-first-message";
        assertNotExists(topic);

        Descriptor descriptor = getFileDescriptor(getProtoFile("protobuf/multiple_messages.proto")).findMessageTypeByName("Order");
        ImmutableList.Builder<ProducerRecord<DynamicMessage, DynamicMessage>> producerRecordBuilder = ImmutableList.builder();
        for (long key = 0; key < 3; key++) {
            producerRecordBuilder.add(new ProducerRecord<>(topic, createKeySchema(key, getKeySchema()), DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("id"), key * 10)
                    .setField(descriptor.findFieldByName("customer"), "customer-" + key)
                    .build()));
        }
        testingKafka.sendMessages(
                producerRecordBuilder.build().stream(),
                ImmutableMap.<String, String>builder()
                        .putAll(producerProperties())
                        .put(VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName())
                        .buildOrThrow());
        waitUntilTableExists(topic);

        String table = format("\"%s&value-subject=io.trino.protobuf.Order\"", topic);
        assertQuery(
                format("SELECT key, id, customer FROM %s", table),
                "VALUES (0, 0, 'customer-0'), (1, 10, 'customer-1'), (2, 20, 'customer-2')");

        // rows are written with the subject's message as well; its map and enum fields do not track presence, so they need a value
        assertUpdate(format("INSERT INTO %s (key, id, customer, attributes, status) VALUES (3, 30, 'customer-3', MAP(ARRAY['channel'], ARRAY['web']), 'SHIPPED')", table), 1);
        assertQuery(
                format("SELECT key, id, customer, attributes['channel'], status FROM %s WHERE id = 30", table),
                "VALUES (3, 30, 'customer-3', 'web', 'SHIPPED')");
    }

    @Test
    public void testNestedMessageWithTopicRecordNameStrategy()
            throws Exception
    {
        String topic = "topic-nested-message";
        assertNotExists(topic);

        Descriptor descriptor = getFileDescriptor(getProtoFile("protobuf/multiple_messages.proto")).findMessageTypeByName("Order").findNestedTypeByName("Line");
        ImmutableList.Builder<ProducerRecord<DynamicMessage, DynamicMessage>> producerRecordBuilder = ImmutableList.builder();
        for (long key = 0; key < 3; key++) {
            producerRecordBuilder.add(new ProducerRecord<>(topic, createKeySchema(key, getKeySchema()), DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("sku"), "sku-" + key)
                    .setField(descriptor.findFieldByName("quantity"), (int) key + 1)
                    .build()));
        }
        testingKafka.sendMessages(
                producerRecordBuilder.build().stream(),
                ImmutableMap.<String, String>builder()
                        .putAll(producerProperties())
                        .put(VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
                        .buildOrThrow());
        waitUntilTableExists(topic);

        String table = format("\"%1$s&value-subject=%1$s-io.trino.protobuf.Order.Line\"", topic);
        assertQuery(
                format("SELECT key, sku, quantity FROM %s", table),
                "VALUES (0, 'sku-0', 1), (1, 'sku-1', 2), (2, 'sku-2', 3)");

        // rows are written with the nested message, whose message indexes name its position within Order
        assertUpdate(format("INSERT INTO %s (key, sku, quantity) VALUES (3, 'sku-3', 4)", table), 1);
        assertQuery(
                format("SELECT key, sku, quantity FROM %s", table),
                "VALUES (0, 'sku-0', 1), (1, 'sku-1', 2), (2, 'sku-2', 3), (3, 'sku-3', 4)");
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

        // rows are written with the latest schema of the key and value subjects
        assertUpdate(format("INSERT INTO %s (key, col_1, col_2, col_3) VALUES (1000, 'Trino', 14, 1.4)", toDoubleQuoted(topic)), 1);
        assertCount(topic, MESSAGE_COUNT * 2 + 1);
        assertQuery(format("SELECT key, col_1, col_2, col_3 FROM %s WHERE key = 1000", toDoubleQuoted(topic)), "VALUES (1000, 'Trino', 14, 1.4)");

        // a proto3 field that is not optional has no value for NULL
        assertQueryFails(
                format("INSERT INTO %s (key, col_1, col_2, col_3) VALUES (1001, NULL, 1, 1.0)", toDoubleQuoted(topic)),
                "NULL value not allowed for protobuf field 'schema.col_1', which does not track presence; declare it optional to allow NULL");
    }

    @Test
    public void testInsertWithoutKeySubject()
            throws Exception
    {
        String topic = "topic-inserts-without-key";
        assertNotExists(topic);

        List<ProducerRecord<byte[], DynamicMessage>> messages = ImmutableList.of(new ProducerRecord<>(topic, null, createRecordWithInitialSchema(0, getInitialSchema())));
        testingKafka.sendMessages(
                messages.stream(),
                ImmutableMap.<String, String>builder()
                        .putAll(producerProperties())
                        .put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName())
                        .buildKeepingLast());
        waitUntilTableExists(topic);
        assertQuery(format("SELECT col_1, col_2 FROM %s", toDoubleQuoted(topic)), "VALUES ('string-0', 0)");

        // the records of a table without a key subject are written with a null key
        assertUpdate(format("INSERT INTO %s (col_1, col_2) VALUES ('Trino', 14)", toDoubleQuoted(topic)), 1);
        assertQuery(format("SELECT col_1, col_2 FROM %s", toDoubleQuoted(topic)), "VALUES ('string-0', 0), ('Trino', 14)");
        // SQL sees a null key as an empty one; TestNullRowEncoderFactory checks that the key is null
        assertQuery(format("SELECT _key_length FROM %s WHERE col_1 = 'Trino'", toDoubleQuoted(topic)), "VALUES 0");
    }

    @Test
    public void testSchemaWithReferences()
            throws Exception
    {
        String topic = "topic-schema-with-references";
        assertNotExists(topic);

        Map<String, String> imports = ImmutableMap.of("address.proto",
                """
                syntax = "proto3";
                package common;
                message Address {
                  string city = 1;
                }
                """);
        Descriptor descriptor = buildFileDescriptor(
                "order.proto",
                """
                syntax = "proto3";
                import "address.proto";
                message schema {
                  string col_1 = 1;
                  common.Address address = 2;
                }
                """,
                importPath -> Optional.ofNullable(imports.get(importPath)))
                .findMessageTypeByName(DEFAULT_MESSAGE);
        Descriptor addressDescriptor = descriptor.findFieldByName("address").getMessageType();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("col_1"), "Trino")
                .setField(descriptor.findFieldByName("address"), DynamicMessage.newBuilder(addressDescriptor).setField(addressDescriptor.findFieldByName("city"), "Boston").build())
                .build();

        // the serializer registers the imported file as its own subject and references it from the topic's subject
        testingKafka.sendMessages(Stream.of(new ProducerRecord<>(topic, createKeySchema(0, getKeySchema()), message)), producerProperties());
        waitUntilTableExists(topic);

        assertQuery(format("SELECT col_1, address.city FROM %s", toDoubleQuoted(topic)), "VALUES ('Trino', 'Boston')");

        assertUpdate(format("INSERT INTO %s (key, col_1, address) VALUES (1, 'Inserted', ROW('Berlin'))", toDoubleQuoted(topic)), 1);
        assertQuery(format("SELECT key, col_1, address.city FROM %s", toDoubleQuoted(topic)), "VALUES (0, 'Trino', 'Boston'), (1, 'Inserted', 'Berlin')");
    }

    @Test
    public void testNestedSchemaEvolution()
            throws Exception
    {
        String topic = "topic-nested-schema-evolution";
        assertNotExists(topic);

        Descriptor initial = getFileDescriptor(
                """
                syntax = "proto3";
                message schema {
                  string col_1 = 1;
                  message Detail {
                    string note = 1;
                  }
                  Detail detail = 2;
                }
                """).findMessageTypeByName(DEFAULT_MESSAGE);
        Descriptor initialDetail = initial.findFieldByName("detail").getMessageType();
        DynamicMessage initialMessage = DynamicMessage.newBuilder(initial)
                .setField(initial.findFieldByName("col_1"), "first")
                .setField(initial.findFieldByName("detail"), DynamicMessage.newBuilder(initialDetail).setField(initialDetail.findFieldByName("note"), "old").build())
                .build();
        testingKafka.sendMessages(Stream.of(new ProducerRecord<>(topic, createKeySchema(0, getKeySchema()), initialMessage)), producerProperties());
        waitUntilTableExists(topic);
        assertQuery(format("SELECT col_1, detail.note FROM %s", toDoubleQuoted(topic)), "VALUES ('first', 'old')");

        Descriptor evolved = getFileDescriptor(
                """
                syntax = "proto3";
                message schema {
                  string col_1 = 1;
                  message Detail {
                    string note = 1;
                    int64 weight = 2;
                  }
                  Detail detail = 2;
                  string col_2 = 3;
                }
                """).findMessageTypeByName(DEFAULT_MESSAGE);
        Descriptor evolvedDetail = evolved.findFieldByName("detail").getMessageType();
        DynamicMessage evolvedMessage = DynamicMessage.newBuilder(evolved)
                .setField(evolved.findFieldByName("col_1"), "second")
                .setField(evolved.findFieldByName("detail"), DynamicMessage.newBuilder(evolvedDetail)
                        .setField(evolvedDetail.findFieldByName("note"), "new")
                        .setField(evolvedDetail.findFieldByName("weight"), 7L)
                        .build())
                .setField(evolved.findFieldByName("col_2"), "added")
                .build();
        testingKafka.sendMessages(Stream.of(new ProducerRecord<>(topic, createKeySchema(1, getKeySchema()), evolvedMessage)), producerProperties());

        // records written with the previous schema decode with it: the fields it lacks are NULL, at the top level and within rows
        assertQueryEventually(
                getSession(),
                format("SELECT key, col_1, col_2, detail.note, detail.weight FROM %s", toDoubleQuoted(topic)),
                "VALUES (0, 'first', NULL, 'old', NULL), (1, 'second', 'added', 'new', 7)",
                io.airlift.units.Duration.valueOf("5s"));
    }

    @Test
    public void testRenamedFieldsAreReadByNumber()
            throws Exception
    {
        String topic = "topic-renamed-fields";
        assertNotExists(topic);

        Descriptor initial = getFileDescriptor(
                """
                syntax = "proto3";
                message schema {
                  string col_1 = 1;
                  message Detail {
                    string note = 1;
                  }
                  Detail detail = 2;
                }
                """).findMessageTypeByName(DEFAULT_MESSAGE);
        Descriptor initialDetail = initial.findFieldByName("detail").getMessageType();
        DynamicMessage initialMessage = DynamicMessage.newBuilder(initial)
                .setField(initial.findFieldByName("col_1"), "first")
                .setField(initial.findFieldByName("detail"), DynamicMessage.newBuilder(initialDetail).setField(initialDetail.findFieldByName("note"), "old").build())
                .build();
        testingKafka.sendMessages(Stream.of(new ProducerRecord<>(topic, createKeySchema(0, getKeySchema()), initialMessage)), producerProperties());
        waitUntilTableExists(topic);
        assertQuery(format("SELECT col_1, detail.note FROM %s", toDoubleQuoted(topic)), "VALUES ('first', 'old')");

        Descriptor renamed = getFileDescriptor(
                """
                syntax = "proto3";
                message schema {
                  string name = 1;
                  message Detail {
                    string text = 1;
                  }
                  Detail info = 2;
                }
                """).findMessageTypeByName(DEFAULT_MESSAGE);
        Descriptor renamedInfo = renamed.findFieldByName("info").getMessageType();
        DynamicMessage renamedMessage = DynamicMessage.newBuilder(renamed)
                .setField(renamed.findFieldByName("name"), "second")
                .setField(renamed.findFieldByName("info"), DynamicMessage.newBuilder(renamedInfo).setField(renamedInfo.findFieldByName("text"), "new").build())
                .build();
        testingKafka.sendMessages(Stream.of(new ProducerRecord<>(topic, createKeySchema(1, getKeySchema()), renamedMessage)), producerProperties());

        // a field keeps its number across renames, so records written before the rename show up in the renamed columns
        assertQueryEventually(
                getSession(),
                format("SELECT key, name, info.text FROM %s", toDoubleQuoted(topic)),
                "VALUES (0, 'first', 'old'), (1, 'second', 'new')",
                io.airlift.units.Duration.valueOf("5s"));
    }

    @Test
    public void testWidenedFieldsAndRenamedEnumValues()
            throws Exception
    {
        String topic = "topic-widened-fields";
        assertNotExists(topic);

        Descriptor initial = getFileDescriptor(
                """
                syntax = "proto3";
                message schema {
                  int32 count = 1;
                  enum Status {
                    ACTIVE = 0;
                    RETIRED = 1;
                  }
                  Status status = 2;
                }
                """).findMessageTypeByName(DEFAULT_MESSAGE);
        DynamicMessage initialMessage = DynamicMessage.newBuilder(initial)
                .setField(initial.findFieldByName("count"), -5)
                .setField(initial.findFieldByName("status"), initial.findEnumTypeByName("Status").findValueByName("RETIRED"))
                .build();
        testingKafka.sendMessages(Stream.of(new ProducerRecord<>(topic, createKeySchema(0, getKeySchema()), initialMessage)), producerProperties());
        waitUntilTableExists(topic);
        assertQuery(format("SELECT count, status FROM %s", toDoubleQuoted(topic)), "VALUES (-5, 'RETIRED')");

        Descriptor evolved = getFileDescriptor(
                """
                syntax = "proto3";
                message schema {
                  int64 count = 1;
                  enum Status {
                    ACTIVE = 0;
                    INACTIVE = 1;
                  }
                  Status status = 2;
                }
                """).findMessageTypeByName(DEFAULT_MESSAGE);
        DynamicMessage evolvedMessage = DynamicMessage.newBuilder(evolved)
                .setField(evolved.findFieldByName("count"), 1L << 40)
                .setField(evolved.findFieldByName("status"), evolved.findEnumTypeByName("Status").findValueByName("INACTIVE"))
                .build();
        testingKafka.sendMessages(Stream.of(new ProducerRecord<>(topic, createKeySchema(1, getKeySchema()), evolvedMessage)), producerProperties());

        // records written before the change are read the way the latest schema reads their bytes
        assertQueryEventually(
                getSession(),
                format("SELECT key, count, status FROM %s", toDoubleQuoted(topic)),
                "VALUES (0, -5, 'INACTIVE'), (1, 1099511627776, 'INACTIVE')",
                io.airlift.units.Duration.valueOf("5s"));
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
                .matches(
                        """
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
                                    bytes_column VARBINARY)))
                        """);
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
                .matches(
                        """
                        VALUES (JSON '{"stringColumn":"%s"}')
                        """.formatted(stringData));
    }

    @Test
    public void testAny()
            throws Exception
    {
        String topic = "topic-schema-with-any";
        assertNotExists(topic);

        Descriptor structuralDataTypesDescriptor = getDescriptor("structural_datatypes.proto");

        Timestamp timestamp = getTimestamp(sqlTimestampOf(3, LocalDateTime.parse("2020-12-12T15:35:45.923")));
        DynamicMessage structuralDataTypeMessage = buildDynamicMessage(
                structuralDataTypesDescriptor,
                ImmutableMap.<String, Object>builder()
                        .put("list", ImmutableList.of("Search"))
                        .put("map", ImmutableList.of(buildDynamicMessage(
                                structuralDataTypesDescriptor.findFieldByName("map").getMessageType(),
                                ImmutableMap.of("key", "Key1", "value", "Value1"))))
                        .put("row", ImmutableMap.<String, Object>builder()
                                .put("string_column", "Trino")
                                .put("integer_column", 1)
                                .put("long_column", 493857959588286460L)
                                .put("double_column", 3.14159265358979323846)
                                .put("float_column", 3.14f)
                                .put("boolean_column", true)
                                .put("number_column", structuralDataTypesDescriptor.findEnumTypeByName("Number").findValueByName("ONE"))
                                .put("timestamp_column", timestamp)
                                .put("bytes_column", "Trino".getBytes(UTF_8))
                                .buildOrThrow())
                        .buildOrThrow());

        ProtobufSchema schema = (ProtobufSchema) new ProtobufSchemaProvider().parseSchema(Resources.toString(getResource("protobuf/test_any.proto"), UTF_8), List.of(), true).get();

        // Get URI of parent directory of the descriptor file
        // Any.pack concatenates the message type's full name to the given prefix
        URI anySchemaTypeUrl = new File(Resources.getResource("protobuf/any/structural_datatypes/schema").getFile()).getParentFile().toURI();
        Descriptor descriptor = schema.toDescriptor();
        DynamicMessage message = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), 1)
                .setField(descriptor.findFieldByName("anyMessage"), Any.pack(structuralDataTypeMessage, anySchemaTypeUrl.toString()))
                .build();

        ImmutableList.Builder<ProducerRecord<DynamicMessage, DynamicMessage>> producerRecordBuilder = ImmutableList.builder();
        producerRecordBuilder.add(new ProducerRecord<>(topic, createKeySchema(0, getKeySchema()), message));
        List<ProducerRecord<DynamicMessage, DynamicMessage>> messages = producerRecordBuilder.build();
        testingKafka.sendMessages(messages.stream(), producerProperties());
        waitUntilTableExists(topic);

        URI anySchemaFile = new File(Resources.getResource("protobuf/any/structural_datatypes/schema").getFile()).toURI();
        assertThat(query(format("SELECT id, anyMessage FROM %s", toDoubleQuoted(topic))))
                .matches(
                        """
                        VALUES (1, JSON '{"@type":"%s","list":["Search"],"map":{"Key1":"Value1"},"row":{"booleanColumn":true,"bytesColumn":"VHJpbm8=","doubleColumn":3.141592653589793,"floatColumn":3.14,"integerColumn":1,"longColumn":"493857959588286460","numberColumn":"ONE","stringColumn":"Trino","timestampColumn":"2020-12-12T15:35:45.923Z"}}')
                        """.formatted(anySchemaFile));
    }

    private DynamicMessage buildDynamicMessage(Descriptor descriptor, Map<String, Object> data)
    {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (Entry<String, Object> entry : data.entrySet()) {
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
        String expectedValues = getExpectedValues(allMessages, getEvolvedSchema(), isKeyIncluded);
        assertQueryEventually(getSession(), evolvedQuery, expectedValues, io.airlift.units.Duration.valueOf("5s"));
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
                .run(() -> assertThat(schemaExists()).isTrue());
        Failsafe.with(
                        RetryPolicy.builder()
                                .withMaxAttempts(10)
                                .withDelay(Duration.ofMillis(100))
                                .build())
                .run(() -> assertThat(tableExists(tableName)).isTrue());
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
                .setField(descriptor.findFieldByName("col_3"), (key + 10.1d) / 10.0d)
                .build();
    }
}
