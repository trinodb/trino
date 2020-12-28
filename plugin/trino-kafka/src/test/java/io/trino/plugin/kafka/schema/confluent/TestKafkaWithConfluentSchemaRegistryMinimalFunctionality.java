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
package io.trino.plugin.kafka.schema.confluent;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafkaWithSchemaRegistry;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static io.airlift.units.Duration.succinctDuration;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestKafkaWithConfluentSchemaRegistryMinimalFunctionality
        extends AbstractTestQueryFramework
{
    private static final String RECORD_NAME = "test_record";
    private static final int MESSAGE_COUNT = 100;
    private static final Schema INITIAL_SCHEMA = SchemaBuilder.record(RECORD_NAME)
            .fields()
            .name("col_1").type().longType().noDefault()
            .name("col_2").type().stringType().noDefault()
            .endRecord();
    private static final Schema EVOLVED_SCHEMA = SchemaBuilder.record(RECORD_NAME)
            .fields()
            .name("col_1").type().longType().noDefault()
            .name("col_2").type().stringType().noDefault()
            .name("col_3").type().optional().doubleType()
            .endRecord();

    private TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafkaWithSchemaRegistry = closeAfterClass(new TestingKafkaWithSchemaRegistry());
        return KafkaWithConfluentSchemaRegistryQueryRunner.builder(testingKafkaWithSchemaRegistry)
                .setExtraKafkaProperties(ImmutableMap.<String, String>builder()
                        .put("kafka.confluent-subjects-cache-refresh-interval", "1ms")
                        .build())
                .build();
    }

    @Test
    public void testBasicTopic()
    {
        String topic = "topic-basic-MixedCase";
        assertTopic(
                topic,
                format("SELECT col_1, col_2 FROM %s", toDoubleQuoted(topic)),
                format("SELECT col_1, col_2, col_3 FROM %s", toDoubleQuoted(topic)),
                false,
                schemaRegistryAwareProducer()
                        .put(KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
    }

    @Test
    public void testTopicWithKeySubject()
    {
        String topic = "topic-Key-Subject";
        assertTopic(
                topic,
                format("SELECT \"%s-key\", col_1, col_2 FROM %s", topic, toDoubleQuoted(topic)),
                format("SELECT \"%s-key\", col_1, col_2, col_3 FROM %s", topic, toDoubleQuoted(topic)),
                true,
                schemaRegistryAwareProducer()
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
    }

    @Test
    public void testTopicWithRecordNameStrategy()
    {
        String topic = "topic-Record-Name-Strategy";
        assertTopic(
                topic,
                format("SELECT \"%1$s-key\", col_1, col_2 FROM \"%1$s&value-subject=%2$s\"", topic, RECORD_NAME),
                format("SELECT \"%1$s-key\", col_1, col_2, col_3 FROM \"%1$s&value-subject=%2$s\"", topic, RECORD_NAME),
                true,
                schemaRegistryAwareProducer()
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName())
                        .build());
    }

    @Test
    public void testTopicWithTopicRecordNameStrategy()
    {
        String topic = "topic-Topic-Record-Name-Strategy";
        assertTopic(
                topic,
                format("SELECT \"%1$s-key\", col_1, col_2 FROM \"%1$s&value-subject=%1$s-%2$s\"", topic, RECORD_NAME),
                format("SELECT \"%1$s-key\", col_1, col_2, col_3 FROM \"%1$s&value-subject=%1$s-%2$s\"", topic, RECORD_NAME),
                true,
                schemaRegistryAwareProducer()
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
                        .build());
    }

    @Test
    public void testUnsupportedInsert()
    {
        String topicName = "topic-unsupported-insert";

        assertNotExists(topicName);

        List<ProducerRecord<Long, GenericRecord>> messages = createMessages(topicName, MESSAGE_COUNT, true);
        testingKafkaWithSchemaRegistry.sendMessages(
                messages.stream(),
                schemaRegistryAwareProducer()
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());

        waitUntilTableExists(topicName);

        assertQueryFails(format("INSERT INTO \"%s\" VALUES (0, 0, '')", topicName), "Insert not supported");
    }

    @Test
    public void testUnsupportedFormat()
    {
        String topicName = "topic-unsupported-format";

        assertNotExists(topicName);

        testingKafkaWithSchemaRegistry.sendMessages(
                IntStream.range(0, MESSAGE_COUNT)
                .mapToObj(id -> new ProducerRecord<>(topicName, (long) id, new JsonValue(id, "value_" + id))),
                schemaRegistryAwareProducer()
                        .put(KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName())
                        .build());

        String errorMessage = "Not supported schema: JSON";
        assertEventually(
                succinctDuration(10, SECONDS),
                () -> assertThatThrownBy(() -> tableExists(topicName))
                        .isInstanceOf(RuntimeException.class)
                        .hasMessage(errorMessage));

        assertQueryFails(format("SHOW COLUMNS FROM \"%s\"", topicName), errorMessage);
        assertQueryFails(format("SELECT * FROM \"%s\"", topicName), errorMessage);
        assertQueryFails(format("INSERT INTO \"%s\" VALUES (0, 0, '')", topicName), errorMessage);
    }

    private ImmutableMap.Builder<String, String> schemaRegistryAwareProducer()
    {
        return ImmutableMap.<String, String>builder()
                .put(SCHEMA_REGISTRY_URL_CONFIG, testingKafkaWithSchemaRegistry.getSchemaRegistryConnectString());
    }

    private void assertTopic(String topicName, String initialQuery, String evolvedQuery, boolean isKeyIncluded, Map<String, String> producerConfig)
    {
        assertNotExists(topicName);

        List<ProducerRecord<Long, GenericRecord>> messages = createMessages(topicName, MESSAGE_COUNT, true);
        testingKafkaWithSchemaRegistry.sendMessages(messages.stream(), producerConfig);

        waitUntilTableExists(topicName);
        assertCount(topicName, MESSAGE_COUNT);

        assertQuery(initialQuery, getExpectedValues(messages, INITIAL_SCHEMA, isKeyIncluded));

        List<ProducerRecord<Long, GenericRecord>> newMessages = createMessages(topicName, MESSAGE_COUNT, false);
        testingKafkaWithSchemaRegistry.sendMessages(newMessages.stream(), producerConfig);

        List<ProducerRecord<Long, GenericRecord>> allMessages = ImmutableList.<ProducerRecord<Long, GenericRecord>>builder()
                .addAll(messages)
                .addAll(newMessages)
                .build();
        assertCount(topicName, allMessages.size());
        assertQuery(evolvedQuery, getExpectedValues(allMessages, EVOLVED_SCHEMA, isKeyIncluded));
    }

    private static String getExpectedValues(List<ProducerRecord<Long, GenericRecord>> messages, Schema schema, boolean isKeyIncluded)
    {
        StringBuilder valuesBuilder = new StringBuilder("VALUES ");
        ImmutableList.Builder<String> rowsBuilder = ImmutableList.builder();
        for (ProducerRecord<Long, GenericRecord> message : messages) {
            ImmutableList.Builder<String> columnsBuilder = ImmutableList.builder();

            if (isKeyIncluded) {
                columnsBuilder.add(String.valueOf(message.key()));
            }

            addExpectedColumns(schema, message.value(), columnsBuilder);

            rowsBuilder.add(format("(%s)", String.join(", ", columnsBuilder.build())));
        }
        valuesBuilder.append(String.join(", ", rowsBuilder.build()));
        return valuesBuilder.toString();
    }

    private static void addExpectedColumns(Schema schema, GenericRecord record, ImmutableList.Builder<String> columnsBuilder)
    {
        for (Schema.Field field : schema.getFields()) {
            Object value = record.get(field.name());
            if (value == null) {
                columnsBuilder.add("null");
            }
            else if (field.schema().getType().equals(Schema.Type.STRING)) {
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
                new RetryPolicy<>()
                        .withMaxAttempts(10)
                        .withDelay(Duration.ofMillis(100)))
                .run(() -> assertTrue(schemaExists()));
        Failsafe.with(
                new RetryPolicy<>()
                        .withMaxAttempts(10)
                        .withDelay(Duration.ofMillis(100)))
                .run(() -> assertTrue(tableExists(tableName)));
    }

    private boolean schemaExists()
    {
        return computeActual(format("SHOW SCHEMAS FROM %s LIKE '%s'", getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow())).getRowCount() == 1;
    }

    private boolean tableExists(String tableName)
    {
        return computeActual(format("SHOW TABLES LIKE '%s'", tableName.toLowerCase(ENGLISH))).getRowCount() == 1;
    }

    private void assertCount(String tableName, int count)
    {
        assertQuery(format("SELECT count(*) FROM %s", toDoubleQuoted(tableName)), format("VALUES (%s)", count));
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

    private static List<ProducerRecord<Long, GenericRecord>> createMessages(String topicName, int messageCount, boolean useInitialSchema)
    {
        ImmutableList.Builder<ProducerRecord<Long, GenericRecord>> producerRecordBuilder = ImmutableList.builder();
        if (useInitialSchema) {
            for (long key = 0; key < messageCount; key++) {
                producerRecordBuilder.add(new ProducerRecord<>(topicName, key, createRecordWithInitialSchema(key)));
            }
        }
        else {
            for (long key = 0; key < messageCount; key++) {
                producerRecordBuilder.add(new ProducerRecord<>(topicName, key, createRecordWithEvolvedSchema(key)));
            }
        }
        return producerRecordBuilder.build();
    }

    private static GenericRecord createRecordWithInitialSchema(long key)
    {
        return new GenericRecordBuilder(INITIAL_SCHEMA)
                .set("col_1", multiplyExact(key, 100))
                .set("col_2", format("string-%s", key))
                .build();
    }

    private static GenericRecord createRecordWithEvolvedSchema(long key)
    {
        return new GenericRecordBuilder(EVOLVED_SCHEMA)
                .set("col_1", multiplyExact(key, 100))
                .set("col_2", format("string-%s", key))
                .set("col_3", (key + 10.1D) / 10.0D)
                .build();
    }

    private static class JsonValue
    {
        private final int id;
        private final String value;

        @JsonCreator
        public JsonValue(
                @JsonProperty("id") int id,
                @JsonProperty("value") String value)
        {
            this.id = id;
            this.value = requireNonNull(value, "value is null");
        }

        @JsonProperty("id")
        public int getId()
        {
            return id;
        }

        @JsonProperty("value")
        public String getValue()
        {
            return value;
        }
    }
}
