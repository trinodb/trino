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
package io.prestosql.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.kafka.TestingKafka;
import io.prestosql.testing.kafka.TestingKafkaWithSchemaRegistry;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
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
        testingKafkaWithSchemaRegistry = new TestingKafkaWithSchemaRegistry(new TestingKafka());
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
        assertTopic(topic,
                format("SELECT col_1, col_2 FROM %s", toDoubleQuoted(topic)),
                format("SELECT col_1, col_2, col_3 FROM %s", toDoubleQuoted(topic)),
                false,
                () -> testingKafkaWithSchemaRegistry.createConfluentProducerWithLongKeys());
    }

    @Test
    public void testTopicWithKeySubject()
    {
        String topic = "topic-key-subject";
        assertTopic(topic,
                format("SELECT \"%s-key\", col_1, col_2 FROM %s", topic, toDoubleQuoted(topic)),
                format("SELECT \"%s-key\", col_1, col_2, col_3 FROM %s", topic, toDoubleQuoted(topic)),
                true,
                () -> testingKafkaWithSchemaRegistry.createConfluentProducer());
    }

    @Test
    public void testTopicWithRecordNameStrategy()
    {
        String topic = "topic-record-name-strategy";
        assertTopic(topic,
                format("SELECT \"%1$s-key\", col_1, col_2 FROM \"%1$s&message-subject=%2$s\"", topic, RECORD_NAME),
                format("SELECT \"%1$s-key\", col_1, col_2, col_3 FROM \"%1$s&message-subject=%2$s\"", topic, RECORD_NAME),
                true,
                () -> testingKafkaWithSchemaRegistry.createConfluentProducer(ImmutableMap.<String, String>builder()
                        .put(VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName())
                        .build()));
    }

    @Test
    public void testTopicWithTopicRecordNameStrategy()
    {
        String topic = "topic-topic-record-name-strategy";
        assertTopic(topic,
                format("SELECT \"%1$s-key\", col_1, col_2 FROM \"%1$s&message-subject=%1$s-%2$s\"", topic, RECORD_NAME),
                format("SELECT \"%1$s-key\", col_1, col_2, col_3 FROM \"%1$s&message-subject=%1$s-%2$s\"", topic, RECORD_NAME),
                true,
                () -> testingKafkaWithSchemaRegistry.createConfluentProducer(ImmutableMap.<String, String>builder()
                        .put(VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName())
                        .build()));
    }

    @Test
    public void testInternalMessageField()
            throws IOException, RestClientException
    {
        String topic = "internal-message-topic";
        Supplier<KafkaProducer<Long, GenericRecord>> producerSupplier = () -> testingKafkaWithSchemaRegistry.createConfluentProducerWithLongKeys();
        testingKafkaWithSchemaRegistry.createTopic(topic);
        assertNotExists(topic);

        List<ProducerRecord<Long, GenericRecord>> messages = createMessages(topic, MESSAGE_COUNT, true);
        sendMessages(messages, producerSupplier);

        waitUntilTableExists(topic);
        assertCount(topic, MESSAGE_COUNT);
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(testingKafkaWithSchemaRegistry.getSchemaRegistryConnectString(), 1000);
        int schemaId = schemaRegistryClient.getLatestSchemaMetadata(format("%s-value", topic)).getId();
        String initialQuery = format("SELECT _key_schema_id, _message_schema_id, col_1, col_2 FROM %s", toDoubleQuoted(topic));
        String expectedValues = getExpectedValues(messages, INITIAL_SCHEMA, false, true, OptionalInt.empty(), OptionalInt.of(schemaId));
        assertCount(topic, MESSAGE_COUNT);
        assertQuery(initialQuery, expectedValues);

        List<ProducerRecord<Long, GenericRecord>> newMessages = createMessages(topic, MESSAGE_COUNT, false);
        sendMessages(newMessages, producerSupplier);

        String evolvedQuery = format("SELECT _key_schema_id, _message_schema_id, col_1, col_2, col_3 FROM %s WHERE col_3 is not null", toDoubleQuoted(topic));
        assertCount(topic, MESSAGE_COUNT * 2);
        schemaId = schemaRegistryClient.getLatestSchemaMetadata(format("%s-value", topic)).getId();

        String expectedNewValues = getExpectedValues(newMessages, EVOLVED_SCHEMA, false, true, OptionalInt.empty(), OptionalInt.of(schemaId));
        assertQuery(evolvedQuery, expectedNewValues);
    }

    @Test
    public void testInternalKeyAndMessageField()
            throws IOException, RestClientException
    {
        String topic = "internal-key-message-topic";
        Supplier<KafkaProducer<Long, GenericRecord>> producerSupplier = () -> testingKafkaWithSchemaRegistry.createConfluentProducer();
        testingKafkaWithSchemaRegistry.createTopic(topic);
        assertNotExists(topic);

        List<ProducerRecord<Long, GenericRecord>> messages = createMessages(topic, MESSAGE_COUNT, true);
        sendMessages(messages, producerSupplier);

        waitUntilTableExists(topic);
        assertCount(topic, MESSAGE_COUNT);
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(testingKafkaWithSchemaRegistry.getSchemaRegistryConnectString(), 1000);
        int keySchemaId = schemaRegistryClient.getLatestSchemaMetadata(format("%s-key", topic)).getId();
        int messageSchemaId = schemaRegistryClient.getLatestSchemaMetadata(format("%s-value", topic)).getId();
        String initialQuery = format("SELECT _key_schema_id, _message_schema_id, \"%s-key\", col_1, col_2 FROM %s", topic, toDoubleQuoted(topic));
        String expectedValues = getExpectedValues(messages, INITIAL_SCHEMA, true, true, OptionalInt.of(keySchemaId), OptionalInt.of(messageSchemaId));
        assertCount(topic, MESSAGE_COUNT);
        assertQuery(initialQuery, expectedValues);

        List<ProducerRecord<Long, GenericRecord>> newMessages = createMessages(topic, MESSAGE_COUNT, false);
        sendMessages(newMessages, producerSupplier);

        String evolvedQuery = format("SELECT _key_schema_id, _message_schema_id, \"%s-key\", col_1, col_2, col_3 FROM %s WHERE col_3 is not null", topic, toDoubleQuoted(topic));
        assertCount(topic, MESSAGE_COUNT * 2);
        keySchemaId = schemaRegistryClient.getLatestSchemaMetadata(format("%s-key", topic)).getId();
        messageSchemaId = schemaRegistryClient.getLatestSchemaMetadata(format("%s-value", topic)).getId();

        String expectedNewValues = getExpectedValues(newMessages, EVOLVED_SCHEMA, true, true, OptionalInt.of(keySchemaId), OptionalInt.of(messageSchemaId));
        assertQuery(evolvedQuery, expectedNewValues);
    }

    private void assertTopic(String topicName, String initialQuery, String evolvedQuery, boolean isKeyIncluded, Supplier<KafkaProducer<Long, GenericRecord>> producerSupplier)
    {
        testingKafkaWithSchemaRegistry.createTopic(topicName);

        assertNotExists(topicName);

        List<ProducerRecord<Long, GenericRecord>> messages = createMessages(topicName, MESSAGE_COUNT, true);
        sendMessages(messages, producerSupplier);

        waitUntilTableExists(topicName);
        assertCount(topicName, MESSAGE_COUNT);

        assertQuery(initialQuery, getExpectedValues(messages, INITIAL_SCHEMA, isKeyIncluded, false, OptionalInt.empty(), OptionalInt.empty()));

        List<ProducerRecord<Long, GenericRecord>> newMessages = createMessages(topicName, MESSAGE_COUNT, false);
        sendMessages(newMessages, producerSupplier);

        List<ProducerRecord<Long, GenericRecord>> allMessages = ImmutableList.<ProducerRecord<Long, GenericRecord>>builder()
                .addAll(messages)
                .addAll(newMessages)
                .build();
        assertCount(topicName, allMessages.size());
        assertQuery(evolvedQuery, getExpectedValues(allMessages, EVOLVED_SCHEMA, isKeyIncluded, false, OptionalInt.empty(), OptionalInt.empty()));
    }

    private String getExpectedValues(List<ProducerRecord<Long, GenericRecord>> messages, Schema schema, boolean isKeyIncluded, boolean isIncludeInternalFields, OptionalInt keySchemaId, OptionalInt messageSchemaId)
    {
        StringBuilder valuesBuilder = new StringBuilder("VALUES ");
        ImmutableList.Builder<String> rowsBuilder = ImmutableList.builder();
        for (ProducerRecord<Long, GenericRecord> message : messages) {
            ImmutableList.Builder<String> columnsBuilder = ImmutableList.builder();
            if (isIncludeInternalFields) {
                if (keySchemaId.isPresent()) {
                    columnsBuilder.add(String.valueOf(keySchemaId.getAsInt()));
                }
                else {
                    columnsBuilder.add("null");
                }

                if (messageSchemaId.isPresent()) {
                    columnsBuilder.add(String.valueOf(messageSchemaId.getAsInt()));
                }
                else {
                    columnsBuilder.add("null");
                }
            }
            if (isKeyIncluded) {
                columnsBuilder.add(String.valueOf(message.key()));
            }

            addExpectedColumns(schema, message.value(), columnsBuilder);

            rowsBuilder.add(format("(%s)", String.join(", ", columnsBuilder.build())));
        }
        valuesBuilder.append(String.join(", ", rowsBuilder.build()));
        return valuesBuilder.toString();
    }

    private void addExpectedColumns(Schema schema, GenericRecord record, ImmutableList.Builder<String> columnsBuilder)
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
        int attempts = 10;
        int waitMs = 100;
        for (int attempt = 0; !schemaExists() && attempt < attempts; attempt++) {
            try {
                Thread.sleep(waitMs);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        assertTrue(schemaExists());
        for (int attempt = 0; !tableExists(tableName) && attempt < attempts; attempt++) {
            try {
                Thread.sleep(waitMs);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        assertTrue(tableExists(tableName));
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

    private static String toDoubleQuoted(String tableName)
    {
        return format("\"%s\"", tableName);
    }

    private static String toSingleQuoted(Object value)
    {
        requireNonNull(value, "value is null");
        return format("'%s'", value);
    }

    private void sendMessages(List<ProducerRecord<Long, GenericRecord>> messages, Supplier<KafkaProducer<Long, GenericRecord>> producerSupplier)
    {
        try (KafkaProducer<Long, GenericRecord> producer = producerSupplier.get()) {
            messages.stream().forEach(producer::send);
        }
    }

    private List<ProducerRecord<Long, GenericRecord>> createMessages(String topicName, int messageCount, boolean useInitialSchema)
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

    private GenericRecord createRecordWithInitialSchema(long key)
    {
        return new GenericRecordBuilder(INITIAL_SCHEMA)
                .set("col_1", multiplyExact(key, 100))
                .set("col_2", format("string-%s", key))
                .build();
    }

    private GenericRecord createRecordWithEvolvedSchema(long key)
    {
        return new GenericRecordBuilder(EVOLVED_SCHEMA)
                .set("col_1", multiplyExact(key, 100))
                .set("col_2", format("string-%s", key))
                .set("col_3", (key + 10.1D) / 10.0D)
                .build();
    }
}
