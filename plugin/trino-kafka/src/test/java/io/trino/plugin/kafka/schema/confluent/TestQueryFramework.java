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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.trino.plugin.kafka.KafkaQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.parallel.Execution;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestQueryFramework
        extends AbstractTestQueryFramework
{
    protected static final String RECORD_NAME = "test_record";
    protected static final int MESSAGE_COUNT = 100;
    protected static final Schema INITIAL_SCHEMA = SchemaBuilder.record(RECORD_NAME)
            .fields()
            .name("col_1").type().nullable().longType().noDefault()
            .name("col_2").type().nullable().stringType().noDefault()
            .endRecord();
    protected static final Schema EVOLVED_SCHEMA = SchemaBuilder.record(RECORD_NAME)
            .fields()
            .name("col_1").type().nullable().longType().noDefault()
            .name("col_2").type().nullable().stringType().noDefault()
            .name("col_3").type().optional().doubleType()
            .endRecord();

    protected TestingKafka testingKafka;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(TestingKafka.createWithSchemaRegistry());
        testingKafka.start();
        return KafkaQueryRunner.builderForConfluentSchemaRegistry(testingKafka)
                .addConnectorProperties(ImmutableMap.of("kafka.confluent-subjects-cache-refresh-interval", "1ms"))
                .build();
    }

    protected void assertTopic(
            String topicName,
            String initialQuery,
            String evolvedQuery,
            boolean isKeyIncluded,
            Map<String, String> producerConfig)
    {
        assertNotExists(topicName);

        List<ProducerRecord<Long, GenericRecord>> messages = createMessages(topicName, MESSAGE_COUNT, true);
        testingKafka.sendMessages(messages.stream(), producerConfig);

        waitUntilTableExists(topicName);
        assertCount(topicName, MESSAGE_COUNT);

        assertThat(query(initialQuery)).matches(getExpectedValues(messages, INITIAL_SCHEMA, isKeyIncluded));

        List<ProducerRecord<Long, GenericRecord>> newMessages = createMessages(topicName, MESSAGE_COUNT, false);
        testingKafka.sendMessages(newMessages.stream(), producerConfig);

        List<ProducerRecord<Long, GenericRecord>> allMessages = ImmutableList.<ProducerRecord<Long, GenericRecord>>builder()
                .addAll(messages)
                .addAll(newMessages)
                .build();
        assertCount(topicName, allMessages.size());

        assertThat(query(evolvedQuery)).containsAll(getExpectedValues(messages, EVOLVED_SCHEMA, isKeyIncluded));
    }

    protected static String getExpectedValues(List<ProducerRecord<Long, GenericRecord>> messages, Schema schema, boolean isKeyIncluded)
    {
        StringBuilder valuesBuilder = new StringBuilder("VALUES ");
        ImmutableList.Builder<String> rowsBuilder = ImmutableList.builder();
        for (ProducerRecord<Long, GenericRecord> message : messages) {
            ImmutableList.Builder<String> columnsBuilder = ImmutableList.builder();

            if (isKeyIncluded) {
                columnsBuilder.add(format("CAST(%s as bigint)", message.key()));
            }

            addExpectedColumns(schema, message.value(), columnsBuilder);

            rowsBuilder.add(format("(%s)", String.join(", ", columnsBuilder.build())));
        }
        valuesBuilder.append(String.join(", ", rowsBuilder.build()));
        return valuesBuilder.toString();
    }

    protected static void addExpectedColumns(Schema schema, GenericRecord record, ImmutableList.Builder<String> columnsBuilder)
    {
        for (Schema.Field field : schema.getFields()) {
            Object value = getValue(record, field.name());
            if (value == null && field.schema().getType().equals(Schema.Type.UNION) && field.schema().getTypes().contains(Schema.create(Schema.Type.NULL))) {
                if (field.schema().getTypes().contains(Schema.create(Schema.Type.DOUBLE))) {
                    columnsBuilder.add("CAST(null AS double)");
                }
                else {
                    throw new IllegalArgumentException("Unsupported field: " + field);
                }
            }
            else if (field.schema().getType().equals(Schema.Type.STRING)
                    || (field.schema().getType().equals(Schema.Type.UNION) && field.schema().getTypes().contains(Schema.create(Schema.Type.STRING)))) {
                columnsBuilder.add(format("VARCHAR '%s'", value));
            }
            else if (field.schema().getType().equals(Schema.Type.LONG)
                    || (field.schema().getType().equals(Schema.Type.UNION) && field.schema().getTypes().contains(Schema.create(Schema.Type.LONG)))) {
                columnsBuilder.add(format("CAST(%s AS bigint)", value));
            }
            else {
                throw new IllegalArgumentException("Unsupported field: " + field);
            }
        }
    }

    protected static Object getValue(GenericRecord record, String columnName)
    {
        try {
            return record.get(columnName);
        }
        catch (AvroRuntimeException e) {
            if (e.getMessage().contains("Not a valid schema field")) {
                return null;
            }

            throw e;
        }
    }

    protected void assertNotExists(String tableName)
    {
        if (schemaExists()) {
            assertThat(getQueryRunner().execute("SHOW TABLES LIKE " + toSingleQuoted(tableName)).getRowCount()).isZero();
        }
    }

    protected void waitUntilTableExists(String tableName)
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

    protected boolean schemaExists()
    {
        return getQueryRunner().execute(format(
                        "SHOW SCHEMAS FROM %s LIKE '%s'",
                        getSession().getCatalog().orElseThrow(),
                        getSession().getSchema().orElseThrow()))
                .getRowCount() == 1;
    }

    protected boolean tableExists(String tableName)
    {
        return getQueryRunner().execute(format("SHOW TABLES LIKE '%s'", tableName.toLowerCase(ENGLISH))).getRowCount() == 1;
    }

    protected void assertCount(String tableName, long count)
    {
        assertThat(getQueryRunner().execute("SELECT count(*) FROM " + toDoubleQuoted(tableName)).getOnlyValue()).isEqualTo(count);
    }

    protected static String toDoubleQuoted(String tableName)
    {
        return format("\"%s\"", tableName);
    }

    protected static String toSingleQuoted(Object value)
    {
        requireNonNull(value, "value is null");
        return format("'%s'", value);
    }

    protected static List<ProducerRecord<Long, GenericRecord>> createMessages(String topicName, int messageCount, boolean useInitialSchema)
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

    protected static GenericRecord createRecordWithInitialSchema(long key)
    {
        return new GenericRecordBuilder(INITIAL_SCHEMA)
                .set("col_1", multiplyExact(key, 100))
                .set("col_2", format("string-%s", key))
                .build();
    }

    protected static GenericRecord createRecordWithEvolvedSchema(long key)
    {
        return new GenericRecordBuilder(EVOLVED_SCHEMA)
                .set("col_1", multiplyExact(key, 100))
                .set("col_2", format("string-%s", key))
                .set("col_3", (key + 10.1D) / 10.0D)
                .build();
    }

    protected record JsonValue(int id, String value)
    {
        protected JsonValue
        {
            requireNonNull(value, "value is null");
        }
    }
}
