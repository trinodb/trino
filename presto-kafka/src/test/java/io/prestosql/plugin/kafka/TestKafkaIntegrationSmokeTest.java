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
package io.prestosql.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.kafka.util.TestingKafka;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.TestngUtils.toDataProvider;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class TestKafkaIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private TestingKafka testingKafka;
    private String rawFormatTopic;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = new TestingKafka();
        rawFormatTopic = "test_raw_" + UUID.randomUUID().toString().replaceAll("-", "_");
        Map<SchemaTableName, KafkaTopicDescription> extraTopicDescriptions = ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                .put(new SchemaTableName("default", rawFormatTopic),
                        createDescription(rawFormatTopic, "default", rawFormatTopic,
                                createFieldGroup("raw", ImmutableList.of(
                                        createOneFieldDescription("bigint_long", BigintType.BIGINT, "0", "LONG"),
                                        createOneFieldDescription("bigint_int", BigintType.BIGINT, "8", "INT"),
                                        createOneFieldDescription("bigint_short", BigintType.BIGINT, "12", "SHORT"),
                                        createOneFieldDescription("bigint_byte", BigintType.BIGINT, "14", "BYTE"),
                                        createOneFieldDescription("double_double", DoubleType.DOUBLE, "15", "DOUBLE"),
                                        createOneFieldDescription("double_float", DoubleType.DOUBLE, "23", "FLOAT"),
                                        createOneFieldDescription("varchar_byte", createVarcharType(6), "27:33", "BYTE"),
                                        createOneFieldDescription("boolean_long", BooleanType.BOOLEAN, "33", "LONG"),
                                        createOneFieldDescription("boolean_int", BooleanType.BOOLEAN, "41", "INT"),
                                        createOneFieldDescription("boolean_short", BooleanType.BOOLEAN, "45", "SHORT"),
                                        createOneFieldDescription("boolean_byte", BooleanType.BOOLEAN, "47", "BYTE")))))
                .build();

        QueryRunner queryRunner = KafkaQueryRunner.builder(testingKafka)
                .setTables(TpchTable.getTables())
                .setExtraTopicDescription(ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .putAll(extraTopicDescriptions)
                        .build())
                .build();

        testingKafka.createTopics(rawFormatTopic);
        return queryRunner;
    }

    @Test
    public void testColumnReferencedTwice()
    {
        ByteBuffer buf = ByteBuffer.allocate(48);
        buf.putLong(1234567890123L); // 0-8
        buf.putInt(123456789); // 8-12
        buf.putShort((short) 12345); // 12-14
        buf.put((byte) 127); // 14
        buf.putDouble(123456789.123); // 15-23
        buf.putFloat(123456.789f); // 23-27
        buf.put("abcdef".getBytes(StandardCharsets.UTF_8)); // 27-33
        buf.putLong(1234567890123L); // 33-41
        buf.putInt(123456789); // 41-45
        buf.putShort((short) 12345); // 45-47
        buf.put((byte) 127); // 47

        insertData(buf.array());

        assertQuery("SELECT " +
                          "bigint_long, bigint_int, bigint_short, bigint_byte, " +
                          "double_double, double_float, varchar_byte, " +
                          "boolean_long, boolean_int, boolean_short, boolean_byte " +
                        "FROM default." + rawFormatTopic + " WHERE " +
                          "bigint_long = 1234567890123 AND bigint_int = 123456789 AND bigint_short = 12345 AND bigint_byte = 127 AND " +
                          "double_double = 123456789.123 AND double_float != 1.0 AND varchar_byte = 'abcdef' AND " +
                          "boolean_long = TRUE AND boolean_int = TRUE AND boolean_short = TRUE AND boolean_byte = TRUE",
                "VALUES (1234567890123, 123456789, 12345, 127, 123456789.123, 123456.789, 'abcdef', TRUE, TRUE, TRUE, TRUE)");
        assertQuery("SELECT " +
                          "bigint_long, bigint_int, bigint_short, bigint_byte, " +
                          "double_double, double_float, varchar_byte, " +
                          "boolean_long, boolean_int, boolean_short, boolean_byte " +
                        "FROM default." + rawFormatTopic + " WHERE " +
                          "bigint_long < 1234567890124 AND bigint_int < 123456790 AND bigint_short < 12346 AND bigint_byte < 128 AND " +
                          "double_double < 123456789.124 AND double_float > 2 AND varchar_byte <= 'abcdef' AND " +
                          "boolean_long != FALSE AND boolean_int != FALSE AND boolean_short != FALSE AND boolean_byte != FALSE",
                "VALUES (1234567890123, 123456789, 12345, 127, 123456789.123, 123456.789, 'abcdef', TRUE, TRUE, TRUE, TRUE)");
    }

    private void insertData(byte[] data)
    {
        try (KafkaProducer<byte[], byte[]> producer = createProducer()) {
            producer.send(new ProducerRecord<>(rawFormatTopic, data));
        }
    }

    private KafkaProducer<byte[], byte[]> createProducer()
    {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, testingKafka.getConnectString());
        properties.put(ACKS_CONFIG, "all");
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    private KafkaTopicDescription createDescription(String name, String schema, String topic, Optional<KafkaTopicFieldGroup> message)
    {
        return new KafkaTopicDescription(name, Optional.of(schema), topic, Optional.empty(), message);
    }

    private Optional<KafkaTopicFieldGroup> createFieldGroup(String dataFormat, List<KafkaTopicFieldDescription> fields)
    {
        return Optional.of(new KafkaTopicFieldGroup(dataFormat, Optional.empty(), fields));
    }

    private KafkaTopicFieldDescription createOneFieldDescription(String name, Type type, String mapping, String dataFormat)
    {
        return new KafkaTopicFieldDescription(name, type, mapping, null, dataFormat, null, false);
    }

    @Test(dataProvider = "testRoundTripAllFormatsDataProvider")
    public void testRoundTripAllFormats(RoundTripTestCase testCase)
    {
        assertUpdate("INSERT into write_test." + testCase.getTableName() +
                " (" + testCase.getFieldNames() + ")" +
                " VALUES (" + testCase.getFieldValues() + "), (" + testCase.getFieldValues() + ")", 2);
        assertQuery("SELECT " + testCase.getFieldNames() + " FROM write_test." + testCase.getTableName() +
                " WHERE " + testCase.getFieldName("f_bigint") + " = " + testCase.getFieldValue("f_bigint"),
                "VALUES (" + testCase.getFieldValues() + "), (" + testCase.getFieldValues() + ")");
    }

    @DataProvider(name = "testRoundTripAllFormatsDataProvider")
    public final Object[][] testRoundTripAllFormatsDataProvider()
    {
        return testRoundTripAllFormatsData().stream()
                .collect(toDataProvider());
    }

    private List<RoundTripTestCase> testRoundTripAllFormatsData()
    {
        return ImmutableList.<RoundTripTestCase>builder()
                .add(new RoundTripTestCase(
                        "all_datatypes_avro",
                        ImmutableList.of("f_bigint", "f_double", "f_boolean", "f_varchar"),
                        ImmutableList.of(100000, 1000.001, true, "'test'")))
                .add(new RoundTripTestCase(
                        "all_datatypes_csv",
                        ImmutableList.of("f_bigint", "f_int", "f_smallint", "f_tinyint", "f_double", "f_boolean", "f_varchar"),
                        ImmutableList.of(100000, 1000, 100, 10, 1000.001, true, "'test'")))
                .build();
    }

    protected static final class RoundTripTestCase
    {
        private final String tableName;
        private final List<String> fieldNames;
        private final List<Object> fieldValues;
        private final int length;

        public RoundTripTestCase(String tableName, List<String> fieldNames, List<Object> fieldValues)
        {
            checkArgument(fieldNames.size() == fieldValues.size(), "sizes of fieldNames and fieldValues are not equal");
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.fieldNames = ImmutableList.copyOf(fieldNames);
            this.fieldValues = ImmutableList.copyOf(fieldValues);
            this.length = fieldNames.size();
        }

        public String getTableName()
        {
            return tableName;
        }

        private int getIndex(String fieldName)
        {
            return fieldNames.indexOf(fieldName);
        }

        public String getFieldName(String fieldName)
        {
            int index = getIndex(fieldName);
            checkArgument(index >= 0 && index < length, "index out of bounds");
            return fieldNames.get(index);
        }

        public String getFieldNames()
        {
            return String.join(", ", fieldNames);
        }

        public Object getFieldValue(String fieldName)
        {
            int index = getIndex(fieldName);
            checkArgument(index >= 0 && index < length, "index out of bounds");
            return fieldValues.get(index);
        }

        public String getFieldValues()
        {
            return fieldValues.stream().map(Object::toString).collect(Collectors.joining(", "));
        }

        @Override
        public String toString()
        {
            return tableName; // for test case label in IDE
        }
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        if (testingKafka != null) {
            testingKafka.close();
            testingKafka = null;
        }
    }
}
