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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
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
                                        createOneFieldDescription("id", BigintType.BIGINT, "0", "LONG")))))
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
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(0, 1);

        insertData(buf.array());

        assertQuery("SELECT id FROM default." + rawFormatTopic + " WHERE id = 1", "VALUES (1)");
        assertQuery("SELECT id FROM default." + rawFormatTopic + " WHERE id < 2", "VALUES (1)");
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
                " VALUES (" + testCase.getFieldValues() + ")", 1);
        assertQuery("SELECT " + testCase.getFieldNames() + " FROM write_test." + testCase.getTableName() +
                " WHERE " + testCase.getFieldName("f_bigint") + " = " + testCase.getFieldValue("f_bigint"),
                "VALUES (" + testCase.getFieldValues() + ")");
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
