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
import io.prestosql.spi.PrestoException;
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

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class TestKafkaIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private TestingKafka testingKafka;
    private String rawTopicName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = new TestingKafka();
        rawTopicName = "test_raw_" + UUID.randomUUID().toString().replaceAll("-", "_");
        Map<SchemaTableName, KafkaTopicDescription> extraTopicDescriptions = ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                .put(new SchemaTableName("default", rawTopicName),
                        createDescription(rawTopicName, "default", rawTopicName,
                                createFieldGroup("raw", ImmutableList.of(
                                        createOneFieldDescription("id", BigintType.BIGINT, "0", "LONG")))))
                .build();

        QueryRunner queryRunner = KafkaQueryRunner.builder(testingKafka)
                .setTables(TpchTable.getTables())
                .setExtraTopicDescription(ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .putAll(extraTopicDescriptions)
                        .build())
                .build();

        testingKafka.createTopics(rawTopicName);
        return queryRunner;
    }

    @Test
    public void testColumnReferencedTwice()
    {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(0, 1);

        insertData(buf.array());

        assertQuery("SELECT id FROM default." + rawTopicName + " WHERE id = 1", "VALUES (1)");
        assertQuery("SELECT id FROM default." + rawTopicName + " WHERE id < 2", "VALUES (1)");
    }

    private void insertData(byte[] data)
    {
        try (KafkaProducer<byte[], byte[]> producer = createProducer()) {
            producer.send(new ProducerRecord<>(rawTopicName, data));
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
    public void testRoundTripAllFormats(RoundTripTestSetup roundTripTestSetup)
    {
        String tableName = roundTripTestSetup.getTableName();
        String fieldNames = roundTripTestSetup.getFieldNames();
        String fieldValues = roundTripTestSetup.getFieldValues();
        String comparableName = roundTripTestSetup.getFieldName("f_bigint");
        Object comparableValue = roundTripTestSetup.getFieldValue("f_bigint");

        assertUpdate("INSERT into write_test." + tableName + " (" + fieldNames + ") VALUES (" + fieldValues + ")", 1);
        assertQuery("SELECT " + fieldNames + " FROM write_test." + tableName + " WHERE " + comparableName + " = " + comparableValue, "VALUES (" + fieldValues + ")");
    }

    @DataProvider(name = "testRoundTripAllFormatsDataProvider")
    public final Object[][] testRoundTripAllFormatsDataProvider()
    {
        return testRoundTripAllFormatsData().stream()
                .map(roundTripTestSetup -> new Object[] {roundTripTestSetup})
                .toArray(Object[][]::new);
    }

    private List<RoundTripTestSetup> testRoundTripAllFormatsData()
    {
        return ImmutableList.<TestKafkaIntegrationSmokeTest.RoundTripTestSetup>builder()
                .add(new RoundTripTestSetup(
                        "all_datatypes_avro",
                        ImmutableList.of("f_bigint", "f_double", "f_boolean", "f_varchar"),
                        ImmutableList.of(100000, 1000.001, true, "'test'")))
                .add(new RoundTripTestSetup(
                        "all_datatypes_csv",
                        ImmutableList.of("f_bigint", "f_int", "f_smallint", "f_tinyint", "f_double", "f_boolean", "f_varchar"),
                        ImmutableList.of(100000, 1000, 100, 10, 1000.001, true, "'test'")))
                .add(new RoundTripTestSetup(
                        "all_datatypes_json",
                        ImmutableList.of("f_bigint", "f_int", "f_smallint", "f_tinyint", "f_double", "f_boolean", "f_varchar"),
                        ImmutableList.of(100000, 1000, 100, 10, 1000.001, true, "'test'")))
                .add(new RoundTripTestSetup(
                        "all_datatypes_raw",
                        ImmutableList.of("f_varchar", "f_bigint", "f_int", "f_smallint", "f_tinyint", "f_double", "f_boolean"),
                        ImmutableList.of("'test'", 100000, 1000, 100, 10, 1000.001, true)))
                .build();
    }

    protected static final class RoundTripTestSetup
    {
        private final String tableName;
        private final List<String> fieldNames;
        private final List<Object> fieldValues;
        private final int length;

        public RoundTripTestSetup(String tableName, String fieldName, Object fieldValue)
        {
            this(tableName, ImmutableList.of(fieldName), ImmutableList.of(fieldValue));
        }

        public RoundTripTestSetup(String tableName, List<String> fieldNames, List<Object> fieldValues)
        {
            try {
                checkArgument(fieldNames.size() == fieldValues.size(), "sizes of fieldNames and fieldValues are not equal");
                this.tableName = requireNonNull(tableName, "tableName is null");
                this.fieldNames = ImmutableList.copyOf(fieldNames);
                this.fieldValues = ImmutableList.copyOf(fieldValues);
                this.length = fieldNames.size();
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(GENERIC_USER_ERROR, e);
            }
        }

        public String getTableName()
        {
            return tableName;
        }

        private int getIndex(String fieldName)
        {
            int index = -1;
            for (int i = 0; i < fieldNames.size(); i++) {
                if (fieldNames.get(i).equals(fieldName)) {
                    index = i;
                }
            }
            return index;
        }

        public String getFieldName(int index)
        {
            try {
                checkArgument(index >= 0 && index < length, "index out of bounds");
                return fieldNames.get(index);
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(GENERIC_USER_ERROR, e);
            }
        }

        public String getFieldName(String fieldName)
        {
            int index = getIndex(fieldName);
            if (index > -1) {
                return fieldNames.get(index);
            }
            else {
                throw new PrestoException(GENERIC_USER_ERROR, format("field with name %s does not exist", fieldName));
            }
        }

        public String getFieldNames()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(getFieldName(0));
            for (int i = 1; i < length; i++) {
                sb.append(", ");
                sb.append(fieldNames.get(i));
            }
            return sb.toString();
        }

        public Object getFieldValue(int index)
        {
            try {
                checkArgument(index >= 0 && index < length, "index out of bounds");
                return fieldValues.get(index);
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(GENERIC_USER_ERROR, e);
            }
        }

        public Object getFieldValue(String fieldName)
        {
            int index = getIndex(fieldName);
            if (index > -1) {
                return fieldValues.get(index);
            }
            else {
                throw new PrestoException(GENERIC_USER_ERROR, format("field with name %s does not exist", fieldName));
            }
        }

        public String getFieldValues()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(getFieldValue(0));
            for (int i = 1; i < length; i++) {
                sb.append(", ");
                sb.append(fieldValues.get(i));
            }
            return sb.toString();
        }

        @Override
        public String toString()
        {
            return tableName;  // for test case label in IDE
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
