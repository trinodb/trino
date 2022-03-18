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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.kafka.TestingKafka;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.kafka.encoder.json.format.DateTimeFormat.CUSTOM_DATE_TIME;
import static io.trino.plugin.kafka.encoder.json.format.DateTimeFormat.ISO8601;
import static io.trino.plugin.kafka.encoder.json.format.DateTimeFormat.MILLISECONDS_SINCE_EPOCH;
import static io.trino.plugin.kafka.encoder.json.format.DateTimeFormat.RFC2822;
import static io.trino.plugin.kafka.encoder.json.format.DateTimeFormat.SECONDS_SINCE_EPOCH;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeType.TIME;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.trino.spi.type.TimestampType.TIMESTAMP;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;

public class TestKafkaConnectorTest
        extends BaseConnectorTest
{
    private TestingKafka testingKafka;
    private String rawFormatTopic;
    private String headersTopic;
    private static final String JSON_CUSTOM_DATE_TIME_TABLE_NAME = "custom_date_time_table";
    private static final String JSON_ISO8601_TABLE_NAME = "iso8601_table";
    private static final String JSON_RFC2822_TABLE_NAME = "rfc2822_table";
    private static final String JSON_MILLISECONDS_TABLE_NAME = "milliseconds_since_epoch_table";
    private static final String JSON_SECONDS_TABLE_NAME = "seconds_since_epoch_table";

    // These tables must not be reused because the data will be modified during tests
    private static final SchemaTableName TABLE_INSERT_NEGATIVE_DATE = new SchemaTableName("write_test", "test_insert_negative_date_" + randomTableSuffix());
    private static final SchemaTableName TABLE_INSERT_CUSTOMER = new SchemaTableName("write_test", "test_insert_customer_" + randomTableSuffix());
    private static final SchemaTableName TABLE_INSERT_ARRAY = new SchemaTableName("write_test", "test_insert_array_" + randomTableSuffix());
    private static final SchemaTableName TABLE_INSERT_UNICODE_1 = new SchemaTableName("write_test", "test_unicode_1_" + randomTableSuffix());
    private static final SchemaTableName TABLE_INSERT_UNICODE_2 = new SchemaTableName("write_test", "test_unicode_2_" + randomTableSuffix());
    private static final SchemaTableName TABLE_INSERT_UNICODE_3 = new SchemaTableName("write_test", "test_unicode_3_" + randomTableSuffix());
    private static final SchemaTableName TABLE_INSERT_HIGHEST_UNICODE = new SchemaTableName("write_test", "test_highest_unicode_" + randomTableSuffix());

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(TestingKafka.create());
        rawFormatTopic = "test_raw_" + UUID.randomUUID().toString().replaceAll("-", "_");
        headersTopic = "test_header_" + UUID.randomUUID().toString().replaceAll("-", "_");

        Map<SchemaTableName, KafkaTopicDescription> extraTopicDescriptions = ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                .put(new SchemaTableName("default", rawFormatTopic),
                        createDescription(rawFormatTopic, "default", rawFormatTopic,
                                createFieldGroup("raw", ImmutableList.of(
                                        createOneFieldDescription("bigint_long", BIGINT, "0", "LONG"),
                                        createOneFieldDescription("bigint_int", BIGINT, "8", "INT"),
                                        createOneFieldDescription("bigint_short", BIGINT, "12", "SHORT"),
                                        createOneFieldDescription("bigint_byte", BIGINT, "14", "BYTE"),
                                        createOneFieldDescription("double_double", DOUBLE, "15", "DOUBLE"),
                                        createOneFieldDescription("double_float", DOUBLE, "23", "FLOAT"),
                                        createOneFieldDescription("varchar_byte", createVarcharType(6), "27:33", "BYTE"),
                                        createOneFieldDescription("boolean_long", BOOLEAN, "33", "LONG"),
                                        createOneFieldDescription("boolean_int", BOOLEAN, "41", "INT"),
                                        createOneFieldDescription("boolean_short", BOOLEAN, "45", "SHORT"),
                                        createOneFieldDescription("boolean_byte", BOOLEAN, "47", "BYTE")))))
                .put(new SchemaTableName("default", headersTopic),
                        new KafkaTopicDescription(headersTopic, Optional.empty(), headersTopic, Optional.empty(), Optional.empty()))
                .putAll(createJsonDateTimeTestTopic())
                .put(TABLE_INSERT_NEGATIVE_DATE, createDescription(
                        TABLE_INSERT_NEGATIVE_DATE,
                        createOneFieldDescription("key", BIGINT),
                        ImmutableList.of(createOneFieldDescription("dt", DATE, ISO8601.toString()))))
                .put(TABLE_INSERT_CUSTOMER, createDescription(
                        TABLE_INSERT_CUSTOMER,
                        createOneFieldDescription("phone", createVarcharType(15)),
                        ImmutableList.of(createOneFieldDescription("custkey", BIGINT), createOneFieldDescription("acctbal", DOUBLE))))
                .put(TABLE_INSERT_ARRAY, createDescription(
                        TABLE_INSERT_ARRAY,
                        createOneFieldDescription("a", new ArrayType(DOUBLE)),
                        ImmutableList.of(createOneFieldDescription("b", new ArrayType(DOUBLE)))))
                .put(TABLE_INSERT_UNICODE_1, createDescription(
                        TABLE_INSERT_UNICODE_1,
                        createOneFieldDescription("key", BIGINT),
                        ImmutableList.of(createOneFieldDescription("test", createVarcharType(50)))))
                .put(TABLE_INSERT_UNICODE_2, createDescription(
                        TABLE_INSERT_UNICODE_2,
                        createOneFieldDescription("key", BIGINT),
                        ImmutableList.of(createOneFieldDescription("test", createVarcharType(50)))))
                .put(TABLE_INSERT_UNICODE_3, createDescription(
                        TABLE_INSERT_UNICODE_3,
                        createOneFieldDescription("key", BIGINT),
                        ImmutableList.of(createOneFieldDescription("test", createVarcharType(50)))))
                .put(TABLE_INSERT_HIGHEST_UNICODE, createDescription(
                        TABLE_INSERT_HIGHEST_UNICODE,
                        createOneFieldDescription("key", BIGINT),
                        ImmutableList.of(createOneFieldDescription("test", createVarcharType(50)))))
                .buildOrThrow();

        QueryRunner queryRunner = KafkaQueryRunner.builder(testingKafka)
                .setTables(TpchTable.getTables())
                .setExtraTopicDescription(extraTopicDescriptions)
                .build();

        return queryRunner;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN:
            case SUPPORTS_DROP_COLUMN:
            case SUPPORTS_CREATE_SCHEMA:
            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_CREATE_TABLE_WITH_DATA:
            case SUPPORTS_DELETE:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_RENAME_TABLE:
            case SUPPORTS_RENAME_COLUMN:
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Kafka connector does not support column default values");
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
        buf.put("abcdef".getBytes(UTF_8)); // 27-33
        buf.putLong(1234567890123L); // 33-41
        buf.putInt(123456789); // 41-45
        buf.putShort((short) 12345); // 45-47
        buf.put((byte) 127); // 47

        insertData(rawFormatTopic, buf.array());

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

    private void insertData(String topic, byte[] data)
    {
        testingKafka.sendMessages(Stream.of(new ProducerRecord<>(topic, data)), getProducerProperties());
    }

    private void createMessagesWithHeader(String topicName)
    {
        testingKafka.sendMessages(
                Stream.of(
                        // Messages without headers
                        new ProducerRecord<>(topicName, null, "1".getBytes(UTF_8)),
                        new ProducerRecord<>(topicName, null, "2".getBytes(UTF_8)),
                        // Message with simple header
                        setHeader(new ProducerRecord<>(topicName, null, "3".getBytes(UTF_8)), "notfoo", "some value"),
                        // Message with multiple same key headers
                        setHeader(
                                setHeader(
                                        setHeader(new ProducerRecord<>(topicName, null, "4".getBytes(UTF_8)), "foo", "bar"),
                                        "foo",
                                        null),
                                "foo",
                                "baz")),
                getProducerProperties());
    }

    private static <K, V> ProducerRecord<K, V> setHeader(ProducerRecord<K, V> record, String key, String value)
    {
        record.headers()
                .add(key, value != null ? value.getBytes(UTF_8) : null);
        return record;
    }

    private Map<String, String> getProducerProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put(BOOTSTRAP_SERVERS_CONFIG, testingKafka.getConnectString())
                .put(ACKS_CONFIG, "all")
                .put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName())
                .put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName())
                .buildOrThrow();
    }

    @Test
    public void testReadAllDataTypes()
    {
        String json = "{" +
                "\"j_varchar\"                              : \"ala ma kota\"                    ," +
                "\"j_bigint\"                               : \"9223372036854775807\"            ," +
                "\"j_integer\"                              : \"2147483647\"                     ," +
                "\"j_smallint\"                             : \"32767\"                          ," +
                "\"j_tinyint\"                              : \"127\"                            ," +
                "\"j_double\"                               : \"1234567890.123456789\"           ," +
                "\"j_boolean\"                              : \"true\"                           ," +
                "\"j_timestamp_milliseconds_since_epoch\"   : \"1518182116000\"                  ," +
                "\"j_timestamp_seconds_since_epoch\"        : \"1518182117\"                     ," +
                "\"j_timestamp_iso8601\"                    : \"2018-02-09T13:15:18\"            ," +
                "\"j_timestamp_rfc2822\"                    : \"Fri Feb 09 13:15:19 Z 2018\"     ," +
                "\"j_timestamp_custom\"                     : \"02/2018/09 13:15:20\"            ," +
                "\"j_date_iso8601\"                         : \"2018-02-11\"                     ," +
                "\"j_date_custom\"                          : \"2018/13/02\"                     ," +
                "\"j_time_milliseconds_since_epoch\"        : \"47716000\"                       ," +
                "\"j_time_seconds_since_epoch\"             : \"47717\"                          ," +
                "\"j_time_iso8601\"                         : \"13:15:18\"                       ," +
                "\"j_time_custom\"                          : \"15:13:20\"                       ," +
                "\"j_timestamptz_milliseconds_since_epoch\" : \"1518182116000\"                  ," +
                "\"j_timestamptz_seconds_since_epoch\"      : \"1518182117\"                     ," +
                "\"j_timestamptz_iso8601\"                  : \"2018-02-09T13:15:18Z\"           ," +
                "\"j_timestamptz_rfc2822\"                  : \"Fri Feb 09 13:15:19 Z 2018\"     ," +
                "\"j_timestamptz_custom\"                   : \"02/2018/09 13:15:20\"            ," +
                "\"j_timetz_milliseconds_since_epoch\"      : \"47716000\"                       ," +
                "\"j_timetz_seconds_since_epoch\"           : \"47717\"                          ," +
                "\"j_timetz_iso8601\"                       : \"13:15:18+00:00\"                 ," +
                "\"j_timetz_custom\"                        : \"15:13:20\"                       }";

        insertData("read_test.all_datatypes_json", json.getBytes(UTF_8));
        assertQuery(
                "SELECT " +
                        "  c_varchar " +
                        ", c_bigint " +
                        ", c_integer " +
                        ", c_smallint " +
                        ", c_tinyint " +
                        ", c_double " +
                        ", c_boolean " +
                        ", c_timestamp_milliseconds_since_epoch " +
                        ", c_timestamp_seconds_since_epoch " +
                        ", c_timestamp_iso8601 " +
                        ", c_timestamp_rfc2822 " +
                        ", c_timestamp_custom " +
                        ", c_date_iso8601 " +
                        ", c_date_custom " +
                        ", c_time_milliseconds_since_epoch " +
                        ", c_time_seconds_since_epoch " +
                        ", c_time_iso8601 " +
                        ", c_time_custom " +
                        // H2 does not support TIMESTAMP WITH TIME ZONE so cast to VARCHAR
                        ", cast(c_timestamptz_milliseconds_since_epoch as VARCHAR) " +
                        ", cast(c_timestamptz_seconds_since_epoch as VARCHAR) " +
                        ", cast(c_timestamptz_iso8601 as VARCHAR) " +
                        ", cast(c_timestamptz_rfc2822 as VARCHAR) " +
                        ", cast(c_timestamptz_custom as VARCHAR) " +
                        // H2 does not support TIME WITH TIME ZONE so cast to VARCHAR
                        ", cast(c_timetz_milliseconds_since_epoch as VARCHAR) " +
                        ", cast(c_timetz_seconds_since_epoch as VARCHAR) " +
                        ", cast(c_timetz_iso8601 as VARCHAR) " +
                        ", cast(c_timetz_custom as VARCHAR) " +
                        "FROM read_test.all_datatypes_json ",
                "VALUES (" +
                        "  'ala ma kota'" +
                        ", 9223372036854775807" +
                        ", 2147483647" +
                        ", 32767" +
                        ", 127" +
                        ", 1234567890.123456789" +
                        ", true" +
                        ", TIMESTAMP '2018-02-09 13:15:16'" +
                        ", TIMESTAMP '2018-02-09 13:15:17'" +
                        ", TIMESTAMP '2018-02-09 13:15:18'" +
                        ", TIMESTAMP '2018-02-09 13:15:19'" +
                        ", TIMESTAMP '2018-02-09 13:15:20'" +
                        ", DATE '2018-02-11'" +
                        ", DATE '2018-02-13'" +
                        ", TIME '13:15:16'" +
                        ", TIME '13:15:17'" +
                        ", TIME '13:15:18'" +
                        ", TIME '13:15:20'" +
                        ", '2018-02-09 13:15:16.000 UTC'" +
                        ", '2018-02-09 13:15:17.000 UTC'" +
                        ", '2018-02-09 13:15:18.000 UTC'" +
                        ", '2018-02-09 13:15:19.000 UTC'" +
                        ", '2018-02-09 13:15:20.000 UTC'" +
                        ", '13:15:16.000+00:00'" +
                        ", '13:15:17.000+00:00'" +
                        ", '13:15:18.000+00:00'" +
                        ", '13:15:20.000+00:00'" +
                        ")");
    }

    @Test
    @Override
    public void testInsert()
    {
        // Override because the base test uses CREATE TABLE AS SELECT statement that is unsupported in Kafka connector
        assertFalse(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        String query = "SELECT phone, custkey, acctbal FROM customer";

        assertQuery("SELECT count(*) FROM " + TABLE_INSERT_CUSTOMER + "", "SELECT 0");

        assertUpdate("INSERT INTO " + TABLE_INSERT_CUSTOMER + " " + query, "SELECT count(*) FROM customer");
        assertQuery("SELECT * FROM " + TABLE_INSERT_CUSTOMER + "", query);

        assertUpdate("INSERT INTO " + TABLE_INSERT_CUSTOMER + " (custkey) VALUES (-1)", 1);
        assertUpdate("INSERT INTO " + TABLE_INSERT_CUSTOMER + " (custkey) VALUES (null)", 1);
        assertUpdate("INSERT INTO " + TABLE_INSERT_CUSTOMER + " (phone) VALUES ('3283-2001-01-01')", 1);
        assertUpdate("INSERT INTO " + TABLE_INSERT_CUSTOMER + " (custkey, phone) VALUES (-2, '3283-2001-01-02')", 1);
        assertUpdate("INSERT INTO " + TABLE_INSERT_CUSTOMER + " (phone, custkey) VALUES ('3283-2001-01-03', -3)", 1);
        assertUpdate("INSERT INTO " + TABLE_INSERT_CUSTOMER + " (acctbal) VALUES (1234)", 1);

        assertQuery("SELECT * FROM " + TABLE_INSERT_CUSTOMER + "", query
                + " UNION ALL SELECT null, -1, null"
                + " UNION ALL SELECT null, null, null"
                + " UNION ALL SELECT '3283-2001-01-01', null, null"
                + " UNION ALL SELECT '3283-2001-01-02', -2, null"
                + " UNION ALL SELECT '3283-2001-01-03', -3, null"
                + " UNION ALL SELECT null, null, 1234");

        // UNION query produces columns in the opposite order
        // of how they are declared in the table schema
        assertUpdate(
                "INSERT INTO " + TABLE_INSERT_CUSTOMER + " (custkey, phone, acctbal) " +
                        "SELECT custkey, phone, acctbal FROM customer " +
                        "UNION ALL " +
                        "SELECT custkey, phone, acctbal FROM customer",
                "SELECT 2 * count(*) FROM customer");
    }

    @Test
    @Override
    public void testInsertNegativeDate()
    {
        // Override because the base test uses CREATE TABLE statement that is unsupported in Kafka connector
        assertQueryReturnsEmptyResult("SELECT dt FROM " + TABLE_INSERT_NEGATIVE_DATE);
        assertUpdate(format("INSERT INTO %s (dt) VALUES (DATE '-0001-01-01')", TABLE_INSERT_NEGATIVE_DATE), 1);
        assertQuery("SELECT dt FROM " + TABLE_INSERT_NEGATIVE_DATE, "VALUES date '-0001-01-01'");
        assertQuery(format("SELECT dt FROM %s WHERE dt = date '-0001-01-01'", TABLE_INSERT_NEGATIVE_DATE), "VALUES date '-0001-01-01'");
    }

    @Test
    @Override
    public void testInsertArray()
    {
        // Override because the base test uses CREATE TABLE statement that is unsupported in Kafka connector
        assertThatThrownBy(() -> query("INSERT INTO " + TABLE_INSERT_ARRAY + " (a) VALUES (ARRAY[null])"))
                .hasMessage("Unsupported column type 'array(double)' for column 'a'");
        throw new SkipException("not supported");
    }

    @Test
    @Override
    public void testInsertUnicode()
    {
        // Override because the base test uses CREATE TABLE statement that is unsupported in Kafka connector
        assertUpdate("INSERT INTO " + TABLE_INSERT_UNICODE_1 + "(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5world\\7F16\\7801' ", 2);
        assertThat(computeActual("SELECT test FROM " + TABLE_INSERT_UNICODE_1).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("Hello", "hello测试world编码");

        assertUpdate("INSERT INTO " + TABLE_INSERT_UNICODE_2 + "(test) VALUES 'aa', 'bé'", 2);
        assertQuery("SELECT test FROM " + TABLE_INSERT_UNICODE_2, "VALUES 'aa', 'bé'");
        assertQuery("SELECT test FROM " + TABLE_INSERT_UNICODE_2 + " WHERE test = 'aa'", "VALUES 'aa'");
        assertQuery("SELECT test FROM " + TABLE_INSERT_UNICODE_2 + " WHERE test > 'ba'", "VALUES 'bé'");
        assertQuery("SELECT test FROM " + TABLE_INSERT_UNICODE_2 + " WHERE test < 'ba'", "VALUES 'aa'");
        assertQueryReturnsEmptyResult("SELECT test FROM " + TABLE_INSERT_UNICODE_2 + " WHERE test = 'ba'");

        assertUpdate("INSERT INTO " + TABLE_INSERT_UNICODE_3 + "(test) VALUES 'a', 'é'", 2);
        assertQuery("SELECT test FROM " + TABLE_INSERT_UNICODE_3, "VALUES 'a', 'é'");
        assertQuery("SELECT test FROM " + TABLE_INSERT_UNICODE_3 + " WHERE test = 'a'", "VALUES 'a'");
        assertQuery("SELECT test FROM " + TABLE_INSERT_UNICODE_3 + " WHERE test > 'b'", "VALUES 'é'");
        assertQuery("SELECT test FROM " + TABLE_INSERT_UNICODE_3 + " WHERE test < 'b'", "VALUES 'a'");
        assertQueryReturnsEmptyResult("SELECT test FROM " + TABLE_INSERT_UNICODE_3 + " WHERE test = 'b'");
    }

    @Test
    @Override
    public void testInsertHighestUnicodeCharacter()
    {
        // Override because the base test uses CREATE TABLE statement that is unsupported in Kafka connector
        assertUpdate("INSERT INTO " + TABLE_INSERT_HIGHEST_UNICODE + "(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801' ", 2);
        assertThat(computeActual("SELECT test FROM " + TABLE_INSERT_HIGHEST_UNICODE).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("Hello", "hello测试􏿿world编码");
    }

    private static KafkaTopicDescription createDescription(SchemaTableName schemaTableName, KafkaTopicFieldDescription key, List<KafkaTopicFieldDescription> fields)
    {
        return new KafkaTopicDescription(
                schemaTableName.getTableName(),
                Optional.of(schemaTableName.getSchemaName()),
                schemaTableName.getTableName(),
                Optional.of(new KafkaTopicFieldGroup("json", Optional.empty(), Optional.empty(), ImmutableList.of(key))),
                Optional.of(new KafkaTopicFieldGroup("json", Optional.empty(), Optional.empty(), fields)));
    }

    private static KafkaTopicDescription createDescription(String name, String schema, String topic, Optional<KafkaTopicFieldGroup> message)
    {
        return new KafkaTopicDescription(name, Optional.of(schema), topic, Optional.empty(), message);
    }

    private static Optional<KafkaTopicFieldGroup> createFieldGroup(String dataFormat, List<KafkaTopicFieldDescription> fields)
    {
        return Optional.of(new KafkaTopicFieldGroup(dataFormat, Optional.empty(), Optional.empty(), fields));
    }

    private static KafkaTopicFieldDescription createOneFieldDescription(String name, Type type)
    {
        return new KafkaTopicFieldDescription(name, type, name, null, null, null, false);
    }

    private static KafkaTopicFieldDescription createOneFieldDescription(String name, Type type, String dataFormat)
    {
        return new KafkaTopicFieldDescription(name, type, name, null, dataFormat, null, false);
    }

    private static KafkaTopicFieldDescription createOneFieldDescription(String name, Type type, String dataFormat, Optional<String> formatHint)
    {
        return formatHint.map(s -> new KafkaTopicFieldDescription(name, type, name, null, dataFormat, s, false))
                .orElseGet(() -> new KafkaTopicFieldDescription(name, type, name, null, dataFormat, null, false));
    }

    private static KafkaTopicFieldDescription createOneFieldDescription(String name, Type type, String mapping, String dataFormat)
    {
        return new KafkaTopicFieldDescription(name, type, mapping, null, dataFormat, null, false);
    }

    @Test
    public void testKafkaHeaders()
    {
        createMessagesWithHeader(headersTopic);

        // Query the two messages without header and compare with empty object as JSON
        assertQuery("SELECT _message" +
                        " FROM default." + headersTopic +
                        " WHERE cardinality(_headers) = 0",
                "VALUES ('1'),('2')");

        assertQuery("SELECT from_utf8(value) FROM default." + headersTopic +
                        " CROSS JOIN UNNEST(_headers['foo']) AS arr (value)" +
                        " WHERE _message = '4'",
                "VALUES ('bar'), (null), ('baz')");
    }

    @Test(dataProvider = "jsonDateTimeFormatsDataProvider")
    public void testJsonDateTimeFormatsRoundTrip(JsonDateTimeTestCase testCase)
    {
        assertUpdate("INSERT into write_test." + testCase.getTopicName() +
                " (" + testCase.getFieldNames() + ")" +
                " VALUES " + testCase.getFieldValues(), 1);
        for (JsonDateTimeTestCase.Field field : testCase.getFields()) {
            Object actual = computeScalar("SELECT " + field.getFieldName() + " FROM write_test." + testCase.getTopicName());
            Object expected = computeScalar("SELECT " + field.getFieldValue());
            try {
                assertEquals(actual, expected, "Equality assertion failed for field: " + field.getFieldName());
            }
            catch (AssertionError e) {
                throw new AssertionError(format("Equality assertion failed for field '%s'\n%s", field.getFieldName(), e.getMessage()), e);
            }
        }
    }

    @DataProvider
    public static Object[][] jsonDateTimeFormatsDataProvider()
    {
        return jsonDateTimeFormatsData().stream()
                .collect(toDataProvider());
    }

    private static List<JsonDateTimeTestCase> jsonDateTimeFormatsData()
    {
        return ImmutableList.<JsonDateTimeTestCase>builder()
                .add(JsonDateTimeTestCase.builder()
                        .setTopicName(JSON_CUSTOM_DATE_TIME_TABLE_NAME)
                        .addField(DATE, CUSTOM_DATE_TIME.toString(), "yyyy-MM-dd", "DATE '2020-07-15'")
                        .addField(TIME, CUSTOM_DATE_TIME.toString(), "HH:mm:ss.SSS", "TIME '01:02:03.456'")
                        .addField(TIME_WITH_TIME_ZONE, CUSTOM_DATE_TIME.toString(), "HH:mm:ss.SSS Z", "TIME '01:02:03.456 -04:00'")
                        .addField(TIMESTAMP, CUSTOM_DATE_TIME.toString(), "yyyy-dd-MM HH:mm:ss.SSS", "TIMESTAMP '2020-07-15 01:02:03.456'")
                        .addField(TIMESTAMP_WITH_TIME_ZONE, CUSTOM_DATE_TIME.toString(), "yyyy-dd-MM HH:mm:ss.SSS Z", "TIMESTAMP '2020-07-15 01:02:03.456 -04:00'")
                        .build())
                .add(JsonDateTimeTestCase.builder()
                        .setTopicName(JSON_ISO8601_TABLE_NAME)
                        .addField(DATE, ISO8601.toString(), "DATE '2020-07-15'")
                        .addField(TIME, ISO8601.toString(), "TIME '01:02:03.456'")
                        .addField(TIME_WITH_TIME_ZONE, ISO8601.toString(), "TIME '01:02:03.456 -04:00'")
                        .addField(TIMESTAMP, ISO8601.toString(), "TIMESTAMP '2020-07-15 01:02:03.456'")
                        .addField(TIMESTAMP_WITH_TIME_ZONE, ISO8601.toString(), "TIMESTAMP '2020-07-15 01:02:03.456 -04:00'")
                        .build())
                .add(JsonDateTimeTestCase.builder()
                        .setTopicName(JSON_RFC2822_TABLE_NAME)
                        .addField(TIMESTAMP, RFC2822.toString(), "TIMESTAMP '2020-07-15 01:02:03'")
                        .addField(TIMESTAMP_WITH_TIME_ZONE, RFC2822.toString(), "TIMESTAMP '2020-07-15 01:02:03 -04:00'")
                        .build())
                .add(JsonDateTimeTestCase.builder()
                        .setTopicName(JSON_MILLISECONDS_TABLE_NAME)
                        .addField(TIME, MILLISECONDS_SINCE_EPOCH.toString(), "TIME '01:02:03.456'")
                        .addField(TIMESTAMP, MILLISECONDS_SINCE_EPOCH.toString(), "TIMESTAMP '2020-07-15 01:02:03.456'")
                        .build())
                .add(JsonDateTimeTestCase.builder()
                        .setTopicName(JSON_SECONDS_TABLE_NAME)
                        .addField(TIME, SECONDS_SINCE_EPOCH.toString(), "TIME '01:02:03'")
                        .addField(TIMESTAMP, SECONDS_SINCE_EPOCH.toString(), "TIMESTAMP '2020-07-15 01:02:03'")
                        .build())
                .build();
    }

    private static Map<SchemaTableName, KafkaTopicDescription> createJsonDateTimeTestTopic()
    {
        return jsonDateTimeFormatsData().stream().collect(toImmutableMap(
                testCase -> new SchemaTableName("write_test", testCase.getTopicName()),
                testCase -> new KafkaTopicDescription(
                        testCase.getTopicName(),
                        Optional.of("write_test"),
                        testCase.getTopicName(),
                        Optional.of(new KafkaTopicFieldGroup("json", Optional.empty(), Optional.empty(), ImmutableList.of(createOneFieldDescription("key", BIGINT, "key", (String) null)))),
                        Optional.of(new KafkaTopicFieldGroup("json", Optional.empty(), Optional.empty(), testCase.getFields().stream()
                                .map(field -> createOneFieldDescription(
                                        field.getFieldName(),
                                        field.getType(),
                                        field.getDataFormat(),
                                        field.getFormatHint()))
                                .collect(toImmutableList()))))));
    }

    private static final class JsonDateTimeTestCase
    {
        private final String topicName;
        private final List<Field> fields;

        public JsonDateTimeTestCase(String topicName, List<Field> fields)
        {
            this.topicName = requireNonNull(topicName, "topicName is null");
            requireNonNull(fields, "fields is null");
            this.fields = ImmutableList.copyOf(fields);
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public String getTopicName()
        {
            return topicName;
        }

        public String getFieldNames()
        {
            return fields.stream().map(Field::getFieldName).collect(joining(", "));
        }

        public String getFieldValues()
        {
            return fields.stream().map(Field::getFieldValue).collect(joining(", ", "(", ")"));
        }

        public List<Field> getFields()
        {
            return fields;
        }

        @Override
        public String toString()
        {
            return topicName; // for test case label in IDE
        }

        public static class Builder
        {
            private String topicName = "";
            private final ImmutableList.Builder<Field> fields = ImmutableList.builder();

            public Builder setTopicName(String topicName)
            {
                this.topicName = topicName;
                return this;
            }

            public Builder addField(Type type, String dataFormat, String fieldValue)
            {
                String fieldName = getFieldName(type, dataFormat);
                this.fields.add(new Field(fieldName, type, dataFormat, Optional.empty(), fieldValue));
                return this;
            }

            public Builder addField(Type type, String dataFormat, String formatHint, String fieldValue)
            {
                String fieldName = getFieldName(type, dataFormat);
                this.fields.add(new Field(fieldName, type, dataFormat, Optional.of(formatHint), fieldValue));
                return this;
            }

            private static String getFieldName(Type type, String dataFormat)
            {
                return String.join("_", dataFormat.replaceAll("-", "_"), type.getDisplayName().replaceAll("\\s|[(]|[)]", "_"));
            }

            public JsonDateTimeTestCase build()
            {
                return new JsonDateTimeTestCase(topicName, fields.build());
            }
        }

        public static class Field
        {
            private final String fieldName;
            private final Type type;
            private final String dataFormat;
            private final Optional<String> formatHint;
            private final String fieldValue;

            public Field(String fieldName, Type type, String dataFormat, Optional<String> formatHint, String fieldValue)
            {
                this.fieldName = requireNonNull(fieldName, "fieldName is null");
                this.type = requireNonNull(type, "type is null");
                this.dataFormat = requireNonNull(dataFormat, "dataFormat is null");
                this.formatHint = requireNonNull(formatHint, "formatHint is null");
                this.fieldValue = requireNonNull(fieldValue, "fieldValue is null");
            }

            public String getFieldName()
            {
                return fieldName;
            }

            public Type getType()
            {
                return type;
            }

            public String getDataFormat()
            {
                return dataFormat;
            }

            public Optional<String> getFormatHint()
            {
                return formatHint;
            }

            public String getFieldValue()
            {
                return fieldValue;
            }
        }
    }

    @Test(dataProvider = "roundTripAllFormatsDataProvider")
    public void testRoundTripAllFormats(RoundTripTestCase testCase)
    {
        assertUpdate("INSERT into write_test." + testCase.getTableName() +
                " (" + testCase.getFieldNames() + ")" +
                " VALUES " + testCase.getRowValues(), testCase.getNumRows());
        assertQuery("SELECT " + testCase.getFieldNames() + " FROM write_test." + testCase.getTableName() +
                        " WHERE f_bigint > 1",
                "VALUES " + testCase.getRowValues());
    }

    @DataProvider
    public static Object[][] roundTripAllFormatsDataProvider()
    {
        return roundTripAllFormatsData().stream()
                .collect(toDataProvider());
    }

    private static List<RoundTripTestCase> roundTripAllFormatsData()
    {
        return ImmutableList.<RoundTripTestCase>builder()
                .add(new RoundTripTestCase(
                        "all_datatypes_avro",
                        ImmutableList.of("f_bigint", "f_double", "f_boolean", "f_varchar"),
                        ImmutableList.of(
                                ImmutableList.of(100000, 1000.001, true, "'test'"),
                                ImmutableList.of(123456, 1234.123, false, "'abcd'"))))
                .add(new RoundTripTestCase(
                        "all_datatypes_csv",
                        ImmutableList.of("f_bigint", "f_int", "f_smallint", "f_tinyint", "f_double", "f_boolean", "f_varchar"),
                        ImmutableList.of(
                                ImmutableList.of(100000, 1000, 100, 10, 1000.001, true, "'test'"),
                                ImmutableList.of(123456, 1234, 123, 12, 12345.123, false, "'abcd'"))))
                .add(new RoundTripTestCase(
                        "all_datatypes_raw",
                        ImmutableList.of("kafka_key", "f_varchar", "f_bigint", "f_int", "f_smallint", "f_tinyint", "f_double", "f_boolean"),
                        ImmutableList.of(
                                ImmutableList.of(1, "'test'", 100000, 1000, 100, 10, 1000.001, true),
                                ImmutableList.of(1, "'abcd'", 123456, 1234, 123, 12, 12345.123, false))))
                .add(new RoundTripTestCase(
                        "all_datatypes_json",
                        ImmutableList.of("f_bigint", "f_int", "f_smallint", "f_tinyint", "f_double", "f_boolean", "f_varchar"),
                        ImmutableList.of(
                                ImmutableList.of(100000, 1000, 100, 10, 1000.001, true, "'test'"),
                                ImmutableList.of(123748, 1234, 123, 12, 12345.123, false, "'abcd'"))))
                .build();
    }

    private static final class RoundTripTestCase
    {
        private final String tableName;
        private final List<String> fieldNames;
        private final List<List<Object>> rowValues;
        private final int numRows;

        public RoundTripTestCase(String tableName, List<String> fieldNames, List<List<Object>> rowValues)
        {
            for (List<Object> row : rowValues) {
                checkArgument(fieldNames.size() == row.size(), "sizes of fieldNames and rowValues are not equal");
            }
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.fieldNames = ImmutableList.copyOf(fieldNames);
            this.rowValues = ImmutableList.copyOf(rowValues);
            this.numRows = this.rowValues.size();
        }

        public String getTableName()
        {
            return tableName;
        }

        public String getFieldNames()
        {
            return String.join(", ", fieldNames);
        }

        public String getRowValues()
        {
            String[] rows = new String[numRows];
            for (int i = 0; i < numRows; i++) {
                rows[i] = rowValues.get(i).stream().map(Object::toString).collect(joining(", ", "(", ")"));
            }
            return String.join(", ", rows);
        }

        public int getNumRows()
        {
            return numRows;
        }

        @Override
        public String toString()
        {
            return tableName; // for test case label in IDE
        }
    }
}
