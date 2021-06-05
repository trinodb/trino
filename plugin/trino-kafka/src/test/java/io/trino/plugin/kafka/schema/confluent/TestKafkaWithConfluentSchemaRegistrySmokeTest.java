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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.trino.Session;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.Type;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.trino.plugin.kafka.schema.confluent.AbstractConfluentRowEncoder.extractBaseType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MICROSECOND;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MILLISECOND;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.testng.Assert.assertTrue;

public class TestKafkaWithConfluentSchemaRegistrySmokeTest
        extends AbstractTestQueryFramework
{
    private static final LongSerializer LONG_SERIALIZER = new LongSerializer();
    private static final IntegerSerializer INTEGER_SERIALIZER = new IntegerSerializer();

    private TestingKafka testingKafka;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(TestingKafka.createWithSchemaRegistry());
        return KafkaWithConfluentSchemaRegistryQueryRunner.builder(testingKafka)
                .setExtraKafkaProperties(ImmutableMap.<String, String>builder()
                        .put("kafka.confluent-subjects-cache-refresh-interval", "1ms")
                        .build())
                .setCoordinatorProperties(ImmutableMap.of())
                .build();
    }

    private static ImmutableMap.Builder<String, String> schemaRegistryAwareProducer(TestingKafka testingKafka)
    {
        return ImmutableMap.<String, String>builder()
                .put(SCHEMA_REGISTRY_URL_CONFIG, testingKafka.getSchemaRegistryConnectString());
    }

    private static String toDoubleQuoted(String tableName)
    {
        return format("\"%s\"", tableName);
    }

    private void waitUntilTableExists(String tableName)
    {
        waitUntilTableExists(getQueryRunner().getDefaultSession(), tableName);
    }

    private void waitUntilTableExists(Session session, String tableName)
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
                .run(() -> assertTrue(tableExists(session, tableName)));
    }

    private boolean schemaExists()
    {
        return getQueryRunner().execute(format(
                "SHOW SCHEMAS FROM %s LIKE '%s'",
                getSession().getCatalog().orElseThrow(),
                getSession().getSchema().orElseThrow()))
                .getRowCount() == 1;
    }

    private boolean tableExists(Session session, String tableName)
    {
        return getQueryRunner().execute(session, format("SHOW TABLES LIKE '%s'", tableName.toLowerCase(ENGLISH))).getRowCount() == 1;
    }

    public static class BooleanSerializer
            implements Serializer<Boolean>
    {
        private static final byte[] SERIALIZED_TRUE = new byte[]{1};
        private static final byte[] SERIALIZED_FALSE = new byte[]{0};

        @Override
        public byte[] serialize(String topic, Boolean data)
        {
            if (data == null) {
                return null;
            }
            return data ? SERIALIZED_TRUE : SERIALIZED_FALSE;
        }
    }

    public static class SqlDateSerializer
            implements Serializer<SqlDate>
    {
        @Override
        public byte[] serialize(String topic, SqlDate data)
        {
            if (data == null) {
                return null;
            }
            return INTEGER_SERIALIZER.serialize(topic, data.getDays());
        }
    }

    public static class SqlTimeStampSerializer
            implements Serializer<SqlTimestamp>
    {
        @Override
        public byte[] serialize(String topic, SqlTimestamp data)
        {
            if (data == null) {
                return null;
            }
            return LONG_SERIALIZER.serialize(topic, data.getEpochMicros());
        }
    }

    public static class SqlTimeSerializer
            implements Serializer<SqlTime>
    {
        @Override
        public byte[] serialize(String topic, SqlTime data)
        {
            if (data == null) {
                return null;
            }
            return LONG_SERIALIZER.serialize(topic, data.getPicos());
        }
    }

    public static class SqlVarbinarySerializer
            implements Serializer<SqlVarbinary>
    {
        @Override
        public byte[] serialize(String topic, SqlVarbinary data)
        {
            if (data == null) {
                return null;
            }
            return data.getBytes();
        }
    }

    private <T> void testKey(Type keyType, Class<? extends Serializer> keySerializerClass, T firstValue, T secondValue)
    {
        String topic = "topic-" + getDisplayNameForTopic(keyType) + "-" + UUID.randomUUID();
        String tableName = toDoubleQuoted(topic + "&key-columns=key_col:" + keyType.getDisplayName());
        Schema schema = SchemaBuilder.record("test").fields()
                .name("string_optional_field").type().optional().stringType()
                .endRecord();
        testingKafka.sendMessages(Stream.of(new ProducerRecord<T, GenericRecord>(topic, firstValue, new GenericRecordBuilder(schema).build())), schemaRegistryAwareProducer(testingKafka)
                .put(KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getName())
                .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                .build());
        waitUntilTableExists(topic);
        QueryAssertions queryAssertions = new QueryAssertions(getQueryRunner());

        queryAssertions.query("SELECT key_col, string_optional_field FROM " + tableName)
                .assertThat()
                .matches("VALUES (CAST(" + toSingleQuotedOrNullLiteral(firstValue) + " AS " + keyType.getDisplayName() + "), CAST(NULL AS VARCHAR))");
        assertUpdate("INSERT INTO " + tableName + " (key_col) VALUES" +
                "  (CAST(" + toSingleQuotedOrNullLiteral(secondValue) + " AS " + keyType.getDisplayName() + "))," +
                "  (CAST(NULL AS " + keyType.getDisplayName() + "))", 2);
        queryAssertions.query("SELECT key_col, string_optional_field FROM " + tableName)
                .assertThat()
                .matches("VALUES (CAST(" + toSingleQuotedOrNullLiteral(firstValue) + " AS " + keyType.getDisplayName() + "), CAST(NULL AS VARCHAR))," +
                        "  (CAST(" + toSingleQuotedOrNullLiteral(secondValue) + " AS " + keyType.getDisplayName() + "), CAST(NULL AS VARCHAR))," +
                        "  (CAST(NULL AS " + keyType.getDisplayName() + "), CAST(NULL AS VARCHAR))");
    }

    private static String getDisplayNameForTopic(Type type)
    {
        return type.getDisplayName().replaceAll("[()]", "_");
    }

    private String toSingleQuotedOrNullLiteral(Object value)
    {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof SqlVarbinary) {
            return "X'" + BaseEncoding.base16().encode(((SqlVarbinary) value).getBytes()) + "'";
        }
        else if (value instanceof byte[]) {
            return "X'" + BaseEncoding.base16().encode(deserializeBytes((byte[]) value)) + "'";
        }
        return "'" + value + "'";
    }

    @Test
    public void testSupportedKeyColumnTypes()
    {
        testKey(VARCHAR, StringSerializer.class, "string_1", "string_2");
        testKey(VARBINARY, SqlVarbinarySerializer.class, new SqlVarbinary(new byte[] {1, 2, 3}), new SqlVarbinary(new byte[] {2, 3, 4}));
        testKey(DATE, SqlDateSerializer.class, new SqlDate(50), new SqlDate(51));
        testKey(TIMESTAMP_MILLIS, SqlTimeStampSerializer.class, SqlTimestamp.fromMillis(TIMESTAMP_MILLIS.getPrecision(), 1111L), SqlTimestamp.fromMillis(TIMESTAMP_MILLIS.getPrecision(), 1112L));
        testKey(TIMESTAMP_MICROS, SqlTimeStampSerializer.class, SqlTimestamp.newInstance(TIMESTAMP_MICROS.getPrecision(), 1111L, 0), SqlTimestamp.newInstance(TIMESTAMP_MICROS.getPrecision(), 1112L, 0));
        testKey(TIME_MILLIS, SqlTimeSerializer.class, SqlTime.newInstance(TIME_MILLIS.getPrecision(), 1111L * PICOSECONDS_PER_MILLISECOND), SqlTime.newInstance(TIME_MILLIS.getPrecision(), 1112L * PICOSECONDS_PER_MILLISECOND));
        testKey(TIME_MICROS, SqlTimeSerializer.class, SqlTime.newInstance(TIME_MICROS.getPrecision(), 1111L * PICOSECONDS_PER_MICROSECOND), SqlTime.newInstance(TIME_MICROS.getPrecision(), 1112L * PICOSECONDS_PER_MICROSECOND));
        testKey(BIGINT, LongSerializer.class, 1L, 2L);
        testKey(INTEGER, IntegerSerializer.class, 1, 2);
        testKey(REAL, FloatSerializer.class, -1.5F, 2.5F);
        testKey(DOUBLE, DoubleSerializer.class, 1.5D, 2.5D);
        testKey(BOOLEAN, BooleanSerializer.class, false, true);
    }

    private byte[] serializeBytes(ByteBuffer byteBuffer)
    {
        // Varbinary needs to be serialized using the avro encoder.
        // The returned array prepends the length with zigzag varint encoding
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
        try {
            encoder.writeBytes(byteBuffer);
            encoder.flush();
            return outputStream.toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private byte[] deserializeBytes(byte[] bytes)
    {
        // Varbinary need to be deserialized using avro decoder.
        BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(bytes), null);
        try {
            return decoder.readBytes(null).array();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private <T> void testConfluentPrimitiveKey(Type keyType, T firstValue, T secondValue, T thirdValue)
    {
        String topic = "primitive-key-" + getDisplayNameForTopic(keyType) + "-" + UUID.randomUUID();
        String tableName = toDoubleQuoted(topic);
        Schema schema = SchemaBuilder.record("test").fields()
                .name("string_optional_field").type().optional().stringType()
                .endRecord();
        testingKafka.sendMessages(Stream.of(new ProducerRecord<T, GenericRecord>(topic, firstValue, new GenericRecordBuilder(schema).build())), schemaRegistryAwareProducer(testingKafka)
                .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                .build());
        waitUntilTableExists(topic);
        QueryAssertions queryAssertions = new QueryAssertions(getQueryRunner());
        String keyColumnName = toDoubleQuoted(topic + "-key");
        queryAssertions.query("SELECT " + keyColumnName + ", string_optional_field FROM " + tableName)
                .assertThat()
                .matches("VALUES (CAST(" + toSingleQuotedOrNullLiteral(firstValue) + " AS " + keyType.getDisplayName() + "), CAST(NULL AS VARCHAR))");
        assertUpdate("INSERT INTO " + tableName + " (" + keyColumnName + ") VALUES" +
                        "  (CAST(" + toSingleQuotedOrNullLiteral(secondValue) + " AS " + keyType.getDisplayName() + "))," +
                        "  (CAST(" + toSingleQuotedOrNullLiteral(thirdValue) + " AS " + keyType.getDisplayName() + "))",
                2);

        queryAssertions.query("SELECT " + keyColumnName + ", string_optional_field FROM " + tableName)
                .assertThat()
                .matches("VALUES (CAST(" + toSingleQuotedOrNullLiteral(firstValue) + " AS " + keyType.getDisplayName() + "), CAST(NULL AS VARCHAR))," +
                        "  (CAST(" + toSingleQuotedOrNullLiteral(secondValue) + " AS " + keyType.getDisplayName() + "), CAST(NULL AS VARCHAR))," +
                        "  (CAST(" + toSingleQuotedOrNullLiteral(thirdValue) + " AS " + keyType.getDisplayName() + "), CAST(NULL AS VARCHAR))");
    }

    private static GenericData.EnumSymbol enumSymbol(Schema enumSchema, String symbol)
    {
        return new GenericData.EnumSymbol(enumSchema, symbol);
    }

    @Test
    public void testSupportedConfluentPrimitiveKeys()
    {
        testConfluentPrimitiveKey(VARBINARY, serializeBytes(ByteBuffer.wrap(new byte[] {1, 2, 3})),
                serializeBytes(ByteBuffer.wrap(new byte[] {2, 3, 4})),
                serializeBytes(ByteBuffer.wrap(new byte[] {3, 4, 5})));
        testConfluentPrimitiveKey(VARCHAR, "string_1", "string_2", "string_3");
        testConfluentPrimitiveKey(BIGINT, 1L, 2L, 3L);
        testConfluentPrimitiveKey(DOUBLE, 1.5D, -2.5D, 3.5D);
        testConfluentPrimitiveKey(BOOLEAN, false, true, false);
        testConfluentPrimitiveKey(REAL, -1.5F, 2.5F, -3.5F);
        testConfluentPrimitiveKey(INTEGER, 1, 2, 3);
        Schema enumSchema = SchemaBuilder.enumeration("color").symbols("BLUE", "YELLOW", "RED");
        testConfluentPrimitiveKey(VARCHAR, enumSymbol(enumSchema, "YELLOW"), enumSymbol(enumSchema, "RED"), enumSymbol(enumSchema, "BLUE"));
    }

    @Test
    public void testConfluentPrimitiveMessage()
    {
        Schema schema = SchemaBuilder.record("test").fields()
                .name("string_col").type().optional().stringType()
                .name("bool_col").type().optional().booleanType()
                .name("int_col").type().optional().intType()
                .name("long_col").type().optional().longType()
                .name("float_col").type().optional().floatType()
                .name("double_col").type().optional().doubleType()
                .name("bytes_col").type().optional().bytesType()
                .name("enum_col").type().optional().enumeration("color").symbols("BLUE", "YELLOW", "RED")
                .endRecord();
        String topic = "primitive-" + UUID.randomUUID();
        String tableName = toDoubleQuoted(topic);
        String keyColumnName = toDoubleQuoted(topic + "-key");

        testingKafka.sendMessages(Stream.of(new ProducerRecord<Long, GenericRecord>(topic, 1L, new GenericRecordBuilder(schema)
                        .set("string_col", "string_1")
                        .set("bool_col", true)
                        .set("int_col", -123)
                        .set("long_col", 123L)
                        .set("float_col", -1.5F)
                        .set("double_col", 2.5D)
                        .set("bytes_col", ByteBuffer.wrap(new byte[] {1, 2, 3}))
                        .set("enum_col", new GenericData.EnumSymbol(extractBaseType(schema.getField("enum_col").schema()), "YELLOW"))
                        .build())),
                schemaRegistryAwareProducer(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
        waitUntilTableExists(topic);
        QueryAssertions queryAssertions = new QueryAssertions(getQueryRunner());
        queryAssertions.query("SELECT " + keyColumnName + ", string_col, bool_col, int_col, long_col, float_col, double_col, bytes_col, enum_col FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1'," +
                        "  VARCHAR 'string_1'," +
                        "  true," +
                        "  INTEGER '-123'," +
                        "  BIGINT '123'," +
                        "  REAL '-1.5'," +
                        "  DOUBLE '2.5'," +
                        "  X'01 02 03'," +
                        "  VARCHAR 'YELLOW')");
        assertUpdate("INSERT INTO " + tableName + " (" + keyColumnName + ", string_col, bool_col, int_col, long_col, float_col, double_col, bytes_col, enum_col)" +
                        "  VALUES (BIGINT '2'," +
                        "  VARCHAR 'string_2'," +
                        "  false," +
                        "  INTEGER '-124'," +
                        "  BIGINT '124'," +
                        "  REAL '-2.5'," +
                        "  DOUBLE '3.5'," +
                        "  X'02 03 04'," +
                        "  VARCHAR 'RED')," +
                        "  (BIGINT '3'," +
                        "  VARCHAR 'string_3'," +
                        "  true," +
                        "  INTEGER '-125'," +
                        "  BIGINT '125'," +
                        "  REAL '-3.5'," +
                        "  DOUBLE '4.5'," +
                        "  X'03 04 05'," +
                        "  VARCHAR 'BLUE')," +
                        "  (BIGINT '4'," +
                        "  CAST(NULL AS VARCHAR)," +
                        "  CAST(NULL AS BOOLEAN)," +
                        "  CAST(NULL AS INTEGER)," +
                        "  CAST(NULL AS BIGINT)," +
                        "  CAST(NULL AS REAL)," +
                        "  CAST(NULL AS DOUBLE)," +
                        "  CAST(NULL AS VARBINARY)," +
                        "  CAST(NULL AS VARCHAR))",
                3);
        queryAssertions.query("SELECT " + keyColumnName + ", string_col, bool_col, int_col, long_col, float_col, double_col, bytes_col, enum_col FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1'," +
                        "  VARCHAR 'string_1'," +
                        "  true," +
                        "  INTEGER '-123'," +
                        "  BIGINT '123'," +
                        "  REAL '-1.5'," +
                        "  DOUBLE '2.5'," +
                        "  X'01 02 03'," +
                        "  VARCHAR 'YELLOW')," +
                        "  (BIGINT '2'," +
                        "  VARCHAR 'string_2'," +
                        "  false," +
                        "  INTEGER '-124'," +
                        "  BIGINT '124'," +
                        "  REAL '-2.5'," +
                        "  DOUBLE '3.5'," +
                        "  X'02 03 04'," +
                        "  VARCHAR 'RED')," +
                        "  (BIGINT '3'," +
                        "  VARCHAR 'string_3'," +
                        "  true," +
                        "  INTEGER '-125'," +
                        "  BIGINT '125'," +
                        "  REAL '-3.5'," +
                        "  DOUBLE '4.5'," +
                        "  X'03 04 05'," +
                        "  VARCHAR 'BLUE')," +
                        "  (BIGINT '4'," +
                        "  CAST(NULL AS VARCHAR)," +
                        "  CAST(NULL AS BOOLEAN)," +
                        "  CAST(NULL AS INTEGER)," +
                        "  CAST(NULL AS BIGINT)," +
                        "  CAST(NULL AS REAL)," +
                        "  CAST(NULL AS DOUBLE)," +
                        "  CAST(NULL AS VARBINARY)," +
                        "  CAST(NULL AS VARCHAR))");
    }

    @Test
    public void testConfluentArrayMessage()
    {
        String topic = "primitive-array-" + UUID.randomUUID();
        String tableName = toDoubleQuoted(topic);
        String keyColumnName = toDoubleQuoted(topic + "-key");

        Schema schema = SchemaBuilder.record("test").fields()
                .name("string_array_col").type().optional().array().items().nullable().stringType()
                .name("bool_array_col").type().optional().array().items().nullable().booleanType()
                .name("int_array_col").type().optional().array().items().nullable().intType()
                .name("long_array_col").type().optional().array().items().nullable().longType()
                .name("float_array_col").type().optional().array().items().nullable().floatType()
                .name("double_array_col").type().optional().array().items().nullable().doubleType()
                .name("bytes_array_col").type().optional().array().items().nullable().bytesType()
                .endRecord();
        testingKafka.sendMessages(Stream.of(new ProducerRecord<Long, GenericRecord>(topic, 1L, new GenericRecordBuilder(schema)
                        .set("string_array_col", Arrays.asList("string_1", "string_2", "string_3"))
                        .set("bool_array_col", Arrays.asList(true, false, true))
                        .set("int_array_col", Arrays.asList(-123, 124, -125))
                        .set("long_array_col", Arrays.asList(123L, -124L, 125L))
                        .set("float_array_col", Arrays.asList(-1.5F, 2.5F, -3.5F))
                        .set("double_array_col", Arrays.asList(2.5D, -3.5D, 4.5D))
                        .set("bytes_array_col", Arrays.asList(ByteBuffer.wrap(new byte[] {1, 2, 3}), ByteBuffer.wrap(new byte[] {2, 3, 4}), ByteBuffer.wrap(new byte[] {3, 4, 5})))
                        .build())),
                schemaRegistryAwareProducer(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
        waitUntilTableExists(topic);
        QueryAssertions queryAssertions = new QueryAssertions(getQueryRunner());
        queryAssertions.query("SELECT " + keyColumnName + ", string_array_col, bool_array_col, int_array_col, long_array_col, float_array_col, double_array_col, bytes_array_col FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1'," +
                        "  CAST(ARRAY['string_1', 'string_2', 'string_3'] AS ARRAY(VARCHAR))," +
                        "  CAST(ARRAY[true, false, true] AS ARRAY(BOOLEAN))," +
                        "  CAST(ARRAY[-123, 124, -125] AS ARRAY(INTEGER))," +
                        "  CAST(ARRAY[123, -124, 125] AS ARRAY(BIGINT))," +
                        "  CAST(ARRAY[-1.5, 2.5, -3.5] AS ARRAY(REAL))," +
                        "  CAST(ARRAY[2.5, -3.5, 4.5] AS ARRAY(DOUBLE))," +
                        "  CAST(ARRAY[X'01 02 03', X'02 03 04', X'03 04 05'] AS ARRAY(VARBINARY)))");
        assertUpdate("INSERT INTO " + tableName + " (" + keyColumnName + ", string_array_col, bool_array_col, int_array_col, long_array_col, float_array_col, double_array_col, bytes_array_col)" +
                "  VALUES (BIGINT '2'," +
                "  CAST(ARRAY['string_1', NULL, 'string_3'] AS ARRAY(VARCHAR))," +
                "  CAST(ARRAY[false, NULL, false] AS ARRAY(BOOLEAN))," +
                "  CAST(ARRAY[-223, NULL, -225] AS ARRAY(INTEGER))," +
                "  CAST(ARRAY[223, NULL, 225] AS ARRAY(BIGINT))," +
                "  CAST(ARRAY[-2.5, NULL, -4.5] AS ARRAY(REAL))," +
                "  CAST(ARRAY[3.53, NULL, 5.53] AS ARRAY(DOUBLE))," +
                "  CAST(ARRAY[X'11 12 13', NULL, X'13 14 15'] AS ARRAY(VARBINARY)))," +
                "  (BIGINT '3'," +
                "  CAST(ARRAY['string_1', 'string_2', 'string_3'] AS ARRAY(VARCHAR))," +
                "  CAST(ARRAY[true, true, false] AS ARRAY(BOOLEAN))," +
                "  CAST(ARRAY[-323, 324, -325] AS ARRAY(INTEGER))," +
                "  CAST(ARRAY[323, -324, 325] AS ARRAY(BIGINT))," +
                "  CAST(ARRAY[-3.5, 4.5, -5.5] AS ARRAY(REAL))," +
                "  CAST(ARRAY[3.5, -4.5, 5.5] AS ARRAY(DOUBLE))," +
                "  CAST(ARRAY[X'21 22 03', X'22 23 24', X'23 24 25'] AS ARRAY(VARBINARY)))", 2);
        queryAssertions.query("SELECT " + keyColumnName + ", string_array_col, bool_array_col, int_array_col, long_array_col, float_array_col, double_array_col, bytes_array_col FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1'," +
                        "  CAST(ARRAY['string_1', 'string_2', 'string_3'] AS ARRAY(VARCHAR))," +
                        "  CAST(ARRAY[true, false, true] AS ARRAY(BOOLEAN))," +
                        "  CAST(ARRAY[-123, 124, -125] AS ARRAY(INTEGER))," +
                        "  CAST(ARRAY[123, -124, 125] AS ARRAY(BIGINT))," +
                        "  CAST(ARRAY[-1.5, 2.5, -3.5] AS ARRAY(REAL))," +
                        "  CAST(ARRAY[2.5, -3.5, 4.5] AS ARRAY(DOUBLE))," +
                        "  CAST(ARRAY[X'01 02 03', X'02 03 04', X'03 04 05'] AS ARRAY(VARBINARY)))," +
                        "  (BIGINT '2'," +
                        "  CAST(ARRAY['string_1', NULL, 'string_3'] AS ARRAY(VARCHAR))," +
                        "  CAST(ARRAY[false, NULL, false] AS ARRAY(BOOLEAN))," +
                        "  CAST(ARRAY[-223, NULL, -225] AS ARRAY(INTEGER))," +
                        "  CAST(ARRAY[223, NULL, 225] AS ARRAY(BIGINT))," +
                        "  CAST(ARRAY[-2.5, NULL, -4.5] AS ARRAY(REAL))," +
                        "  CAST(ARRAY[3.53, NULL, 5.53] AS ARRAY(DOUBLE))," +
                        "  CAST(ARRAY[X'11 12 13', NULL, X'13 14 15'] AS ARRAY(VARBINARY)))," +
                        "  (BIGINT '3'," +
                        "  CAST(ARRAY['string_1', 'string_2', 'string_3'] AS ARRAY(VARCHAR))," +
                        "  CAST(ARRAY[true, true, false] AS ARRAY(BOOLEAN))," +
                        "  CAST(ARRAY[-323, 324, -325] AS ARRAY(INTEGER))," +
                        "  CAST(ARRAY[323, -324, 325] AS ARRAY(BIGINT))," +
                        "  CAST(ARRAY[-3.5, 4.5, -5.5] AS ARRAY(REAL))," +
                        "  CAST(ARRAY[3.5, -4.5, 5.5] AS ARRAY(DOUBLE))," +
                        "  CAST(ARRAY[X'21 22 03', X'22 23 24', X'23 24 25'] AS ARRAY(VARBINARY)))");
    }

    @Test
    public void testConfluentMapMessage()
    {
        String topic = "primitive-map-" + UUID.randomUUID();
        String tableName = toDoubleQuoted(topic);
        String keyColumnName = toDoubleQuoted(topic + "-key");

        Schema schema = SchemaBuilder.record("test").fields()
                .name("string_map_col").type().optional().map().values().nullable().stringType()
                .name("bool_map_col").type().optional().map().values().nullable().booleanType()
                .name("int_map_col").type().optional().map().values().nullable().intType()
                .name("long_map_col").type().optional().map().values().nullable().longType()
                .name("float_map_col").type().optional().map().values().nullable().floatType()
                .name("double_map_col").type().optional().map().values().nullable().doubleType()
                .name("bytes_map_col").type().optional().map().values().nullable().bytesType()
                .endRecord();
        testingKafka.sendMessages(Stream.of(new ProducerRecord<Long, GenericRecord>(topic, 1L, new GenericRecordBuilder(schema)
                        .set("string_map_col", ImmutableMap.of("key1", "val1", "key2", "val2", "key3", "val3"))
                        .set("bool_map_col", ImmutableMap.of("key1", true, "key2", false, "key3", true))
                        .set("int_map_col", ImmutableMap.of("key1", -123, "key2", 124, "key3", -125))
                        .set("long_map_col", ImmutableMap.of("key1", 123L, "key2", -124L, "key3", 125L))
                        .set("float_map_col", ImmutableMap.of("key1", -1.5F, "key2", 2.5F, "key3", -3.5F))
                        .set("double_map_col", ImmutableMap.of("key1", 2.5D, "key2", -3.5D, "key3", 4.5D))
                        .set("bytes_map_col", ImmutableMap.of("key1", ByteBuffer.wrap(new byte[] {1, 2, 3}), "key2", ByteBuffer.wrap(new byte[] {2, 3, 4}), "key3", ByteBuffer.wrap(new byte[] {3, 4, 5})))
                        .build())),
                schemaRegistryAwareProducer(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
        waitUntilTableExists(topic);
        QueryAssertions queryAssertions = new QueryAssertions(getQueryRunner());
        queryAssertions.query("SELECT " + keyColumnName + ", string_map_col, bool_map_col, int_map_col, long_map_col, float_map_col, double_map_col, bytes_map_col FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1'," +
                        "  CAST(MAP(ARRAY['key1', 'key2', 'key3'], ARRAY['val1', 'val2', 'val3']) AS MAP(VARCHAR, VARCHAR))," +
                        "  CAST(MAP(ARRAY['key1', 'key2', 'key3'], ARRAY[true, false, true]) AS MAP(VARCHAR, BOOLEAN))," +
                        "  CAST(MAP(ARRAY['key1', 'key2', 'key3'], ARRAY[-123, 124, -125]) AS MAP(VARCHAR, INTEGER))," +
                        "  CAST(MAP(ARRAY['key1', 'key2', 'key3'], ARRAY[123, -124, 125]) AS MAP(VARCHAR, BIGINT))," +
                        "  CAST(MAP(ARRAY['key1', 'key2', 'key3'], ARRAY[-1.5, 2.5, -3.5]) AS MAP(VARCHAR, REAL))," +
                        "  CAST(MAP(ARRAY['key1', 'key2', 'key3'], ARRAY[2.5, -3.5, 4.5]) AS MAP(VARCHAR, DOUBLE))," +
                        "  CAST(MAP(ARRAY['key1', 'key2', 'key3'], ARRAY[X'01 02 03', X'02 03 04', X'03 04 05']) AS MAP(VARCHAR, VARBINARY)))");

        assertUpdate("INSERT INTO " + tableName + " (" + keyColumnName + ", string_map_col, bool_map_col, int_map_col, long_map_col, float_map_col, double_map_col, bytes_map_col)" +
                        "  VALUES (BIGINT '2'," +
                        "  CAST(MAP(ARRAY['key11', 'key12', 'key13'], ARRAY['val11', NULL, 'val13']) AS MAP(VARCHAR, VARCHAR))," +
                        "  CAST(MAP(ARRAY['key11', 'key12', 'key13'], ARRAY[false, NULL, false]) AS MAP(VARCHAR, BOOLEAN))," +
                        "  CAST(MAP(ARRAY['key11', 'key12', 'key13'], ARRAY[-223, NULL, -225]) AS MAP(VARCHAR, INTEGER))," +
                        "  CAST(MAP(ARRAY['key11', 'key12', 'key13'], ARRAY[223, NULL, 225]) AS MAP(VARCHAR, BIGINT))," +
                        "  CAST(MAP(ARRAY['key11', 'key12', 'key13'], ARRAY[-2.5, NULL, -4.5]) AS MAP(VARCHAR, REAL))," +
                        "  CAST(MAP(ARRAY['key11', 'key12', 'key13'], ARRAY[3.5, NULL, 5.5]) AS MAP(VARCHAR, DOUBLE))," +
                        "  CAST(MAP(ARRAY['key11', 'key12', 'key13'], ARRAY[X'11 12 13', NULL, X'13 14 15']) AS MAP(VARCHAR, VARBINARY)))," +
                        "  (BIGINT '3'," +
                        "  CAST(MAP(ARRAY['key21', 'key22', 'key23'], ARRAY['val21', 'val22', 'val23']) AS MAP(VARCHAR, VARCHAR))," +
                        "  CAST(MAP(ARRAY['key21', 'key22', 'key23'], ARRAY[true, true, false]) AS MAP(VARCHAR, BOOLEAN))," +
                        "  CAST(MAP(ARRAY['key21', 'key22', 'key23'], ARRAY[-323, 324, -325]) AS MAP(VARCHAR, INTEGER))," +
                        "  CAST(MAP(ARRAY['key21', 'key22', 'key23'], ARRAY[323, -324, 325]) AS MAP(VARCHAR, BIGINT))," +
                        "  CAST(MAP(ARRAY['key21', 'key22', 'key23'], ARRAY[-3.5, 4.5, -5.5]) AS MAP(VARCHAR, REAL))," +
                        "  CAST(MAP(ARRAY['key21', 'key22', 'key23'], ARRAY[4.5, -5.5, 6.5]) AS MAP(VARCHAR, DOUBLE))," +
                        "  CAST(MAP(ARRAY['key21', 'key22', 'key23'], ARRAY[X'21 22 23', X'22 23 24', X'23 24 25']) AS MAP(VARCHAR, VARBINARY)))",
                2);
        queryAssertions.query("SELECT " + keyColumnName + ", string_map_col, bool_map_col, int_map_col, long_map_col, float_map_col, double_map_col, bytes_map_col FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1'," +
                        "  CAST(MAP(ARRAY['key1', 'key2', 'key3'], ARRAY['val1', 'val2', 'val3']) AS MAP(VARCHAR, VARCHAR))," +
                        "  CAST(MAP(ARRAY['key1', 'key2', 'key3'], ARRAY[true, false, true]) AS MAP(VARCHAR, BOOLEAN))," +
                        "  CAST(MAP(ARRAY['key1', 'key2', 'key3'], ARRAY[-123, 124, -125]) AS MAP(VARCHAR, INTEGER))," +
                        "  CAST(MAP(ARRAY['key1', 'key2', 'key3'], ARRAY[123, -124, 125]) AS MAP(VARCHAR, BIGINT))," +
                        "  CAST(MAP(ARRAY['key1', 'key2', 'key3'], ARRAY[-1.5, 2.5, -3.5]) AS MAP(VARCHAR, REAL))," +
                        "  CAST(MAP(ARRAY['key1', 'key2', 'key3'], ARRAY[2.5, -3.5, 4.5]) AS MAP(VARCHAR, DOUBLE))," +
                        "  CAST(MAP(ARRAY['key1', 'key2', 'key3'], ARRAY[X'01 02 03', X'02 03 04', X'03 04 05']) AS MAP(VARCHAR, VARBINARY)))," +
                        "  (BIGINT '2'," +
                        "  CAST(MAP(ARRAY['key11', 'key12', 'key13'], ARRAY['val11', NULL, 'val13']) AS MAP(VARCHAR, VARCHAR))," +
                        "  CAST(MAP(ARRAY['key11', 'key12', 'key13'], ARRAY[false, NULL, false]) AS MAP(VARCHAR, BOOLEAN))," +
                        "  CAST(MAP(ARRAY['key11', 'key12', 'key13'], ARRAY[-223, NULL, -225]) AS MAP(VARCHAR, INTEGER))," +
                        "  CAST(MAP(ARRAY['key11', 'key12', 'key13'], ARRAY[223, NULL, 225]) AS MAP(VARCHAR, BIGINT))," +
                        "  CAST(MAP(ARRAY['key11', 'key12', 'key13'], ARRAY[-2.5, NULL, -4.5]) AS MAP(VARCHAR, REAL))," +
                        "  CAST(MAP(ARRAY['key11', 'key12', 'key13'], ARRAY[3.5, NULL, 5.5]) AS MAP(VARCHAR, DOUBLE))," +
                        "  CAST(MAP(ARRAY['key11', 'key12', 'key13'], ARRAY[X'11 12 13', NULL, X'13 14 15']) AS MAP(VARCHAR, VARBINARY)))," +
                        "  (BIGINT '3'," +
                        "  CAST(MAP(ARRAY['key21', 'key22', 'key23'], ARRAY['val21', 'val22', 'val23']) AS MAP(VARCHAR, VARCHAR))," +
                        "  CAST(MAP(ARRAY['key21', 'key22', 'key23'], ARRAY[true, true, false]) AS MAP(VARCHAR, BOOLEAN))," +
                        "  CAST(MAP(ARRAY['key21', 'key22', 'key23'], ARRAY[-323, 324, -325]) AS MAP(VARCHAR, INTEGER))," +
                        "  CAST(MAP(ARRAY['key21', 'key22', 'key23'], ARRAY[323, -324, 325]) AS MAP(VARCHAR, BIGINT))," +
                        "  CAST(MAP(ARRAY['key21', 'key22', 'key23'], ARRAY[-3.5, 4.5, -5.5]) AS MAP(VARCHAR, REAL))," +
                        "  CAST(MAP(ARRAY['key21', 'key22', 'key23'], ARRAY[4.5, -5.5, 6.5]) AS MAP(VARCHAR, DOUBLE))," +
                        "  CAST(MAP(ARRAY['key21', 'key22', 'key23'], ARRAY[X'21 22 23', X'22 23 24', X'23 24 25']) AS MAP(VARCHAR, VARBINARY)))");
    }

    @Test
    public void testConfluentNestedPrimitiveRowMessage()
    {
        String topic = "nested-row-" + UUID.randomUUID();
        String tableName = toDoubleQuoted(topic);
        String keyColumnName = toDoubleQuoted(topic + "-key");

        Schema schema = SchemaBuilder.record("test").fields()
                .name("record_field").type().optional().record("sub_record").fields()
                .name("string_col").type().optional().stringType()
                .name("bool_col").type().optional().booleanType()
                .name("int_col").type().optional().intType()
                .name("long_col").type().optional().longType()
                .name("float_col").type().optional().floatType()
                .name("double_col").type().optional().doubleType()
                .name("bytes_col").type().optional().bytesType()
                .endRecord()
                .endRecord();

        Schema nestedSchema = extractBaseType(schema.getField("record_field").schema());

        testingKafka.sendMessages(Stream.of(new ProducerRecord<Long, GenericRecord>(topic, 1L,
                        new GenericRecordBuilder(schema)
                                .set("record_field", new GenericRecordBuilder(nestedSchema)
                                        .set("string_col", "string_1")
                                        .set("bool_col", true)
                                        .set("int_col", -123)
                                        .set("long_col", 123L)
                                        .set("float_col", -1.5F)
                                        .set("double_col", 2.5D)
                                        .set("bytes_col", ByteBuffer.wrap(new byte[] {1, 2, 3}))
                                        .build())
                                .build())),
                schemaRegistryAwareProducer(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
        waitUntilTableExists(topic);
        QueryAssertions queryAssertions = new QueryAssertions(getQueryRunner());
        queryAssertions.query("SELECT " + keyColumnName + ", record_field FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1'," +
                        "  CAST(ROW('string_1', true, -123, 123, -1.5, 2.5, X'01 02 03') AS " +
                        "  ROW(string_col VARCHAR," +
                        "  bool_col BOOLEAN," +
                        "  int_col INTEGER," +
                        "  long_col BIGINT," +
                        "  float_col REAL," +
                        "  double_col DOUBLE," +
                        "  bytes_col VARBINARY)))");

        assertUpdate("INSERT INTO " + tableName + " (" + keyColumnName + ", record_field)" +
                "  VALUES (BIGINT '2'," +
                "  ROW('string_2', false, -124, 124, -2.5, 3.5, X'02 03 04'))," +
                "  (BIGINT '3'," +
                "  ROW('string_3', true, -125, 125, -3.5, 4.5, X'03 04 05'))," +
                "  (BIGINT '4'," +
                "  CAST(NULL AS ROW(VARCHAR, BOOLEAN, INTEGER, BIGINT, REAL, DOUBLE, VARBINARY)))," +
                "  (BIGINT '5'," +
                "  CAST(ROW(NULL, NULL, NULL, NULL, NULL, NULL, NULL) AS ROW(VARCHAR, BOOLEAN, INTEGER, BIGINT, REAL, DOUBLE, VARBINARY)))",
                4);
        queryAssertions.query("SELECT " + keyColumnName + ", record_field FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1'," +
                        "  CAST(ROW('string_1', true, -123, 123, -1.5, 2.5, X'01 02 03') AS " +
                        "  ROW(string_col VARCHAR," +
                        "  bool_col BOOLEAN," +
                        "  int_col INTEGER," +
                        "  long_col BIGINT," +
                        "  float_col REAL," +
                        "  double_col DOUBLE," +
                        "  bytes_col VARBINARY)))," +
                        "  (BIGINT '2'," +
                        "  CAST(ROW('string_2', false, -124, 124, -2.5, 3.5, X'02 03 04') AS " +
                        "  ROW(string_col VARCHAR," +
                        "  bool_col BOOLEAN," +
                        "  int_col INTEGER," +
                        "  long_col BIGINT," +
                        "  float_col REAL," +
                        "  double_col DOUBLE," +
                        "  bytes_col VARBINARY)))," +
                        "  (BIGINT '3'," +
                        "  CAST(ROW('string_3', true, -125, 125, -3.5, 4.5, X'03 04 05') AS " +
                        "  ROW(string_col VARCHAR," +
                        "  bool_col BOOLEAN," +
                        "  int_col INTEGER," +
                        "  long_col BIGINT," +
                        "  float_col REAL," +
                        "  double_col DOUBLE," +
                        "  bytes_col VARBINARY)))," +
                        "  (BIGINT '4'," +
                        "  CAST(NULL AS" +
                        "  ROW(string_col VARCHAR," +
                        "  bool_col BOOLEAN," +
                        "  int_col INTEGER," +
                        "  long_col BIGINT," +
                        "  float_col REAL," +
                        "  double_col DOUBLE," +
                        "  bytes_col VARBINARY)))," +
                        "  (BIGINT '5'," +
                        "  CAST(ROW(NULL, NULL, NULL, NULL, NULL, NULL, NULL) AS" +
                        "  ROW(string_col VARCHAR," +
                        "  bool_col BOOLEAN," +
                        "  int_col INTEGER," +
                        "  long_col BIGINT," +
                        "  float_col REAL," +
                        "  double_col DOUBLE," +
                        "  bytes_col VARBINARY)))");
    }

    @Test
    public void testEmptyStructWithAddDummyStrategy()
    {
        /**
         * Avro allows records with no fields. Although it is not a recommended practice,
         * this can occur when converting from a protobuf.
         *
         * Test using the add dummy value strategy.
         * This will return the empty struct with a dummy field, so it does not throw and error:
         * RowType must have a non-empty field list.
         */

        String topic = "empty-struct-add-dummy-" + UUID.randomUUID();
        String tableName = toDoubleQuoted(topic);
        String keyColumnName = toDoubleQuoted(topic + "-key");

        Schema schema = SchemaBuilder.record("test").fields()
                .name("record_field").type().optional().record("sub_record").fields()
                .endRecord()
                .endRecord();

        Schema recordField = extractBaseType(schema.getField("record_field").schema());

        testingKafka.sendMessages(Stream.of(new ProducerRecord<Long, GenericRecord>(topic, 1L,
                        new GenericRecordBuilder(schema)
                                .set("record_field", new GenericRecordBuilder(recordField).build())
                                .build())),
                schemaRegistryAwareProducer(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
        Session addDummySession = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty("kafka", "empty_field_strategy", "ADD_DUMMY").build();
        waitUntilTableExists(addDummySession, topic);

        QueryAssertions queryAssertions = new QueryAssertions(getQueryRunner());
        queryAssertions.query(addDummySession, "SELECT " + keyColumnName + ", record_field FROM " + tableName)
                .assertThat()
                .matches("VALUES (CAST(1 AS BIGINT)," +
                        "  CAST(ROW(NULL) AS " +
                        "  ROW(dummy BOOLEAN)))");
        assertUpdate(addDummySession, "INSERT INTO " + tableName + " (" + keyColumnName + ", record_field)" +
                "  VALUES (CAST(2 AS BIGINT)," +
                "  ROW(NULL))," +
                "  (CAST(3 AS BIGINT)," +
                "  ROW(NULL))", 2);
        queryAssertions.query(addDummySession, "SELECT " + keyColumnName + ", record_field FROM " + tableName)
                .assertThat()
                .matches("VALUES (CAST(1 AS BIGINT)," +
                        "  CAST(ROW(NULL) AS " +
                        "  ROW(dummy BOOLEAN)))," +
                        "  (CAST(2 AS BIGINT)," +
                        "  CAST(ROW(NULL) AS " +
                        "  ROW(dummy BOOLEAN)))," +
                        "  (CAST(3 AS BIGINT)," +
                        "  CAST(ROW(NULL) AS " +
                        "  ROW(dummy BOOLEAN)))");
    }

    @Test
    public void testEmptyStructWithIgnoreStrategy()
    {
        /**
         * Avro allows records with no fields. Although it is not a recommended practice,
         * this can occur when converting from a protobuf.
         *
         * The default empty struct strategy is to ignore these fields.
         */

        String topic = "empty-struct-ignore-" + UUID.randomUUID();
        String tableName = toDoubleQuoted(topic);
        String keyColumnName = toDoubleQuoted(topic + "-key");

        Schema schema = SchemaBuilder.record("test").fields()
                .name("string_col").type().optional().stringType()
                .name("record_field").type().optional().record("sub_record").fields()
                .endRecord()
                .endRecord();

        testingKafka.sendMessages(Stream.of(new ProducerRecord<Long, GenericRecord>(topic, 1L,
                        new GenericRecordBuilder(schema)
                                .set("string_col", "string_1")
                                .build())),
                schemaRegistryAwareProducer(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
        waitUntilTableExists(topic);

        QueryAssertions queryAssertions = new QueryAssertions(getQueryRunner());
        queryAssertions.query("SELECT " + keyColumnName + ", string_col FROM " + tableName)
                .assertThat()
                .matches("VALUES (CAST(1 AS BIGINT)," +
                        "  VARCHAR 'string_1')");
        assertUpdate("INSERT INTO " + tableName + " (" + keyColumnName + ", string_col)" +
                "  VALUES (CAST(2 AS BIGINT)," +
                "  VARCHAR 'string_2')," +
                "  (CAST(3 AS BIGINT)," +
                "  VARCHAR 'string_3')", 2);
        queryAssertions.query("SELECT " + keyColumnName + ", string_col FROM " + tableName)
                .assertThat()
                .matches("VALUES (CAST(1 AS BIGINT)," +
                        "  VARCHAR 'string_1')," +
                        "  (CAST(2 AS BIGINT)," +
                        "  VARCHAR 'string_2')," +
                        "  (CAST(3 AS BIGINT)," +
                        "  VARCHAR 'string_3')");
    }

    @Test
    public void testBoundaryValues()
    {
        // Test Nan, -Infinity, +Infinity, -0, 0, min and max values for numeric data types.
        Schema schema = SchemaBuilder.record("test").fields()
                .name("int_col").type().optional().intType()
                .name("long_col").type().optional().longType()
                .name("float_col").type().optional().floatType()
                .name("double_col").type().optional().doubleType()
                .endRecord();
        String topic = "boundary-" + UUID.randomUUID();
        String tableName = toDoubleQuoted(topic);
        String keyColumnName = toDoubleQuoted(topic + "-key");

        testingKafka.sendMessages(Stream.of(new ProducerRecord<Long, GenericRecord>(topic, 1L, new GenericRecordBuilder(schema)
                        .set("int_col", -0)
                        .set("long_col", -0L)
                        .set("float_col", -0.0F)
                        .set("double_col", -0.0D)
                        .build())),
                schemaRegistryAwareProducer(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
        waitUntilTableExists(topic);
        QueryAssertions queryAssertions = new QueryAssertions(getQueryRunner());
        queryAssertions.query("SELECT " + keyColumnName + ", int_col, long_col, float_col, double_col FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1'," +
                        "  INTEGER '0'," +
                        "  BIGINT '0'," +
                        "  REAL '-0.0'," +
                        "  DOUBLE '-0.0')");
        assertUpdate("INSERT INTO " + tableName + " (" + keyColumnName + ", int_col, long_col, float_col, double_col)" +
                "  VALUES (BIGINT '2'," +
                "  INTEGER '" + Integer.MIN_VALUE + "'," +
                "  BIGINT '" + Long.MIN_VALUE + "'," +
                "  REAL '" + Float.MIN_VALUE + "'," +
                "  DOUBLE '" + Double.MIN_VALUE + "')," +
                "  (BIGINT '3'," +
                "  INTEGER '" + Integer.MAX_VALUE + "'," +
                "  BIGINT '" + Long.MAX_VALUE + "'," +
                "  REAL '" + Float.MAX_VALUE + "'," +
                "  DOUBLE '" + Double.MAX_VALUE + "')," +
                "  (BIGINT '4'," +
                "  INTEGER '" + Integer.MIN_VALUE + "'," +
                "  BIGINT '" + Long.MIN_VALUE + "'," +
                "  REAL '" + Float.NEGATIVE_INFINITY + "'," +
                "  DOUBLE '" + Double.NEGATIVE_INFINITY + "')," +
                "  (BIGINT '5'," +
                "  INTEGER '" + Integer.MAX_VALUE + "'," +
                "  BIGINT '" + Long.MAX_VALUE + "'," +
                "  REAL '" + Float.POSITIVE_INFINITY + "'," +
                "  DOUBLE '" + Double.POSITIVE_INFINITY + "')," +
                "  (BIGINT '6'," +
                "  INTEGER '" + Integer.MAX_VALUE + "'," +
                "  BIGINT '" + Long.MAX_VALUE + "'," +
                "  REAL '" + Float.NaN + "'," +
                "  DOUBLE '" + Double.NaN + "')",
                5);
        queryAssertions.query("SELECT " + keyColumnName + ", int_col, long_col, float_col, double_col FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1'," +
                        "  INTEGER '0'," +
                        "  BIGINT '0'," +
                        "  REAL '-0.0'," +
                        "  DOUBLE '-0.0')," +
                        "  (BIGINT '2'," +
                        "  INTEGER '" + Integer.MIN_VALUE + "'," +
                        "  BIGINT '" + Long.MIN_VALUE + "'," +
                        "  REAL '" + Float.MIN_VALUE + "'," +
                        "  DOUBLE '" + Double.MIN_VALUE + "')," +
                        "  (BIGINT '3'," +
                        "  INTEGER '" + Integer.MAX_VALUE + "'," +
                        "  BIGINT '" + Long.MAX_VALUE + "'," +
                        "  REAL '" + Float.MAX_VALUE + "'," +
                        "  DOUBLE '" + Double.MAX_VALUE + "')," +
                        "  (BIGINT '4'," +
                        "  INTEGER '" + Integer.MIN_VALUE + "'," +
                        "  BIGINT '" + Long.MIN_VALUE + "'," +
                        "  REAL '" + Float.NEGATIVE_INFINITY + "'," +
                        "  DOUBLE '" + Double.NEGATIVE_INFINITY + "')," +
                        "  (BIGINT '5'," +
                        "  INTEGER '" + Integer.MAX_VALUE + "'," +
                        "  BIGINT '" + Long.MAX_VALUE + "'," +
                        "  REAL '" + Float.POSITIVE_INFINITY + "'," +
                        "  DOUBLE '" + Double.POSITIVE_INFINITY + "')," +
                        "  (BIGINT '6'," +
                        "  INTEGER '" + Integer.MAX_VALUE + "'," +
                        "  BIGINT '" + Long.MAX_VALUE + "'," +
                        "  REAL '" + Float.NaN + "'," +
                        "  DOUBLE '" + Double.NaN + "')");
    }

    @Test
    public void testNestedArrayMessages()
    {
        String topic = "array-of-array-" + UUID.randomUUID();
        String tableName = toDoubleQuoted(topic);
        String keyColumnName = toDoubleQuoted(topic + "-key");

        Schema schema = SchemaBuilder.record("nested").fields()
                .name("array_array").type().optional().array().items().nullable().array().items().nullable().stringType()
                .endRecord();

        testingKafka.sendMessages(Stream.of(new ProducerRecord<Long, GenericRecord>(topic, 1L,
                        new GenericRecordBuilder(schema)
                                .set("array_array", Arrays.asList(Arrays.asList("string_1", null, "string_2")))
                                .build())),
                schemaRegistryAwareProducer(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
        waitUntilTableExists(topic);
        QueryAssertions queryAssertions = new QueryAssertions(getQueryRunner());
        queryAssertions.query("SELECT " + keyColumnName + ", array_array FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(ARRAY[ARRAY['string_1', NULL, 'string_2']] AS ARRAY(ARRAY(VARCHAR))))");
        assertUpdate("INSERT INTO " + tableName + " (" + keyColumnName + ", array_array)" +
                        "  VALUES (BIGINT '2', CAST(ARRAY[ARRAY['string_3', NULL, 'string_4']] AS ARRAY(ARRAY(VARCHAR))))," +
                        "  (BIGINT '3', CAST(ARRAY[ARRAY['string_5', NULL, 'string_6']] AS ARRAY(ARRAY(VARCHAR))))",
                2);
        queryAssertions.query("SELECT " + keyColumnName + ", array_array FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(ARRAY[ARRAY['string_1', NULL, 'string_2']] AS ARRAY(ARRAY(VARCHAR))))," +
                        "  (BIGINT '2', CAST(ARRAY[ARRAY['string_3', NULL, 'string_4']] AS ARRAY(ARRAY(VARCHAR))))," +
                        "  (BIGINT '3', CAST(ARRAY[ARRAY['string_5', NULL, 'string_6']] AS ARRAY(ARRAY(VARCHAR))))");

        topic = "array-of-map-" + UUID.randomUUID();
        tableName = toDoubleQuoted(topic);
        keyColumnName = toDoubleQuoted(topic + "-key");

        schema = SchemaBuilder.record("nested").fields()
                .name("array_map").type().optional().array().items().nullable().map().values().nullable().stringType()
                .endRecord();

        testingKafka.sendMessages(Stream.of(new ProducerRecord<Long, GenericRecord>(topic, 1L,
                        new GenericRecordBuilder(schema)
                                .set("array_map", Arrays.asList(ImmutableMap.of("key_1", "value_1", "key_2", "value_2")))
                                .build())),
                schemaRegistryAwareProducer(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
        waitUntilTableExists(topic);
        queryAssertions.query("SELECT " + keyColumnName + ", array_map FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(ARRAY[MAP(ARRAY['key_1', 'key_2'], ARRAY['value_1', 'value_2'])] AS ARRAY(MAP(VARCHAR, VARCHAR))))");
        assertUpdate("INSERT INTO " + tableName + " (" + keyColumnName + ", array_map)" +
                        "  VALUES (BIGINT '2', CAST(ARRAY[MAP(ARRAY['key_3', 'key_4'], ARRAY['value_3', 'value_4'])] AS ARRAY(MAP(VARCHAR, VARCHAR))))," +
                        "  (BIGINT '3', CAST(ARRAY[MAP(ARRAY['key_5', 'key_6'], ARRAY['value_5', 'value_6'])] AS ARRAY(MAP(VARCHAR, VARCHAR))))",
                2);
        queryAssertions.query("SELECT " + keyColumnName + ", array_map FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(ARRAY[MAP(ARRAY['key_1', 'key_2'], ARRAY['value_1', 'value_2'])] AS ARRAY(MAP(VARCHAR, VARCHAR))))," +
                        "  (BIGINT '2', CAST(ARRAY[MAP(ARRAY['key_3', 'key_4'], ARRAY['value_3', 'value_4'])] AS ARRAY(MAP(VARCHAR, VARCHAR))))," +
                        "  (BIGINT '3', CAST(ARRAY[MAP(ARRAY['key_5', 'key_6'], ARRAY['value_5', 'value_6'])] AS ARRAY(MAP(VARCHAR, VARCHAR))))");

        topic = "array-of-row-" + UUID.randomUUID();
        tableName = toDoubleQuoted(topic);
        keyColumnName = toDoubleQuoted(topic + "-key");

        schema = SchemaBuilder.record("nested").fields()
                .name("array_row").type().optional().array().items().nullable()
                .record("row").fields().name("string_col").type().optional().stringType().endRecord()
                .endRecord();
        Schema nestedSchema = extractBaseType(extractBaseType(schema.getField("array_row").schema()).getElementType());
        testingKafka.sendMessages(Stream.of(new ProducerRecord<Long, GenericRecord>(topic, 1L,
                        new GenericRecordBuilder(schema)
                                .set("array_row", Arrays.asList(new GenericRecordBuilder(nestedSchema)
                                        .set("string_col", "string_1")
                                        .build()))
                                .build())),
                schemaRegistryAwareProducer(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
        waitUntilTableExists(topic);
        queryAssertions.query("SELECT " + keyColumnName + ", array_row FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(ARRAY[ROW('string_1')] AS ARRAY(ROW(string_col VARCHAR))))");
        assertUpdate("INSERT INTO " + tableName + " (" + keyColumnName + ", array_row)" +
                        "  VALUES (BIGINT '2', CAST(ARRAY[ROW('string_2')] AS ARRAY(ROW(string_col VARCHAR))))," +
                        "  (BIGINT '3', CAST(ARRAY[ROW('string_3')] AS ARRAY(ROW(string_col VARCHAR))))",
                2);
        queryAssertions.query("SELECT " + keyColumnName + ", array_row FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(ARRAY[ROW('string_1')] AS ARRAY(ROW(string_col VARCHAR))))," +
                        "  (BIGINT '2', CAST(ARRAY[ROW('string_2')] AS ARRAY(ROW(string_col VARCHAR))))," +
                        "  (BIGINT '3', CAST(ARRAY[ROW('string_3')] AS ARRAY(ROW(string_col VARCHAR))))");
    }

    @Test
    public void testNestedMapMessages()
    {
        String topic = "map-of-array-" + UUID.randomUUID();
        String tableName = toDoubleQuoted(topic);
        String keyColumnName = toDoubleQuoted(topic + "-key");

        Schema schema = SchemaBuilder.record("nested").fields()
                .name("map_array").type().optional().map().values().nullable().array().items().nullable().stringType()
                .endRecord();

        testingKafka.sendMessages(Stream.of(new ProducerRecord<Long, GenericRecord>(topic, 1L,
                        new GenericRecordBuilder(schema)
                                .set("map_array", ImmutableMap.of("key_1", Arrays.asList("string_1", null, "string_2")))
                                .build())),
                schemaRegistryAwareProducer(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
        waitUntilTableExists(topic);
        QueryAssertions queryAssertions = new QueryAssertions(getQueryRunner());
        queryAssertions.query("SELECT " + keyColumnName + ", map_array FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(MAP(ARRAY['key_1'], ARRAY[ARRAY['string_1', NULL, 'string_2']]) AS MAP(VARCHAR, ARRAY(VARCHAR))))");
        assertUpdate("INSERT INTO " + tableName + " (" + keyColumnName + ", map_array)" +
                        "  VALUES (BIGINT '2', CAST(MAP(ARRAY['key_2'], ARRAY[ARRAY['string_3', NULL, 'string_4']]) AS MAP(VARCHAR, ARRAY(VARCHAR))))," +
                        "  (BIGINT '3', CAST(MAP(ARRAY['key_3'], ARRAY[ARRAY['string_5', NULL, 'string_6']]) AS MAP(VARCHAR, ARRAY(VARCHAR))))",
                2);
        queryAssertions.query("SELECT " + keyColumnName + ", map_array FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(MAP(ARRAY['key_1'], ARRAY[ARRAY['string_1', NULL, 'string_2']]) AS MAP(VARCHAR, ARRAY(VARCHAR))))," +
                        "  (BIGINT '2', CAST(MAP(ARRAY['key_2'], ARRAY[ARRAY['string_3', NULL, 'string_4']]) AS MAP(VARCHAR, ARRAY(VARCHAR))))," +
                        "  (BIGINT '3', CAST(MAP(ARRAY['key_3'], ARRAY[ARRAY['string_5', NULL, 'string_6']]) AS MAP(VARCHAR, ARRAY(VARCHAR))))");

        topic = "map-of-map-" + UUID.randomUUID();
        tableName = toDoubleQuoted(topic);
        keyColumnName = toDoubleQuoted(topic + "-key");

        schema = SchemaBuilder.record("nested").fields()
                .name("map_map").type().optional().map().values().nullable().map().values().nullable().stringType()
                .endRecord();

        testingKafka.sendMessages(Stream.of(new ProducerRecord<Long, GenericRecord>(topic, 1L,
                        new GenericRecordBuilder(schema)
                                .set("map_map", ImmutableMap.of("key_1", ImmutableMap.of("key_1", "value_1", "key_2", "value_2")))
                                .build())),
                schemaRegistryAwareProducer(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
        waitUntilTableExists(topic);
        queryAssertions.query("SELECT " + keyColumnName + ", map_map FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(MAP(ARRAY['key_1'], ARRAY[MAP(ARRAY['key_1', 'key_2'], ARRAY['value_1', 'value_2'])]) AS MAP(VARCHAR, MAP(VARCHAR, VARCHAR))))");
        assertUpdate("INSERT INTO " + tableName + " (" + keyColumnName + ", map_map)" +
                        "  VALUES (BIGINT '2', CAST(MAP(ARRAY['key_2'], ARRAY[MAP(ARRAY['key_1', 'key_2'], ARRAY['value_1', 'value_2'])]) AS MAP(VARCHAR, MAP(VARCHAR, VARCHAR))))," +
                        "  (BIGINT '3', CAST(MAP(ARRAY['key_3'], ARRAY[MAP(ARRAY['key_1', 'key_2'], ARRAY['value_1', 'value_2'])]) AS MAP(VARCHAR, MAP(VARCHAR, VARCHAR))))",
                2);
        queryAssertions.query("SELECT " + keyColumnName + ", map_map FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(MAP(ARRAY['key_1'], ARRAY[MAP(ARRAY['key_1', 'key_2'], ARRAY['value_1', 'value_2'])]) AS MAP(VARCHAR, MAP(VARCHAR, VARCHAR))))," +
                        "  (BIGINT '2', CAST(MAP(ARRAY['key_2'], ARRAY[MAP(ARRAY['key_1', 'key_2'], ARRAY['value_1', 'value_2'])]) AS MAP(VARCHAR, MAP(VARCHAR, VARCHAR))))," +
                        "  (BIGINT '3', CAST(MAP(ARRAY['key_3'], ARRAY[MAP(ARRAY['key_1', 'key_2'], ARRAY['value_1', 'value_2'])]) AS MAP(VARCHAR, MAP(VARCHAR, VARCHAR))))");

        topic = "map-of-row-" + UUID.randomUUID();
        tableName = toDoubleQuoted(topic);
        keyColumnName = toDoubleQuoted(topic + "-key");

        schema = SchemaBuilder.record("nested").fields()
                .name("map_row").type().optional().map().values().nullable()
                .record("row").fields().name("string_col").type().optional().stringType().endRecord()
                .endRecord();
        Schema nestedSchema = extractBaseType(extractBaseType(schema.getField("map_row").schema()).getValueType());
        testingKafka.sendMessages(Stream.of(new ProducerRecord<Long, GenericRecord>(topic, 1L,
                        new GenericRecordBuilder(schema)
                                .set("map_row", ImmutableMap.of("key_1", new GenericRecordBuilder(nestedSchema)
                                        .set("string_col", "string_1")
                                        .build()))
                                .build())),
                schemaRegistryAwareProducer(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
        waitUntilTableExists(topic);
        queryAssertions.query("SELECT " + keyColumnName + ", map_row FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(MAP(ARRAY['key_1'], ARRAY[ROW('string_1')]) AS MAP(VARCHAR, ROW(string_col VARCHAR))))");
        assertUpdate("INSERT INTO " + tableName + " (" + keyColumnName + ", map_row)" +
                        "  VALUES (BIGINT '2', CAST(MAP(ARRAY['key_2'], ARRAY[ROW('string_1')]) AS MAP(VARCHAR, ROW(string_col VARCHAR))))," +
                        "  (BIGINT '3', CAST(MAP(ARRAY['key_3'], ARRAY[ROW('string_1')]) AS MAP(VARCHAR, ROW(string_col VARCHAR))))",
                2);
        queryAssertions.query("SELECT " + keyColumnName + ", map_row FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(MAP(ARRAY['key_1'], ARRAY[ROW('string_1')]) AS MAP(VARCHAR, ROW(string_col VARCHAR))))," +
                        "  (BIGINT '2', CAST(MAP(ARRAY['key_2'], ARRAY[ROW('string_1')]) AS MAP(VARCHAR, ROW(string_col VARCHAR))))," +
                        "  (BIGINT '3', CAST(MAP(ARRAY['key_3'], ARRAY[ROW('string_1')]) AS MAP(VARCHAR, ROW(string_col VARCHAR))))");
    }

    @Test
    public void testNestedRowMessages()
    {
        String topic = "row-of-array-" + UUID.randomUUID();
        String tableName = toDoubleQuoted(topic);
        String keyColumnName = toDoubleQuoted(topic + "-key");

        Schema schema = SchemaBuilder.record("nested").fields()
                .name("row_array").type().optional()
                .record("row").fields().name("array_field").type()
                .optional().array().items().nullable().stringType().endRecord()
                .endRecord();

        Schema nestedSchema = extractBaseType(schema.getField("row_array").schema());
        testingKafka.sendMessages(Stream.of(new ProducerRecord<Long, GenericRecord>(topic, 1L,
                        new GenericRecordBuilder(schema)
                                .set("row_array", new GenericRecordBuilder(nestedSchema)
                                        .set("array_field", Arrays.asList("string_1", null, "string_2"))
                                        .build())
                                .build())),
                schemaRegistryAwareProducer(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
        waitUntilTableExists(topic);
        QueryAssertions queryAssertions = new QueryAssertions(getQueryRunner());
        queryAssertions.query("SELECT " + keyColumnName + ", row_array FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(ROW(ARRAY['string_1', NULL, 'string_2']) AS ROW(array_field ARRAY(VARCHAR))))");
        assertUpdate("INSERT INTO " + tableName + " (" + keyColumnName + ", row_array)" +
                        "  VALUES (BIGINT '2', CAST(ROW(ARRAY['string_3', NULL, 'string_4']) AS ROW(array_field ARRAY(VARCHAR))))," +
                        "  (BIGINT '3', CAST(ROW(ARRAY['string_5', NULL, 'string_6']) AS ROW(array_field ARRAY(VARCHAR))))",
                2);
        queryAssertions.query("SELECT " + keyColumnName + ", row_array FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(ROW(ARRAY['string_1', NULL, 'string_2']) AS ROW(array_field ARRAY(VARCHAR))))," +
                        "  (BIGINT '2', CAST(ROW(ARRAY['string_3', NULL, 'string_4']) AS ROW(array_field ARRAY(VARCHAR))))," +
                        "  (BIGINT '3', CAST(ROW(ARRAY['string_5', NULL, 'string_6']) AS ROW(array_field ARRAY(VARCHAR))))");

        topic = "row-of-map-" + UUID.randomUUID();
        tableName = toDoubleQuoted(topic);
        keyColumnName = toDoubleQuoted(topic + "-key");

        schema = SchemaBuilder.record("nested").fields()
                .name("row_map").type().optional().record("row").fields()
                .name("map_field").type().nullable().map().values().stringType().noDefault().endRecord()
                .endRecord();

        nestedSchema = extractBaseType(schema.getField("row_map").schema());
        testingKafka.sendMessages(Stream.of(new ProducerRecord<Long, GenericRecord>(topic, 1L,
                        new GenericRecordBuilder(schema)
                                .set("row_map", new GenericRecordBuilder(nestedSchema)
                                        .set("map_field", ImmutableMap.of("key_1", "value_1", "key_2", "value_2"))
                                        .build())
                                .build())),
                schemaRegistryAwareProducer(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
        waitUntilTableExists(topic);
        queryAssertions.query("SELECT " + keyColumnName + ", row_map FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(ROW(MAP(ARRAY['key_1', 'key_2'], ARRAY['value_1', 'value_2'])) AS ROW(map_field MAP(VARCHAR, VARCHAR))))");
        assertUpdate("INSERT INTO " + tableName + " (" + keyColumnName + ", row_map)" +
                        "  VALUES (BIGINT '2', CAST(ROW(MAP(ARRAY['key_3', 'key_4'], ARRAY['value_3', 'value_4'])) AS ROW(map_field MAP(VARCHAR, VARCHAR))))," +
                        "  (BIGINT '3', CAST(ROW(MAP(ARRAY['key_5', 'key_6'], ARRAY['value_5', 'value_6'])) AS ROW(map_field MAP(VARCHAR, VARCHAR))))",
                2);
        queryAssertions.query("SELECT " + keyColumnName + ", row_map FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(ROW(MAP(ARRAY['key_1', 'key_2'], ARRAY['value_1', 'value_2'])) AS ROW(map_field MAP(VARCHAR, VARCHAR))))," +
                        "  (BIGINT '2', CAST(ROW(MAP(ARRAY['key_3', 'key_4'], ARRAY['value_3', 'value_4'])) AS ROW(map_field MAP(VARCHAR, VARCHAR))))," +
                        "  (BIGINT '3', CAST(ROW(MAP(ARRAY['key_5', 'key_6'], ARRAY['value_5', 'value_6'])) AS ROW(map_field MAP(VARCHAR, VARCHAR))))");

        // Row of row is tested in testConfluentNestedPrimitiveRowMessage
        // Test deeply nested values:
        topic = "array-map-row-array" + UUID.randomUUID();
        tableName = toDoubleQuoted(topic);
        keyColumnName = toDoubleQuoted(topic + "-key");

        schema = SchemaBuilder.record("nested").fields()
                .name("array_map_row_array").type().optional().array().items().nullable()
                .map().values().nullable()
                .record("row").fields().name("array_field").type().optional().array().items().nullable().stringType().endRecord()
                .endRecord();
        nestedSchema = extractBaseType(extractBaseType(extractBaseType(schema.getField("array_map_row_array").schema()).getElementType()).getValueType());
        testingKafka.sendMessages(Stream.of(new ProducerRecord<Long, GenericRecord>(topic, 1L,
                        new GenericRecordBuilder(schema)
                                .set("array_map_row_array", Arrays.asList(ImmutableMap.of("key_1", new GenericRecordBuilder(nestedSchema)
                                        .set("array_field", Arrays.asList("string_1"))
                                        .build())))
                                .build())),
                schemaRegistryAwareProducer(testingKafka)
                        .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                        .build());
        waitUntilTableExists(topic);
        queryAssertions.query("SELECT " + keyColumnName + ", array_map_row_array FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(ARRAY[MAP(ARRAY['key_1'], ARRAY[ROW(ARRAY['string_1'])])] AS ARRAY(MAP(VARCHAR, ROW(array_field ARRAY(VARCHAR))))))");
        assertUpdate("INSERT INTO " + tableName + " (" + keyColumnName + ", array_map_row_array)" +
                        "  VALUES (BIGINT '2', CAST(ARRAY[MAP(ARRAY['key_2'], ARRAY[ROW(ARRAY['string_2'])])] AS ARRAY(MAP(VARCHAR, ROW(array_field ARRAY(VARCHAR))))))," +
                        "  (BIGINT '3', CAST(ARRAY[MAP(ARRAY['key_3'], ARRAY[ROW(ARRAY['string_3'])])] AS ARRAY(MAP(VARCHAR, ROW(array_field ARRAY(VARCHAR))))))",
                2);
        queryAssertions.query("SELECT " + keyColumnName + ", array_map_row_array FROM " + tableName)
                .assertThat()
                .matches("VALUES (BIGINT '1', CAST(ARRAY[MAP(ARRAY['key_1'], ARRAY[ROW(ARRAY['string_1'])])] AS ARRAY(MAP(VARCHAR, ROW(array_field ARRAY(VARCHAR))))))," +
                        "  (BIGINT '2', CAST(ARRAY[MAP(ARRAY['key_2'], ARRAY[ROW(ARRAY['string_2'])])] AS ARRAY(MAP(VARCHAR, ROW(array_field ARRAY(VARCHAR))))))," +
                        "  (BIGINT '3', CAST(ARRAY[MAP(ARRAY['key_3'], ARRAY[ROW(ARRAY['string_3'])])] AS ARRAY(MAP(VARCHAR, ROW(array_field ARRAY(VARCHAR))))))");
    }
}
