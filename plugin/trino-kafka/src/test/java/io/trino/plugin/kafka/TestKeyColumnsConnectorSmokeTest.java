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

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.Type;
import io.trino.sql.query.QueryAssertions;
import io.trino.testng.services.ManageTestResources;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.stream.Stream;

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
import static java.lang.Math.toIntExact;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

// This test exercises the KafkaRowDecoder and KafkaSerializerRowEncoder
public class TestKeyColumnsConnectorSmokeTest
        extends BaseKafkaWithConfluentLatestSmokeTest
{
    @ManageTestResources.Suppress(because = "This class is stateless, it just invokes one method.")
    private static final LongSerializer LONG_SERIALIZER = new LongSerializer();
    @ManageTestResources.Suppress(because = "This class is stateless, it just invokes one method.")
    private static final IntegerSerializer INTEGER_SERIALIZER = new IntegerSerializer();

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
    public void testSupportedKeyColumnTypes()
    {
        testKey(VARCHAR, StringSerializer.class, "string_1", "string_2");
        testKey(VARBINARY, SqlVarbinarySerializer.class, new SqlVarbinary(new byte[] {1, 2, 3}), new SqlVarbinary(new byte[] {2, 3, 4}));
        testKey(DATE, SqlDateSerializer.class, new SqlDate(50), new SqlDate(51));
        testKey(TIMESTAMP_MICROS, SqlTimeStampMicrosSerializer.class, SqlTimestamp.newInstance(TIMESTAMP_MICROS.getPrecision(), 1111L, 0), SqlTimestamp.newInstance(TIMESTAMP_MICROS.getPrecision(), 1112L, 0));
        // Time millis avro logical type is for underlying integer type only so result needs to be converted in the column decoder: https://avro.apache.org/docs/1.11.0/spec.html#Time+%28millisecond+precision%29
        testKey(TIME_MILLIS, SqlTimeMillisSerializer.class, SqlTime.newInstance(TIME_MILLIS.getPrecision(), 1111L * PICOSECONDS_PER_MILLISECOND), SqlTime.newInstance(TIME_MILLIS.getPrecision(), 1112L * PICOSECONDS_PER_MILLISECOND));
        testKey(TIMESTAMP_MILLIS, SqlTimeStampMillisSerializer.class, SqlTimestamp.fromMillis(TIMESTAMP_MILLIS.getPrecision(), 1111L), SqlTimestamp.fromMillis(TIMESTAMP_MILLIS.getPrecision(), 1112L));
        testKey(TIME_MICROS, SqlTimeMicrosSerializer.class, SqlTime.newInstance(TIME_MICROS.getPrecision(), 1111L * PICOSECONDS_PER_MICROSECOND), SqlTime.newInstance(TIME_MICROS.getPrecision(), 1112L * PICOSECONDS_PER_MICROSECOND));
        testKey(BIGINT, LongSerializer.class, 1L, 2L);
        testKey(INTEGER, IntegerSerializer.class, 1, 2);
        testKey(REAL, FloatSerializer.class, -1.5F, 2.5F);
        testKey(DOUBLE, DoubleSerializer.class, 1.5D, 2.5D);
        testKey(BOOLEAN, BooleanSerializer.class, false, true);
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

    public static class SqlTimeStampMillisSerializer
            implements Serializer<SqlTimestamp>
    {
        @Override
        public byte[] serialize(String topic, SqlTimestamp data)
        {
            if (data == null) {
                return null;
            }
            return LONG_SERIALIZER.serialize(topic, data.getMillis());
        }
    }

    public static class SqlTimeStampMicrosSerializer
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

    public static class SqlTimeMillisSerializer
            implements Serializer<SqlTime>
    {
        @Override
        public byte[] serialize(String topic, SqlTime data)
        {
            if (data == null) {
                return null;
            }
            return INTEGER_SERIALIZER.serialize(topic, toIntExact(data.getPicos() / PICOSECONDS_PER_MILLISECOND));
        }
    }

    public static class SqlTimeMicrosSerializer
            implements Serializer<SqlTime>
    {
        @Override
        public byte[] serialize(String topic, SqlTime data)
        {
            if (data == null) {
                return null;
            }
            return LONG_SERIALIZER.serialize(topic, data.getPicos() / PICOSECONDS_PER_MICROSECOND);
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
        String tableName = toDoubleQuoted(topic + "&key-column=key_col:" + keyType.getDisplayName());
        Schema schema = SchemaBuilder.record("test").fields()
                .name("string_optional_field").type().optional().stringType()
                .endRecord();
        getTestingKafka().sendMessages(Stream.of(new ProducerRecord<T, GenericRecord>(topic, firstValue, new GenericRecordBuilder(schema).build())), schemaRegistryAwareProducer(getTestingKafka())
                .put(KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getName())
                .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                .buildOrThrow());
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

    private <T> void testConfluentPrimitiveKey(Type keyType, T firstValue, T secondValue, T thirdValue)
    {
        String topic = "primitive-key-" + getDisplayNameForTopic(keyType) + "-" + UUID.randomUUID();
        String tableName = toDoubleQuoted(topic);
        Schema schema = SchemaBuilder.record("test").fields()
                .name("string_optional_field").type().optional().stringType()
                .endRecord();
        getTestingKafka().sendMessages(Stream.of(new ProducerRecord<T, GenericRecord>(topic, firstValue, new GenericRecordBuilder(schema).build())), schemaRegistryAwareProducer(getTestingKafka())
                .put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                .put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                .buildOrThrow());
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
}
