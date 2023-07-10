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
package io.trino.hive.formats.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import io.trino.filesystem.TrinoInputFile;
import io.trino.spi.Page;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import static io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.DATE_SCHEMA;
import static io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.TIMESTAMP_MICROS_SCHEMA;
import static io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.TIMESTAMP_MILLIS_SCHEMA;
import static io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.TIME_MICROS_SCHEMA;
import static io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.TIME_MILLIS_SCHEMA;
import static io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.UUID_SCHEMA;
import static io.trino.hive.formats.avro.TestAvroPageDataReaderWithoutTypeManager.createWrittenFileWithData;
import static io.trino.hive.formats.avro.TestAvroPageDataReaderWithoutTypeManager.createWrittenFileWithSchema;
import static io.trino.hive.formats.avro.TestLongFromBigEndian.padBigEndianCorrectly;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestAvroPageDataReaderWithAvroNativeTypeManagement
{
    private static final Schema DECIMAL_SMALL_BYTES_SCHEMA;
    private static final int SMALL_FIXED_SIZE = 8;
    private static final int LARGE_FIXED_SIZE = 9;
    private static final Schema DECIMAL_SMALL_FIXED_SCHEMA;
    private static final Schema DECIMAL_LARGE_BYTES_SCHEMA;
    private static final Schema DECIMAL_LARGE_FIXED_SCHEMA;
    private static final Date testTime = new Date(780681600000L);
    private static final Type SMALL_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_SHORT_PRECISION - 1, 2);
    private static final Type LARGE_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_SHORT_PRECISION + 1, 2);

    static {
        LogicalTypes.Decimal small = LogicalTypes.decimal(MAX_SHORT_PRECISION - 1, 2);
        LogicalTypes.Decimal large = LogicalTypes.decimal(MAX_SHORT_PRECISION + 1, 2);
        DECIMAL_SMALL_BYTES_SCHEMA = Schema.create(Schema.Type.BYTES);
        small.addToSchema(DECIMAL_SMALL_BYTES_SCHEMA);
        DECIMAL_SMALL_FIXED_SCHEMA = Schema.createFixed("smallDecimal", "myFixed", "namespace", SMALL_FIXED_SIZE);
        small.addToSchema(DECIMAL_SMALL_FIXED_SCHEMA);
        DECIMAL_LARGE_BYTES_SCHEMA = Schema.create(Schema.Type.BYTES);
        large.addToSchema(DECIMAL_LARGE_BYTES_SCHEMA);
        DECIMAL_LARGE_FIXED_SCHEMA = Schema.createFixed("largeDecimal", "myFixed", "namespace", (int) ((MAX_SHORT_PRECISION + 2) * Math.log(10) / Math.log(2) / 8) + 1);
        large.addToSchema(DECIMAL_LARGE_FIXED_SCHEMA);
    }

    @Test
    public void testTypesSimple()
            throws IOException, AvroTypeException
    {
        Schema schema = SchemaBuilder.builder()
                .record("allSupported")
                .fields()
                .name("timestampMillis")
                .type(TIMESTAMP_MILLIS_SCHEMA).noDefault()
                .name("timestampMicros")
                .type(TIMESTAMP_MICROS_SCHEMA).noDefault()
                .name("smallBytesDecimal")
                .type(DECIMAL_SMALL_BYTES_SCHEMA).noDefault()
                .name("smallFixedDecimal")
                .type(DECIMAL_SMALL_FIXED_SCHEMA).noDefault()
                .name("largeBytesDecimal")
                .type(DECIMAL_LARGE_BYTES_SCHEMA).noDefault()
                .name("largeFixedDecimal")
                .type(DECIMAL_LARGE_FIXED_SCHEMA).noDefault()
                .name("date")
                .type(DATE_SCHEMA).noDefault()
                .name("timeMillis")
                .type(TIME_MILLIS_SCHEMA).noDefault()
                .name("timeMicros")
                .type(TIME_MICROS_SCHEMA).noDefault()
                .name("id")
                .type(UUID_SCHEMA).noDefault()
                .endRecord();

        GenericData.Fixed genericSmallFixedDecimal = new GenericData.Fixed(DECIMAL_SMALL_FIXED_SCHEMA);
        genericSmallFixedDecimal.bytes(padBigEndianCorrectly(78068160000000L, SMALL_FIXED_SIZE));
        GenericData.Fixed genericLargeFixedDecimal = new GenericData.Fixed(DECIMAL_LARGE_FIXED_SCHEMA);
        genericLargeFixedDecimal.bytes(padBigEndianCorrectly(78068160000000L, LARGE_FIXED_SIZE));
        UUID id = UUID.randomUUID();

        GenericData.Record myRecord = new GenericData.Record(schema);
        myRecord.put("timestampMillis", testTime.getTime());
        myRecord.put("timestampMicros", testTime.getTime() * 1000);
        myRecord.put("smallBytesDecimal", ByteBuffer.wrap(Longs.toByteArray(78068160000000L)));
        myRecord.put("smallFixedDecimal", genericSmallFixedDecimal);
        myRecord.put("largeBytesDecimal", ByteBuffer.wrap(Int128.fromBigEndian(Longs.toByteArray(78068160000000L)).toBigEndianBytes()));
        myRecord.put("largeFixedDecimal", genericLargeFixedDecimal);
        myRecord.put("date", 9035);
        myRecord.put("timeMillis", 39_600_000);
        myRecord.put("timeMicros", 39_600_000_000L);
        myRecord.put("id", id.toString());

        TrinoInputFile input = createWrittenFileWithData(schema, ImmutableList.of(myRecord));
        try (AvroFileReader avroFileReader = new AvroFileReader(input, schema, new NativeLogicalTypesAvroTypeManager())) {
            int totalRecords = 0;
            while (avroFileReader.hasNext()) {
                Page p = avroFileReader.next();
                // Timestamps equal
                SqlTimestamp milliTimestamp = (SqlTimestamp) TimestampType.TIMESTAMP_MILLIS.getObjectValue(null, p.getBlock(0), 0);
                SqlTimestamp microTimestamp = (SqlTimestamp) TimestampType.TIMESTAMP_MICROS.getObjectValue(null, p.getBlock(1), 0);
                assertThat(milliTimestamp).isEqualTo(microTimestamp.roundTo(3));
                assertThat(microTimestamp.getEpochMicros()).isEqualTo(testTime.getTime() * 1000);

                // Decimals Equal
                SqlDecimal smallBytesDecimal = (SqlDecimal) SMALL_DECIMAL_TYPE.getObjectValue(null, p.getBlock(2), 0);
                SqlDecimal smallFixedDecimal = (SqlDecimal) SMALL_DECIMAL_TYPE.getObjectValue(null, p.getBlock(3), 0);
                SqlDecimal largeBytesDecimal = (SqlDecimal) LARGE_DECIMAL_TYPE.getObjectValue(null, p.getBlock(4), 0);
                SqlDecimal largeFixedDecimal = (SqlDecimal) LARGE_DECIMAL_TYPE.getObjectValue(null, p.getBlock(5), 0);

                assertThat(smallBytesDecimal).isEqualTo(smallFixedDecimal);
                assertThat(largeBytesDecimal).isEqualTo(largeFixedDecimal);
                assertThat(smallBytesDecimal.toBigDecimal()).isEqualTo(largeBytesDecimal.toBigDecimal());
                assertThat(smallBytesDecimal.getUnscaledValue()).isEqualTo(new BigInteger(Longs.toByteArray(78068160000000L)));

                // Get date
                SqlDate date = (SqlDate) DateType.DATE.getObjectValue(null, p.getBlock(6), 0);
                assertThat(date.getDays()).isEqualTo(9035);

                // Time equals
                SqlTime timeMillis = (SqlTime) TimeType.TIME_MILLIS.getObjectValue(null, p.getBlock(7), 0);
                SqlTime timeMicros = (SqlTime) TimeType.TIME_MICROS.getObjectValue(null, p.getBlock(8), 0);
                assertThat(timeMillis).isEqualTo(timeMicros.roundTo(3));
                assertThat(timeMillis.getPicos()).isEqualTo(timeMicros.getPicos()).isEqualTo(39_600_000_000L * 1_000_000L);

                //UUID
                assertThat(id.toString()).isEqualTo(UuidType.UUID.getObjectValue(null, p.getBlock(9), 0));

                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(1);
        }
    }

    @Test
    public void testWithDefaults()
            throws IOException, AvroTypeException
    {
        String id = UUID.randomUUID().toString();
        Schema schema = SchemaBuilder.builder()
                .record("testDefaults")
                .fields()
                .name("timestampMillis")
                .type(TIMESTAMP_MILLIS_SCHEMA).withDefault(testTime.getTime())
                .name("smallBytesDecimal")
                .type(DECIMAL_SMALL_BYTES_SCHEMA).withDefault(ByteBuffer.wrap(Longs.toByteArray(testTime.getTime())))
                .name("timeMicros")
                .type(TIME_MICROS_SCHEMA).withDefault(39_600_000_000L)
                .name("id")
                .type(UUID_SCHEMA).withDefault(id)
                .endRecord();
        Schema writeSchema = SchemaBuilder.builder()
                .record("testDefaults")
                .fields()
                .name("notRead").type().optional().booleanType()
                .endRecord();

        TrinoInputFile input = createWrittenFileWithSchema(10, writeSchema);
        try (AvroFileReader avroFileReader = new AvroFileReader(input, schema, new NativeLogicalTypesAvroTypeManager())) {
            int totalRecords = 0;
            while (avroFileReader.hasNext()) {
                Page p = avroFileReader.next();
                for (int i = 0; i < p.getPositionCount(); i++) {
                    // millis timestamp const
                    SqlTimestamp milliTimestamp = (SqlTimestamp) TimestampType.TIMESTAMP_MILLIS.getObjectValue(null, p.getBlock(0), i);
                    assertThat(milliTimestamp.getEpochMicros()).isEqualTo(testTime.getTime() * 1000);

                    // decimal bytes const
                    SqlDecimal smallBytesDecimal = (SqlDecimal) SMALL_DECIMAL_TYPE.getObjectValue(null, p.getBlock(1), i);
                    assertThat(smallBytesDecimal.getUnscaledValue()).isEqualTo(new BigInteger(Longs.toByteArray(testTime.getTime())));

                    // time micros const
                    SqlTime timeMicros = (SqlTime) TimeType.TIME_MICROS.getObjectValue(null, p.getBlock(2), i);
                    assertThat(timeMicros.getPicos()).isEqualTo(39_600_000_000L * 1_000_000L);

                    //UUID const assert
                    assertThat(id).isEqualTo(UuidType.UUID.getObjectValue(null, p.getBlock(3), i));
                }
                totalRecords += p.getPositionCount();
            }
            assertThat(totalRecords).isEqualTo(10);
        }
    }
}
