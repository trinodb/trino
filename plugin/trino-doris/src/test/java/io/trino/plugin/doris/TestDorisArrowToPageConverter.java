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
package io.trino.plugin.doris;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.TimestampType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDorisArrowToPageConverter
{
    private final DorisArrowToPageConverter converter = new DorisArrowToPageConverter();

    @Test
    void testConvertPrimitiveAndCharacterColumns()
    {
        List<DorisColumnHandle> columns = List.of(
                new DorisColumnHandle("flag", BOOLEAN, 0),
                new DorisColumnHandle("tiny", TINYINT, 1),
                new DorisColumnHandle("small", SMALLINT, 2),
                new DorisColumnHandle("id", BIGINT, 3),
                new DorisColumnHandle("ratio", REAL, 4),
                new DorisColumnHandle("name", createVarcharType(20), 5),
                new DorisColumnHandle("code", createCharType(3), 6));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                BitVector flag = new BitVector("flag", allocator);
                TinyIntVector tiny = new TinyIntVector("tiny", allocator);
                SmallIntVector small = new SmallIntVector("small", allocator);
                BigIntVector id = new BigIntVector("id", allocator);
                Float4Vector ratio = new Float4Vector("ratio", allocator);
                VarCharVector name = new VarCharVector("name", allocator);
                VarCharVector code = new VarCharVector("code", allocator);
                VectorSchemaRoot root = new VectorSchemaRoot(List.of(flag, tiny, small, id, ratio, name, code))) {
            flag.setInitialCapacity(2);
            flag.allocateNew();
            flag.setSafe(0, 1);
            flag.setNull(1);
            flag.setValueCount(2);

            tiny.setInitialCapacity(2);
            tiny.allocateNew();
            tiny.setSafe(0, 7);
            tiny.setNull(1);
            tiny.setValueCount(2);

            small.setInitialCapacity(2);
            small.allocateNew();
            small.setSafe(0, 123);
            small.setNull(1);
            small.setValueCount(2);

            id.setInitialCapacity(2);
            id.allocateNew();
            id.setSafe(0, 456789L);
            id.setNull(1);
            id.setValueCount(2);

            ratio.setInitialCapacity(2);
            ratio.allocateNew();
            ratio.setSafe(0, 1.5f);
            ratio.setNull(1);
            ratio.setValueCount(2);

            name.setInitialCapacity(2);
            name.allocateNew();
            name.setSafe(0, "alpha".getBytes(UTF_8));
            name.setNull(1);
            name.setValueCount(2);

            code.setInitialCapacity(2);
            code.allocateNew();
            code.setSafe(0, "xy   ".getBytes(UTF_8));
            code.setNull(1);
            code.setValueCount(2);

            root.setRowCount(2);

            Page page = convert(columns, root);

            assertThat(BOOLEAN.getObjectValue(page.getBlock(0), 0)).isEqualTo(Boolean.TRUE);
            assertThat(page.getBlock(0).isNull(1)).isTrue();
            assertThat(TINYINT.getLong(page.getBlock(1), 0)).isEqualTo(7L);
            assertThat(SMALLINT.getLong(page.getBlock(2), 0)).isEqualTo(123L);
            assertThat(BIGINT.getLong(page.getBlock(3), 0)).isEqualTo(456789L);
            assertThat(REAL.getFloat(page.getBlock(4), 0)).isEqualTo(1.5f);
            assertThat(createVarcharType(20).getObjectValue(page.getBlock(5), 0)).isEqualTo("alpha");
            assertThat(createCharType(3).getObjectValue(page.getBlock(6), 0)).isEqualTo("xy ");
        }
    }

    @Test
    void testConvertBitVectorForIntegralColumns()
    {
        List<DorisColumnHandle> columns = List.of(
                new DorisColumnHandle("flag_as_tinyint", TINYINT, 0),
                new DorisColumnHandle("flag_as_bigint", BIGINT, 1));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                BitVector tinyintFlag = new BitVector("flag_as_tinyint", allocator);
                BitVector bigintFlag = new BitVector("flag_as_bigint", allocator);
                VectorSchemaRoot root = new VectorSchemaRoot(List.of(tinyintFlag, bigintFlag))) {
            tinyintFlag.setInitialCapacity(2);
            tinyintFlag.allocateNew();
            tinyintFlag.setSafe(0, 1);
            tinyintFlag.setNull(1);
            tinyintFlag.setValueCount(2);

            bigintFlag.setInitialCapacity(2);
            bigintFlag.allocateNew();
            bigintFlag.setSafe(0, 0);
            bigintFlag.setNull(1);
            bigintFlag.setValueCount(2);

            root.setRowCount(2);

            Page page = convert(columns, root);

            assertThat(TINYINT.getLong(page.getBlock(0), 0)).isEqualTo(1L);
            assertThat(BIGINT.getLong(page.getBlock(1), 0)).isEqualTo(0L);
            assertThat(page.getBlock(0).isNull(1)).isTrue();
            assertThat(page.getBlock(1).isNull(1)).isTrue();
        }
    }

    @Test
    void testConvertTemporalDecimalAndLargeintColumns()
    {
        DecimalType shortDecimal = createDecimalType(18, 2);
        DecimalType longDecimal = createDecimalType(20, 0);
        TimestampType timestamp = createTimestampType(6);
        List<DorisColumnHandle> columns = List.of(
                new DorisColumnHandle("created_on", DATE, 0),
                new DorisColumnHandle("created_at", timestamp, 1),
                new DorisColumnHandle("amount", shortDecimal, 2),
                new DorisColumnHandle("unsigned_id", longDecimal, 3),
                new DorisColumnHandle("order_key", createUnboundedVarcharType(), 4));

        LocalDate date = LocalDate.of(2024, 3, 20);
        LocalDateTime timestampValue = LocalDateTime.of(2024, 7, 25, 10, 2, 23, 586_123_000);

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                DateDayVector createdOn = new DateDayVector("created_on", allocator);
                TimeStampMicroVector createdAt = new TimeStampMicroVector("created_at", allocator);
                DecimalVector amount = new DecimalVector("amount", allocator, 18, 2);
                DecimalVector unsignedId = new DecimalVector("unsigned_id", allocator, 20, 0);
                FixedSizeBinaryVector orderKey = new FixedSizeBinaryVector("order_key", allocator, 16);
                VectorSchemaRoot root = new VectorSchemaRoot(List.of(createdOn, createdAt, amount, unsignedId, orderKey))) {
            createdOn.setInitialCapacity(3);
            createdOn.allocateNew();
            createdOn.setSafe(0, (int) date.toEpochDay());
            createdOn.setSafe(1, (int) date.toEpochDay());
            createdOn.setSafe(2, (int) date.toEpochDay());
            createdOn.setValueCount(3);

            long epochSecond = timestampValue.toEpochSecond(UTC);
            long epochMillis = epochSecond * 1_000 + 586;
            long epochMicros = epochSecond * 1_000_000 + 586_123;
            createdAt.setInitialCapacity(3);
            createdAt.allocateNew();
            createdAt.setSafe(0, epochSecond);
            createdAt.setSafe(1, epochMillis);
            createdAt.setSafe(2, epochMicros);
            createdAt.setValueCount(3);

            amount.setInitialCapacity(3);
            amount.allocateNew();
            amount.setSafe(0, new BigDecimal("12.34"));
            amount.setNull(1);
            amount.setSafe(2, new BigDecimal("56.78"));
            amount.setValueCount(3);

            unsignedId.setInitialCapacity(3);
            unsignedId.allocateNew();
            unsignedId.setSafe(0, new BigDecimal("18446744073709551615"));
            unsignedId.setNull(1);
            unsignedId.setSafe(2, new BigDecimal("42"));
            unsignedId.setValueCount(3);

            orderKey.setInitialCapacity(3);
            orderKey.allocateNew();
            orderKey.setSafe(0, toLittleEndianLargeint(new BigInteger("9223372036854775809")));
            orderKey.setNull(1);
            orderKey.setSafe(2, toLittleEndianLargeint(BigInteger.valueOf(-1)));
            orderKey.setValueCount(3);

            root.setRowCount(3);

            Page page = convert(columns, root);

            assertThat(DATE.getLong(page.getBlock(0), 0)).isEqualTo(date.toEpochDay());
            assertThat(timestamp.getObjectValue(page.getBlock(1), 0).toString()).isEqualTo("2024-07-25 10:02:23.000000");
            assertThat(timestamp.getObjectValue(page.getBlock(1), 1).toString()).isEqualTo("2024-07-25 10:02:23.586000");
            assertThat(timestamp.getObjectValue(page.getBlock(1), 2).toString()).isEqualTo("2024-07-25 10:02:23.586123");
            assertThat(((SqlDecimal) shortDecimal.getObjectValue(page.getBlock(2), 0)).toString()).isEqualTo("12.34");
            assertThat(page.getBlock(2).isNull(1)).isTrue();
            assertThat(((SqlDecimal) longDecimal.getObjectValue(page.getBlock(3), 0)).toString()).isEqualTo("18446744073709551615");
            assertThat(createUnboundedVarcharType().getObjectValue(page.getBlock(4), 0)).isEqualTo("9223372036854775809");
            assertThat(page.getBlock(4).isNull(1)).isTrue();
            assertThat(createUnboundedVarcharType().getObjectValue(page.getBlock(4), 2)).isEqualTo("-1");
        }
    }

    @Test
    void testConvertTemporalTextAndDecimalFallbackColumns()
    {
        List<DorisColumnHandle> columns = List.of(
                new DorisColumnHandle("created_on", DATE, 0),
                new DorisColumnHandle("created_at", createTimestampType(6), 1),
                new DorisColumnHandle("oversized_amount", createUnboundedVarcharType(), 2));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VarCharVector createdOn = new VarCharVector("created_on", allocator);
                VarCharVector createdAt = new VarCharVector("created_at", allocator);
                Decimal256Vector oversizedAmount = new Decimal256Vector("oversized_amount", allocator, 76, 6);
                VectorSchemaRoot root = new VectorSchemaRoot(List.of(createdOn, createdAt, oversizedAmount))) {
            createdOn.setInitialCapacity(1);
            createdOn.allocateNew();
            createdOn.setSafe(0, "2024-03-20 00:00:00".getBytes(UTF_8));
            createdOn.setValueCount(1);

            createdAt.setInitialCapacity(1);
            createdAt.allocateNew();
            createdAt.setSafe(0, " 2024-07-25T10:02:23.5 ".getBytes(UTF_8));
            createdAt.setValueCount(1);

            oversizedAmount.setInitialCapacity(1);
            oversizedAmount.allocateNew();
            oversizedAmount.setSafe(0, new BigDecimal("1234567890123456789012345678901234567890.123456"));
            oversizedAmount.setValueCount(1);

            root.setRowCount(1);

            Page page = convert(columns, root);

            assertThat(DATE.getLong(page.getBlock(0), 0)).isEqualTo(LocalDate.of(2024, 3, 20).toEpochDay());
            assertThat(createTimestampType(6).getObjectValue(page.getBlock(1), 0).toString()).isEqualTo("2024-07-25 10:02:23.500000");
            assertThat(createUnboundedVarcharType().getObjectValue(page.getBlock(2), 0)).isEqualTo("1234567890123456789012345678901234567890.123456");
        }
    }

    @Test
    void testConvertDoubleColumnsFromIntegralDecimalAndTextVectors()
    {
        List<DorisColumnHandle> columns = List.of(
                new DorisColumnHandle("avg_from_double", DOUBLE, 0),
                new DorisColumnHandle("avg_from_bigint", DOUBLE, 1),
                new DorisColumnHandle("avg_from_decimal", DOUBLE, 2),
                new DorisColumnHandle("avg_from_text", DOUBLE, 3),
                new DorisColumnHandle("tiny_as_text", createUnboundedVarcharType(), 4));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                Float8Vector avgFromDouble = new Float8Vector("avg_from_double", allocator);
                BigIntVector avgFromBigint = new BigIntVector("avg_from_bigint", allocator);
                DecimalVector avgFromDecimal = new DecimalVector("avg_from_decimal", allocator, 18, 6);
                VarCharVector avgFromText = new VarCharVector("avg_from_text", allocator);
                TinyIntVector tinyAsText = new TinyIntVector("tiny_as_text", allocator);
                VectorSchemaRoot root = new VectorSchemaRoot(List.of(avgFromDouble, avgFromBigint, avgFromDecimal, avgFromText, tinyAsText))) {
            avgFromDouble.setInitialCapacity(1);
            avgFromDouble.allocateNew();
            avgFromDouble.setSafe(0, 12.5);
            avgFromDouble.setValueCount(1);

            avgFromBigint.setInitialCapacity(1);
            avgFromBigint.allocateNew();
            avgFromBigint.setSafe(0, 42L);
            avgFromBigint.setValueCount(1);

            avgFromDecimal.setInitialCapacity(1);
            avgFromDecimal.allocateNew();
            avgFromDecimal.setSafe(0, new BigDecimal("7.125000"));
            avgFromDecimal.setValueCount(1);

            avgFromText.setInitialCapacity(1);
            avgFromText.allocateNew();
            avgFromText.setSafe(0, " 3.75 ".getBytes(UTF_8));
            avgFromText.setValueCount(1);

            tinyAsText.setInitialCapacity(1);
            tinyAsText.allocateNew();
            tinyAsText.setSafe(0, 7);
            tinyAsText.setValueCount(1);

            root.setRowCount(1);

            Page page = convert(columns, root);

            assertThat(DOUBLE.getDouble(page.getBlock(0), 0)).isEqualTo(12.5);
            assertThat(DOUBLE.getDouble(page.getBlock(1), 0)).isEqualTo(42.0);
            assertThat(DOUBLE.getDouble(page.getBlock(2), 0)).isEqualTo(7.125);
            assertThat(DOUBLE.getDouble(page.getBlock(3), 0)).isEqualTo(3.75);
            assertThat(createUnboundedVarcharType().getObjectValue(page.getBlock(4), 0)).isEqualTo("7");
        }
    }

    @Test
    void testConvertTimestampTimezoneVectorPreservesLocalTime()
    {
        TimestampType timestamp = createTimestampType(6);
        List<DorisColumnHandle> columns = List.of(new DorisColumnHandle("created_at", timestamp, 0));
        LocalDateTime localTimestamp = LocalDateTime.of(2024, 7, 25, 10, 2, 23, 586_123_000);
        long instantMicros = localTimestamp.atZone(ZoneId.of("Asia/Shanghai")).toInstant().getEpochSecond() * 1_000_000
                + (localTimestamp.getNano() / 1_000);

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                TimeStampMicroTZVector createdAt = new TimeStampMicroTZVector("created_at", allocator, "Asia/Shanghai");
                VectorSchemaRoot root = new VectorSchemaRoot(List.of(createdAt))) {
            createdAt.setInitialCapacity(1);
            createdAt.allocateNew();
            createdAt.setSafe(0, instantMicros);
            createdAt.setValueCount(1);

            root.setRowCount(1);

            Page page = convert(columns, root);

            assertThat(timestamp.getObjectValue(page.getBlock(0), 0).toString()).isEqualTo("2024-07-25 10:02:23.586123");
        }
    }

    @Test
    void testConvertSecondAndMillisecondTimezoneVectorsPreserveLocalTime()
    {
        TimestampType secondsTimestamp = createTimestampType(0);
        TimestampType millisTimestamp = createTimestampType(3);
        List<DorisColumnHandle> columns = List.of(
                new DorisColumnHandle("created_at_seconds", secondsTimestamp, 0),
                new DorisColumnHandle("created_at_millis", millisTimestamp, 1));

        LocalDateTime secondsLocalTimestamp = LocalDateTime.of(2024, 7, 25, 10, 2, 23);
        LocalDateTime millisLocalTimestamp = LocalDateTime.of(2024, 7, 25, 10, 2, 23, 586_000_000);
        long secondsInstant = secondsLocalTimestamp.atZone(ZoneId.of("Asia/Shanghai")).toInstant().getEpochSecond();
        long millisInstant = millisLocalTimestamp.atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                TimeStampSecTZVector createdAtSeconds = new TimeStampSecTZVector("created_at_seconds", allocator, "Asia/Shanghai");
                TimeStampMilliTZVector createdAtMillis = new TimeStampMilliTZVector("created_at_millis", allocator, "Asia/Shanghai");
                VectorSchemaRoot root = new VectorSchemaRoot(List.of(createdAtSeconds, createdAtMillis))) {
            createdAtSeconds.setInitialCapacity(1);
            createdAtSeconds.allocateNew();
            createdAtSeconds.setSafe(0, secondsInstant);
            createdAtSeconds.setValueCount(1);

            createdAtMillis.setInitialCapacity(1);
            createdAtMillis.allocateNew();
            createdAtMillis.setSafe(0, millisInstant);
            createdAtMillis.setValueCount(1);

            root.setRowCount(1);

            Page page = convert(columns, root);

            assertThat(secondsTimestamp.getObjectValue(page.getBlock(0), 0).toString()).isEqualTo("2024-07-25 10:02:23");
            assertThat(millisTimestamp.getObjectValue(page.getBlock(1), 0).toString()).isEqualTo("2024-07-25 10:02:23.586");
        }
    }

    @Test
    void testLargeintDecimalOverflowFailsClearly()
    {
        DecimalType largeintAsDecimal = createDecimalType(38, 0);
        List<DorisColumnHandle> columns = List.of(new DorisColumnHandle("order_key", largeintAsDecimal, 0));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                FixedSizeBinaryVector orderKey = new FixedSizeBinaryVector("order_key", allocator, 16);
                VectorSchemaRoot root = new VectorSchemaRoot(List.of(orderKey))) {
            orderKey.setInitialCapacity(1);
            orderKey.allocateNew();
            orderKey.setSafe(0, toLittleEndianLargeint(new BigInteger("170141183460469231731687303715884105727")));
            orderKey.setValueCount(1);
            root.setRowCount(1);

            assertThatThrownBy(() -> convert(columns, root))
                    .isInstanceOf(TrinoException.class)
                    .hasMessageContaining("exceeds Trino type decimal(38,0)")
                    .hasMessageContaining("order_key");
        }
    }

    private Page convert(List<DorisColumnHandle> columns, VectorSchemaRoot root)
    {
        PageBuilder pageBuilder = new PageBuilder(columns.stream()
                .map(DorisColumnHandle::columnType)
                .toList());
        return converter.convert(pageBuilder, columns, root);
    }

    private static byte[] toLittleEndianLargeint(BigInteger value)
    {
        byte[] bytes = value.toByteArray();
        byte[] fixedBytes = new byte[16];
        if (value.signum() < 0) {
            Arrays.fill(fixedBytes, (byte) 0xFF);
        }
        System.arraycopy(bytes, 0, fixedBytes, fixedBytes.length - bytes.length, bytes.length);
        for (int left = 0, right = fixedBytes.length - 1; left < right; left++, right--) {
            byte temp = fixedBytes[left];
            fixedBytes[left] = fixedBytes[right];
            fixedBytes[right] = temp;
        }
        return fixedBytes;
    }
}
