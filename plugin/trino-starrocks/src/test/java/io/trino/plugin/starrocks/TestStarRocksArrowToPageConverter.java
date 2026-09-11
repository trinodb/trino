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
package io.trino.plugin.starrocks;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
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
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static io.trino.type.JsonType.JSON;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

final class TestStarRocksArrowToPageConverter
{
    private final StarRocksArrowToPageConverter converter = new StarRocksArrowToPageConverter();

    @Test
    void testConvertPrimitiveAndCharacterColumns()
    {
        List<StarRocksColumnHandle> columns = List.of(
                new StarRocksColumnHandle("flag", "flag", BOOLEAN, 0),
                new StarRocksColumnHandle("tiny", "tiny", TINYINT, 1),
                new StarRocksColumnHandle("small", "small", SMALLINT, 2),
                new StarRocksColumnHandle("id", "id", BIGINT, 3),
                new StarRocksColumnHandle("ratio", "ratio", REAL, 4),
                new StarRocksColumnHandle("name", "name", createVarcharType(20), 5),
                new StarRocksColumnHandle("code", "code", createCharType(3), 6),
                new StarRocksColumnHandle("payload", "payload", JSON, 7));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                BitVector flag = new BitVector("flag", allocator);
                TinyIntVector tiny = new TinyIntVector("tiny", allocator);
                SmallIntVector small = new SmallIntVector("small", allocator);
                BigIntVector id = new BigIntVector("id", allocator);
                Float4Vector ratio = new Float4Vector("ratio", allocator);
                VarCharVector name = new VarCharVector("name", allocator);
                VarCharVector code = new VarCharVector("code", allocator);
                VarCharVector payload = new VarCharVector("payload", allocator);
                VectorSchemaRoot root = new VectorSchemaRoot(List.of(flag, tiny, small, id, ratio, name, code, payload))) {
            flag.allocateNew();
            flag.setSafe(0, 1);
            flag.setNull(1);
            flag.setValueCount(2);

            tiny.allocateNew();
            tiny.setSafe(0, 7);
            tiny.setNull(1);
            tiny.setValueCount(2);

            small.allocateNew();
            small.setSafe(0, 123);
            small.setNull(1);
            small.setValueCount(2);

            id.allocateNew();
            id.setSafe(0, 456789L);
            id.setNull(1);
            id.setValueCount(2);

            ratio.allocateNew();
            ratio.setSafe(0, 1.5f);
            ratio.setNull(1);
            ratio.setValueCount(2);

            name.allocateNew();
            name.setSafe(0, "alpha".getBytes(UTF_8));
            name.setNull(1);
            name.setValueCount(2);

            code.allocateNew();
            code.setSafe(0, "xy   ".getBytes(UTF_8));
            code.setNull(1);
            code.setValueCount(2);

            payload.allocateNew();
            payload.setSafe(0, "{\"a\":1}".getBytes(UTF_8));
            payload.setNull(1);
            payload.setValueCount(2);

            root.setRowCount(2);

            Page page = convert(columns, root);

            assertThat(BOOLEAN.getObjectValue(page.getBlock(0), 0)).isEqualTo(true);
            assertThat(page.getBlock(0).isNull(1)).isTrue();
            assertThat(TINYINT.getLong(page.getBlock(1), 0)).isEqualTo(7L);
            assertThat(SMALLINT.getLong(page.getBlock(2), 0)).isEqualTo(123L);
            assertThat(BIGINT.getLong(page.getBlock(3), 0)).isEqualTo(456789L);
            assertThat(REAL.getFloat(page.getBlock(4), 0)).isEqualTo(1.5f);
            assertThat(createVarcharType(20).getObjectValue(page.getBlock(5), 0)).isEqualTo("alpha");
            assertThat(createCharType(3).getObjectValue(page.getBlock(6), 0)).isEqualTo("xy ");
            assertThat(JSON.getObjectValue(page.getBlock(7), 0)).isEqualTo("{\"a\":1}");
            assertThat(page.getBlock(7).isNull(1)).isTrue();
        }
    }

    @Test
    void testConvertTextualComplexColumns()
    {
        ArrayType tagsType = new ArrayType(createUnboundedVarcharType());
        MapType scoresType = new MapType(createUnboundedVarcharType(), BIGINT, TESTING_TYPE_MANAGER.getTypeOperators());
        RowType detailsType = rowType(
                field("a", INTEGER),
                field("b", createVarcharType(12)),
                field("c", new ArrayType(BIGINT)));
        List<StarRocksColumnHandle> columns = List.of(
                new StarRocksColumnHandle("tags", "tags", tagsType, 0),
                new StarRocksColumnHandle("scores", "scores", scoresType, 1),
                new StarRocksColumnHandle("details", "details", detailsType, 2));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VarCharVector tags = new VarCharVector("tags", allocator);
                VarCharVector scores = new VarCharVector("scores", allocator);
                VarCharVector details = new VarCharVector("details", allocator);
                VectorSchemaRoot root = new VectorSchemaRoot(List.of(tags, scores, details))) {
            tags.allocateNew();
            tags.setSafe(0, "[\"red\",\"blue\"]".getBytes(UTF_8));
            tags.setValueCount(1);

            scores.allocateNew();
            scores.setSafe(0, "{\"x\":10,\"y\":20}".getBytes(UTF_8));
            scores.setValueCount(1);

            details.allocateNew();
            details.setSafe(0, "{\"a\":7,\"b\":\"ok\",\"c\":[3,4]}".getBytes(UTF_8));
            details.setValueCount(1);

            root.setRowCount(1);

            Page page = convert(columns, root);

            assertThat(tagsType.getObjectValue(page.getBlock(0), 0)).isEqualTo(List.of("red", "blue"));
            assertThat(scoresType.getObjectValue(page.getBlock(1), 0)).isEqualTo(Map.of("x", 10L, "y", 20L));
            assertThat(detailsType.getObjectValue(page.getBlock(2), 0)).isEqualTo(List.of(7, "ok", List.of(3L, 4L)));
        }
    }

    @Test
    void testConvertTemporalDecimalAndLargeintFallbackColumns()
    {
        DecimalType shortDecimal = createDecimalType(18, 2);
        TimestampType timestamp = createTimestampType(6);
        List<StarRocksColumnHandle> columns = List.of(
                new StarRocksColumnHandle("created_on", "created_on", DATE, 0),
                new StarRocksColumnHandle("created_at", "created_at", timestamp, 1),
                new StarRocksColumnHandle("amount", "amount", shortDecimal, 2),
                new StarRocksColumnHandle("largeint_summary", "largeint_summary", createUnboundedVarcharType(), 3));

        LocalDate date = LocalDate.of(2024, 3, 20);
        LocalDateTime timestampValue = LocalDateTime.of(2024, 7, 25, 10, 2, 23, 586_123_000);

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                DateDayVector createdOn = new DateDayVector("created_on", allocator);
                TimeStampMicroVector createdAt = new TimeStampMicroVector("created_at", allocator);
                DecimalVector amount = new DecimalVector("amount", allocator, 18, 2);
                FixedSizeBinaryVector largeintSummary = new FixedSizeBinaryVector("largeint_summary", allocator, 16);
                VectorSchemaRoot root = new VectorSchemaRoot(List.of(createdOn, createdAt, amount, largeintSummary))) {
            createdOn.allocateNew();
            createdOn.setSafe(0, (int) date.toEpochDay());
            createdOn.setSafe(1, (int) date.toEpochDay());
            createdOn.setValueCount(2);

            long epochMicros = timestampValue.toEpochSecond(UTC) * 1_000_000L + 586_123;
            createdAt.allocateNew();
            createdAt.setSafe(0, epochMicros);
            createdAt.setNull(1);
            createdAt.setValueCount(2);

            amount.allocateNew();
            amount.setSafe(0, new BigDecimal("12.34"));
            amount.setNull(1);
            amount.setValueCount(2);

            largeintSummary.allocateNew();
            largeintSummary.setSafe(0, toLittleEndianSignedInteger(new BigInteger("9223372036854775809")));
            largeintSummary.setSafe(1, toLittleEndianSignedInteger(BigInteger.valueOf(-1)));
            largeintSummary.setValueCount(2);

            root.setRowCount(2);

            Page page = convert(columns, root);

            assertThat(DATE.getLong(page.getBlock(0), 0)).isEqualTo(date.toEpochDay());
            assertThat(timestamp.getObjectValue(page.getBlock(1), 0).toString()).isEqualTo("2024-07-25 10:02:23.586123");
            assertThat(page.getBlock(1).isNull(1)).isTrue();
            assertThat(((SqlDecimal) shortDecimal.getObjectValue(page.getBlock(2), 0)).toString()).isEqualTo("12.34");
            assertThat(page.getBlock(2).isNull(1)).isTrue();
            assertThat(createUnboundedVarcharType().getObjectValue(page.getBlock(3), 0)).isEqualTo("9223372036854775809");
            assertThat(createUnboundedVarcharType().getObjectValue(page.getBlock(3), 1)).isEqualTo("-1");
        }
    }

    @Test
    void testConvertTextualTemporalAndDecimal256FallbackColumns()
    {
        List<StarRocksColumnHandle> columns = List.of(
                new StarRocksColumnHandle("created_on", "created_on", DATE, 0),
                new StarRocksColumnHandle("created_at", "created_at", createTimestampType(6), 1),
                new StarRocksColumnHandle("oversized_amount", "oversized_amount", createUnboundedVarcharType(), 2));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VarCharVector createdOn = new VarCharVector("created_on", allocator);
                VarCharVector createdAt = new VarCharVector("created_at", allocator);
                Decimal256Vector oversizedAmount = new Decimal256Vector("oversized_amount", allocator, 76, 6);
                VectorSchemaRoot root = new VectorSchemaRoot(List.of(createdOn, createdAt, oversizedAmount))) {
            createdOn.allocateNew();
            createdOn.setSafe(0, "2024-03-20 00:00:00".getBytes(UTF_8));
            createdOn.setValueCount(1);

            createdAt.allocateNew();
            createdAt.setSafe(0, " 2024-07-25T10:02:23.5 ".getBytes(UTF_8));
            createdAt.setValueCount(1);

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
    void testConvertArrowTimestampUnits()
    {
        TimestampType timestamp = createTimestampType(6);
        List<StarRocksColumnHandle> columns = List.of(
                new StarRocksColumnHandle("ts_sec", "ts_sec", timestamp, 0),
                new StarRocksColumnHandle("ts_milli", "ts_milli", timestamp, 1),
                new StarRocksColumnHandle("ts_micro", "ts_micro", timestamp, 2),
                new StarRocksColumnHandle("ts_nano", "ts_nano", timestamp, 3));

        LocalDateTime timestampValue = LocalDateTime.of(2024, 7, 25, 10, 2, 23, 586_123_000);
        long epochSeconds = timestampValue.toEpochSecond(UTC);
        long epochMicros = epochSeconds * 1_000_000L + 586_123L;

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                TimeStampSecVector tsSec = new TimeStampSecVector("ts_sec", allocator);
                TimeStampMilliVector tsMilli = new TimeStampMilliVector("ts_milli", allocator);
                TimeStampMicroVector tsMicro = new TimeStampMicroVector("ts_micro", allocator);
                TimeStampNanoVector tsNano = new TimeStampNanoVector("ts_nano", allocator);
                VectorSchemaRoot root = new VectorSchemaRoot(List.of(tsSec, tsMilli, tsMicro, tsNano))) {
            tsSec.allocateNew();
            tsSec.setSafe(0, epochSeconds);
            tsSec.setValueCount(1);

            tsMilli.allocateNew();
            tsMilli.setSafe(0, epochSeconds * 1_000L + 586L);
            tsMilli.setValueCount(1);

            tsMicro.allocateNew();
            tsMicro.setSafe(0, epochMicros);
            tsMicro.setValueCount(1);

            tsNano.allocateNew();
            tsNano.setSafe(0, epochSeconds * 1_000_000_000L + 586_123_000L);
            tsNano.setValueCount(1);

            root.setRowCount(1);

            Page page = convert(columns, root);

            assertThat(timestamp.getLong(page.getBlock(0), 0)).isEqualTo(epochSeconds * 1_000_000L);
            assertThat(timestamp.getLong(page.getBlock(1), 0)).isEqualTo(epochSeconds * 1_000_000L + 586_000L);
            assertThat(timestamp.getLong(page.getBlock(2), 0)).isEqualTo(epochMicros);
            assertThat(timestamp.getLong(page.getBlock(3), 0)).isEqualTo(epochMicros);
        }
    }

    private Page convert(List<StarRocksColumnHandle> columns, VectorSchemaRoot root)
    {
        return converter.convert(PageBuilder.withMaxPageSize(1024, columns.stream().map(StarRocksColumnHandle::columnType).toList()), columns, root);
    }

    private static byte[] toLittleEndianSignedInteger(BigInteger value)
    {
        byte[] bigEndian = value.toByteArray();
        byte[] littleEndian = new byte[16];
        byte fill = value.signum() < 0 ? (byte) 0xFF : 0;
        for (int index = 0; index < littleEndian.length; index++) {
            littleEndian[index] = fill;
        }
        for (int index = 0; index < bigEndian.length; index++) {
            littleEndian[index] = bigEndian[bigEndian.length - 1 - index];
        }
        return littleEndian;
    }
}
