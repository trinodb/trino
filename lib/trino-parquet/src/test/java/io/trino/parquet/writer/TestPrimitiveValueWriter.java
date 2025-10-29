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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.parquet.writer.valuewriter.PrimitiveValueWriter;
import io.trino.parquet.writer.valuewriter.TrinoValuesWriterFactory;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.parquet.writer.ParquetWriters.getValueWriter;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;

final class TestPrimitiveValueWriter
{
    // a small dictionary page forces a fallback to plain encoding, a large one keeps dictionary encoding
    private static final List<Integer> MAX_DICTIONARY_PAGE_SIZES = ImmutableList.of(64, 1048576);
    // boolean values are written in chunks of 64 bits, so counts either side of a chunk boundary are covered
    private static final List<Integer> REPEATED_COUNTS = ImmutableList.of(2, 8, 65, 130);

    @Test
    void testRepeatedBlockMatchesValueBlock()
            throws IOException
    {
        for (TestCase testCase : testCases()) {
            for (int maxDictionaryPageSize : MAX_DICTIONARY_PAGE_SIZES) {
                for (int count : REPEATED_COUNTS) {
                    for (Object value : testCase.values()) {
                        Block rleBlock = RunLengthEncodedBlock.create(createBlock(testCase.trinoType(), singletonList(value)), count);
                        assertThat(rleBlock).isInstanceOf(RunLengthEncodedBlock.class);
                        assertWritesMatch(testCase, maxDictionaryPageSize, rleBlock, createBlock(testCase.trinoType(), nCopies(count, value)));
                    }
                }
            }
        }
    }

    @Test
    void testRepeatedNullBlockWritesNothing()
            throws IOException
    {
        for (TestCase testCase : testCases()) {
            for (int maxDictionaryPageSize : MAX_DICTIONARY_PAGE_SIZES) {
                Block rleBlock = RunLengthEncodedBlock.create(createBlock(testCase.trinoType(), singletonList(null)), 8);
                assertWritesMatch(testCase, maxDictionaryPageSize, rleBlock, createBlock(testCase.trinoType(), ImmutableList.of()));
            }
        }
    }

    @Test
    void testDictionaryBlockMatchesValueBlock()
            throws IOException
    {
        for (TestCase testCase : testCases()) {
            for (int maxDictionaryPageSize : MAX_DICTIONARY_PAGE_SIZES) {
                List<Object> dictionaryValues = new ArrayList<>(testCase.values());
                dictionaryValues.add(null);
                int[] ids = {2, 0, 3, 1, 1, 3, 2, 0};
                Block dictionaryBlock = DictionaryBlock.create(ids.length, createBlock(testCase.trinoType(), dictionaryValues), ids);
                assertThat(dictionaryBlock).isInstanceOf(DictionaryBlock.class);
                List<Object> expandedValues = Arrays.stream(ids).mapToObj(dictionaryValues::get).toList();
                assertWritesMatch(testCase, maxDictionaryPageSize, dictionaryBlock, createBlock(testCase.trinoType(), expandedValues));
            }
        }
    }

    private static void assertWritesMatch(TestCase testCase, int maxDictionaryPageSize, Block actualBlock, Block expectedBlock)
            throws IOException
    {
        byte[] actualBytes;
        Statistics<?> actualStatistics;
        try (PrimitiveValueWriter writer = createValueWriter(testCase, maxDictionaryPageSize)) {
            writer.write(actualBlock);
            actualBytes = writer.getBytes().toByteArray();
            actualStatistics = writer.getStatistics();
        }

        byte[] expectedBytes;
        Statistics<?> expectedStatistics;
        try (PrimitiveValueWriter writer = createValueWriter(testCase, maxDictionaryPageSize)) {
            writer.write(expectedBlock);
            expectedBytes = writer.getBytes().toByteArray();
            expectedStatistics = writer.getStatistics();
        }

        assertThat(actualBytes).describedAs("%s", testCase).isEqualTo(expectedBytes);
        assertThat(actualStatistics.hasNonNullValue()).describedAs("%s", testCase).isEqualTo(expectedStatistics.hasNonNullValue());
        if (expectedStatistics.hasNonNullValue()) {
            assertThat(actualStatistics.getMinBytes()).describedAs("%s", testCase).isEqualTo(expectedStatistics.getMinBytes());
            assertThat(actualStatistics.getMaxBytes()).describedAs("%s", testCase).isEqualTo(expectedStatistics.getMaxBytes());
        }
    }

    private static PrimitiveValueWriter createValueWriter(TestCase testCase, int maxDictionaryPageSize)
    {
        PrimitiveType parquetType = testCase.parquetType();
        ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[] {"column"}, parquetType, 0, 1);
        TrinoValuesWriterFactory valuesWriterFactory = new TrinoValuesWriterFactory(ParquetWriterOptions.builder().build(), maxDictionaryPageSize);
        return getValueWriter(valuesWriterFactory.newValuesWriter(columnDescriptor, Optional.empty()), testCase.trinoType(), parquetType, testCase.parquetTimeZone());
    }

    private static Block createBlock(Type type, List<Object> values)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, values.size());
        for (Object value : values) {
            writeNativeValue(type, blockBuilder, value);
        }
        return blockBuilder.build();
    }

    private static List<TestCase> testCases()
    {
        return ImmutableList.<TestCase>builder()
                .add(testCase(BOOLEAN, true, false, true))
                .add(testCase(TINYINT, 1L, -2L, 3L))
                .add(testCase(SMALLINT, 1L, -300L, 3L))
                .add(testCase(INTEGER, 1L, -70000L, 3L))
                .add(testCase(BIGINT, 1L, -70000L, 12345678901L))
                .add(testCase(DATE, 0L, -365L, 19000L))
                .add(testCase(REAL, (long) floatToRawIntBits(1.5f), (long) floatToRawIntBits(-2.5f), (long) floatToRawIntBits(0f)))
                .add(testCase(DOUBLE, 1.5, -2.5, 0.0))
                .add(testCase(VARCHAR, utf8Slice("a"), utf8Slice("bbb"), utf8Slice("")))
                .add(testCase(VARBINARY, wrappedBuffer(new byte[] {1, 2}), wrappedBuffer(new byte[] {3}), wrappedBuffer(new byte[] {})))
                .add(explicitTypeTestCase(
                        UUID,
                        Types.optional(FIXED_LEN_BYTE_ARRAY).length(16).as(LogicalTypeAnnotation.uuidType()).named("column"),
                        uuidSlice(1),
                        uuidSlice(2),
                        uuidSlice(3)))
                .add(testCase(createDecimalType(9, 2), 1L, -22L, 333L))
                .add(testCase(createDecimalType(18, 2), 1L, -22L, 333L))
                .add(testCase(createDecimalType(38, 2), Int128.valueOf(1), Int128.valueOf(-22), Int128.valueOf(333)))
                .add(legacyDecimalTestCase(createDecimalType(9, 2), 1L, -22L, 333L))
                .add(legacyDecimalTestCase(createDecimalType(38, 2), Int128.valueOf(1), Int128.valueOf(-22), Int128.valueOf(333)))
                .add(explicitTypeTestCase(
                        createTimeType(6),
                        Types.optional(INT64).as(LogicalTypeAnnotation.timeType(false, MICROS)).named("column"),
                        1_000_000_000L,
                        2_000_000_000L,
                        3_000_000_000L))
                .add(testCase(createTimestampType(3), 1000L, -2000L, 3000L))
                .add(testCase(createTimestampType(6), 1001L, -2002L, 3003L))
                .add(testCase(createTimestampType(9), new LongTimestamp(1001, 1000), new LongTimestamp(-2002, 2000), new LongTimestamp(3003, 3000)))
                .add(explicitTypeTestCase(
                        createTimestampWithTimeZoneType(3),
                        Types.optional(INT64).as(LogicalTypeAnnotation.timestampType(true, MILLIS)).named("column"),
                        packDateTimeWithZone(1000L, UTC_KEY),
                        packDateTimeWithZone(-2000L, UTC_KEY),
                        packDateTimeWithZone(3000L, UTC_KEY)))
                .add(explicitTypeTestCase(
                        createTimestampWithTimeZoneType(6),
                        Types.optional(INT64).as(LogicalTypeAnnotation.timestampType(true, MICROS)).named("column"),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(1000L, 1_000_000, UTC_KEY),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(-2000L, 2_000_000, UTC_KEY),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(3000L, 3_000_000, UTC_KEY)))
                .add(explicitTypeTestCase(
                        createTimestampWithTimeZoneType(9),
                        Types.optional(INT64).as(LogicalTypeAnnotation.timestampType(true, NANOS)).named("column"),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(1000L, 1_000, UTC_KEY),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(-2000L, 2_000, UTC_KEY),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(3000L, 3_000, UTC_KEY)))
                .add(int96TimestampTestCase(createTimestampType(3), 1000L, -2000L, 3000L))
                .build();
    }

    private static TestCase testCase(Type trinoType, Object... values)
    {
        return new TestCase(trinoType, convertToParquetType(trinoType, false, false), Optional.empty(), Arrays.stream(values).collect(toImmutableList()));
    }

    private static TestCase legacyDecimalTestCase(Type trinoType, Object... values)
    {
        return new TestCase(trinoType, convertToParquetType(trinoType, true, false), Optional.empty(), Arrays.stream(values).collect(toImmutableList()));
    }

    private static TestCase int96TimestampTestCase(Type trinoType, Object... values)
    {
        return new TestCase(trinoType, convertToParquetType(trinoType, false, true), Optional.of(DateTimeZone.UTC), Arrays.stream(values).collect(toImmutableList()));
    }

    // types which ParquetSchemaConverter does not map, so the parquet type is spelled out
    private static TestCase explicitTypeTestCase(Type trinoType, PrimitiveType parquetType, Object... values)
    {
        return new TestCase(trinoType, parquetType, Optional.empty(), Arrays.stream(values).collect(toImmutableList()));
    }

    private static PrimitiveType convertToParquetType(Type trinoType, boolean useLegacyDecimalEncoding, boolean useInt96TimestampEncoding)
    {
        return new ParquetSchemaConverter(ImmutableList.of(trinoType), ImmutableList.of("column"), useLegacyDecimalEncoding, useInt96TimestampEncoding)
                .getMessageType()
                .getType("column")
                .asPrimitiveType();
    }

    private static Slice uuidSlice(int seed)
    {
        byte[] bytes = new byte[16];
        Arrays.fill(bytes, (byte) seed);
        return wrappedBuffer(bytes);
    }

    private record TestCase(Type trinoType, PrimitiveType parquetType, Optional<DateTimeZone> parquetTimeZone, List<Object> values)
    {
        @Override
        public String toString()
        {
            return "%s as %s".formatted(trinoType, parquetType);
        }
    }
}
