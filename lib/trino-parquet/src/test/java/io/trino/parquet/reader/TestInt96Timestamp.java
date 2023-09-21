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
package io.trino.parquet.reader;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV2;
import io.trino.parquet.PrimitiveField;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.block.Block;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Timestamps;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.schema.Types;
import org.joda.time.DateTimeZone;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.BiFunction;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.reader.TestingColumnReader.encodeInt96Timestamp;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.assertj.core.api.Assertions.assertThat;

public class TestInt96Timestamp
{
    private static final LocalDateTime[] TIMESTAMPS = new LocalDateTime[] {
            LocalDateTime.of(-5000, 1, 1, 1, 1, 1),
            LocalDateTime.of(-1, 4, 2, 4, 2, 4, 2),
            LocalDateTime.of(1, 1, 1, 0, 0),
            LocalDateTime.of(1410, 7, 15, 14, 30, 12),
            LocalDateTime.of(1920, 8, 15, 23, 59, 59, 10020030),
            LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999),
            LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999),
            LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999999),
            LocalDateTime.of(1970, 1, 1, 0, 0),
            LocalDateTime.of(1970, 1, 1, 0, 0, 0, 1),
            LocalDateTime.of(1970, 1, 1, 0, 0, 0, 1000),
            LocalDateTime.of(1970, 1, 1, 0, 0, 0, 1000000),
            LocalDateTime.of(2022, 2, 3, 12, 8, 51, 123456789),
            LocalDateTime.of(2022, 2, 3, 12, 8, 51, 1),
            LocalDateTime.of(2022, 2, 3, 12, 8, 51, 999999999),
            LocalDateTime.of(123456, 1, 2, 3, 4, 5, 678901234)};

    @Test(dataProvider = "testVariousTimestampsDataProvider")
    public void testVariousTimestamps(TimestampType type, BiFunction<Block, Integer, DecodedTimestamp> actualValuesProvider)
            throws IOException
    {
        int valueCount = TIMESTAMPS.length;

        PrimitiveField field = new PrimitiveField(type, true, new ColumnDescriptor(new String[] {"dummy"}, Types.required(INT96).named("dummy"), 0, 0), 0);
        ValuesWriter writer = new FixedLenByteArrayPlainValuesWriter(12, 1024, 1024, HeapByteBufferAllocator.getInstance());

        for (LocalDateTime timestamp : TIMESTAMPS) {
            long expectedEpochSeconds = timestamp.toEpochSecond(UTC);
            int nanos = timestamp.get(NANO_OF_SECOND);
            writer.writeBytes(encodeInt96Timestamp(expectedEpochSeconds, nanos));
        }

        Slice slice = Slices.wrappedBuffer(writer.getBytes().toByteArray());
        DataPage dataPage = new DataPageV2(
                valueCount,
                0,
                valueCount,
                EMPTY_SLICE,
                EMPTY_SLICE,
                PLAIN,
                slice,
                slice.length(),
                OptionalLong.empty(),
                null,
                false);
        // Read and assert
        ColumnReaderFactory columnReaderFactory = new ColumnReaderFactory(DateTimeZone.UTC);
        ColumnReader reader = columnReaderFactory.create(field, newSimpleAggregatedMemoryContext());
        reader.setPageReader(
                new PageReader(UNCOMPRESSED, List.of(dataPage).iterator(), false, false),
                Optional.empty());
        reader.prepareNextRead(valueCount);
        Block block = reader.readPrimitive().getBlock();

        for (int i = 0; i < valueCount; i++) {
            LocalDateTime timestamp = TIMESTAMPS[i];
            long expectedEpochSeconds = timestamp.toEpochSecond(UTC);
            int nanos = timestamp.get(NANO_OF_SECOND);
            int precisionToNanos = Math.max(0, 9 - type.getPrecision());
            int expectedNanos = (int) Timestamps.round(nanos, precisionToNanos);
            if (expectedNanos == 1_000_000_000) {
                expectedEpochSeconds++;
                expectedNanos = 0;
            }

            DecodedTimestamp actual = actualValuesProvider.apply(block, i);
            assertThat(actual.epochSeconds()).isEqualTo(expectedEpochSeconds);
            assertThat(actual.nanosOfSecond()).isEqualTo(expectedNanos);
        }
    }

    @DataProvider(name = "testVariousTimestampsDataProvider")
    public Object[][] testVariousTimestampsDataProvider()
    {
        BiFunction<Block, Integer, DecodedTimestamp> shortTimestamp = (block, i) ->
                new DecodedTimestamp(floorDiv(block.getLong(i, 0), MICROSECONDS_PER_SECOND), floorMod(block.getLong(i, 0), MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND);
        BiFunction<Block, Integer, DecodedTimestamp> longTimestamp = (block, i) ->
                new DecodedTimestamp(
                        floorDiv(block.getLong(i, 0), MICROSECONDS_PER_SECOND),
                        floorMod(block.getLong(i, 0), MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND + block.getInt(i, 8) / PICOSECONDS_PER_NANOSECOND);

        return new Object[][] {
                new Object[] {TIMESTAMP_MILLIS, shortTimestamp},
                new Object[] {TIMESTAMP_MICROS, shortTimestamp},
                new Object[] {TIMESTAMP_NANOS, longTimestamp},
                new Object[] {TIMESTAMP_PICOS, longTimestamp}};
    }
}
