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
package io.trino.parquet.reader.decoders;

import com.google.common.collect.ImmutableList;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.SimpleSliceInputStream;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.block.Fixed12Block;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;

import java.time.LocalDateTime;
import java.time.Year;
import java.util.OptionalInt;
import java.util.Random;

import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.ParquetTimestampUtils.decodeInt96Timestamp;
import static io.trino.parquet.reader.TestingColumnReader.encodeInt96Timestamp;
import static io.trino.parquet.reader.flat.Fixed12ColumnAdapter.FIXED12_ADAPTER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestInt96ValueDecoder
        extends AbstractValueDecodersTest
{
    @Override
    protected Object[][] tests()
    {
        PrimitiveField field = createField(INT96, OptionalInt.empty(), TIMESTAMP_NANOS);
        ValueDecoders valueDecoders = new ValueDecoders(field);
        return testArgs(
                new TestType<>(
                        field,
                        valueDecoders::getInt96TimestampDecoder,
                        Int96ApacheParquetValueDecoder::new,
                        FIXED12_ADAPTER,
                        (actual, expected) -> assertThat(actual).isEqualTo(expected)),
                ImmutableList.of(PLAIN, RLE_DICTIONARY),
                TimestampInputProvider.values());
    }

    private enum TimestampInputProvider
            implements InputDataProvider
    {
        INT96_RANDOM {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                long[] epochSeconds = new long[dataSize];
                int[] nanos = new int[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    int month = random.nextInt(1, 13);
                    int dateMax = 30;
                    if (month == 2) {
                        dateMax = 28;
                    }
                    else if ((month < 8 && month % 2 == 1) || (month >= 8 && month % 2 == 0)) {
                        dateMax = 31;
                    }
                    LocalDateTime timestamp = LocalDateTime.of(
                            random.nextInt(Year.MIN_VALUE, Year.MAX_VALUE + 1),
                            month,
                            random.nextInt(1, dateMax + 1),
                            random.nextInt(24),
                            random.nextInt(60),
                            random.nextInt(60));
                    epochSeconds[i] = timestamp.toEpochSecond(UTC);
                    nanos[i] = timestamp.get(NANO_OF_SECOND);
                }
                return writeValues(valuesWriter, epochSeconds, nanos);
            }
        },
        INT96_REPEAT {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                LocalDateTime[] constants = new LocalDateTime[] {
                        LocalDateTime.MIN,
                        LocalDateTime.of(1410, 7, 15, 14, 30, 12),
                        LocalDateTime.of(1920, 8, 15, 23, 59, 59, 10020030),
                        LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999),
                        LocalDateTime.of(1970, 1, 1, 0, 0, 0, 1000000),
                        LocalDateTime.of(2022, 2, 3, 12, 8, 51, 1),
                        LocalDateTime.of(123456, 1, 2, 3, 4, 5, 678901234),
                        LocalDateTime.MAX};
                long[] epochSeconds = new long[dataSize];
                int[] nanos = new int[dataSize];
                for (int i = 0; i < dataSize; i++) {
                    LocalDateTime timestamp = constants[random.nextInt(constants.length)];
                    epochSeconds[i] = timestamp.toEpochSecond(UTC);
                    nanos[i] = timestamp.get(NANO_OF_SECOND);
                }
                return writeValues(valuesWriter, epochSeconds, nanos);
            }
        }
    }

    private static DataBuffer writeValues(ValuesWriter valuesWriter, long[] epochSeconds, int[] nanos)
    {
        for (int i = 0; i < epochSeconds.length; i++) {
            valuesWriter.writeBytes(encodeInt96Timestamp(epochSeconds[i], nanos[i]));
        }

        return getWrittenBuffer(valuesWriter);
    }

    public static final class Int96ApacheParquetValueDecoder
            implements ValueDecoder<int[]>
    {
        private final ValuesReader delegate;

        public Int96ApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(int[] values, int offset, int length)
        {
            int endOffset = offset + length;
            for (int i = offset; i < endOffset; i++) {
                DecodedTimestamp decodedTimestamp = decodeInt96Timestamp(delegate.readBytes());
                Fixed12Block.encodeFixed12(
                        decodedTimestamp.epochSeconds(),
                        decodedTimestamp.nanosOfSecond(),
                        values,
                        i);
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }
}
