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
package io.trino.parquet.writer.valuewriter;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.joda.time.DateTimeZone;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetTimestampUtils.JULIAN_EPOCH_OFFSET_DAYS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class Int96TimestampValueWriter
        extends PrimitiveValueWriter
{
    private final TimestampType timestampType;
    private final DateTimeZone parquetTimeZone;

    public Int96TimestampValueWriter(ValuesWriter valuesWriter, Type type, PrimitiveType parquetType, DateTimeZone parquetTimeZone)
    {
        super(parquetType, valuesWriter);
        requireNonNull(type, "type is null");
        checkArgument(
                type instanceof TimestampType timestampType && timestampType.getPrecision() <= 9,
                "type %s is not a TimestampType with precision <= 9",
                type);
        this.timestampType = (TimestampType) type;
        checkArgument(
                parquetType.getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.INT96),
                "parquetType %s is not INT96",
                parquetType);
        this.parquetTimeZone = requireNonNull(parquetTimeZone, "parquetTimeZone is null");
    }

    @Override
    protected void writeValueBlock(ValueBlock block)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        boolean mayHaveNull = block.mayHaveNull();
        byte[] buffer = new byte[Long.BYTES + Integer.BYTES];
        Slice reusedSlice = Slices.wrappedBuffer(buffer);
        Binary reusedBinary = Binary.fromReusedByteArray(buffer);

        if (timestampType.isShort()) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (!mayHaveNull || !block.isNull(position)) {
                    readShortTimestampAndWriteToBuffer(block, position, buffer);

                    valuesWriter.writeBytes(reusedSlice);
                    statistics.updateStats(reusedBinary);
                }
            }
        }
        else {
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (!mayHaveNull || !block.isNull(position)) {
                    readLongTimestampAndWriteToBuffer(block, position, buffer);

                    valuesWriter.writeBytes(reusedSlice);
                    statistics.updateStats(reusedBinary);
                }
            }
        }
    }

    @Override
    protected void writeRepeated(ValueBlock block, int count)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        byte[] buffer = new byte[Long.BYTES + Integer.BYTES];
        Slice reusedSlice = Slices.wrappedBuffer(buffer);
        Binary reusedBinary = Binary.fromReusedByteArray(buffer);
        if (timestampType.isShort()) {
            readShortTimestampAndWriteToBuffer(block, 0, buffer);
        }
        else {
            readLongTimestampAndWriteToBuffer(block, 0, buffer);
        }

        for (int i = 0; i < count; i++) {
            valuesWriter.writeBytes(reusedSlice);
        }
        statistics.updateStats(reusedBinary);
    }

    @Override
    protected void writePositions(ValueBlock block, int[] positions, int offset, int length)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        boolean mayHaveNull = block.mayHaveNull();
        byte[] buffer = new byte[Long.BYTES + Integer.BYTES];
        Slice reusedSlice = Slices.wrappedBuffer(buffer);
        Binary reusedBinary = Binary.fromReusedByteArray(buffer);

        if (timestampType.isShort()) {
            for (int index = 0; index < length; index++) {
                int position = positions[offset + index];
                if (!mayHaveNull || !block.isNull(position)) {
                    readShortTimestampAndWriteToBuffer(block, position, buffer);

                    valuesWriter.writeBytes(reusedSlice);
                    statistics.updateStats(reusedBinary);
                }
            }
        }
        else {
            for (int index = 0; index < length; index++) {
                int position = positions[offset + index];
                if (!mayHaveNull || !block.isNull(position)) {
                    readLongTimestampAndWriteToBuffer(block, position, buffer);

                    valuesWriter.writeBytes(reusedSlice);
                    statistics.updateStats(reusedBinary);
                }
            }
        }
    }

    private void readShortTimestampAndWriteToBuffer(ValueBlock block, int position, byte[] buffer)
    {
        long epochMicros = timestampType.getLong(block, position);
        long localEpochMillis = floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND);
        int nanosOfMillis = floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND) * NANOSECONDS_PER_MICROSECOND;

        convertAndWriteToBuffer(localEpochMillis, nanosOfMillis, buffer);
    }

    private void readLongTimestampAndWriteToBuffer(ValueBlock block, int position, byte[] buffer)
    {
        LongTimestamp timestamp = (LongTimestamp) timestampType.getObject(block, position);
        long epochMicros = timestamp.getEpochMicros();
        // This should divide exactly because timestamp precision is <= 9
        int nanosOfMicro = timestamp.getPicosOfMicro() / PICOSECONDS_PER_NANOSECOND;
        long localEpochMillis = floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND);
        int nanosOfMillis = floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND) * NANOSECONDS_PER_MICROSECOND + nanosOfMicro;

        convertAndWriteToBuffer(localEpochMillis, nanosOfMillis, buffer);
    }

    private void convertAndWriteToBuffer(long localEpochMillis, int nanosOfMillis, byte[] buffer)
    {
        long epochMillis = parquetTimeZone.convertLocalToUTC(localEpochMillis, false);
        long epochDay = floorDiv(epochMillis, MILLISECONDS_PER_DAY);
        int julianDay = JULIAN_EPOCH_OFFSET_DAYS + toIntExact(epochDay);

        long nanosOfEpochDay = nanosOfMillis + ((long) floorMod(epochMillis, MILLISECONDS_PER_DAY) * NANOSECONDS_PER_MILLISECOND);
        ByteBuffer.wrap(buffer)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(0, nanosOfEpochDay)
                .putInt(Long.BYTES, julianDay);
    }
}
