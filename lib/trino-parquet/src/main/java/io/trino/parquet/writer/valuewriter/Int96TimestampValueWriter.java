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

import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesWriter;
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
                type instanceof TimestampType && ((TimestampType) type).getPrecision() <= 9,
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
    public void write(Block block)
    {
        if (timestampType.isShort()) {
            writeShortTimestamps(block);
        }
        else {
            writeLongTimestamps(block);
        }
    }

    private void writeShortTimestamps(Block block)
    {
        ValuesWriter valuesWriter = requireNonNull(getValuesWriter(), "valuesWriter is null");
        Statistics<?> statistics = requireNonNull(getStatistics(), "statistics is null");
        boolean mayHaveNull = block.mayHaveNull();
        byte[] buffer = new byte[Long.BYTES + Integer.BYTES];
        Binary reusedBinary = Binary.fromReusedByteArray(buffer);

        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!mayHaveNull || !block.isNull(position)) {
                long epochMicros = timestampType.getLong(block, position);
                long localEpochMillis = floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND);
                int nanosOfMillis = floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND) * NANOSECONDS_PER_MICROSECOND;

                convertAndWriteToBuffer(localEpochMillis, nanosOfMillis, buffer);
                valuesWriter.writeBytes(reusedBinary);
                statistics.updateStats(reusedBinary);
            }
        }
    }

    private void writeLongTimestamps(Block block)
    {
        ValuesWriter valuesWriter = requireNonNull(getValuesWriter(), "valuesWriter is null");
        Statistics<?> statistics = requireNonNull(getStatistics(), "statistics is null");
        boolean mayHaveNull = block.mayHaveNull();
        byte[] buffer = new byte[Long.BYTES + Integer.BYTES];
        Binary reusedBinary = Binary.fromReusedByteArray(buffer);

        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!mayHaveNull || !block.isNull(position)) {
                LongTimestamp timestamp = (LongTimestamp) timestampType.getObject(block, position);
                long epochMicros = timestamp.getEpochMicros();
                // This should divide exactly because timestamp precision is <= 9
                int nanosOfMicro = timestamp.getPicosOfMicro() / PICOSECONDS_PER_NANOSECOND;
                long localEpochMillis = floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND);
                int nanosOfMillis = floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND) * NANOSECONDS_PER_MICROSECOND + nanosOfMicro;

                convertAndWriteToBuffer(localEpochMillis, nanosOfMillis, buffer);
                valuesWriter.writeBytes(reusedBinary);
                statistics.updateStats(reusedBinary);
            }
        }
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
