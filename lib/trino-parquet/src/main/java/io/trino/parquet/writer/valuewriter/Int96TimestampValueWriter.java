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
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.joda.time.DateTimeZone;

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
        int positionCount = block.getPositionCount();
        long[] localEpochMillis = new long[positionCount];
        int[] nanosOfMillis = new int[positionCount];
        int nonNullsCount = 0;
        if (timestampType.isShort()) {
            for (int position = 0; position < positionCount; position++) {
                if (!block.isNull(position)) {
                    long epochMicros = timestampType.getLong(block, position);
                    localEpochMillis[nonNullsCount] = floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND);
                    nanosOfMillis[nonNullsCount] = floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND) * NANOSECONDS_PER_MICROSECOND;
                    nonNullsCount++;
                }
            }
        }
        else {
            for (int position = 0; position < positionCount; position++) {
                if (!block.isNull(position)) {
                    LongTimestamp timestamp = (LongTimestamp) timestampType.getObject(block, position);
                    long epochMicros = timestamp.getEpochMicros();
                    // This should divide exactly because timestamp precision is <= 9
                    int nanosOfMicro = timestamp.getPicosOfMicro() / PICOSECONDS_PER_NANOSECOND;
                    localEpochMillis[nonNullsCount] = floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND);
                    nanosOfMillis[nonNullsCount] = floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND) * NANOSECONDS_PER_MICROSECOND + nanosOfMicro;
                    nonNullsCount++;
                }
            }
        }

        for (int i = 0; i < nonNullsCount; i++) {
            long epochMillis = parquetTimeZone.convertLocalToUTC(localEpochMillis[i], false);
            long epochDay = floorDiv(epochMillis, MILLISECONDS_PER_DAY);
            int julianDay = JULIAN_EPOCH_OFFSET_DAYS + toIntExact(epochDay);

            long nanosOfEpochDay = nanosOfMillis[i] + ((long) floorMod(epochMillis, MILLISECONDS_PER_DAY) * NANOSECONDS_PER_MILLISECOND);
            Binary binary = toBinary(julianDay, nanosOfEpochDay);
            getValueWriter().writeBytes(binary);
            getStatistics().updateStats(binary);
        }
    }

    private Binary toBinary(int julianDay, long nanosOfEpochDay)
    {
        byte[] buffer = new byte[Long.BYTES + Integer.BYTES];
        longToBytes(buffer, nanosOfEpochDay);
        intToBytes(buffer, julianDay);
        return Binary.fromConstantByteArray(buffer);
    }

    private static void intToBytes(byte[] outBuffer, int value)
    {
        outBuffer[Long.BYTES + 3] = (byte) (value >>> 24);
        outBuffer[Long.BYTES + 2] = (byte) (value >>> 16);
        outBuffer[Long.BYTES + 1] = (byte) (value >>> 8);
        outBuffer[Long.BYTES] = (byte) value;
    }

    private static void longToBytes(byte[] outBuffer, long value)
    {
        outBuffer[7] = (byte) (value >>> 56);
        outBuffer[6] = (byte) (value >>> 48);
        outBuffer[5] = (byte) (value >>> 40);
        outBuffer[4] = (byte) (value >>> 32);
        outBuffer[3] = (byte) (value >>> 24);
        outBuffer[2] = (byte) (value >>> 16);
        outBuffer[1] = (byte) (value >>> 8);
        outBuffer[0] = (byte) value;
    }
}
