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
package io.trino.plugin.deltalake.util;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;

import static com.google.common.io.BaseEncoding.base16;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static io.trino.plugin.hive.HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.nio.charset.StandardCharsets.UTF_8;

// Copied from io.trino.plugin.hive.util.HiveWriteUtils
public final class DeltaLakeWriteUtils
{
    private static final DateTimeFormatter DELTA_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DELTA_TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
            .toFormatter();

    private DeltaLakeWriteUtils() {}

    public static List<String> createPartitionValues(List<Type> partitionColumnTypes, Page partitionColumns, int position)
    {
        ImmutableList.Builder<String> partitionValues = ImmutableList.builder();
        for (int field = 0; field < partitionColumns.getChannelCount(); field++) {
            String value = toPartitionValue(partitionColumnTypes.get(field), partitionColumns.getBlock(field), position);
            // TODO https://github.com/trinodb/trino/issues/18950 Remove or fix the following condition
            if (!CharMatcher.inRange((char) 0x20, (char) 0x7E).matchesAllOf(value)) {
                String encoded = base16().withSeparator(" ", 2).encode(value.getBytes(UTF_8));
                throw new TrinoException(HIVE_INVALID_PARTITION_VALUE, "Hive partition keys can only contain printable ASCII characters (0x20 - 0x7E). Invalid value: " + encoded);
            }
            partitionValues.add(value);
        }
        return partitionValues.build();
    }

    private static String toPartitionValue(Type type, Block block, int position)
    {
        // see HiveUtil#isValidPartitionType
        if (block.isNull(position)) {
            return HIVE_DEFAULT_DYNAMIC_PARTITION;
        }
        if (BOOLEAN.equals(type)) {
            return String.valueOf(BOOLEAN.getBoolean(block, position));
        }
        if (BIGINT.equals(type)) {
            return String.valueOf(BIGINT.getLong(block, position));
        }
        if (INTEGER.equals(type)) {
            return String.valueOf(INTEGER.getInt(block, position));
        }
        if (SMALLINT.equals(type)) {
            return String.valueOf(SMALLINT.getShort(block, position));
        }
        if (TINYINT.equals(type)) {
            return String.valueOf(TINYINT.getByte(block, position));
        }
        if (REAL.equals(type)) {
            return String.valueOf(REAL.getFloat(block, position));
        }
        if (DOUBLE.equals(type)) {
            return String.valueOf(DOUBLE.getDouble(block, position));
        }
        if (type instanceof VarcharType varcharType) {
            return varcharType.getSlice(block, position).toStringUtf8();
        }
        if (DATE.equals(type)) {
            return LocalDate.ofEpochDay(DATE.getInt(block, position)).format(DELTA_DATE_FORMATTER);
        }
        if (TIMESTAMP_MILLIS.equals(type) || TIMESTAMP_MICROS.equals(type)) {
            long epochMicros = type.getLong(block, position);
            long epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
            int nanosOfSecond = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
            return LocalDateTime.ofEpochSecond(epochSeconds, nanosOfSecond, ZoneOffset.UTC).format(DELTA_TIMESTAMP_FORMATTER);
        }
        if (TIMESTAMP_TZ_MILLIS.equals(type)) {
            long epochMillis = unpackMillisUtc(type.getLong(block, position));
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC).format(DELTA_TIMESTAMP_FORMATTER);
        }
        if (type instanceof DecimalType decimalType) {
            return readBigDecimal(decimalType, block, position).stripTrailingZeros().toPlainString();
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported type for partition: " + type);
    }
}
