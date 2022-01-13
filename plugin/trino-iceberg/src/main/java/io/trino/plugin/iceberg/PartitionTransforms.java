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
package io.trino.plugin.iceberg;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Murmur3Hash32;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.PartitionField;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;
import java.util.function.ToLongFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static io.trino.plugin.iceberg.util.Timestamps.getTimestampTz;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzToMicros;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.Decimals.isLongDecimal;
import static io.trino.spi.type.Decimals.isShortDecimal;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_HOUR;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Integer.parseInt;
import static java.lang.Math.floorDiv;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;

public final class PartitionTransforms
{
    private static final Pattern BUCKET_PATTERN = Pattern.compile("bucket\\[(\\d+)]");
    private static final Pattern TRUNCATE_PATTERN = Pattern.compile("truncate\\[(\\d+)]");

    private static final DateTimeField YEAR_FIELD = ISOChronology.getInstanceUTC().year();
    private static final DateTimeField MONTH_FIELD = ISOChronology.getInstanceUTC().monthOfYear();

    private PartitionTransforms() {}

    public static ColumnTransform getColumnTransform(PartitionField field, Type type)
    {
        String transform = field.transform().toString();

        switch (transform) {
            case "identity":
                return identity(type);
            case "year":
                if (type.equals(DATE)) {
                    return yearsFromDate();
                }
                if (type.equals(TIMESTAMP_MICROS)) {
                    return yearsFromTimestamp();
                }
                if (type.equals(TIMESTAMP_TZ_MICROS)) {
                    return yearsFromTimestampWithTimeZone();
                }
                throw new UnsupportedOperationException("Unsupported type for 'year': " + field);
            case "month":
                if (type.equals(DATE)) {
                    return monthsFromDate();
                }
                if (type.equals(TIMESTAMP_MICROS)) {
                    return monthsFromTimestamp();
                }
                if (type.equals(TIMESTAMP_TZ_MICROS)) {
                    return monthsFromTimestampWithTimeZone();
                }
                throw new UnsupportedOperationException("Unsupported type for 'month': " + field);
            case "day":
                if (type.equals(DATE)) {
                    return daysFromDate();
                }
                if (type.equals(TIMESTAMP_MICROS)) {
                    return daysFromTimestamp();
                }
                if (type.equals(TIMESTAMP_TZ_MICROS)) {
                    return daysFromTimestampWithTimeZone();
                }
                throw new UnsupportedOperationException("Unsupported type for 'day': " + field);
            case "hour":
                if (type.equals(TIMESTAMP_MICROS)) {
                    return hoursFromTimestamp();
                }
                if (type.equals(TIMESTAMP_TZ_MICROS)) {
                    return hoursFromTimestampWithTimeZone();
                }
                throw new UnsupportedOperationException("Unsupported type for 'hour': " + field);
            case "void":
                return voidTransform(type);
        }

        Matcher matcher = BUCKET_PATTERN.matcher(transform);
        if (matcher.matches()) {
            int count = parseInt(matcher.group(1));
            return bucket(type, count);
        }

        matcher = TRUNCATE_PATTERN.matcher(transform);
        if (matcher.matches()) {
            int width = parseInt(matcher.group(1));
            if (type.equals(INTEGER)) {
                return truncateInteger(width);
            }
            if (type.equals(BIGINT)) {
                return truncateBigint(width);
            }
            if (isShortDecimal(type)) {
                DecimalType decimal = (DecimalType) type;
                return truncateShortDecimal(type, width, decimal);
            }
            if (isLongDecimal(type)) {
                DecimalType decimal = (DecimalType) type;
                return truncateLongDecimal(type, width, decimal);
            }
            if (type instanceof VarcharType) {
                return truncateVarchar(width);
            }
            if (type.equals(VARBINARY)) {
                return truncateVarbinary(width);
            }
            throw new UnsupportedOperationException("Unsupported type for 'truncate': " + field);
        }

        throw new UnsupportedOperationException("Unsupported partition transform: " + field);
    }

    private static ColumnTransform identity(Type type)
    {
        return new ColumnTransform(type, Function.identity(), ValueTransform.identity(type));
    }

    @VisibleForTesting
    static ColumnTransform bucket(Type type, int count)
    {
        Hasher hasher = getBucketingHash(type);
        return new ColumnTransform(
                INTEGER,
                block -> bucketBlock(block, count, hasher),
                (block, position) -> {
                    if (block.isNull(position)) {
                        return null;
                    }
                    int hash = hasher.hash(block, position);
                    int bucket = (hash & Integer.MAX_VALUE) % count;
                    return (long) bucket;
                });
    }

    private static Hasher getBucketingHash(Type type)
    {
        if (type.equals(INTEGER)) {
            return PartitionTransforms::hashInteger;
        }
        if (type.equals(BIGINT)) {
            return PartitionTransforms::hashBigint;
        }
        if (isShortDecimal(type)) {
            return hashShortDecimal((DecimalType) type);
        }
        if (isLongDecimal(type)) {
            return hashLongDecimal((DecimalType) type);
        }
        if (type.equals(DATE)) {
            return PartitionTransforms::hashDate;
        }
        if (type.equals(TIME_MICROS)) {
            return PartitionTransforms::hashTime;
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            return PartitionTransforms::hashTimestamp;
        }
        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            return PartitionTransforms::hashTimestampWithTimeZone;
        }
        if (type instanceof VarcharType) {
            return PartitionTransforms::hashVarchar;
        }
        if (type.equals(VARBINARY)) {
            return PartitionTransforms::hashVarbinary;
        }
        if (type.equals(UUID)) {
            return PartitionTransforms::hashUuid;
        }
        throw new UnsupportedOperationException("Unsupported type for 'bucket': " + type);
    }

    private static ColumnTransform yearsFromDate()
    {
        LongUnaryOperator transform = value -> epochYear(DAYS.toMillis(value));
        return new ColumnTransform(
                INTEGER,
                block -> transformBlock(DATE, INTEGER, block, transform),
                ValueTransform.from(DATE, transform));
    }

    private static ColumnTransform monthsFromDate()
    {
        LongUnaryOperator transform = value -> epochMonth(DAYS.toMillis(value));
        return new ColumnTransform(
                INTEGER,
                block -> transformBlock(DATE, INTEGER, block, transform),
                ValueTransform.from(DATE, transform));
    }

    private static ColumnTransform daysFromDate()
    {
        LongUnaryOperator transform = LongUnaryOperator.identity();
        return new ColumnTransform(
                INTEGER,
                block -> transformBlock(DATE, INTEGER, block, transform),
                ValueTransform.from(DATE, transform));
    }

    private static ColumnTransform yearsFromTimestamp()
    {
        LongUnaryOperator transform = epochMicros -> epochYear(floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND));
        return new ColumnTransform(
                INTEGER,
                block -> transformBlock(TIMESTAMP_MICROS, INTEGER, block, transform),
                ValueTransform.from(TIMESTAMP_MICROS, transform));
    }

    private static ColumnTransform monthsFromTimestamp()
    {
        LongUnaryOperator transform = epochMicros -> epochMonth(floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND));
        return new ColumnTransform(
                INTEGER,
                block -> transformBlock(TIMESTAMP_MICROS, INTEGER, block, transform),
                ValueTransform.from(TIMESTAMP_MICROS, transform));
    }

    private static ColumnTransform daysFromTimestamp()
    {
        LongUnaryOperator transform = epochMicros -> epochDay(floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND));
        return new ColumnTransform(
                INTEGER,
                block -> transformBlock(TIMESTAMP_MICROS, INTEGER, block, transform),
                ValueTransform.from(TIMESTAMP_MICROS, transform));
    }

    private static ColumnTransform hoursFromTimestamp()
    {
        LongUnaryOperator transform = epochMicros -> epochHour(floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND));
        return new ColumnTransform(
                INTEGER,
                block -> transformBlock(TIMESTAMP_MICROS, INTEGER, block, transform),
                ValueTransform.from(TIMESTAMP_MICROS, transform));
    }

    private static ColumnTransform yearsFromTimestampWithTimeZone()
    {
        ToLongFunction<LongTimestampWithTimeZone> transform = value -> epochYear(value.getEpochMillis());
        return new ColumnTransform(
                INTEGER,
                block -> extractTimestampWithTimeZone(block, transform),
                ValueTransform.fromTimestampTzTransform(transform));
    }

    private static ColumnTransform monthsFromTimestampWithTimeZone()
    {
        ToLongFunction<LongTimestampWithTimeZone> transform = value -> epochMonth(value.getEpochMillis());
        return new ColumnTransform(
                INTEGER,
                block -> extractTimestampWithTimeZone(block, transform),
                ValueTransform.fromTimestampTzTransform(transform));
    }

    private static ColumnTransform daysFromTimestampWithTimeZone()
    {
        ToLongFunction<LongTimestampWithTimeZone> transform = value -> epochDay(value.getEpochMillis());
        return new ColumnTransform(
                INTEGER,
                block -> extractTimestampWithTimeZone(block, transform),
                ValueTransform.fromTimestampTzTransform(transform));
    }

    private static ColumnTransform hoursFromTimestampWithTimeZone()
    {
        ToLongFunction<LongTimestampWithTimeZone> transform = value -> epochHour(value.getEpochMillis());
        return new ColumnTransform(
                INTEGER,
                block -> extractTimestampWithTimeZone(block, transform),
                ValueTransform.fromTimestampTzTransform(transform));
    }

    private static Block extractTimestampWithTimeZone(Block block, ToLongFunction<LongTimestampWithTimeZone> function)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            LongTimestampWithTimeZone value = getTimestampTz(block, position);
            INTEGER.writeLong(builder, function.applyAsLong(value));
        }
        return builder.build();
    }

    private static int hashInteger(Block block, int position)
    {
        return bucketHash(INTEGER.getLong(block, position));
    }

    private static int hashBigint(Block block, int position)
    {
        return bucketHash(BIGINT.getLong(block, position));
    }

    private static Hasher hashShortDecimal(DecimalType decimal)
    {
        return (block, position) -> {
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(decimal, block, position);
            return bucketHash(Slices.wrappedBuffer(value.unscaledValue().toByteArray()));
        };
    }

    private static Hasher hashLongDecimal(DecimalType decimal)
    {
        return (block, position) -> {
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(decimal, block, position);
            return bucketHash(Slices.wrappedBuffer(value.unscaledValue().toByteArray()));
        };
    }

    private static int hashDate(Block block, int position)
    {
        return bucketHash(DATE.getLong(block, position));
    }

    private static int hashTime(Block block, int position)
    {
        long picos = TIME_MICROS.getLong(block, position);
        return bucketHash(picos / PICOSECONDS_PER_MICROSECOND);
    }

    private static int hashTimestamp(Block block, int position)
    {
        return bucketHash(TIMESTAMP_MICROS.getLong(block, position));
    }

    private static int hashTimestampWithTimeZone(Block block, int position)
    {
        return bucketHash(timestampTzToMicros(getTimestampTz(block, position)));
    }

    private static int hashVarchar(Block block, int position)
    {
        return bucketHash(VARCHAR.getSlice(block, position));
    }

    private static int hashVarbinary(Block block, int position)
    {
        return bucketHash(VARBINARY.getSlice(block, position));
    }

    private static int hashUuid(Block block, int position)
    {
        return bucketHash(UUID.getSlice(block, position));
    }

    private static Block bucketBlock(Block block, int count, Hasher hasher)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            int hash = hasher.hash(block, position);
            int bucket = (hash & Integer.MAX_VALUE) % count;
            INTEGER.writeLong(builder, bucket);
        }
        return builder.build();
    }

    private static int bucketHash(long value)
    {
        return Murmur3Hash32.hash(value);
    }

    private static int bucketHash(Slice value)
    {
        return Murmur3Hash32.hash(value);
    }

    private static ColumnTransform truncateInteger(int width)
    {
        return new ColumnTransform(
                INTEGER,
                block -> truncateInteger(block, width),
                (block, position) -> {
                    if (block.isNull(position)) {
                        return null;
                    }
                    return truncateInteger(block, position, width);
                });
    }

    private static Block truncateInteger(Block block, int width)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            INTEGER.writeLong(builder, truncateInteger(block, position, width));
        }
        return builder.build();
    }

    private static long truncateInteger(Block block, int position, int width)
    {
        long value = INTEGER.getLong(block, position);
        return value - ((value % width) + width) % width;
    }

    private static ColumnTransform truncateBigint(int width)
    {
        return new ColumnTransform(
                BIGINT,
                block -> truncateBigint(block, width),
                (block, position) -> {
                    if (block.isNull(position)) {
                        return null;
                    }
                    return truncateBigint(block, position, width);
                });
    }

    private static Block truncateBigint(Block block, int width)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            BIGINT.writeLong(builder, truncateBigint(block, position, width));
        }
        return builder.build();
    }

    private static long truncateBigint(Block block, int position, int width)
    {
        long value = BIGINT.getLong(block, position);
        return value - ((value % width) + width) % width;
    }

    private static ColumnTransform truncateShortDecimal(Type type, int width, DecimalType decimal)
    {
        BigInteger unscaledWidth = BigInteger.valueOf(width);
        return new ColumnTransform(
                type,
                block -> truncateShortDecimal(decimal, block, unscaledWidth),
                (block, position) -> {
                    if (block.isNull(position)) {
                        return null;
                    }
                    return truncateShortDecimal(decimal, block, position, unscaledWidth);
                });
    }

    private static Block truncateShortDecimal(DecimalType type, Block block, BigInteger unscaledWidth)
    {
        BlockBuilder builder = type.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            type.writeLong(builder, truncateShortDecimal(type, block, position, unscaledWidth));
        }
        return builder.build();
    }

    private static long truncateShortDecimal(DecimalType type, Block block, int position, BigInteger unscaledWidth)
    {
        // TODO: write optimized implementation
        BigDecimal value = readBigDecimal(type, block, position);
        BigDecimal truncated = truncateDecimal(value, unscaledWidth);
        return encodeShortScaledValue(truncated, type.getScale());
    }

    private static ColumnTransform truncateLongDecimal(Type type, int width, DecimalType decimal)
    {
        BigInteger unscaledWidth = BigInteger.valueOf(width);
        return new ColumnTransform(
                type,
                block -> truncateLongDecimal(decimal, block, unscaledWidth),
                (block, position) -> {
                    if (block.isNull(position)) {
                        return null;
                    }
                    return truncateLongDecimal(decimal, block, position, unscaledWidth);
                });
    }

    private static Block truncateLongDecimal(DecimalType type, Block block, BigInteger unscaledWidth)
    {
        BlockBuilder builder = type.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            type.writeObject(builder, truncateLongDecimal(type, block, position, unscaledWidth));
        }
        return builder.build();
    }

    private static Int128 truncateLongDecimal(DecimalType type, Block block, int position, BigInteger unscaledWidth)
    {
        // TODO: write optimized implementation
        BigDecimal value = readBigDecimal(type, block, position);
        BigDecimal truncated = truncateDecimal(value, unscaledWidth);
        return encodeScaledValue(truncated, type.getScale());
    }

    private static BigDecimal truncateDecimal(BigDecimal value, BigInteger unscaledWidth)
    {
        BigDecimal remainder = new BigDecimal(
                value.unscaledValue()
                        .remainder(unscaledWidth)
                        .add(unscaledWidth)
                        .remainder(unscaledWidth),
                value.scale());
        return value.subtract(remainder);
    }

    private static ColumnTransform truncateVarchar(int width)
    {
        return new ColumnTransform(
                VARCHAR,
                block -> truncateVarchar(block, width),
                (block, position) -> {
                    if (block.isNull(position)) {
                        return null;
                    }
                    return truncateVarchar(VARCHAR.getSlice(block, position), width);
                });
    }

    private static Block truncateVarchar(Block block, int width)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            Slice value = VARCHAR.getSlice(block, position);
            VARCHAR.writeSlice(builder, truncateVarchar(value, width));
        }
        return builder.build();
    }

    private static Slice truncateVarchar(Slice value, int max)
    {
        if (value.length() <= max) {
            return value;
        }
        int end = offsetOfCodePoint(value, 0, max);
        if (end < 0) {
            return value;
        }
        return value.slice(0, end);
    }

    private static ColumnTransform truncateVarbinary(int width)
    {
        return new ColumnTransform(
                VARBINARY,
                block -> truncateVarbinary(block, width),
                (block, position) -> {
                    if (block.isNull(position)) {
                        return null;
                    }
                    return truncateVarbinary(VARBINARY.getSlice(block, position), width);
                });
    }

    private static Block truncateVarbinary(Block block, int width)
    {
        BlockBuilder builder = VARBINARY.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            Slice value = VARBINARY.getSlice(block, position);
            VARBINARY.writeSlice(builder, truncateVarbinary(value, width));
        }
        return builder.build();
    }

    private static Slice truncateVarbinary(Slice value, int width)
    {
        if (value.length() <= width) {
            return value;
        }
        return value.slice(0, width);
    }

    private static ColumnTransform voidTransform(Type type)
    {
        Block nullBlock = nativeValueToBlock(type, null);
        return new ColumnTransform(
                type,
                block -> new RunLengthEncodedBlock(nullBlock, block.getPositionCount()),
                (block, position) -> null);
    }

    private static Block transformBlock(Type sourceType, FixedWidthType resultType, Block block, LongUnaryOperator function)
    {
        BlockBuilder builder = resultType.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long value = sourceType.getLong(block, position);
            resultType.writeLong(builder, function.applyAsLong(value));
        }
        return builder.build();
    }

    @VisibleForTesting
    static long epochYear(long epochMilli)
    {
        return YEAR_FIELD.get(epochMilli) - 1970;
    }

    @VisibleForTesting
    static long epochMonth(long epochMilli)
    {
        long year = epochYear(epochMilli);
        int month = MONTH_FIELD.get(epochMilli) - 1;
        return (year * 12) + month;
    }

    @VisibleForTesting
    static long epochDay(long epochMilli)
    {
        return floorDiv(epochMilli, MILLISECONDS_PER_DAY);
    }

    @VisibleForTesting
    static long epochHour(long epochMilli)
    {
        return floorDiv(epochMilli, MILLISECONDS_PER_HOUR);
    }

    private interface Hasher
    {
        int hash(Block block, int position);
    }

    public static class ColumnTransform
    {
        private final Type type;
        private final Function<Block, Block> blockTransform;
        private final ValueTransform valueTransform;

        public ColumnTransform(Type type, Function<Block, Block> blockTransform, ValueTransform valueTransform)
        {
            this.type = requireNonNull(type, "type is null");
            this.blockTransform = requireNonNull(blockTransform, "transform is null");
            this.valueTransform = requireNonNull(valueTransform, "valueTransform is null");
        }

        public Type getType()
        {
            return type;
        }

        public Function<Block, Block> getBlockTransform()
        {
            return blockTransform;
        }

        public ValueTransform getValueTransform()
        {
            return valueTransform;
        }
    }

    public interface ValueTransform
    {
        static ValueTransform identity(Type type)
        {
            return (block, position) -> readNativeValue(type, block, position);
        }

        static ValueTransform from(Type sourceType, LongUnaryOperator transform)
        {
            return (block, position) -> {
                if (block.isNull(position)) {
                    return null;
                }
                return transform.applyAsLong(sourceType.getLong(block, position));
            };
        }

        static ValueTransform fromTimestampTzTransform(ToLongFunction<LongTimestampWithTimeZone> transform)
        {
            return (block, position) -> {
                if (block.isNull(position)) {
                    return null;
                }
                return transform.applyAsLong(getTimestampTz(block, position));
            };
        }

        @Nullable
        Object apply(Block block, int position);
    }
}
