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
package io.prestosql.plugin.iceberg;

import io.airlift.slice.Murmur3Hash32;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import org.apache.iceberg.PartitionField;
import org.joda.time.DurationField;
import org.joda.time.chrono.ISOChronology;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.encodeScaledValue;
import static io.prestosql.spi.type.Decimals.encodeShortScaledValue;
import static io.prestosql.spi.type.Decimals.isLongDecimal;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static io.prestosql.spi.type.Decimals.readBigDecimal;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class PartitionTransforms
{
    private static final Pattern BUCKET_PATTERN = Pattern.compile("bucket\\[(\\d+)]");
    private static final Pattern TRUNCATE_PATTERN = Pattern.compile("truncate\\[(\\d+)]");

    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();
    private static final DurationField YEARS_DURATION = UTC_CHRONOLOGY.years();
    private static final DurationField MONTHS_DURATION = UTC_CHRONOLOGY.months();
    private static final DurationField DAYS_DURATION = UTC_CHRONOLOGY.days();
    private static final DurationField HOURS_DURATION = UTC_CHRONOLOGY.hours();

    private PartitionTransforms() {}

    public static ColumnTransform getColumnTransform(PartitionField field, Type type)
    {
        String transform = field.transform().toString();

        switch (transform) {
            case "identity":
                return new ColumnTransform(type, Function.identity());
            case "year":
                if (type.equals(DATE)) {
                    return new ColumnTransform(INTEGER, PartitionTransforms::yearsFromDate);
                }
                if (type.equals(TIMESTAMP)) {
                    return new ColumnTransform(INTEGER, PartitionTransforms::yearsFromTimestamp);
                }
                if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
                    return new ColumnTransform(INTEGER, PartitionTransforms::yearsFromTimestampWithTimeZone);
                }
                throw new UnsupportedOperationException("Unsupported type for 'year': " + field);
            case "month":
                if (type.equals(DATE)) {
                    return new ColumnTransform(INTEGER, PartitionTransforms::monthsFromDate);
                }
                if (type.equals(TIMESTAMP)) {
                    return new ColumnTransform(INTEGER, PartitionTransforms::monthsFromTimestamp);
                }
                if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
                    return new ColumnTransform(INTEGER, PartitionTransforms::monthsFromTimestampWithTimeZone);
                }
                throw new UnsupportedOperationException("Unsupported type for 'month': " + field);
            case "day":
                if (type.equals(DATE)) {
                    return new ColumnTransform(INTEGER, PartitionTransforms::daysFromDate);
                }
                if (type.equals(TIMESTAMP)) {
                    return new ColumnTransform(INTEGER, PartitionTransforms::daysFromTimestamp);
                }
                if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
                    return new ColumnTransform(INTEGER, PartitionTransforms::daysFromTimestampWithTimeZone);
                }
                throw new UnsupportedOperationException("Unsupported type for 'day': " + field);
            case "hour":
                if (type.equals(TIMESTAMP)) {
                    return new ColumnTransform(INTEGER, PartitionTransforms::hoursFromTimestamp);
                }
                if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
                    return new ColumnTransform(INTEGER, PartitionTransforms::hoursFromTimestampWithTimeZone);
                }
                throw new UnsupportedOperationException("Unsupported type for 'hour': " + field);
        }

        Matcher matcher = BUCKET_PATTERN.matcher(transform);
        if (matcher.matches()) {
            int count = parseInt(matcher.group(1));
            if (type.equals(INTEGER)) {
                return new ColumnTransform(INTEGER, block -> bucketInteger(block, count));
            }
            if (type.equals(BIGINT)) {
                return new ColumnTransform(INTEGER, block -> bucketBigint(block, count));
            }
            if (isShortDecimal(type)) {
                DecimalType decimal = (DecimalType) type;
                return new ColumnTransform(INTEGER, block -> bucketShortDecimal(decimal, block, count));
            }
            if (isLongDecimal(type)) {
                DecimalType decimal = (DecimalType) type;
                return new ColumnTransform(INTEGER, block -> bucketLongDecimal(decimal, block, count));
            }
            if (type.equals(DATE)) {
                return new ColumnTransform(INTEGER, block -> bucketDate(block, count));
            }
            if (type.equals(TIME)) {
                return new ColumnTransform(INTEGER, block -> bucketTime(block, count));
            }
            if (type.equals(TIMESTAMP)) {
                return new ColumnTransform(INTEGER, block -> bucketTimestamp(block, count));
            }
            if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
                return new ColumnTransform(INTEGER, block -> bucketTimestampWithTimeZone(block, count));
            }
            if (isVarcharType(type)) {
                return new ColumnTransform(INTEGER, block -> bucketVarchar(block, count));
            }
            if (type.equals(VARBINARY)) {
                return new ColumnTransform(INTEGER, block -> bucketVarbinary(block, count));
            }
            if (type.getTypeSignature().getBase().equals(StandardTypes.UUID)) {
                return new ColumnTransform(INTEGER, block -> bucketUuid(block, count));
            }
            throw new UnsupportedOperationException("Unsupported type for 'bucket': " + field);
        }

        matcher = TRUNCATE_PATTERN.matcher(transform);
        if (matcher.matches()) {
            int width = parseInt(matcher.group(1));
            if (type.equals(INTEGER)) {
                return new ColumnTransform(INTEGER, block -> truncateInteger(block, width));
            }
            if (type.equals(BIGINT)) {
                return new ColumnTransform(BIGINT, block -> truncateBigint(block, width));
            }
            if (isShortDecimal(type)) {
                DecimalType decimal = (DecimalType) type;
                return new ColumnTransform(type, block -> truncateShortDecimal(decimal, block, width));
            }
            if (isLongDecimal(type)) {
                DecimalType decimal = (DecimalType) type;
                return new ColumnTransform(type, block -> truncateLongDecimal(decimal, block, width));
            }
            if (isVarcharType(type)) {
                return new ColumnTransform(VARCHAR, block -> truncateVarchar(block, width));
            }
            if (type.equals(VARBINARY)) {
                return new ColumnTransform(VARBINARY, block -> truncateVarbinary(block, width));
            }
            throw new UnsupportedOperationException("Unsupported type for 'truncate': " + field);
        }

        throw new UnsupportedOperationException("Unsupported partition transform: " + field);
    }

    private static Block yearsFromDate(Block block)
    {
        return extractDate(block, value -> YEARS_DURATION.getValueAsLong(DAYS.toMillis(value)));
    }

    private static Block monthsFromDate(Block block)
    {
        return extractDate(block, value -> MONTHS_DURATION.getValueAsLong(DAYS.toMillis(value)));
    }

    private static Block daysFromDate(Block block)
    {
        return extractDate(block, LongUnaryOperator.identity());
    }

    private static Block extractDate(Block block, LongUnaryOperator function)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long value = DATE.getLong(block, position);
            value = function.applyAsLong(value);
            INTEGER.writeLong(builder, value);
        }
        return builder.build();
    }

    private static Block yearsFromTimestamp(Block block)
    {
        return extractTimestamp(block, YEARS_DURATION::getValueAsLong);
    }

    private static Block monthsFromTimestamp(Block block)
    {
        return extractTimestamp(block, MONTHS_DURATION::getValueAsLong);
    }

    private static Block daysFromTimestamp(Block block)
    {
        return extractTimestamp(block, DAYS_DURATION::getValueAsLong);
    }

    private static Block hoursFromTimestamp(Block block)
    {
        return extractTimestamp(block, HOURS_DURATION::getValueAsLong);
    }

    private static Block extractTimestamp(Block block, LongUnaryOperator function)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long value = TIMESTAMP.getLong(block, position);
            value = function.applyAsLong(value);
            INTEGER.writeLong(builder, value);
        }
        return builder.build();
    }

    private static Block yearsFromTimestampWithTimeZone(Block block)
    {
        return extractTimestampWithTimeZone(block, YEARS_DURATION::getValueAsLong);
    }

    private static Block monthsFromTimestampWithTimeZone(Block block)
    {
        return extractTimestampWithTimeZone(block, MONTHS_DURATION::getValueAsLong);
    }

    private static Block daysFromTimestampWithTimeZone(Block block)
    {
        return extractTimestampWithTimeZone(block, DAYS_DURATION::getValueAsLong);
    }

    private static Block hoursFromTimestampWithTimeZone(Block block)
    {
        return extractTimestampWithTimeZone(block, HOURS_DURATION::getValueAsLong);
    }

    private static Block extractTimestampWithTimeZone(Block block, LongUnaryOperator function)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long value = unpackMillisUtc(TIMESTAMP_WITH_TIME_ZONE.getLong(block, position));
            value = function.applyAsLong(value);
            INTEGER.writeLong(builder, value);
        }
        return builder.build();
    }

    private static Block bucketInteger(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(INTEGER.getLong(block, position)));
    }

    private static Block bucketBigint(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(BIGINT.getLong(block, position)));
    }

    private static Block bucketShortDecimal(DecimalType decimal, Block block, int count)
    {
        return bucketBlock(block, count, position -> {
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(decimal, block, position);
            return bucketHash(Slices.wrappedBuffer(value.unscaledValue().toByteArray()));
        });
    }

    private static Block bucketLongDecimal(DecimalType decimal, Block block, int count)
    {
        return bucketBlock(block, count, position -> {
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(decimal, block, position);
            return bucketHash(Slices.wrappedBuffer(value.unscaledValue().toByteArray()));
        });
    }

    private static Block bucketDate(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(DATE.getLong(block, position)));
    }

    private static Block bucketTime(Block block, int count)
    {
        return bucketBlock(block, count, position -> {
            long value = TIME.getLong(block, position);
            return bucketHash(MILLISECONDS.toMicros(value));
        });
    }

    private static Block bucketTimestamp(Block block, int count)
    {
        return bucketBlock(block, count, position -> {
            long value = TIMESTAMP.getLong(block, position);
            return bucketHash(MILLISECONDS.toMicros(value));
        });
    }

    private static Block bucketTimestampWithTimeZone(Block block, int count)
    {
        return bucketBlock(block, count, position -> {
            long value = unpackMillisUtc(TIMESTAMP_WITH_TIME_ZONE.getLong(block, position));
            return bucketHash(MILLISECONDS.toMicros(value));
        });
    }

    private static Block bucketVarchar(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(VARCHAR.getSlice(block, position)));
    }

    private static Block bucketVarbinary(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(VARCHAR.getSlice(block, position)));
    }

    private static Block bucketUuid(Block block, int count)
    {
        return bucketBlock(block, count, position -> bucketHash(VARCHAR.getSlice(block, position)));
    }

    private static Block bucketBlock(Block block, int count, IntUnaryOperator hasher)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            int hash = hasher.applyAsInt(position);
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

    private static Block truncateInteger(Block block, int width)
    {
        BlockBuilder builder = INTEGER.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long value = INTEGER.getLong(block, position);
            value -= ((value % width) + width) % width;
            INTEGER.writeLong(builder, value);
        }
        return builder.build();
    }

    private static Block truncateBigint(Block block, int width)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            long value = BIGINT.getLong(block, position);
            value -= ((value % width) + width) % width;
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }

    private static Block truncateShortDecimal(DecimalType type, Block block, int width)
    {
        BigInteger unscaledWidth = BigInteger.valueOf(width);
        BlockBuilder builder = type.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(type, block, position);
            value = truncateDecimal(value, unscaledWidth);
            type.writeLong(builder, encodeShortScaledValue(value, type.getScale()));
        }
        return builder.build();
    }

    private static Block truncateLongDecimal(DecimalType type, Block block, int width)
    {
        BigInteger unscaledWidth = BigInteger.valueOf(width);
        BlockBuilder builder = type.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            // TODO: write optimized implementation
            BigDecimal value = readBigDecimal(type, block, position);
            value = truncateDecimal(value, unscaledWidth);
            type.writeSlice(builder, encodeScaledValue(value, type.getScale()));
        }
        return builder.build();
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

    private static Block truncateVarchar(Block block, int max)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            Slice value = VARCHAR.getSlice(block, position);
            value = truncateVarchar(value, max);
            VARCHAR.writeSlice(builder, value);
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

    private static Block truncateVarbinary(Block block, int max)
    {
        BlockBuilder builder = VARBINARY.createBlockBuilder(null, block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                builder.appendNull();
                continue;
            }
            Slice value = VARBINARY.getSlice(block, position);
            if (value.length() > max) {
                value = value.slice(0, max);
            }
            VARBINARY.writeSlice(builder, value);
        }
        return builder.build();
    }

    public static class ColumnTransform
    {
        private final Type type;
        private final Function<Block, Block> transform;

        public ColumnTransform(Type type, Function<Block, Block> transform)
        {
            this.type = requireNonNull(type, "resultType is null");
            this.transform = requireNonNull(transform, "transform is null");
        }

        public Type getType()
        {
            return type;
        }

        public Function<Block, Block> getTransform()
        {
            return transform;
        }
    }
}
