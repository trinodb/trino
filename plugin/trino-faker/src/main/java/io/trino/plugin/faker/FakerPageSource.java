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
package io.trino.plugin.faker;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Int128Math;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.type.IpAddressType;
import net.datafaker.Faker;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.faker.FakerMetadata.ROW_ID_COLUMN_NAME;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.type.IpAddressType.IPADDRESS;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;

class FakerPageSource
        implements ConnectorPageSource
{
    static final long[] POWERS_OF_TEN = {
            1L,
            10L,
            100L,
            1_000L,
            10_000L,
            100_000L,
            1_000_000L,
            10_000_000L,
            100_000_000L,
            1_000_000_000L,
            10_000_000_000L,
            100_000_000_000L,
            1_000_000_000_000L,
            10_000_000_000_000L,
            100_000_000_000_000L,
            1_000_000_000_000_000L,
            10_000_000_000_000_000L,
            100_000_000_000_000_000L,
            1_000_000_000_000_000_000L
    };

    private static final int ROWS_PER_PAGE = 4096;

    private final Random random;
    private final Faker faker;
    private final long limit;
    private final List<Generator> generators;
    private long completedRows;

    private final PageBuilder pageBuilder;

    private boolean closed;

    FakerPageSource(
            Faker faker,
            Random random,
            List<FakerColumnHandle> columns,
            TupleDomain<ColumnHandle> constraint,
            long offset,
            long limit)
    {
        this.faker = requireNonNull(faker, "faker is null");
        this.random = requireNonNull(random, "random is null");
        List<Type> types = requireNonNull(columns, "columns is null")
                .stream()
                .map(FakerColumnHandle::type)
                .collect(toImmutableList());
        requireNonNull(constraint, "constraint is null");
        this.limit = limit;

        this.generators = columns
                .stream()
                .map(column -> getGenerator(column, constraint, offset))
                .collect(toImmutableList());
        this.pageBuilder = new PageBuilder(types);
    }

    private Generator getGenerator(
            FakerColumnHandle column,
            TupleDomain<ColumnHandle> constraint,
            long offset)
    {
        if (ROW_ID_COLUMN_NAME.equals(column.name())) {
            return new Generator() {
                long currentRowId = offset;
                @Override
                public void accept(BlockBuilder blockBuilder)
                {
                    BIGINT.writeLong(blockBuilder, currentRowId++);
                }
            };
        }

        return constraintedValueGenerator(
                column,
                constraint.getDomains().get().getOrDefault(column, Domain.all(column.type())));
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return closed && pageBuilder.isEmpty();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (!closed) {
            int positions = (int) Math.min(limit - completedRows, ROWS_PER_PAGE);
            if (positions <= 0) {
                closed = true;
            }
            else {
                pageBuilder.declarePositions(positions);
                for (int column = 0; column < generators.size(); column++) {
                    BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(column);
                    Generator generator = generators.get(column);
                    for (int i = 0; i < positions; i++) {
                        generator.accept(blockBuilder);
                    }
                }
                completedRows += positions;
            }
        }

        // only return a page if the buffer is full, or we are finishing
        if ((closed && !pageBuilder.isEmpty()) || pageBuilder.isFull()) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return SourcePage.create(page);
        }

        return null;
    }

    @Override
    public long getMemoryUsage()
    {
        return pageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void close()
    {
        closed = true;
    }

    private Generator constraintedValueGenerator(FakerColumnHandle handle, Domain domain)
    {
        if (domain.isSingleValue()) {
            ObjectWriter singleValueWriter = objectWriter(handle.type());
            return (blockBuilder) -> singleValueWriter.accept(blockBuilder, domain.getSingleValue());
        }
        if (domain.getValues().isDiscreteSet()) {
            List<Object> values = domain.getValues().getDiscreteSet();
            ObjectWriter singleValueWriter = objectWriter(handle.type());
            return (blockBuilder) -> singleValueWriter.accept(blockBuilder, values.get(random.nextInt(values.size())));
        }
        if (domain.getValues().getRanges().getRangeCount() > 1) {
            // this would require calculating weights for each range to retain uniform distribution
            throw new TrinoException(INVALID_ROW_FILTER, "Generating random values from more than one range is not supported");
        }
        Generator generator = randomValueGenerator(handle, domain.getValues().getRanges().getSpan());
        if (handle.nullProbability() == 0) {
            return generator;
        }
        return (blockBuilder) -> {
            if (random.nextDouble() <= handle.nullProbability()) {
                blockBuilder.appendNull();
            }
            else {
                generator.accept(blockBuilder);
            }
        };
    }

    private Generator randomValueGenerator(FakerColumnHandle handle, Range range)
    {
        if (handle.generator() != null) {
            if (!range.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for columns with a generator expression are not supported");
            }
            return (blockBuilder) -> VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(faker.expression(handle.generator())));
        }
        Type type = handle.type();
        // check every type in order defined in StandardTypes
        if (BIGINT.equals(type)) {
            return (blockBuilder) -> BIGINT.writeLong(blockBuilder, generateLong(range, 1));
        }
        if (INTEGER.equals(type)) {
            return (blockBuilder) -> INTEGER.writeLong(blockBuilder, generateInt(range));
        }
        if (SMALLINT.equals(type)) {
            return (blockBuilder) -> SMALLINT.writeLong(blockBuilder, generateShort(range));
        }
        if (TINYINT.equals(type)) {
            return (blockBuilder) -> TINYINT.writeLong(blockBuilder, generateTiny(range));
        }
        if (BOOLEAN.equals(type)) {
            if (!range.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Range or not a single value predicates for boolean columns are not supported");
            }
            return (blockBuilder) -> BOOLEAN.writeBoolean(blockBuilder, random.nextBoolean());
        }
        if (DATE.equals(type)) {
            return (blockBuilder) -> DATE.writeLong(blockBuilder, generateInt(range));
        }
        if (type instanceof DecimalType decimalType) {
            return decimalGenerator(range, decimalType);
        }
        if (REAL.equals(type)) {
            return (blockBuilder) -> REAL.writeLong(blockBuilder, floatToRawIntBits(generateFloat(range)));
        }
        if (DOUBLE.equals(type)) {
            return (blockBuilder) -> DOUBLE.writeDouble(blockBuilder, generateDouble(range));
        }
        // not supported: HYPER_LOG_LOG, QDIGEST, TDIGEST, P4_HYPER_LOG_LOG
        if (INTERVAL_DAY_TIME.equals(type)) {
            return (blockBuilder) -> INTERVAL_DAY_TIME.writeLong(blockBuilder, generateLong(range, 1));
        }
        if (INTERVAL_YEAR_MONTH.equals(type)) {
            return (blockBuilder) -> INTERVAL_YEAR_MONTH.writeLong(blockBuilder, generateInt(range));
        }
        if (type instanceof TimestampType) {
            return timestampGenerator(range, (TimestampType) type);
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return timestampWithTimeZoneGenerator(range, (TimestampWithTimeZoneType) type);
        }
        if (type instanceof TimeType timeType) {
            long factor = POWERS_OF_TEN[12 - timeType.getPrecision()];
            return (blockBuilder) -> timeType.writeLong(blockBuilder, generateLongDefaults(range, factor, 0, PICOSECONDS_PER_DAY));
        }
        if (type instanceof TimeWithTimeZoneType) {
            return timeWithTimeZoneGenerator(range, (TimeWithTimeZoneType) type);
        }
        if (type instanceof VarbinaryType varType) {
            if (!range.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for varbinary columns are not supported");
            }
            return (blockBuilder) -> varType.writeSlice(blockBuilder, Slices.utf8Slice(faker.lorem().sentence(3 + random.nextInt(38))));
        }
        if (type instanceof VarcharType varcharType) {
            if (!range.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for varchar columns are not supported");
            }
            if (varcharType.getLength().isPresent()) {
                int length = varcharType.getLength().get();
                return (blockBuilder) -> varcharType.writeSlice(blockBuilder, Slices.utf8Slice(faker.lorem().maxLengthSentence(random.nextInt(length))));
            }
            return (blockBuilder) -> varcharType.writeSlice(blockBuilder, Slices.utf8Slice(faker.lorem().sentence(3 + random.nextInt(38))));
        }
        if (type instanceof CharType charType) {
            if (!range.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for char columns are not supported");
            }
            return (blockBuilder) -> charType.writeSlice(blockBuilder, Slices.utf8Slice(faker.lorem().maxLengthSentence(charType.getLength())));
        }
        // not supported: ROW, ARRAY, MAP, JSON
        if (type instanceof IpAddressType) {
            return generateIpV4(range);
        }
        // not supported: GEOMETRY
        if (type instanceof UuidType) {
            return generateUUID(range);
        }

        throw new IllegalArgumentException("Unsupported type " + type);
    }

    private ObjectWriter objectWriter(Type type)
    {
        // check every type in order defined in StandardTypes
        if (BIGINT.equals(type)) {
            return (blockBuilder, value) -> BIGINT.writeLong(blockBuilder, (Long) value);
        }
        if (INTEGER.equals(type)) {
            return (blockBuilder, value) -> INTEGER.writeLong(blockBuilder, (Long) value);
        }
        if (SMALLINT.equals(type)) {
            return (blockBuilder, value) -> SMALLINT.writeLong(blockBuilder, (Long) value);
        }
        if (TINYINT.equals(type)) {
            return (blockBuilder, value) -> TINYINT.writeLong(blockBuilder, (Long) value);
        }
        if (BOOLEAN.equals(type)) {
            return (blockBuilder, value) -> BOOLEAN.writeBoolean(blockBuilder, (Boolean) value);
        }
        if (DATE.equals(type)) {
            return (blockBuilder, value) -> DATE.writeLong(blockBuilder, (Long) value);
        }
        if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                return (blockBuilder, value) -> decimalType.writeLong(blockBuilder, (Long) value);
            }
            else {
                return decimalType::writeObject;
            }
        }
        if (REAL.equals(type)) {
            return (blockBuilder, value) -> REAL.writeLong(blockBuilder, (Long) value);
        }
        if (DOUBLE.equals(type)) {
            return (blockBuilder, value) -> DOUBLE.writeDouble(blockBuilder, (Double) value);
        }
        // not supported: HYPER_LOG_LOG, QDIGEST, TDIGEST, P4_HYPER_LOG_LOG
        if (INTERVAL_DAY_TIME.equals(type)) {
            return (blockBuilder, value) -> INTERVAL_DAY_TIME.writeLong(blockBuilder, (Long) value);
        }
        if (INTERVAL_YEAR_MONTH.equals(type)) {
            return (blockBuilder, value) -> INTERVAL_YEAR_MONTH.writeLong(blockBuilder, (Long) value);
        }
        if (type instanceof TimestampType tzType) {
            if (tzType.isShort()) {
                return (blockBuilder, value) -> tzType.writeLong(blockBuilder, (Long) value);
            }
            else {
                return tzType::writeObject;
            }
        }
        if (type instanceof TimestampWithTimeZoneType tzType) {
            if (tzType.isShort()) {
                return (blockBuilder, value) -> tzType.writeLong(blockBuilder, (Long) value);
            }
            else {
                return tzType::writeObject;
            }
        }
        if (type instanceof TimeType timeType) {
            return (blockBuilder, value) -> timeType.writeLong(blockBuilder, (Long) value);
        }
        if (type instanceof TimeWithTimeZoneType tzType) {
            if (tzType.isShort()) {
                return (blockBuilder, value) -> tzType.writeLong(blockBuilder, (Long) value);
            }
            else {
                return tzType::writeObject;
            }
        }
        if (type instanceof VarbinaryType varType) {
            return (blockBuilder, value) -> varType.writeSlice(blockBuilder, (Slice) value);
        }
        if (type instanceof VarcharType varType) {
            return (blockBuilder, value) -> varType.writeSlice(blockBuilder, (Slice) value);
        }
        if (type instanceof CharType charType) {
            return (blockBuilder, value) -> charType.writeSlice(blockBuilder, (Slice) value);
        }
        // not supported: ROW, ARRAY, MAP, JSON
        if (type instanceof IpAddressType) {
            return (blockBuilder, value) -> IPADDRESS.writeSlice(blockBuilder, (Slice) value);
        }
        // not supported: GEOMETRY
        if (type instanceof UuidType) {
            return (blockBuilder, value) -> UUID.writeSlice(blockBuilder, (Slice) value);
        }

        throw new IllegalArgumentException("Unsupported type " + type);
    }

    private long generateLong(Range range, long factor)
    {
        return generateLongDefaults(range, factor, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    private long generateLongDefaults(Range range, long factor, long min, long max)
    {
        return faker.number().numberBetween(
                roundDiv((long) range.getLowValue().orElse(min), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                // TODO does the inclusion only apply to positive numbers?
                roundDiv((long) range.getHighValue().orElse(max), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0)) * factor;
    }

    private int generateInt(Range range)
    {
        return (int) faker.number().numberBetween(
                (long) range.getLowValue().orElse((long) Integer.MIN_VALUE) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                (long) range.getHighValue().orElse((long) Integer.MAX_VALUE) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0));
    }

    private short generateShort(Range range)
    {
        return (short) faker.number().numberBetween(
                (long) range.getLowValue().orElse((long) Short.MIN_VALUE) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                (long) range.getHighValue().orElse((long) Short.MAX_VALUE) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0));
    }

    private byte generateTiny(Range range)
    {
        return (byte) faker.number().numberBetween(
                (long) range.getLowValue().orElse((long) Byte.MIN_VALUE) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                (long) range.getHighValue().orElse((long) Byte.MAX_VALUE) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0));
    }

    private float generateFloat(Range range)
    {
        // TODO normalize ranges in applyFilter, so they always have bounds
        float minValue = range.getLowValue().map(v -> intBitsToFloat(toIntExact((long) v))).orElse(Float.MIN_VALUE);
        if (!range.isLowUnbounded() && !range.isLowInclusive()) {
            minValue = Math.nextUp(minValue);
        }
        float maxValue = range.getHighValue().map(v -> intBitsToFloat(toIntExact((long) v))).orElse(Float.MAX_VALUE);
        if (!range.isHighUnbounded() && !range.isHighInclusive()) {
            maxValue = Math.nextDown(maxValue);
        }
        return minValue + (maxValue - minValue) * random.nextFloat();
    }

    private double generateDouble(Range range)
    {
        double minValue = (double) range.getLowValue().orElse(Double.MIN_VALUE);
        if (!range.isLowUnbounded() && !range.isLowInclusive()) {
            minValue = Math.nextUp(minValue);
        }
        double maxValue = (double) range.getHighValue().orElse(Double.MAX_VALUE);
        if (!range.isHighUnbounded() && !range.isHighInclusive()) {
            maxValue = Math.nextDown(maxValue);
        }
        return minValue + (maxValue - minValue) * random.nextDouble();
    }

    private Generator decimalGenerator(Range range, DecimalType decimalType)
    {
        if (decimalType.isShort()) {
            long min = -999999999999999999L / POWERS_OF_TEN[18 - decimalType.getPrecision()];
            long max = 999999999999999999L / POWERS_OF_TEN[18 - decimalType.getPrecision()];
            return (blockBuilder) -> decimalType.writeLong(blockBuilder, generateLongDefaults(range, 1, min, max));
        }
        Int128 low = (Int128) range.getLowValue().orElse(Decimals.MIN_UNSCALED_DECIMAL);
        Int128 high = (Int128) range.getHighValue().orElse(Decimals.MAX_UNSCALED_DECIMAL);
        if (!range.isLowUnbounded() && !range.isLowInclusive()) {
            long[] result = new long[2];
            Int128Math.add(low.getHigh(), low.getLow(), 0, 1, result, 0);
            low = Int128.valueOf(result);
        }
        if (!range.isHighUnbounded() && range.isHighInclusive()) {
            long[] result = new long[2];
            Int128Math.add(high.getHigh(), high.getLow(), 0, 1, result, 0);
            high = Int128.valueOf(result);
        }

        BigInteger currentRange = BigInteger.valueOf(Long.MAX_VALUE);
        BigInteger desiredRange = high.toBigInteger().subtract(low.toBigInteger());
        Int128 finalLow = low;
        return (blockBuilder) -> decimalType.writeObject(blockBuilder, Int128.valueOf(
                new BigInteger(63, random).multiply(desiredRange).divide(currentRange).add(finalLow.toBigInteger())));
    }

    private Generator timestampGenerator(Range range, TimestampType tzType)
    {
        if (tzType.isShort()) {
            long factor = POWERS_OF_TEN[6 - tzType.getPrecision()];
            return (blockBuilder) -> tzType.writeLong(blockBuilder, generateLong(range, factor));
        }
        LongTimestamp low = (LongTimestamp) range.getLowValue()
                .orElse(new LongTimestamp(Long.MIN_VALUE, 0));
        LongTimestamp high = (LongTimestamp) range.getHighValue()
                .orElse(new LongTimestamp(Long.MAX_VALUE, PICOSECONDS_PER_MICROSECOND - 1));
        int factor;
        if (tzType.getPrecision() <= 6) {
            factor = (int) POWERS_OF_TEN[6 - tzType.getPrecision()];
            low = new LongTimestamp(
                    roundDiv(low.getEpochMicros(), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                    0);
            high = new LongTimestamp(
                    roundDiv(high.getEpochMicros(), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0),
                    0);
        }
        else {
            factor = (int) POWERS_OF_TEN[12 - tzType.getPrecision()];
            int lowPicosOfMicro = roundDiv(low.getPicosOfMicro(), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0);
            low = new LongTimestamp(
                    low.getEpochMicros() - (lowPicosOfMicro < 0 ? 1 : 0),
                    (lowPicosOfMicro + factor) % factor);
            int highPicosOfMicro = roundDiv(high.getPicosOfMicro(), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0);
            high = new LongTimestamp(
                    high.getEpochMicros() + (highPicosOfMicro > factor ? 1 : 0),
                    highPicosOfMicro % factor);
        }
        LongTimestamp finalLow = low;
        LongTimestamp finalHigh = high;
        return (blockBuilder) -> {
            long epochMicros = faker.number().numberBetween(finalLow.getEpochMicros(), finalHigh.getEpochMicros());
            if (tzType.getPrecision() <= 6) {
                epochMicros *= factor;
                tzType.writeObject(blockBuilder, new LongTimestamp(epochMicros * factor, 0));
                return;
            }
            int picosOfMicro;
            if (epochMicros == finalLow.getEpochMicros()) {
                picosOfMicro = faker.number().numberBetween(
                        finalLow.getPicosOfMicro(),
                        finalLow.getEpochMicros() == finalHigh.getEpochMicros() ?
                                finalHigh.getPicosOfMicro()
                                : (int) POWERS_OF_TEN[tzType.getPrecision() - 6] - 1);
            }
            else if (epochMicros == finalHigh.getEpochMicros()) {
                picosOfMicro = faker.number().numberBetween(0, finalHigh.getPicosOfMicro());
            }
            else {
                picosOfMicro = faker.number().numberBetween(0, (int) POWERS_OF_TEN[tzType.getPrecision() - 6] - 1);
            }
            tzType.writeObject(blockBuilder, new LongTimestamp(epochMicros, picosOfMicro * factor));
        };
    }

    private Generator timestampWithTimeZoneGenerator(Range range, TimestampWithTimeZoneType tzType)
    {
        if (tzType.isShort()) {
            TimeZoneKey defaultTZ = range.getLowValue()
                    .map(v -> unpackZoneKey((long) v))
                    .orElse(range.getHighValue()
                            .map(v -> unpackZoneKey((long) v))
                            .orElse(TimeZoneKey.UTC_KEY));
            long factor = POWERS_OF_TEN[3 - tzType.getPrecision()];
            return (blockBuilder) -> {
                long millis = faker.number().numberBetween(
                        roundDiv(unpackMillisUtc((long) range.getLowValue().orElse(Long.MIN_VALUE)), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                        roundDiv(unpackMillisUtc((long) range.getHighValue().orElse(Long.MAX_VALUE)), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0)) * factor;
                tzType.writeLong(blockBuilder, packDateTimeWithZone(millis, defaultTZ));
            };
        }
        short defaultTZ = range.getLowValue()
                .map(v -> ((LongTimestampWithTimeZone) v).getTimeZoneKey())
                .orElse(range.getHighValue()
                        .map(v -> ((LongTimestampWithTimeZone) v).getTimeZoneKey())
                        .orElse(TimeZoneKey.UTC_KEY.getKey()));
        LongTimestampWithTimeZone low = (LongTimestampWithTimeZone) range.getLowValue()
                .orElse(fromEpochMillisAndFraction(Long.MIN_VALUE >> 12, 0, defaultTZ));
        LongTimestampWithTimeZone high = (LongTimestampWithTimeZone) range.getHighValue()
                .orElse(fromEpochMillisAndFraction(Long.MAX_VALUE >> 12, PICOSECONDS_PER_MILLISECOND - 1, defaultTZ));
        if (low.getTimeZoneKey() != high.getTimeZoneKey()) {
            throw new TrinoException(INVALID_ROW_FILTER, "Range boundaries for timestamp with time zone columns must have the same time zone");
        }
        int factor = (int) POWERS_OF_TEN[12 - tzType.getPrecision()];
        int lowPicosOfMilli = roundDiv(low.getPicosOfMilli(), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0);
        low = fromEpochMillisAndFraction(
                low.getEpochMillis() - (lowPicosOfMilli < 0 ? 1 : 0),
                (lowPicosOfMilli + factor) % factor,
                low.getTimeZoneKey());
        int highPicosOfMilli = roundDiv(high.getPicosOfMilli(), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0);
        high = fromEpochMillisAndFraction(
                high.getEpochMillis() + (highPicosOfMilli > factor ? 1 : 0),
                highPicosOfMilli % factor,
                high.getTimeZoneKey());
        LongTimestampWithTimeZone finalLow = low;
        LongTimestampWithTimeZone finalHigh = high;
        return (blockBuilder) -> {
            long millis = faker.number().numberBetween(finalLow.getEpochMillis(), finalHigh.getEpochMillis());
            int picosOfMilli;
            if (millis == finalLow.getEpochMillis()) {
                picosOfMilli = faker.number().numberBetween(
                        finalLow.getPicosOfMilli(),
                        finalLow.getEpochMillis() == finalHigh.getEpochMillis() ?
                                finalHigh.getPicosOfMilli()
                                : (int) POWERS_OF_TEN[tzType.getPrecision() - 3] - 1);
            }
            else if (millis == finalHigh.getEpochMillis()) {
                picosOfMilli = faker.number().numberBetween(0, finalHigh.getPicosOfMilli());
            }
            else {
                picosOfMilli = faker.number().numberBetween(0, (int) POWERS_OF_TEN[tzType.getPrecision() - 3] - 1);
            }
            tzType.writeObject(blockBuilder, fromEpochMillisAndFraction(millis, picosOfMilli * factor, defaultTZ));
        };
    }

    private Generator timeWithTimeZoneGenerator(Range range, TimeWithTimeZoneType timeType)
    {
        if (timeType.isShort()) {
            int offsetMinutes = range.getLowValue()
                    .map(v -> unpackOffsetMinutes((long) v))
                    .orElse(range.getHighValue()
                            .map(v -> unpackOffsetMinutes((long) v))
                            .orElse(0));
            long factor = POWERS_OF_TEN[9 - timeType.getPrecision()];
            long low = roundDiv(range.getLowValue().map(v -> unpackTimeNanos((long) v)).orElse(0L), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0);
            long high = roundDiv(range.getHighValue().map(v -> unpackTimeNanos((long) v)).orElse(NANOSECONDS_PER_DAY), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0);
            return (blockBuilder) -> {
                long nanos = faker.number().numberBetween(low, high) * factor;
                timeType.writeLong(blockBuilder, packTimeWithTimeZone(nanos, offsetMinutes));
            };
        }
        int offsetMinutes = range.getLowValue()
                .map(v -> ((LongTimeWithTimeZone) v).getOffsetMinutes())
                .orElse(range.getHighValue()
                        .map(v -> ((LongTimeWithTimeZone) v).getOffsetMinutes())
                        .orElse(0));
        LongTimeWithTimeZone low = (LongTimeWithTimeZone) range.getLowValue()
                .orElse(new LongTimeWithTimeZone(0, offsetMinutes));
        LongTimeWithTimeZone high = (LongTimeWithTimeZone) range.getHighValue()
                .orElse(new LongTimeWithTimeZone(PICOSECONDS_PER_DAY, offsetMinutes));
        if (low.getOffsetMinutes() != high.getOffsetMinutes()) {
            throw new TrinoException(INVALID_ROW_FILTER, "Range boundaries for time with time zone columns must have the same time zone");
        }
        int factor = (int) POWERS_OF_TEN[12 - timeType.getPrecision()];
        long longLow = roundDiv(low.getPicoseconds(), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0);
        long longHigh = roundDiv(high.getPicoseconds(), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0);
        return (blockBuilder) -> {
            long picoseconds = faker.number().numberBetween(longLow, longHigh) * factor;
            timeType.writeObject(blockBuilder, new LongTimeWithTimeZone(picoseconds, offsetMinutes));
        };
    }

    private Generator generateIpV4(Range range)
    {
        if (!range.isAll()) {
            throw new TrinoException(INVALID_ROW_FILTER, "Predicates for ipaddress columns are not supported");
        }
        return (blockBuilder) -> {
            byte[] address;
            try {
                address = Inet4Address.getByAddress(new byte[] {
                        (byte) (random.nextInt(254) + 2),
                        (byte) (random.nextInt(254) + 2),
                        (byte) (random.nextInt(254) + 2),
                        (byte) (random.nextInt(254) + 2)}).getAddress();
            }
            catch (UnknownHostException e) {
                // ignore
                blockBuilder.appendNull();
                return;
            }

            byte[] bytes = new byte[16];
            bytes[10] = (byte) 0xff;
            bytes[11] = (byte) 0xff;
            arraycopy(address, 0, bytes, 12, 4);

            IPADDRESS.writeSlice(blockBuilder, Slices.wrappedBuffer(bytes, 0, 16));
        };
    }

    private Generator generateUUID(Range range)
    {
        if (!range.isAll()) {
            throw new TrinoException(INVALID_ROW_FILTER, "Predicates for uuid columns are not supported");
        }
        return (blockBuilder) -> {
            java.util.UUID uuid = java.util.UUID.randomUUID();
            ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
            bb.putLong(uuid.getMostSignificantBits());
            bb.putLong(uuid.getLeastSignificantBits());
            UUID.writeSlice(blockBuilder, Slices.wrappedBuffer(bb.array(), 0, 16));
        };
    }

    @FunctionalInterface
    private interface ObjectWriter
    {
        void accept(BlockBuilder blockBuilder, Object value);
    }

    @FunctionalInterface
    private interface Generator
    {
        void accept(BlockBuilder blockBuilder);
    }
}
