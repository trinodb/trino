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
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.Range;
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

import java.math.BigDecimal;
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
    private final SentenceGenerator sentenceGenerator;
    private final BoundedSentenceGenerator boundedSentenceGenerator;
    private final long limit;
    private final List<Generator> generators;
    private long completedRows;

    private final PageBuilder pageBuilder;

    private boolean closed;

    FakerPageSource(
            Faker faker,
            Random random,
            List<FakerColumnHandle> columns,
            long rowOffset,
            long limit)
    {
        this.faker = requireNonNull(faker, "faker is null");
        this.random = requireNonNull(random, "random is null");
        this.sentenceGenerator = () -> Slices.utf8Slice(faker.lorem().sentence(3 + random.nextInt(38)));
        this.boundedSentenceGenerator = (maxLength) -> Slices.utf8Slice(faker.lorem().maxLengthSentence(maxLength));
        List<Type> types = requireNonNull(columns, "columns is null")
                .stream()
                .map(FakerColumnHandle::type)
                .collect(toImmutableList());
        this.limit = limit;

        this.generators = columns
                .stream()
                .map(column -> getGenerator(column, rowOffset))
                .collect(toImmutableList());
        this.pageBuilder = new PageBuilder(types);
    }

    private Generator getGenerator(
            FakerColumnHandle column,
            long rowOffset)
    {
        if (ROW_ID_COLUMN_NAME.equals(column.name())) {
            return new Generator()
            {
                long currentRowId = rowOffset;

                @Override
                public void accept(BlockBuilder blockBuilder)
                {
                    BIGINT.writeLong(blockBuilder, currentRowId++);
                }
            };
        }

        if (column.domain().getValues().isDiscreteSet()) {
            List<Object> values = column.domain().getValues().getDiscreteSet();
            ObjectWriter singleValueWriter = objectWriter(column.type());
            return (blockBuilder) -> singleValueWriter.accept(blockBuilder, values.get(random.nextInt(values.size())));
        }
        Generator generator = randomValueGenerator(column);
        if (column.nullProbability() == 0) {
            return generator;
        }
        return (blockBuilder) -> {
            if (random.nextDouble() <= column.nullProbability()) {
                blockBuilder.appendNull();
            }
            else {
                generator.accept(blockBuilder);
            }
        };
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
    public Page getNextPage()
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
            return page;
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

    private Generator randomValueGenerator(FakerColumnHandle handle)
    {
        Range genericRange = handle.domain().getValues().getRanges().getSpan();
        if (handle.generator() != null) {
            if (!genericRange.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for columns with a generator expression are not supported");
            }
            return (blockBuilder) -> VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(faker.expression(handle.generator())));
        }
        Type type = handle.type();
        // check every type in order defined in StandardTypes
        if (BIGINT.equals(type)) {
            LongRange range = LongRange.of(genericRange);
            return (blockBuilder) -> BIGINT.writeLong(blockBuilder, numberBetween(range.low, range.high));
        }
        if (INTEGER.equals(type)) {
            IntRange range = IntRange.of(genericRange);
            return (blockBuilder) -> INTEGER.writeLong(blockBuilder, numberBetween(range.low, range.high));
        }
        if (SMALLINT.equals(type)) {
            IntRange range = IntRange.of(genericRange, Short.MIN_VALUE, Short.MAX_VALUE);
            return (blockBuilder) -> SMALLINT.writeLong(blockBuilder, numberBetween(range.low, range.high));
        }
        if (TINYINT.equals(type)) {
            IntRange range = IntRange.of(genericRange, Byte.MIN_VALUE, Byte.MAX_VALUE);
            return (blockBuilder) -> TINYINT.writeLong(blockBuilder, numberBetween(range.low, range.high));
        }
        if (BOOLEAN.equals(type)) {
            if (!genericRange.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Range or not a single value predicates for boolean columns are not supported");
            }
            return (blockBuilder) -> BOOLEAN.writeBoolean(blockBuilder, random.nextBoolean());
        }
        if (DATE.equals(type)) {
            IntRange range = IntRange.of(genericRange);
            return (blockBuilder) -> DATE.writeLong(blockBuilder, numberBetween(range.low, range.high));
        }
        if (type instanceof DecimalType decimalType) {
            return decimalGenerator(genericRange, decimalType);
        }
        if (REAL.equals(type)) {
            FloatRange range = FloatRange.of(genericRange);
            return (blockBuilder) -> REAL.writeLong(blockBuilder, floatToRawIntBits(range.low == range.high ? range.low : random.nextFloat(range.low, range.high)));
        }
        if (DOUBLE.equals(type)) {
            DoubleRange range = DoubleRange.of(genericRange);
            return (blockBuilder) -> DOUBLE.writeDouble(blockBuilder, range.low == range.high ? range.low : random.nextDouble(range.low, range.high));
        }
        // not supported: HYPER_LOG_LOG, QDIGEST, TDIGEST, P4_HYPER_LOG_LOG
        if (INTERVAL_DAY_TIME.equals(type)) {
            LongRange range = LongRange.of(genericRange);
            return (blockBuilder) -> INTERVAL_DAY_TIME.writeLong(blockBuilder, numberBetween(range.low, range.high));
        }
        if (INTERVAL_YEAR_MONTH.equals(type)) {
            IntRange range = IntRange.of(genericRange);
            return (blockBuilder) -> INTERVAL_YEAR_MONTH.writeLong(blockBuilder, numberBetween(range.low, range.high));
        }
        if (type instanceof TimestampType timestampType) {
            return timestampGenerator(genericRange, timestampType);
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            return timestampWithTimeZoneGenerator(genericRange, timestampWithTimeZoneType);
        }
        if (type instanceof TimeType timeType) {
            long factor = POWERS_OF_TEN[12 - timeType.getPrecision()];
            LongRange range = LongRange.of(genericRange, factor, 0, PICOSECONDS_PER_DAY);
            return (blockBuilder) -> timeType.writeLong(blockBuilder, numberBetween(range.low, range.high) * factor);
        }
        if (type instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
            return timeWithTimeZoneGenerator(genericRange, timeWithTimeZoneType);
        }
        if (type instanceof VarbinaryType varType) {
            if (!genericRange.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for varbinary columns are not supported");
            }
            return (blockBuilder) -> varType.writeSlice(blockBuilder, sentenceGenerator.get());
        }
        if (type instanceof VarcharType varcharType) {
            if (!genericRange.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for varchar columns are not supported");
            }
            if (varcharType.getLength().isPresent()) {
                int length = varcharType.getLength().get();
                return (blockBuilder) -> varcharType.writeSlice(blockBuilder, boundedSentenceGenerator.get(random.nextInt(length)));
            }
            return (blockBuilder) -> varcharType.writeSlice(blockBuilder, sentenceGenerator.get());
        }
        if (type instanceof CharType charType) {
            if (!genericRange.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for char columns are not supported");
            }
            return (blockBuilder) -> charType.writeSlice(blockBuilder, boundedSentenceGenerator.get(charType.getLength()));
        }
        // not supported: ROW, ARRAY, MAP, JSON
        if (type instanceof IpAddressType) {
            return generateIpV4(genericRange);
        }
        // not supported: GEOMETRY
        if (type instanceof UuidType) {
            return generateUUID(genericRange);
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

    private Generator decimalGenerator(Range genericRange, DecimalType decimalType)
    {
        if (decimalType.isShort()) {
            ShortDecimalRange range = ShortDecimalRange.of(genericRange, decimalType.getPrecision());
            return (blockBuilder) -> decimalType.writeLong(blockBuilder, numberBetween(range.low, range.high));
        }
        Int128Range range = Int128Range.of(genericRange);
        BigInteger currentRange = BigInteger.valueOf(Long.MAX_VALUE);
        BigInteger desiredRange = range.high.toBigInteger().subtract(range.low.toBigInteger());
        return (blockBuilder) -> decimalType.writeObject(blockBuilder, Int128.valueOf(
                new BigInteger(63, random).multiply(desiredRange).divide(currentRange).add(range.low.toBigInteger())));
    }

    private Generator timestampGenerator(Range genericRange, TimestampType tzType)
    {
        if (tzType.isShort()) {
            long factor = POWERS_OF_TEN[6 - tzType.getPrecision()];
            LongRange range = LongRange.of(genericRange, factor);
            return (blockBuilder) -> tzType.writeLong(blockBuilder, numberBetween(range.low, range.high) * factor);
        }
        LongTimestampRange range = LongTimestampRange.of(genericRange, tzType.getPrecision());
        if (tzType.getPrecision() <= 6) {
            return (blockBuilder) -> {
                long epochMicros = numberBetween(range.low.getEpochMicros(), range.high.getEpochMicros());
                tzType.writeObject(blockBuilder, new LongTimestamp(epochMicros * range.factor, 0));
            };
        }
        return (blockBuilder) -> {
            long epochMicros = numberBetween(range.low.getEpochMicros(), range.high.getEpochMicros());
            int picosOfMicro;
            if (epochMicros == range.low.getEpochMicros()) {
                picosOfMicro = numberBetween(
                        range.low.getPicosOfMicro(),
                        range.low.getEpochMicros() == range.high.getEpochMicros() ?
                                range.high.getPicosOfMicro()
                                : (int) POWERS_OF_TEN[tzType.getPrecision() - 6] - 1);
            }
            else if (epochMicros == range.high.getEpochMicros()) {
                picosOfMicro = numberBetween(0, range.high.getPicosOfMicro());
            }
            else {
                picosOfMicro = numberBetween(0, (int) POWERS_OF_TEN[tzType.getPrecision() - 6] - 1);
            }
            tzType.writeObject(blockBuilder, new LongTimestamp(epochMicros, picosOfMicro * range.factor));
        };
    }

    private Generator timestampWithTimeZoneGenerator(Range genericRange, TimestampWithTimeZoneType tzType)
    {
        if (tzType.isShort()) {
            ShortTimestampWithTimeZoneRange range = ShortTimestampWithTimeZoneRange.of(genericRange, tzType.getPrecision());
            return (blockBuilder) -> {
                long millis = numberBetween(range.low, range.high) * range.factor;
                tzType.writeLong(blockBuilder, packDateTimeWithZone(millis, range.defaultTZ));
            };
        }
        LongTimestampWithTimeZoneRange range = LongTimestampWithTimeZoneRange.of(genericRange, tzType.getPrecision());
        int picosOfMilliHigh = (int) POWERS_OF_TEN[tzType.getPrecision() - 3] - 1;
        return (blockBuilder) -> {
            long millis = numberBetween(range.low.getEpochMillis(), range.high.getEpochMillis());
            int picosOfMilli;
            if (millis == range.low.getEpochMillis()) {
                picosOfMilli = numberBetween(
                        range.low.getPicosOfMilli(),
                        range.low.getEpochMillis() == range.high.getEpochMillis() ?
                                range.high.getPicosOfMilli()
                                : picosOfMilliHigh);
            }
            else if (millis == range.high.getEpochMillis()) {
                picosOfMilli = numberBetween(0, range.high.getPicosOfMilli());
            }
            else {
                picosOfMilli = numberBetween(0, picosOfMilliHigh);
            }
            tzType.writeObject(blockBuilder, fromEpochMillisAndFraction(millis, picosOfMilli * range.factor, range.defaultTZ));
        };
    }

    private Generator timeWithTimeZoneGenerator(Range genericRange, TimeWithTimeZoneType timeType)
    {
        if (timeType.isShort()) {
            ShortTimeWithTimeZoneRange range = ShortTimeWithTimeZoneRange.of(genericRange, timeType.getPrecision());
            return (blockBuilder) -> {
                long nanos = numberBetween(range.low, range.high) * range.factor;
                timeType.writeLong(blockBuilder, packTimeWithTimeZone(nanos, range.offsetMinutes));
            };
        }
        LongTimeWithTimeZoneRange range = LongTimeWithTimeZoneRange.of(genericRange, timeType.getPrecision());
        return (blockBuilder) -> {
            long picoseconds = numberBetween(range.low, range.high) * range.factor;
            timeType.writeObject(blockBuilder, new LongTimeWithTimeZone(picoseconds, range.offsetMinutes));
        };
    }

    private record LongRange(long low, long high)
    {
        static LongRange of(Range range)
        {
            return of(range, 1, Long.MIN_VALUE, Long.MAX_VALUE);
        }

        static LongRange of(Range range, long factor)
        {
            return of(range, factor, Long.MIN_VALUE, Long.MAX_VALUE);
        }

        static LongRange of(Range range, long factor, long defaultMin, long defaultMax)
        {
            return new LongRange(
                    roundDiv((long) range.getLowValue().orElse(defaultMin), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                    roundDiv((long) range.getHighValue().orElse(defaultMax), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0));
        }
    }

    private record IntRange(int low, int high)
    {
        static IntRange of(Range range)
        {
            return of(range, Integer.MIN_VALUE, Integer.MAX_VALUE);
        }

        static IntRange of(Range range, long defaultMin, long defaultMax)
        {
            return new IntRange(
                    toIntExact((long) range.getLowValue().orElse(defaultMin)) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                    toIntExact((long) range.getHighValue().orElse(defaultMax)) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0));
        }
    }

    private record FloatRange(float low, float high)
    {
        static FloatRange of(Range range)
        {
            float low = range.getLowValue().map(v -> intBitsToFloat(toIntExact((long) v))).orElse(Float.MIN_VALUE);
            if (!range.isLowUnbounded() && !range.isLowInclusive()) {
                low = Math.nextUp(low);
            }
            float high = range.getHighValue().map(v -> intBitsToFloat(toIntExact((long) v))).orElse(Float.MAX_VALUE);
            if (!range.isHighUnbounded() && range.isHighInclusive()) {
                high = Math.nextUp(high);
            }
            return new FloatRange(low, high);
        }
    }

    private record DoubleRange(double low, double high)
    {
        static DoubleRange of(Range range)
        {
            double low = (double) range.getLowValue().orElse(Double.MIN_VALUE);
            if (!range.isLowUnbounded() && !range.isLowInclusive()) {
                low = Math.nextUp(low);
            }
            double high = (double) range.getHighValue().orElse(Double.MAX_VALUE);
            if (!range.isHighUnbounded() && range.isHighInclusive()) {
                high = Math.nextUp(high);
            }
            return new DoubleRange(low, high);
        }
    }

    private record ShortDecimalRange(long low, long high)
    {
        static ShortDecimalRange of(Range range, int precision)
        {
            long defaultMin = -999999999999999999L / POWERS_OF_TEN[18 - precision];
            long defaultMax = 999999999999999999L / POWERS_OF_TEN[18 - precision];
            long low = (long) range.getLowValue().orElse(defaultMin) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0);
            long high = (long) range.getHighValue().orElse(defaultMax) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0);
            return new ShortDecimalRange(low, high);
        }
    }

    private record Int128Range(Int128 low, Int128 high)
    {
        static Int128Range of(Range range)
        {
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
            return new Int128Range(low, high);
        }
    }

    private record LongTimestampRange(LongTimestamp low, LongTimestamp high, int factor)
    {
        static LongTimestampRange of(Range range, int precision)
        {
            LongTimestamp low = (LongTimestamp) range.getLowValue().orElse(new LongTimestamp(Long.MIN_VALUE, 0));
            LongTimestamp high = (LongTimestamp) range.getHighValue().orElse(new LongTimestamp(Long.MAX_VALUE, PICOSECONDS_PER_MICROSECOND - 1));
            int factor;
            if (precision <= 6) {
                factor = (int) POWERS_OF_TEN[6 - precision];
                low = new LongTimestamp(roundDiv(low.getEpochMicros(), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0), 0);
                high = new LongTimestamp(roundDiv(high.getEpochMicros(), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0), 0);
                return new LongTimestampRange(low, high, factor);
            }
            factor = (int) POWERS_OF_TEN[12 - precision];
            int lowPicosOfMicro = roundDiv(low.getPicosOfMicro(), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0);
            low = new LongTimestamp(
                    low.getEpochMicros() - (lowPicosOfMicro < 0 ? 1 : 0),
                    (lowPicosOfMicro + factor) % factor);
            int highPicosOfMicro = roundDiv(high.getPicosOfMicro(), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0);
            high = new LongTimestamp(
                    high.getEpochMicros() + (highPicosOfMicro > factor ? 1 : 0),
                    highPicosOfMicro % factor);
            return new LongTimestampRange(low, high, factor);
        }
    }

    private record ShortTimestampWithTimeZoneRange(long low, long high, long factor, TimeZoneKey defaultTZ)
    {
        static ShortTimestampWithTimeZoneRange of(Range range, int precision)
        {
            TimeZoneKey defaultTZ = range.getLowValue()
                    .map(v -> unpackZoneKey((long) v))
                    .orElse(range.getHighValue()
                            .map(v -> unpackZoneKey((long) v))
                            .orElse(TimeZoneKey.UTC_KEY));
            long factor = POWERS_OF_TEN[3 - precision];
            long low = roundDiv(unpackMillisUtc((long) range.getLowValue().orElse(Long.MIN_VALUE)), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0);
            long high = roundDiv(unpackMillisUtc((long) range.getHighValue().orElse(Long.MAX_VALUE)), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0);
            return new ShortTimestampWithTimeZoneRange(low, high, factor, defaultTZ);
        }
    }

    private record LongTimestampWithTimeZoneRange(LongTimestampWithTimeZone low, LongTimestampWithTimeZone high, int factor, short defaultTZ)
    {
        static LongTimestampWithTimeZoneRange of(Range range, int precision)
        {
            short defaultTZ = range.getLowValue()
                    .map(v -> ((LongTimestampWithTimeZone) v).getTimeZoneKey())
                    .orElse(range.getHighValue()
                            .map(v -> ((LongTimestampWithTimeZone) v).getTimeZoneKey())
                            .orElse(TimeZoneKey.UTC_KEY.getKey()));
            LongTimestampWithTimeZone low = (LongTimestampWithTimeZone) range.getLowValue().orElse(fromEpochMillisAndFraction(Long.MIN_VALUE >> 12, 0, defaultTZ));
            LongTimestampWithTimeZone high = (LongTimestampWithTimeZone) range.getHighValue().orElse(fromEpochMillisAndFraction(Long.MAX_VALUE >> 12, PICOSECONDS_PER_MILLISECOND - 1, defaultTZ));
            if (low.getTimeZoneKey() != high.getTimeZoneKey()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Range boundaries for timestamp with time zone columns must have the same time zone");
            }
            int factor = (int) POWERS_OF_TEN[12 - precision];
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
            return new LongTimestampWithTimeZoneRange(low, high, factor, defaultTZ);
        }
    }

    private record ShortTimeWithTimeZoneRange(long low, long high, long factor, int offsetMinutes)
    {
        static ShortTimeWithTimeZoneRange of(Range range, int precision)
        {
            int offsetMinutes = range.getLowValue()
                    .map(v -> unpackOffsetMinutes((long) v))
                    .orElse(range.getHighValue()
                            .map(v -> unpackOffsetMinutes((long) v))
                            .orElse(0));
            long factor = POWERS_OF_TEN[9 - precision];
            long low = roundDiv(range.getLowValue().map(v -> unpackTimeNanos((long) v)).orElse(0L), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0);
            long high = roundDiv(range.getHighValue().map(v -> unpackTimeNanos((long) v)).orElse(NANOSECONDS_PER_DAY), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0);
            return new ShortTimeWithTimeZoneRange(low, high, factor, offsetMinutes);
        }
    }

    private record LongTimeWithTimeZoneRange(long low, long high, int factor, int offsetMinutes)
    {
        static LongTimeWithTimeZoneRange of(Range range, int precision)
        {
            int offsetMinutes = range.getLowValue()
                    .map(v -> ((LongTimeWithTimeZone) v).getOffsetMinutes())
                    .orElse(range.getHighValue()
                            .map(v -> ((LongTimeWithTimeZone) v).getOffsetMinutes())
                            .orElse(0));
            LongTimeWithTimeZone low = (LongTimeWithTimeZone) range.getLowValue().orElse(new LongTimeWithTimeZone(0, offsetMinutes));
            LongTimeWithTimeZone high = (LongTimeWithTimeZone) range.getHighValue().orElse(new LongTimeWithTimeZone(PICOSECONDS_PER_DAY, offsetMinutes));
            if (low.getOffsetMinutes() != high.getOffsetMinutes()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Range boundaries for time with time zone columns must have the same time zone");
            }
            int factor = (int) POWERS_OF_TEN[12 - precision];
            long longLow = roundDiv(low.getPicoseconds(), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0);
            long longHigh = roundDiv(high.getPicoseconds(), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0);
            return new LongTimeWithTimeZoneRange(longLow, longHigh, factor, offsetMinutes);
        }
    }

    private int numberBetween(int min, int max)
    {
        if (min == max) {
            return min;
        }
        final int realMin = Math.min(min, max);
        final int realMax = Math.max(min, max);
        final int amplitude = realMax - realMin;
        if (amplitude >= 0) {
            return random.nextInt(amplitude) + realMin;
        }
        // handle overflow
        return (int) numberBetween(realMin, (long) realMax);
    }

    private long numberBetween(long min, long max)
    {
        if (min == max) {
            return min;
        }
        final long realMin = Math.min(min, max);
        final long realMax = Math.max(min, max);
        final long amplitude = realMax - realMin;
        if (amplitude >= 0) {
            return random.nextLong(amplitude) + realMin;
        }
        // handle overflow
        final BigDecimal bigMin = BigDecimal.valueOf(min);
        final BigDecimal bigMax = BigDecimal.valueOf(max);
        final BigDecimal randomValue = BigDecimal.valueOf(random.nextDouble());

        return bigMin.add(bigMax.subtract(bigMin).multiply(randomValue)).longValue();
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

    @FunctionalInterface
    private interface SentenceGenerator
    {
        Slice get();
    }

    @FunctionalInterface
    private interface BoundedSentenceGenerator
    {
        Slice get(int maxLength);
    }
}
