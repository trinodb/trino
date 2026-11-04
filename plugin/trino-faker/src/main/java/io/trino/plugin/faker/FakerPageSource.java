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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.UnsignedLong;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.FloatingPointValueSet;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Int128Math;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import net.datafaker.Faker;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Random;
import java.util.function.LongSupplier;
import java.util.random.RandomGenerator;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.math.LongMath.saturatedAdd;
import static com.google.common.math.LongMath.saturatedMultiply;
import static io.trino.plugin.faker.Distribution.NORMAL;
import static io.trino.plugin.faker.Distribution.UNIFORM;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
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
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.ceilDiv;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
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
            1_000_000_000_000_000_000L,
    };

    private static final int ROWS_PER_PAGE = 4096;
    // limits the size of pages with deeply nested arrays and maps, whose number of values grows exponentially with their depth
    private static final int MAX_VALUES_PER_COLUMN = 16 * ROWS_PER_PAGE;
    private static final int MAX_NORMAL_ATTEMPTS = 10;
    // accepts most samples of the Zipf distribution without evaluating the hat function, see zipfOffset
    private static final double ZIPF_SQUEEZE = 2 - Math.exp(Math.log(2.5) - 0.5);
    // Small enough to keep nested arrays and maps, and the pages holding them, small,
    // while still producing empty, single, and multiple elements often.
    private static final int MAX_CARDINALITY = 5;
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    // Most systems that generated data is inserted into reject or mangle values far from the present, so temporal
    // ranges without bounds default to this window, with an exclusive high bound. It's fixed, instead of relative to
    // the current time, so generated data stays deterministic.
    private static final long DEFAULT_LOW_EPOCH_DAY = LocalDate.of(1970, 1, 1).toEpochDay();
    private static final long DEFAULT_HIGH_EPOCH_DAY = LocalDate.of(2100, 1, 1).toEpochDay();

    private final Locale locale;
    private final long rowOffset;
    private final long limit;
    private final int rowsPerPage;
    private final List<SeededColumnGenerator> generators;
    private long completedRows;

    private final PageBuilder pageBuilder;

    private boolean closed;

    FakerPageSource(
            Locale locale,
            long seed,
            List<FakerColumnHandle> columns,
            long rowOffset,
            long limit)
    {
        this.locale = requireNonNull(locale, "locale is null");
        List<Type> types = requireNonNull(columns, "columns is null")
                .stream()
                .map(FakerColumnHandle::type)
                .collect(toImmutableList());
        this.rowOffset = rowOffset;
        this.limit = limit;
        double maxValuesPerRow = types.stream()
                .mapToDouble(FakerPageSource::expectedValueCount)
                .max()
                .orElse(1);
        this.rowsPerPage = (int) Math.clamp(MAX_VALUES_PER_COLUMN / maxValuesPerRow, 1, ROWS_PER_PAGE);

        this.generators = columns
                .stream()
                .map(column -> seededColumnGenerator(seed, column))
                .collect(toImmutableList());
        this.pageBuilder = new PageBuilder(types);
    }

    private static double expectedValueCount(Type type)
    {
        return switch (type) {
            case ArrayType arrayType -> 1 + MAX_CARDINALITY / 2.0 * expectedValueCount(arrayType.getElementType());
            case MapType mapType -> 1 + MAX_CARDINALITY / 2.0 * (expectedValueCount(mapType.getKeyType()) + expectedValueCount(mapType.getValueType()));
            case RowType rowType -> 1 + rowType.getTypeParameters().stream().mapToDouble(FakerPageSource::expectedValueCount).sum();
            default -> 1;
        };
    }

    private SeededColumnGenerator seededColumnGenerator(long tableSeed, FakerColumnHandle column)
    {
        ReseedableRandom randomGenerator = new ReseedableRandom();
        Random random = Random.from(randomGenerator);
        ColumnGenerator generator = getGenerator(column, random, new Faker(locale, random));
        return new SeededColumnGenerator(generator, randomGenerator, XxHash64.hash(tableSeed, column.columnIndex()));
    }

    private ColumnGenerator getGenerator(
            FakerColumnHandle column,
            Random random,
            Faker faker)
    {
        ColumnGenerator generator;
        if (column.domain().getValues().isDiscreteSet()) {
            generator = discreteValueGenerator(column, random);
        }
        else if (column.domain().getValues() instanceof FloatingPointValueSet floatingPoint && !floatingPoint.isAll() &&
                (floatingPoint.asRanges().isEmpty() || floatingPoint.isAllOrderedValues())) {
            throw new TrinoException(INVALID_ROW_FILTER, "Non-discrete NaN constraints are not supported for generated columns");
        }
        else if (column.referencedColumn().isPresent()) {
            generator = referencedValueGenerator(column, random);
        }
        else if (!column.step().isNone()) {
            generator = sequenceWriter(column);
        }
        else {
            Generator valueGenerator = randomValueGenerator(column, random, faker);
            generator = (blockBuilder, _) -> valueGenerator.accept(blockBuilder);
        }
        if (column.nullProbability() == 0) {
            return generator;
        }
        return (blockBuilder, rowId) -> {
            if (random.nextDouble() <= column.nullProbability()) {
                blockBuilder.appendNull();
            }
            else {
                generator.accept(blockBuilder, rowId);
            }
        };
    }

    private ColumnGenerator discreteValueGenerator(FakerColumnHandle column, Random random)
    {
        ObjectWriter singleValueWriter = objectWriter(column.type());
        if (!column.rankedValues().isEmpty()) {
            List<Object> rankedValues = column.rankedValues().stream()
                    .map(value -> Literal.parse(value, column.type()))
                    .collect(toImmutableList());
            return (blockBuilder, _) -> singleValueWriter.accept(blockBuilder, rankedValues.get((int) zipfOffset(random, rankedValues.size())));
        }
        List<Object> values = column.domain().getValues().getDiscreteSet();
        return (blockBuilder, _) -> singleValueWriter.accept(blockBuilder, values.get(random.nextInt(values.size())));
    }

    private ColumnGenerator referencedValueGenerator(FakerColumnHandle column, Random random)
    {
        ReferencedColumn referencedColumn = column.referencedColumn().orElseThrow();
        ColumnGenerator referencedGenerator = seededColumnGenerator(referencedColumn.tableSeed(), referencedColumn.column());
        long rowCount = referencedColumn.tableRowCount();
        if (referencedColumn.column().nullProbability() == 0) {
            return (blockBuilder, _) -> referencedGenerator.accept(blockBuilder, random.nextLong(rowCount));
        }
        BlockBuilder valueBuilder = referencedColumn.column().type().createBlockBuilder(null, 1);
        return (blockBuilder, _) -> {
            // skip referenced rows with nulls by checking the following rows, which ends even when all of them are null
            long start = random.nextLong(rowCount);
            for (long offset = 0; offset < rowCount; offset++) {
                valueBuilder.resetTo(0);
                referencedGenerator.accept(valueBuilder, (start + offset) % rowCount);
                ValueBlock value = valueBuilder.buildValueBlock();
                if (!value.isNull(0)) {
                    blockBuilder.append(value, 0);
                    return;
                }
            }
            throw new TrinoException(INVALID_COLUMN_PROPERTY, "Column '%s' referenced by column '%s' only has null values".formatted(referencedColumn.column().name(), column.name()));
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
    public SourcePage getNextSourcePage()
    {
        if (!closed) {
            int positions = (int) Math.min(limit - completedRows, rowsPerPage);
            if (positions <= 0) {
                closed = true;
            }
            else {
                pageBuilder.declarePositions(positions);
                for (int column = 0; column < generators.size(); column++) {
                    BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(column);
                    SeededColumnGenerator seededGenerator = generators.get(column);
                    ColumnGenerator generator = seededGenerator.generator();
                    ReseedableRandom random = seededGenerator.random();
                    long columnSeed = seededGenerator.columnSeed();
                    for (int i = 0; i < positions; i++) {
                        long rowId = rowOffset + completedRows + i;
                        // reseeding inline is noticeably faster than calling SeededColumnGenerator.accept
                        random.seekToRow(columnSeed, rowId);
                        generator.accept(blockBuilder, rowId);
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

    private ColumnGenerator sequenceWriter(FakerColumnHandle handle)
    {
        Range genericRange = handle.domain().getValues().getRanges().getSpan();
        Type type = handle.type();
        // check every type in order defined in StandardTypes
        // not supported: BOOLEAN, HYPER_LOG_LOG, QDIGEST, TDIGEST, P4_HYPER_LOG_LOG, VARBINARY, VARCHAR, CHAR, ROW, ARRAY, MAP, JSON, IPADDRESS, GEOMETRY, UUID
        if (BIGINT.equals(type)) {
            LongRange range = LongRange.of(genericRange, 1, (long) handle.step().getSingleValue());
            return (blockBuilder, rowId) -> BIGINT.writeLong(blockBuilder, range.at(rowId));
        }
        if (INTEGER.equals(type)) {
            IntRange range = IntRange.of(genericRange, (long) handle.step().getSingleValue());
            return (blockBuilder, rowId) -> INTEGER.writeLong(blockBuilder, range.at(rowId));
        }
        if (SMALLINT.equals(type)) {
            IntRange range = IntRange.of(genericRange, Short.MIN_VALUE, Short.MAX_VALUE, (long) handle.step().getSingleValue());
            return (blockBuilder, rowId) -> SMALLINT.writeLong(blockBuilder, range.at(rowId));
        }
        if (TINYINT.equals(type)) {
            IntRange range = IntRange.of(genericRange, Byte.MIN_VALUE, Byte.MAX_VALUE, (long) handle.step().getSingleValue());
            return (blockBuilder, rowId) -> TINYINT.writeLong(blockBuilder, range.at(rowId));
        }
        if (DATE.equals(type)) {
            IntRange range = IntRange.of(withDefaultTemporalLow(genericRange), (long) handle.step().getSingleValue());
            return (blockBuilder, rowId) -> DATE.writeLong(blockBuilder, range.at(rowId, NANOSECONDS_PER_DAY));
        }
        if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                ShortDecimalRange range = ShortDecimalRange.of(genericRange, decimalType.getPrecision(), (long) handle.step().getSingleValue());
                return (blockBuilder, rowId) -> decimalType.writeLong(blockBuilder, range.at(rowId));
            }
            Int128Range range = Int128Range.of(genericRange, (Int128) handle.step().getSingleValue());
            return (blockBuilder, rowId) -> decimalType.writeObject(blockBuilder, range.at(rowId));
        }
        if (REAL.equals(type)) {
            FloatRange range = FloatRange.of(genericRange, intBitsToFloat(toIntExact((long) handle.step().getSingleValue())));
            return (blockBuilder, rowId) -> REAL.writeLong(blockBuilder, floatToRawIntBits(range.at(rowId)));
        }
        if (DOUBLE.equals(type)) {
            DoubleRange range = DoubleRange.of(genericRange, (double) handle.step().getSingleValue());
            return (blockBuilder, rowId) -> DOUBLE.writeDouble(blockBuilder, range.at(rowId));
        }
        if (IntervalValues.isInterval(type)) {
            IntervalValues.Bounds bounds = IntervalValues.bounds(type, genericRange);
            long step = (long) handle.step().getSingleValue();
            return (blockBuilder, rowId) -> type.writeLong(blockBuilder, bounds.at(rowId, step));
        }
        if (type instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                long factor = POWERS_OF_TEN[6 - timestampType.getPrecision()];
                LongRange range = LongRange.of(withDefaultTemporalLow(genericRange), factor, (long) handle.step().getSingleValue());
                return (blockBuilder, rowId) -> timestampType.writeLong(blockBuilder, range.at(rowId, factor * NANOSECONDS_PER_MICROSECOND) * factor);
            }
            LongTimestampRange range = LongTimestampRange.of(withDefaultTemporalLow(genericRange), timestampType.getPrecision(), (long) handle.step().getSingleValue());
            return (blockBuilder, rowId) -> timestampType.writeObject(blockBuilder, range.at(rowId));
        }
        if (type instanceof TimestampWithTimeZoneType tzType) {
            if (tzType.isShort()) {
                ShortTimestampWithTimeZoneRange range = ShortTimestampWithTimeZoneRange.of(withDefaultTemporalLow(genericRange), tzType.getPrecision(), (long) handle.step().getSingleValue());
                return (blockBuilder, rowId) -> tzType.writeLong(blockBuilder, range.at(rowId, NANOSECONDS_PER_MILLISECOND));
            }
            LongTimestampWithTimeZoneRange range = LongTimestampWithTimeZoneRange.of(withDefaultTemporalLow(genericRange), tzType.getPrecision(), (long) handle.step().getSingleValue());
            return (blockBuilder, rowId) -> tzType.writeObject(blockBuilder, range.at(rowId));
        }
        if (type instanceof TimeType timeType) {
            long factor = POWERS_OF_TEN[12 - timeType.getPrecision()];
            LongRange range = LongRange.of(genericRange, factor, 0, PICOSECONDS_PER_DAY - 1, (long) handle.step().getSingleValue() * PICOSECONDS_PER_NANOSECOND);
            return (blockBuilder, rowId) -> timeType.writeLong(blockBuilder, range.at(rowId, factor) * factor);
        }
        if (type instanceof TimeWithTimeZoneType timeType) {
            if (timeType.isShort()) {
                ShortTimeWithTimeZoneRange range = ShortTimeWithTimeZoneRange.of(genericRange, timeType.getPrecision(), (long) handle.step().getSingleValue());
                return (blockBuilder, rowId) -> timeType.writeLong(blockBuilder, range.at(rowId));
            }
            LongTimeWithTimeZoneRange range = LongTimeWithTimeZoneRange.of(genericRange, timeType.getPrecision(), (long) handle.step().getSingleValue() * PICOSECONDS_PER_NANOSECOND);
            return (blockBuilder, rowId) -> timeType.writeObject(blockBuilder, range.at(rowId));
        }

        throw new IllegalArgumentException("Unsupported type " + type);
    }

    private Generator randomValueGenerator(FakerColumnHandle handle, Random random, Faker faker)
    {
        Type type = handle.type();
        if (!hasGenerator(type)) {
            return BlockBuilder::appendNull;
        }
        if (type instanceof RowType rowType) {
            List<Generator> fieldGenerators = rowType.getFields().stream()
                    .map(field -> nestedValueGenerator(field.getType(), random, faker))
                    .collect(toImmutableList());
            return blockBuilder -> {
                RowBlockBuilder rowBlockBuilder = rowType.createBlockBuilder(null, 1);
                rowBlockBuilder.buildEntry(fieldBuilders -> {
                    for (int i = 0; i < fieldBuilders.size(); i++) {
                        fieldGenerators.get(i).accept(fieldBuilders.get(i));
                    }
                });
                blockBuilder.append((RowBlock) rowBlockBuilder.build(), 0);
            };
        }
        if (type instanceof ArrayType arrayType) {
            Generator elementGenerator = nestedValueGenerator(arrayType.getElementType(), random, faker);
            return blockBuilder -> ((ArrayBlockBuilder) blockBuilder).buildEntry(elementBuilder -> {
                int cardinality = random.nextInt(MAX_CARDINALITY + 1);
                for (int i = 0; i < cardinality; i++) {
                    elementGenerator.accept(elementBuilder);
                }
            });
        }
        if (type instanceof MapType mapType) {
            return mapGenerator(mapType, random, faker);
        }
        if (type.getBaseName().equals(StandardTypes.JSON)) {
            // An empty slice is not a valid JSON document and would fail the parse on read;
            // the JSON null literal is the canonical empty value.
            return blockBuilder -> type.writeSlice(blockBuilder, Slices.utf8Slice("null"));
        }

        SentenceGenerator sentenceGenerator = () -> Slices.utf8Slice(faker.lorem().sentence(3 + random.nextInt(38)));
        BoundedSentenceGenerator boundedSentenceGenerator = maxLength -> Slices.utf8Slice(faker.lorem().maxLengthSentence(maxLength));
        Range genericRange = handle.domain().getValues().getRanges().getSpan();
        if (handle.generator() != null) {
            if (!genericRange.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for columns with a generator expression are not supported");
            }
            if (type instanceof VarcharType varcharType) {
                return blockBuilder -> varcharType.writeSlice(blockBuilder, truncateToLength(Slices.utf8Slice(faker.expression(handle.generator())), varcharType));
            }
            if (type instanceof CharType charType) {
                return blockBuilder -> charType.writeSlice(blockBuilder, truncateToLengthAndTrimSpaces(Slices.utf8Slice(faker.expression(handle.generator())), charType));
            }
            if (type instanceof VarbinaryType) {
                return blockBuilder -> VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(faker.expression(handle.generator())));
            }
            ObjectWriter writer = objectWriter(type);
            return blockBuilder -> writer.accept(blockBuilder, handle.parseGeneratedValue(faker.expression(handle.generator())));
        }
        Distribution distribution = handle.distribution();
        // check every type in order defined in StandardTypes
        if (BIGINT.equals(type)) {
            LongRange range = LongRange.of(genericRange);
            LongSupplier sampler = numberSampler(random, distribution, range.low, range.high);
            return blockBuilder -> BIGINT.writeLong(blockBuilder, sampler.getAsLong());
        }
        if (INTEGER.equals(type)) {
            IntRange range = IntRange.of(genericRange);
            LongSupplier sampler = numberSampler(random, distribution, range.low, range.high);
            return blockBuilder -> INTEGER.writeLong(blockBuilder, sampler.getAsLong());
        }
        if (SMALLINT.equals(type)) {
            IntRange range = IntRange.of(genericRange, Short.MIN_VALUE, Short.MAX_VALUE);
            LongSupplier sampler = numberSampler(random, distribution, range.low, range.high);
            return blockBuilder -> SMALLINT.writeLong(blockBuilder, sampler.getAsLong());
        }
        if (TINYINT.equals(type)) {
            IntRange range = IntRange.of(genericRange, Byte.MIN_VALUE, Byte.MAX_VALUE);
            LongSupplier sampler = numberSampler(random, distribution, range.low, range.high);
            return blockBuilder -> TINYINT.writeLong(blockBuilder, sampler.getAsLong());
        }
        if (BOOLEAN.equals(type)) {
            if (!genericRange.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Range or not a single value predicates for boolean columns are not supported");
            }
            return blockBuilder -> BOOLEAN.writeBoolean(blockBuilder, random.nextBoolean());
        }
        if (DATE.equals(type)) {
            IntRange range = IntRange.of(withDefaultTemporalBounds(genericRange));
            LongSupplier sampler = numberSampler(random, distribution, range.low, range.high);
            return blockBuilder -> DATE.writeLong(blockBuilder, sampler.getAsLong());
        }
        if (type instanceof DecimalType decimalType) {
            return decimalGenerator(genericRange, decimalType, distribution, random);
        }
        if (REAL.equals(type)) {
            FloatRange range = FloatRange.of(genericRange);
            if (distribution == NORMAL) {
                float inclusiveHigh = Math.nextDown(range.high);
                // rounding to a float can move the value out of the range
                return blockBuilder -> REAL.writeLong(blockBuilder, floatToRawIntBits(Math.clamp((float) normalBetween(random, range.low, inclusiveHigh), range.low, inclusiveHigh)));
            }
            // the exclusive high bound of a range ending at the max value is infinite, and can't be sampled
            float high = Math.min(range.high, Float.MAX_VALUE);
            return blockBuilder -> REAL.writeLong(blockBuilder, floatToRawIntBits(range.low == high ? range.low : random.nextFloat(range.low, high)));
        }
        if (DOUBLE.equals(type)) {
            DoubleRange range = DoubleRange.of(genericRange);
            if (distribution == NORMAL) {
                double inclusiveHigh = Math.nextDown(range.high);
                return blockBuilder -> DOUBLE.writeDouble(blockBuilder, normalBetween(random, range.low, inclusiveHigh));
            }
            // the exclusive high bound of a range ending at the max value is infinite, and can't be sampled
            double high = Math.min(range.high, Double.MAX_VALUE);
            return blockBuilder -> DOUBLE.writeDouble(blockBuilder, range.low == high ? range.low : random.nextDouble(range.low, high));
        }
        // not supported: HYPER_LOG_LOG, QDIGEST, TDIGEST, P4_HYPER_LOG_LOG
        if (IntervalValues.isInterval(type)) {
            IntervalValues.Bounds bounds = IntervalValues.bounds(type, genericRange);
            return blockBuilder -> type.writeLong(blockBuilder, numberBetweenInclusive(random, bounds.low(), bounds.high()) * bounds.quantum());
        }
        if (type instanceof TimestampType timestampType) {
            return timestampGenerator(withDefaultTemporalBounds(genericRange), timestampType, distribution, random);
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            return timestampWithTimeZoneGenerator(withDefaultTemporalBounds(genericRange), timestampWithTimeZoneType, random);
        }
        if (type instanceof TimeType timeType) {
            long factor = POWERS_OF_TEN[12 - timeType.getPrecision()];
            LongRange range = LongRange.of(genericRange, factor, 0, PICOSECONDS_PER_DAY - 1);
            return blockBuilder -> timeType.writeLong(blockBuilder, numberBetweenInclusive(random, range.low, range.high) * factor);
        }
        if (type instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
            return timeWithTimeZoneGenerator(genericRange, timeWithTimeZoneType, random);
        }
        if (type instanceof VarbinaryType varType) {
            if (!genericRange.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for varbinary columns are not supported");
            }
            return blockBuilder -> varType.writeSlice(blockBuilder, sentenceGenerator.get());
        }
        if (type instanceof VarcharType varcharType) {
            if (!genericRange.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for varchar columns are not supported");
            }
            if (varcharType.getLength().isPresent()) {
                int length = varcharType.getLength().get();
                if (length == 0) {
                    return blockBuilder -> varcharType.writeSlice(blockBuilder, Slices.EMPTY_SLICE);
                }
                return blockBuilder -> varcharType.writeSlice(blockBuilder, boundedSentenceGenerator.get(1 + random.nextInt(length)));
            }
            return blockBuilder -> varcharType.writeSlice(blockBuilder, sentenceGenerator.get());
        }
        if (type instanceof CharType charType) {
            if (!genericRange.isAll()) {
                throw new TrinoException(INVALID_ROW_FILTER, "Predicates for char columns are not supported");
            }
            return blockBuilder -> charType.writeSlice(blockBuilder, boundedSentenceGenerator.get(charType.getLength()));
        }
        if (type.getBaseName().equals(StandardTypes.IPADDRESS)) {
            return generateIpV4(type, genericRange, random);
        }
        // not supported: GEOMETRY
        if (type instanceof UuidType) {
            return generateUUID(genericRange, random);
        }

        throw new IllegalArgumentException("Unsupported type " + type);
    }

    // there are no generators for other types, like HyperLogLog, digests, or types from other plugins, like geometries
    static boolean hasGenerator(Type type)
    {
        return BIGINT.equals(type) || INTEGER.equals(type) || SMALLINT.equals(type) || TINYINT.equals(type) ||
                BOOLEAN.equals(type) ||
                DATE.equals(type) ||
                type instanceof DecimalType ||
                REAL.equals(type) || DOUBLE.equals(type) ||
                IntervalValues.isInterval(type) ||
                type instanceof TimestampType || type instanceof TimestampWithTimeZoneType ||
                type instanceof TimeType || type instanceof TimeWithTimeZoneType ||
                type instanceof VarbinaryType || type instanceof VarcharType || type instanceof CharType ||
                type.getBaseName().equals(StandardTypes.IPADDRESS) ||
                type instanceof UuidType ||
                type instanceof RowType || type instanceof ArrayType || type instanceof MapType ||
                type.getBaseName().equals(StandardTypes.JSON);
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
        if (IntervalValues.isInterval(type)) {
            IntervalValues.checkSupported(type);
            return (blockBuilder, value) -> type.writeLong(blockBuilder, (Long) value);
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
        if (type.getBaseName().equals(StandardTypes.IPADDRESS)) {
            return (blockBuilder, value) -> type.writeSlice(blockBuilder, (Slice) value);
        }
        // not supported: GEOMETRY
        if (type instanceof UuidType) {
            return (blockBuilder, value) -> UUID.writeSlice(blockBuilder, (Slice) value);
        }

        throw new IllegalArgumentException("Unsupported type " + type);
    }

    private Generator mapGenerator(MapType mapType, Random random, Faker faker)
    {
        Type keyType = mapType.getKeyType();
        Generator keyGenerator = nestedValueGenerator(keyType, random, faker);
        Generator valueGenerator = nestedValueGenerator(mapType.getValueType(), random, faker);
        MethodHandle keyIndeterminate = TYPE_OPERATORS.getIndeterminateOperator(keyType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));
        MethodHandle keyEqual = TYPE_OPERATORS.getEqualOperator(keyType, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL));
        return blockBuilder -> {
            BlockBuilder keyCandidatesBuilder = keyType.createBlockBuilder(null, MAX_CARDINALITY);
            int cardinality = random.nextInt(MAX_CARDINALITY + 1);
            for (int i = 0; i < cardinality; i++) {
                keyGenerator.accept(keyCandidatesBuilder);
            }
            ValueBlock keyCandidates = keyCandidatesBuilder.buildValueBlock();
            int[] keyPositions = distinctKeyPositions(keyCandidates, keyIndeterminate, keyEqual);
            ((MapBlockBuilder) blockBuilder).buildEntry((keyBuilder, valueBuilder) -> {
                for (int position : keyPositions) {
                    keyBuilder.append(keyCandidates, position);
                    valueGenerator.accept(valueBuilder);
                }
            });
        };
    }

    private Generator nestedValueGenerator(Type type, Random random, Faker faker)
    {
        return randomValueGenerator(new FakerColumnHandle(0, "nested", type, 0, null, Domain.all(type), ValueSet.none(type), Optional.empty(), UNIFORM, ImmutableList.of()), random, faker);
    }

    // Duplicate keys are dropped instead of regenerated, so key types with few values, like boolean,
    // produce smaller maps instead of never finishing. Indeterminate keys are dropped, because map keys
    // can't be null or contain nulls, which is what types without a generator, like color, produce.
    private static int[] distinctKeyPositions(Block keys, MethodHandle keyIndeterminate, MethodHandle keyEqual)
    {
        int[] positions = new int[keys.getPositionCount()];
        int count = 0;
        try {
            for (int position = 0; position < keys.getPositionCount(); position++) {
                if ((boolean) keyIndeterminate.invokeExact(keys, position)) {
                    continue;
                }
                boolean duplicate = false;
                for (int i = 0; i < count && !duplicate; i++) {
                    // equal only returns null for keys containing nulls, which are already skipped
                    duplicate = (Boolean) keyEqual.invokeExact(keys, positions[i], keys, position);
                }
                if (!duplicate) {
                    positions[count++] = position;
                }
            }
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
        return Arrays.copyOf(positions, count);
    }

    private Generator decimalGenerator(Range genericRange, DecimalType decimalType, Distribution distribution, Random random)
    {
        if (decimalType.isShort()) {
            ShortDecimalRange range = ShortDecimalRange.of(genericRange, decimalType.getPrecision());
            LongSupplier sampler = numberSampler(random, distribution, range.low, range.high - 1);
            return blockBuilder -> decimalType.writeLong(blockBuilder, sampler.getAsLong());
        }
        Int128Range range = Int128Range.of(genericRange);
        if (distribution == NORMAL) {
            BigInteger low = range.low.toBigInteger();
            BigInteger span = range.high.toBigInteger().subtract(BigInteger.ONE).subtract(low);
            double spanValue = span.doubleValue();
            // rounding the span to a double can make offsets greater than the span of the range
            return blockBuilder -> decimalType.writeObject(blockBuilder, Int128.valueOf(
                    new BigDecimal(Math.rint(normalBetween(random, 0, spanValue))).toBigInteger().min(span).add(low)));
        }
        BigInteger currentRange = BigInteger.valueOf(Long.MAX_VALUE);
        BigInteger desiredRange = range.high.toBigInteger().subtract(range.low.toBigInteger());
        return blockBuilder -> decimalType.writeObject(blockBuilder, Int128.valueOf(
                new BigInteger(63, random).multiply(desiredRange).divide(currentRange).add(range.low.toBigInteger())));
    }

    private Generator timestampGenerator(Range genericRange, TimestampType tzType, Distribution distribution, Random random)
    {
        if (tzType.isShort()) {
            long factor = POWERS_OF_TEN[6 - tzType.getPrecision()];
            LongRange range = LongRange.of(genericRange, factor);
            LongSupplier sampler = numberSampler(random, distribution, range.low, range.high);
            return blockBuilder -> tzType.writeLong(blockBuilder, sampler.getAsLong() * factor);
        }
        LongTimestampRange range = LongTimestampRange.of(genericRange, tzType.getPrecision());
        // the distribution only applies to whole microseconds, and fractions of a microsecond are uniform
        LongSupplier epochMicrosSampler = numberSampler(random, distribution, range.low.getEpochMicros(), range.high.getEpochMicros());
        if (tzType.getPrecision() <= 6) {
            return blockBuilder -> {
                long epochMicros = epochMicrosSampler.getAsLong();
                tzType.writeObject(blockBuilder, new LongTimestamp(epochMicros * range.factor, 0));
            };
        }
        // numberBetween excludes the high bound
        int picosOfMicroHigh = (int) POWERS_OF_TEN[tzType.getPrecision() - 6];
        return blockBuilder -> {
            long epochMicros = epochMicrosSampler.getAsLong();
            int picosOfMicro;
            if (epochMicros == range.low.getEpochMicros()) {
                picosOfMicro = numberBetween(
                        random,
                        range.low.getPicosOfMicro(),
                        range.low.getEpochMicros() == range.high.getEpochMicros() ?
                                range.high.getPicosOfMicro() + 1
                                : picosOfMicroHigh);
            }
            else if (epochMicros == range.high.getEpochMicros()) {
                picosOfMicro = numberBetween(random, 0, range.high.getPicosOfMicro() + 1);
            }
            else {
                picosOfMicro = numberBetween(random, 0, picosOfMicroHigh);
            }
            tzType.writeObject(blockBuilder, new LongTimestamp(epochMicros, picosOfMicro * range.factor));
        };
    }

    private Generator timestampWithTimeZoneGenerator(Range genericRange, TimestampWithTimeZoneType tzType, Random random)
    {
        if (tzType.isShort()) {
            ShortTimestampWithTimeZoneRange range = ShortTimestampWithTimeZoneRange.of(genericRange, tzType.getPrecision());
            return blockBuilder -> {
                long millis = numberBetween(random, range.low, range.high) * range.factor;
                tzType.writeLong(blockBuilder, packDateTimeWithZone(millis, range.defaultTZ));
            };
        }
        LongTimestampWithTimeZoneRange range = LongTimestampWithTimeZoneRange.of(genericRange, tzType.getPrecision());
        // numberBetween excludes the high bound
        int picosOfMilliHigh = (int) POWERS_OF_TEN[tzType.getPrecision() - 3];
        long lastEpochMillis = range.lastEpochMillis();
        return blockBuilder -> {
            long millis = numberBetweenInclusive(random, range.lowEpochMillis(), lastEpochMillis);
            int picosOfMilli;
            if (millis == range.lowEpochMillis()) {
                picosOfMilli = numberBetween(
                        random,
                        range.lowPicosOfMilli(),
                        range.lowEpochMillis() == range.highEpochMillis() ?
                                range.highPicosOfMilli()
                                : picosOfMilliHigh);
            }
            else if (millis == range.highEpochMillis()) {
                picosOfMilli = numberBetween(random, 0, range.highPicosOfMilli());
            }
            else {
                picosOfMilli = numberBetween(random, 0, picosOfMilliHigh);
            }
            tzType.writeObject(blockBuilder, fromEpochMillisAndFraction(millis, picosOfMilli * range.factor, range.defaultTZ));
        };
    }

    private Generator timeWithTimeZoneGenerator(Range genericRange, TimeWithTimeZoneType timeType, Random random)
    {
        if (timeType.isShort()) {
            ShortTimeWithTimeZoneRange range = ShortTimeWithTimeZoneRange.of(genericRange, timeType.getPrecision());
            return blockBuilder -> {
                long nanos = numberBetween(random, range.low, range.high) * range.factor;
                timeType.writeLong(blockBuilder, packTimeWithTimeZone(nanos, range.offsetMinutes));
            };
        }
        LongTimeWithTimeZoneRange range = LongTimeWithTimeZoneRange.of(genericRange, timeType.getPrecision());
        return blockBuilder -> {
            long picoseconds = numberBetween(random, range.low, range.high) * range.factor;
            timeType.writeObject(blockBuilder, new LongTimeWithTimeZone(picoseconds, range.offsetMinutes));
        };
    }

    private static Range withDefaultTemporalBounds(Range range)
    {
        return withDefaultTemporalHigh(withDefaultTemporalLow(range));
    }

    private static Range withDefaultTemporalLow(Range range)
    {
        if (!range.isLowUnbounded()) {
            return range;
        }
        Range defaultRange = Range.greaterThanOrEqual(range.getType(), startOfDay(range.getType(), DEFAULT_LOW_EPOCH_DAY, range.getHighValue()));
        // keep the min value of the type if the max is before the default window
        return range.intersect(defaultRange).orElse(range);
    }

    private static Range withDefaultTemporalHigh(Range range)
    {
        if (!range.isHighUnbounded()) {
            return range;
        }
        Range defaultRange = Range.lessThan(range.getType(), startOfDay(range.getType(), DEFAULT_HIGH_EPOCH_DAY, range.getLowValue()));
        // keep the max value of the type if the min is after the default window
        return range.intersect(defaultRange).orElse(range);
    }

    private static Object startOfDay(Type type, long epochDay, Optional<Object> otherBound)
    {
        if (DATE.equals(type)) {
            return epochDay;
        }
        long epochMillis = epochDay * MILLISECONDS_PER_DAY;
        if (type instanceof TimestampType timestampType) {
            long epochMicros = epochMillis * MICROSECONDS_PER_MILLISECOND;
            if (timestampType.isShort()) {
                return epochMicros;
            }
            return new LongTimestamp(epochMicros, 0);
        }
        // the day starts in UTC, but uses the time zone of the other bound, because range helpers require both bounds to have the same time zone
        if (((TimestampWithTimeZoneType) type).isShort()) {
            return packDateTimeWithZone(epochMillis, otherBound.map(value -> unpackZoneKey((long) value)).orElse(TimeZoneKey.UTC_KEY));
        }
        short timeZoneKey = otherBound.map(value -> ((LongTimestampWithTimeZone) value).getTimeZoneKey()).orElse(TimeZoneKey.UTC_KEY.getKey());
        return fromEpochMillisAndFraction(epochMillis, 0, timeZoneKey);
    }

    /**
     * Both bounds are inclusive, because an exclusive high bound overflows when the range ends at the max value.
     */
    private record LongRange(long low, long high, long step)
    {
        static LongRange of(Range range)
        {
            return of(range, 1, Long.MIN_VALUE, Long.MAX_VALUE, 1);
        }

        static LongRange of(Range range, long factor)
        {
            return of(range, factor, Long.MIN_VALUE, Long.MAX_VALUE, 1);
        }

        static LongRange of(Range range, long factor, long step)
        {
            return of(range, factor, Long.MIN_VALUE, Long.MAX_VALUE, step);
        }

        static LongRange of(Range range, long factor, long defaultMin, long defaultMax)
        {
            return of(range, factor, defaultMin, defaultMax, 1);
        }

        static LongRange of(Range range, long factor, long defaultMin, long defaultMax, long step)
        {
            // the default bounds are inclusive, so round them inwards, so they don't fall outside the range of the type when multiplied by the factor
            return new LongRange(
                    ceilDiv((long) range.getLowValue().orElse(defaultMin), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                    range.isHighUnbounded() ? floorDiv(defaultMax, factor) : roundDiv((long) range.getHighValue().orElseThrow(), factor) - (range.isHighInclusive() ? 0 : 1),
                    step);
        }

        long at(long index)
        {
            return Math.min(sequenceValue(low, index, step, 1), high);
        }

        long at(long index, long factor)
        {
            return Math.min(sequenceValue(low, index, step, factor), high);
        }
    }

    /**
     * Both bounds are inclusive, because an exclusive high bound overflows when the range ends at the max value.
     */
    private record IntRange(int low, int high, long step)
    {
        static IntRange of(Range range)
        {
            return of(range, Integer.MIN_VALUE, Integer.MAX_VALUE, 1);
        }

        static IntRange of(Range range, long step)
        {
            return of(range, Integer.MIN_VALUE, Integer.MAX_VALUE, step);
        }

        static IntRange of(Range range, long defaultMin, long defaultMax)
        {
            return of(range, defaultMin, defaultMax, 1);
        }

        static IntRange of(Range range, long defaultMin, long defaultMax, long step)
        {
            return new IntRange(
                    toIntExact((long) range.getLowValue().orElse(defaultMin)) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0),
                    toIntExact((long) range.getHighValue().orElse(defaultMax)) - (range.isHighUnbounded() || range.isHighInclusive() ? 0 : 1),
                    step);
        }

        long at(long index)
        {
            return Math.min(sequenceValue(low, index, step, 1), high);
        }

        long at(long index, long factor)
        {
            return Math.min(sequenceValue(low, index, step, factor), high);
        }
    }

    private record FloatRange(float low, float high, float step)
    {
        static FloatRange of(Range range)
        {
            return of(range, 1);
        }

        static FloatRange of(Range range, float step)
        {
            float low = range.getLowValue().map(v -> intBitsToFloat(toIntExact((long) v))).orElse(Float.MIN_VALUE);
            if (!range.isLowUnbounded() && !range.isLowInclusive()) {
                low = Math.nextUp(low);
            }
            float high = range.getHighValue().map(v -> intBitsToFloat(toIntExact((long) v))).orElse(Float.MAX_VALUE);
            if (!range.isHighUnbounded() && range.isHighInclusive()) {
                high = Math.nextUp(high);
            }
            return new FloatRange(low, high, step);
        }

        float at(long index)
        {
            return Math.min(low + index * step, Math.nextDown(high));
        }
    }

    private record DoubleRange(double low, double high, double step)
    {
        static DoubleRange of(Range range)
        {
            return of(range, 1);
        }

        static DoubleRange of(Range range, double step)
        {
            double low = (double) range.getLowValue().orElse(Double.MIN_VALUE);
            if (!range.isLowUnbounded() && !range.isLowInclusive()) {
                low = Math.nextUp(low);
            }
            double high = (double) range.getHighValue().orElse(Double.MAX_VALUE);
            if (!range.isHighUnbounded() && range.isHighInclusive()) {
                high = Math.nextUp(high);
            }
            return new DoubleRange(low, high, step);
        }

        double at(long index)
        {
            return Math.min(low + index * step, Math.nextDown(high));
        }
    }

    private record ShortDecimalRange(long low, long high, long step)
    {
        static ShortDecimalRange of(Range range, int precision)
        {
            return of(range, precision, 1);
        }

        static ShortDecimalRange of(Range range, int precision, long step)
        {
            long defaultMin = -999999999999999999L / POWERS_OF_TEN[18 - precision];
            long defaultMax = 999999999999999999L / POWERS_OF_TEN[18 - precision];
            long low = (long) range.getLowValue().orElse(defaultMin) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0);
            long high = (long) range.getHighValue().orElse(defaultMax) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0);
            return new ShortDecimalRange(low, high, step);
        }

        long at(long index)
        {
            return Math.min(sequenceValue(low, index, step, 1), high - 1);
        }
    }

    private record Int128Range(Int128 low, Int128 high, Int128 step, long lastIndex)
    {
        static Int128Range of(Range range)
        {
            return of(range, Int128.ONE);
        }

        static Int128Range of(Range range, Int128 step)
        {
            Int128 low = (Int128) range.getLowValue().orElse(Decimals.MIN_UNSCALED_DECIMAL);
            Int128 high = (Int128) range.getHighValue().orElse(Decimals.MAX_UNSCALED_DECIMAL);
            if (!range.isLowUnbounded() && !range.isLowInclusive()) {
                low = add(low, Int128.ONE);
            }
            if (!range.isHighUnbounded() && range.isHighInclusive()) {
                high = add(high, Int128.ONE);
            }
            // multiplying the step by an index after the last one that fits in the range can overflow, so such indexes are capped instead
            long lastIndex = high.toBigInteger().subtract(BigInteger.ONE).subtract(low.toBigInteger())
                    .divide(step.toBigInteger())
                    .min(BigInteger.valueOf(Long.MAX_VALUE))
                    .longValue();
            return new Int128Range(low, high, step, lastIndex);
        }

        Int128 at(long index)
        {
            if (index > lastIndex) {
                return Int128Math.subtract(high, Int128.ONE);
            }
            // decimals span almost twice the max value of Int128, so multiplying the step by the index can overflow,
            // even if the result doesn't, but adding each half separately keeps every partial result in the range
            long halfIndex = index / 2;
            Int128 halfway = add(low, Int128Math.multiply(Int128.valueOf(halfIndex), step));
            return add(halfway, Int128Math.multiply(Int128.valueOf(index - halfIndex), step));
        }
    }

    private record SeededColumnGenerator(ColumnGenerator generator, ReseedableRandom random, long columnSeed)
            implements ColumnGenerator
    {
        @Override
        public void accept(BlockBuilder blockBuilder, long rowId)
        {
            // a value only depends on the table seed, the column, and the row number,
            // and not on other columns, the query, or how rows are divided into splits
            random.seekToRow(columnSeed, rowId);
            generator.accept(blockBuilder, rowId);
        }
    }

    // java.util.Random is too slow to reseed for every row: setSeed() is synchronized, and every value requires an atomic update
    private static final class ReseedableRandom
            implements RandomGenerator
    {
        private static final long GAMMA = 0x9E3779B97F4A7C15L;
        // every row starts at its own part of the column's SplitMix64 stream, long enough for 2^20 values,
        // so the start of any row takes a single multiplication, instead of hashing the row number
        private static final long ROW_STRIDE = GAMMA << 20;
        // the stride wraps around the stream after 2^44 rows
        private static final int ROW_STRIDE_PERIOD_BITS = 64 - 20;

        private long state;

        public void seekToRow(long columnSeed, long rowId)
        {
            state = columnSeed + rowId * ROW_STRIDE;
            long period = rowId >>> ROW_STRIDE_PERIOD_BITS;
            if (period != 0) {
                // start rows of later periods at a different offset, so they don't repeat values of earlier rows
                state += mix(period);
            }
        }

        @Override
        public long nextLong()
        {
            // SplitMix64
            state += GAMMA;
            return mix(state);
        }

        private static long mix(long value)
        {
            long z = value;
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }
    }

    private static Int128 add(Int128 left, Int128 right)
    {
        long[] result = new long[2];
        Int128Math.add(left.getHigh(), left.getLow(), right.getHigh(), right.getLow(), result, 0);
        return Int128.valueOf(result);
    }

    /**
     * Both bounds are inclusive, because an exclusive high bound overflows when the range ends at the max value.
     */
    private record LongTimestampRange(LongTimestamp low, LongTimestamp high, int factor, long step)
    {
        static LongTimestampRange of(Range range, int precision)
        {
            return of(range, precision, 1);
        }

        static LongTimestampRange of(Range range, int precision, long step)
        {
            LongTimestamp low = (LongTimestamp) range.getLowValue().orElse(new LongTimestamp(Long.MIN_VALUE, 0));
            LongTimestamp high = (LongTimestamp) range.getHighValue().orElse(new LongTimestamp(Long.MAX_VALUE, PICOSECONDS_PER_MICROSECOND - 1));
            int factor;
            if (precision <= 6) {
                factor = (int) POWERS_OF_TEN[6 - precision];
                low = new LongTimestamp(roundDiv(low.getEpochMicros(), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0), 0);
                high = new LongTimestamp(roundDiv(high.getEpochMicros(), factor) - (range.isHighInclusive() ? 0 : 1), 0);
                return new LongTimestampRange(low, high, factor, step);
            }
            factor = (int) POWERS_OF_TEN[12 - precision];
            int unitsPerMicro = PICOSECONDS_PER_MICROSECOND / factor;
            // an unbounded bound keeps the extreme epoch value, so its fraction must not carry
            int lowPicosOfMicro = range.isLowUnbounded() ? 0 : roundDiv(low.getPicosOfMicro(), factor) + (range.isLowInclusive() ? 0 : 1);
            int highPicosOfMicro = range.isHighUnbounded() ? unitsPerMicro - 1 : roundDiv(high.getPicosOfMicro(), factor) - (range.isHighInclusive() ? 0 : 1);
            low = new LongTimestamp(
                    low.getEpochMicros() + floorDiv(lowPicosOfMicro, unitsPerMicro),
                    floorMod(lowPicosOfMicro, unitsPerMicro));
            high = new LongTimestamp(
                    high.getEpochMicros() + floorDiv(highPicosOfMicro, unitsPerMicro),
                    floorMod(highPicosOfMicro, unitsPerMicro));
            return new LongTimestampRange(low, high, factor, step);
        }

        LongTimestamp at(long index)
        {
            // the fractions of low and high are in units of the precision, as is the offset
            long unitsPerMicro = PICOSECONDS_PER_MICROSECOND / factor;
            long offset = sequenceOffsetUnits(index, step, factor);
            long fraction = low.getPicosOfMicro() + offset % unitsPerMicro;
            long micros = offset / unitsPerMicro + fraction / unitsPerMicro;
            fraction %= unitsPerMicro;
            // the difference between the bounds can overflow, but not as an unsigned value
            int comparison = Long.compareUnsigned(micros, high.getEpochMicros() - low.getEpochMicros());
            if (comparison > 0 || (comparison == 0 && fraction > high.getPicosOfMicro())) {
                return new LongTimestamp(high.getEpochMicros(), high.getPicosOfMicro() * factor);
            }
            return new LongTimestamp(low.getEpochMicros() + micros, toIntExact(fraction * factor));
        }
    }

    private record ShortTimestampWithTimeZoneRange(long low, long high, long factor, TimeZoneKey defaultTZ, long step)
    {
        static ShortTimestampWithTimeZoneRange of(Range range, int precision)
        {
            return of(range, precision, 1);
        }

        static ShortTimestampWithTimeZoneRange of(Range range, int precision, long step)
        {
            TimeZoneKey defaultTZ = range.getLowValue()
                    .map(v -> unpackZoneKey((long) v))
                    .orElse(range.getHighValue()
                            .map(v -> unpackZoneKey((long) v))
                            .orElse(TimeZoneKey.UTC_KEY));
            long factor = POWERS_OF_TEN[3 - precision];
            long low = roundDiv(unpackMillisUtc((long) range.getLowValue().orElse(Long.MIN_VALUE)), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0);
            long high = roundDiv(unpackMillisUtc((long) range.getHighValue().orElse(Long.MAX_VALUE)), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0);
            return new ShortTimestampWithTimeZoneRange(low, high, factor, defaultTZ, step);
        }

        long at(long index, long stepFactor)
        {
            // TODO support nanosecond increments
            long millis = sequenceValue(low, index, step, factor * stepFactor);
            return packDateTimeWithZone((step > 0 ? Math.min(millis, high - 1) : Math.max(millis, high - 1)) * factor, defaultTZ);
        }
    }

    /**
     * The bounds are exclusive at one end and the picoseconds are scaled down by {@code factor}, so neither bound is
     * necessarily a value that {@code timestamp with time zone} can represent. They are kept as plain numbers.
     */
    private record LongTimestampWithTimeZoneRange(long lowEpochMillis, int lowPicosOfMilli, long highEpochMillis, int highPicosOfMilli, int factor, short defaultTZ, long step)
    {
        static LongTimestampWithTimeZoneRange of(Range range, int precision)
        {
            return of(range, precision, 1);
        }

        static LongTimestampWithTimeZoneRange of(Range range, int precision, long step)
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
            int unitsPerMilli = PICOSECONDS_PER_MILLISECOND / factor;
            // an unbounded bound keeps the extreme epoch value, so its fraction must not carry
            int lowPicos = range.isLowUnbounded() ? 0 : roundDiv(low.getPicosOfMilli(), factor) + (range.isLowInclusive() ? 0 : 1);
            int highPicos = range.isHighUnbounded() ? unitsPerMilli - 1 : roundDiv(high.getPicosOfMilli(), factor) + (range.isHighInclusive() ? 1 : 0);
            return new LongTimestampWithTimeZoneRange(
                    low.getEpochMillis() + floorDiv(lowPicos, unitsPerMilli),
                    floorMod(lowPicos, unitsPerMilli),
                    high.getEpochMillis() + floorDiv(highPicos, unitsPerMilli),
                    floorMod(highPicos, unitsPerMilli),
                    factor,
                    defaultTZ,
                    step);
        }

        LongTimestampWithTimeZone at(long index)
        {
            // the fractions of low and high are in units of the precision, as is the offset
            long unitsPerMilli = PICOSECONDS_PER_MILLISECOND / factor;
            long offset = sequenceOffsetUnits(index, step, factor);
            long fraction = lowPicosOfMilli + offset % unitsPerMilli;
            long millis = offset / unitsPerMilli + fraction / unitsPerMilli;
            fraction %= unitsPerMilli;
            // the high bound is exclusive
            long lastEpochMillis = lastEpochMillis();
            long lastFraction = highPicosOfMilli > 0 ? highPicosOfMilli - 1 : unitsPerMilli - 1;
            if (millis > lastEpochMillis - lowEpochMillis || (millis == lastEpochMillis - lowEpochMillis && fraction > lastFraction)) {
                return fromEpochMillisAndFraction(lastEpochMillis, toIntExact(lastFraction * factor), defaultTZ);
            }
            return fromEpochMillisAndFraction(lowEpochMillis + millis, toIntExact(fraction * factor), defaultTZ);
        }

        long lastEpochMillis()
        {
            // the high bound is exclusive, so its millisecond only has values before its fraction
            return highPicosOfMilli > 0 ? highEpochMillis : highEpochMillis - 1;
        }
    }

    private record ShortTimeWithTimeZoneRange(long low, long high, long factor, int offsetMinutes, long step)
    {
        static ShortTimeWithTimeZoneRange of(Range range, int precision)
        {
            return of(range, precision, 1);
        }

        static ShortTimeWithTimeZoneRange of(Range range, int precision, long step)
        {
            int offsetMinutes = range.getLowValue()
                    .map(v -> unpackOffsetMinutes((long) v))
                    .orElse(range.getHighValue()
                            .map(v -> unpackOffsetMinutes((long) v))
                            .orElse(0));
            long factor = POWERS_OF_TEN[9 - precision];
            long low = roundDiv(range.getLowValue().map(v -> unpackTimeNanos((long) v)).orElse(0L), factor) + (!range.isLowUnbounded() && !range.isLowInclusive() ? 1 : 0);
            long high = roundDiv(range.getHighValue().map(v -> unpackTimeNanos((long) v)).orElse(NANOSECONDS_PER_DAY), factor) + (!range.isHighUnbounded() && range.isHighInclusive() ? 1 : 0);
            return new ShortTimeWithTimeZoneRange(low, high, factor, offsetMinutes, step);
        }

        long at(long index)
        {
            long nanos = sequenceValue(low, index, step, factor);
            return packTimeWithTimeZone((step > 0 ? Math.min(nanos, high - 1) : Math.max(nanos, high - 1)) * factor, offsetMinutes);
        }
    }

    private record LongTimeWithTimeZoneRange(long low, long high, int factor, int offsetMinutes, long step)
    {
        static LongTimeWithTimeZoneRange of(Range range, int precision)
        {
            return of(range, precision, 1);
        }

        static LongTimeWithTimeZoneRange of(Range range, int precision, long step)
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
            return new LongTimeWithTimeZoneRange(longLow, longHigh, factor, offsetMinutes, step);
        }

        LongTimeWithTimeZone at(long index)
        {
            long picoseconds = sequenceValue(low, index, step, factor);
            return new LongTimeWithTimeZone((step > 0 ? Math.min(picoseconds, high - 1) : Math.max(picoseconds, high - 1)) * factor, offsetMinutes);
        }
    }

    // the offset of a sequence value in units of a precision, where a unit takes the given number of picoseconds
    private static long sequenceOffsetUnits(long index, long stepNanos, long picosPerUnit)
    {
        if (picosPerUnit >= PICOSECONDS_PER_NANOSECOND) {
            return sequenceValue(0, index, stepNanos, picosPerUnit / PICOSECONDS_PER_NANOSECOND);
        }
        return saturatedMultiply(sequenceValue(0, index, stepNanos, 1), PICOSECONDS_PER_NANOSECOND / picosPerUnit);
    }

    // low + index * step / factor overflows for long sequences with large steps, like a day in nanoseconds,
    // so compute it exactly, and saturate the result, since callers limit it to the high bound anyway
    private static long sequenceValue(long low, long index, long step, long factor)
    {
        long product = index * step;
        if (Math.multiplyHigh(index, step) == product >> 63) {
            return saturatedAdd(low, roundDiv(product, factor));
        }
        BigInteger value = new BigDecimal(BigInteger.valueOf(index).multiply(BigInteger.valueOf(step)))
                .divide(BigDecimal.valueOf(factor), 0, RoundingMode.HALF_UP)
                .toBigInteger()
                .add(BigInteger.valueOf(low));
        // both index and step are not negative, so the value can't be lower than low
        return value.min(BigInteger.valueOf(Long.MAX_VALUE)).longValueExact();
    }

    private LongSupplier numberSampler(Random random, Distribution distribution, int min, int max)
    {
        // uniform ints are sampled differently than longs, so keep sampling them as ints to not change the generated data
        if (distribution == UNIFORM) {
            return () -> numberBetweenInclusive(random, min, max);
        }
        return numberSampler(random, distribution, (long) min, max);
    }

    private LongSupplier numberSampler(Random random, Distribution distribution, long min, long max)
    {
        // the span of a range can exceed the max long value, so treat it as unsigned
        double span = UnsignedLong.fromLongBits(max - min).doubleValue();
        return switch (distribution) {
            case UNIFORM -> () -> numberBetweenInclusive(random, min, max);
            case NORMAL -> () -> addOffset(min, max, Math.rint(normalBetween(random, 0, span)));
            case ZIPF -> () -> addOffset(min, max, zipfOffset(random, span + 1));
        };
    }

    private static long addOffset(long min, long max, double offset)
    {
        // the offset is not negative, and can exceed the max long value, so convert it to an unsigned long
        long unsignedOffset;
        if (offset < 0x1p63) {
            unsignedOffset = (long) offset;
        }
        else {
            unsignedOffset = (long) (offset - 0x1p63) + Long.MIN_VALUE;
        }
        // rounding the span to a double can make offsets greater than the span of the range
        if (Long.compareUnsigned(unsignedOffset, max - min) > 0) {
            return max;
        }
        return min + unsignedOffset;
    }

    /**
     * Sample a normal distribution with the mean in the middle of the range, and the standard deviation of a sixth of it.
     */
    private double normalBetween(Random random, double min, double max)
    {
        // halve the bounds before adding or subtracting them, so they don't overflow
        double mean = min / 2 + max / 2;
        double standardDeviation = max / 6 - min / 6;
        double value = mean;
        // Resample the 0.27% of values farther than 3 standard deviations from the mean, so they don't accumulate
        // at the bounds. Clamping only after a few failed attempts bounds the time to generate a value.
        for (int attempt = 0; attempt < MAX_NORMAL_ATTEMPTS; attempt++) {
            value = random.nextGaussian(mean, standardDeviation);
            if (value >= min && value <= max) {
                return value;
            }
        }
        return Math.clamp(value, min, max);
    }

    /**
     * Sample the offset from the first of {@code count} values, where the offset {@code k} has a probability
     * proportional to {@code 1 / (k + 1)}, which is a bounded Zipf distribution with an exponent of 1.
     * <p>
     * This is the rejection-inversion method from W. Hormann and G. Derflinger, "Rejection-inversion to generate
     * variates from monotone discrete distributions", ACM Transactions on Modeling and Computer Simulation 6(3), 1996,
     * as implemented in {@code RejectionInversionZipfSampler} from Apache Commons RNG, simplified for an exponent of 1.
     * It takes constant expected time, regardless of the number of values.
     */
    private double zipfOffset(Random random, double count)
    {
        // log is the integral of the 1 / x hat function, and exp is its inverse
        double hatIntegralLow = Math.log(1.5) - 1;
        double hatIntegralHigh = Math.log(count + 0.5);
        while (true) {
            double u = hatIntegralHigh + random.nextDouble() * (hatIntegralLow - hatIntegralHigh);
            double x = Math.exp(u);
            double rank = Math.clamp(Math.floor(x + 0.5), 1, count);
            if (rank - x <= ZIPF_SQUEEZE || u >= Math.log(rank + 0.5) - 1 / rank) {
                return rank - 1;
            }
        }
    }

    private int numberBetweenInclusive(Random random, int min, int max)
    {
        if (max < Integer.MAX_VALUE) {
            return numberBetween(random, min, max + 1);
        }
        return (int) numberBetweenInclusive(random, (long) min, max);
    }

    private long numberBetweenInclusive(Random random, long min, long max)
    {
        if (max < Long.MAX_VALUE) {
            return numberBetween(random, min, max + 1);
        }
        if (min == Long.MIN_VALUE) {
            return random.nextLong();
        }
        // Shift the sampling interval when its inclusive upper bound cannot be incremented.
        return numberBetween(random, min - 1, max) + 1;
    }

    private int numberBetween(Random random, int min, int max)
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
        return (int) numberBetween(random, realMin, (long) realMax);
    }

    private long numberBetween(Random random, long min, long max)
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
        // the amplitude overflows, so sample all longs until one is in the range, which takes 2 attempts at most on average
        return random.nextLong(realMin, realMax);
    }

    private Generator generateIpV4(Type type, Range range, Random random)
    {
        if (!range.isAll()) {
            throw new TrinoException(INVALID_ROW_FILTER, "Predicates for ipaddress columns are not supported");
        }
        return blockBuilder -> {
            byte[] address;
            try {
                address = Inet4Address.getByAddress(new byte[] {
                        (byte) (random.nextInt(254) + 2),
                        (byte) (random.nextInt(254) + 2),
                        (byte) (random.nextInt(254) + 2),
                        (byte) (random.nextInt(254) + 2),
                }).getAddress();
            }
            catch (UnknownHostException e) {
                // ignore
                blockBuilder.appendNull();
                return;
            }

            byte[] bytes = new byte[16];
            bytes[10] = (byte) 0xFF;
            bytes[11] = (byte) 0xFF;
            arraycopy(address, 0, bytes, 12, 4);

            type.writeSlice(blockBuilder, Slices.wrappedBuffer(bytes, 0, 16));
        };
    }

    private Generator generateUUID(Range range, Random random)
    {
        if (!range.isAll()) {
            throw new TrinoException(INVALID_ROW_FILTER, "Predicates for uuid columns are not supported");
        }
        return blockBuilder -> {
            ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
            // set the version 4 and IETF variant bits, like UUID.randomUUID() does
            bb.putLong((random.nextLong() & 0xFFFF_FFFF_FFFF_0FFFL) | 0x0000_0000_0000_4000L);
            bb.putLong((random.nextLong() & 0x3FFF_FFFF_FFFF_FFFFL) | 0x8000_0000_0000_0000L);
            UUID.writeSlice(blockBuilder, Slices.wrappedBuffer(bb.array(), 0, 16));
        };
    }

    @FunctionalInterface
    private interface ObjectWriter
    {
        void accept(BlockBuilder blockBuilder, Object value);
    }

    @FunctionalInterface
    private interface ColumnGenerator
    {
        void accept(BlockBuilder blockBuilder, long rowId);
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
