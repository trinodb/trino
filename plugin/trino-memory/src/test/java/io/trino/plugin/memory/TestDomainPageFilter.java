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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDomainPageFilter
{
    private static final DecimalType SHORT_DECIMAL = createDecimalType(10, 2);
    private static final DecimalType LONG_DECIMAL = createDecimalType(30, 2);
    private static final CharType CHAR_TYPE = createCharType(8);

    private static final List<ValueGenerator> GENERATORS = ImmutableList.of(
            longGenerator(BIGINT, Long.MIN_VALUE, Long.MAX_VALUE),
            longGenerator(INTEGER, Integer.MIN_VALUE, Integer.MAX_VALUE),
            longGenerator(SMALLINT, Short.MIN_VALUE, Short.MAX_VALUE),
            longGenerator(TINYINT, Byte.MIN_VALUE, Byte.MAX_VALUE),
            longGenerator(DATE, Integer.MIN_VALUE, Integer.MAX_VALUE),
            longGenerator(SHORT_DECIMAL, -9_999_999_999L, 9_999_999_999L),
            new ValueGenerator(
                    LONG_DECIMAL,
                    LongStream.rangeClosed(-150, 150).mapToObj(value -> (Object) Int128.valueOf(value)).toList(),
                    ImmutableList.of()),
            new ValueGenerator(
                    VARCHAR,
                    varcharValues(),
                    ImmutableList.of()),
            new ValueGenerator(
                    BOOLEAN,
                    ImmutableList.of(false, true),
                    ImmutableList.of()),
            new ValueGenerator(
                    DOUBLE,
                    LongStream.rangeClosed(-150, 150).mapToObj(value -> (Object) (value / 2.0)).toList(),
                    ImmutableList.of(Double.NaN)),
            new ValueGenerator(
                    CHAR_TYPE,
                    stringValues(),
                    ImmutableList.of()),
            new ValueGenerator(
                    VARBINARY,
                    binaryValues(),
                    ImmutableList.of()),
            new ValueGenerator(
                    TIMESTAMP_MILLIS,
                    LongStream.rangeClosed(-150, 150).mapToObj(value -> (Object) (value * 1000)).toList(),
                    ImmutableList.of()),
            new ValueGenerator(
                    TIMESTAMP_NANOS,
                    LongStream.rangeClosed(-60, 60)
                            .boxed()
                            .flatMap(micros -> Stream.of(0, 500_000).map(picos -> (Object) new LongTimestamp(micros, picos)))
                            .toList(),
                    ImmutableList.of()),
            // the same instants as the ordered values in a different zone compare equal to them
            new ValueGenerator(
                    TIMESTAMP_TZ_MILLIS,
                    LongStream.rangeClosed(-150, 150).mapToObj(value -> (Object) packDateTimeWithZone(value, UTC_KEY)).toList(),
                    LongStream.rangeClosed(-150, 150).mapToObj(value -> (Object) packDateTimeWithZone(value, "Europe/Warsaw")).toList()),
            new ValueGenerator(
                    REAL,
                    LongStream.rangeClosed(-150, 150).mapToObj(value -> (Object) (long) floatToRawIntBits(value / 2.0f)).toList(),
                    ImmutableList.of((long) floatToRawIntBits(Float.NaN))));

    @Test
    void testMatchesIncludesNullableValue()
    {
        Random random = new Random(42);
        for (ValueGenerator generator : GENERATORS) {
            for (Domain domain : generator.domains(random)) {
                for (Block block : generator.blocks(random)) {
                    assertFilter(ImmutableMap.of(0, domain), new Page(block));
                }
            }
        }
    }

    @Test
    void testMultipleColumns()
    {
        Random random = new Random(7);
        for (int iteration = 0; iteration < 300; iteration++) {
            int columns = 2 + random.nextInt(3);
            ImmutableMap.Builder<Integer, Domain> domains = ImmutableMap.builder();
            Block[] blocks = new Block[columns];
            int positionCount = random.nextInt(200);
            for (int channel = 0; channel < columns; channel++) {
                ValueGenerator generator = GENERATORS.get(random.nextInt(GENERATORS.size()));
                List<Domain> candidates = generator.domains(random);
                domains.put(channel, candidates.get(random.nextInt(candidates.size())));
                blocks[channel] = generator.block(random, positionCount, random.nextInt(5));
            }
            assertFilter(domains.buildOrThrow(), new Page(positionCount, blocks));
        }
    }

    @Test
    void testChannelsWithoutDomain()
    {
        Page page = new Page(
                createBlock(BIGINT, Arrays.asList(1L, 2L, null, 4L)),
                createBlock(BIGINT, Arrays.asList(10L, 20L, 30L, 40L)));
        assertThat(filter(ImmutableMap.of(1, Domain.singleValue(BIGINT, 30L)), page)).containsExactly(2);
        assertThat(filter(ImmutableMap.of(), page)).containsExactly(0, 1, 2, 3);
        assertThat(filter(ImmutableMap.of(0, Domain.all(BIGINT), 1, Domain.all(BIGINT)), page)).containsExactly(0, 1, 2, 3);
        assertThat(filter(ImmutableMap.of(0, Domain.all(BIGINT), 1, Domain.none(BIGINT)), page)).isEmpty();
    }

    @Test
    void testLongBoundaries()
    {
        Block block = createBlock(BIGINT, Arrays.asList(Long.MIN_VALUE, Long.MIN_VALUE + 1, -1L, 0L, 1L, Long.MAX_VALUE - 1, Long.MAX_VALUE, null));
        Page page = new Page(block);

        assertThat(filter(domain(false, Range.greaterThan(BIGINT, Long.MAX_VALUE)), page)).isEmpty();
        assertThat(filter(domain(false, Range.lessThan(BIGINT, Long.MIN_VALUE)), page)).isEmpty();
        assertThat(filter(domain(false, Range.greaterThan(BIGINT, Long.MAX_VALUE - 1)), page)).containsExactly(6);
        assertThat(filter(domain(false, Range.lessThan(BIGINT, Long.MIN_VALUE + 1)), page)).containsExactly(0);
        assertThat(filter(domain(true, Range.greaterThanOrEqual(BIGINT, Long.MAX_VALUE)), page)).containsExactly(6, 7);
        assertThat(filter(domain(false, Range.lessThanOrEqual(BIGINT, Long.MIN_VALUE)), page)).containsExactly(0);
        assertThat(filter(domain(false, Range.all(BIGINT)), page)).containsExactly(0, 1, 2, 3, 4, 5, 6);
        assertThat(filter(domain(false, Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 0L)), page)).containsExactly(0, 1, 2, 4, 5, 6);
        assertThat(filter(domain(false, Range.range(BIGINT, Long.MIN_VALUE, false, Long.MAX_VALUE, false)), page)).containsExactly(1, 2, 3, 4, 5);
    }

    private static Domain domain(boolean nullAllowed, Range first, Range... rest)
    {
        return Domain.create(ValueSet.ofRanges(first, rest), nullAllowed);
    }

    private static int[] filter(Domain domain, Page page)
    {
        return filter(ImmutableMap.of(0, domain), page);
    }

    private static int[] filter(Map<Integer, Domain> domains, Page page)
    {
        int[] positions = new int[page.getPositionCount()];
        int length = DomainPageFilter.create(domains).filter(SourcePage.create(page), positions);
        return Arrays.copyOf(positions, length);
    }

    private static void assertFilter(Map<Integer, Domain> domains, Page page)
    {
        List<Integer> expected = new ArrayList<>();
        for (int position = 0; position < page.getPositionCount(); position++) {
            boolean matches = true;
            for (Map.Entry<Integer, Domain> entry : domains.entrySet()) {
                Domain domain = entry.getValue();
                Object value = readNativeValue(domain.getType(), page.getBlock(entry.getKey()), position);
                matches &= domain.includesNullableValue(value);
            }
            if (matches) {
                expected.add(position);
            }
        }
        assertThat(filter(domains, page))
                .as("domains %s on blocks %s", domains, Arrays.stream(blocks(page)).map(block -> block.getClass().getSimpleName()).toList())
                .containsExactly(expected.stream().mapToInt(Integer::intValue).toArray());
    }

    private static Block[] blocks(Page page)
    {
        Block[] blocks = new Block[page.getChannelCount()];
        for (int channel = 0; channel < blocks.length; channel++) {
            blocks[channel] = page.getBlock(channel);
        }
        return blocks;
    }

    private static Block createBlock(Type type, List<?> values)
    {
        BlockBuilder builder = type.createBlockBuilder(null, values.size());
        for (Object value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                writeNativeValue(type, builder, value);
            }
        }
        return builder.build();
    }

    private static ValueGenerator longGenerator(Type type, long min, long max)
    {
        // extremes of the type and values around zero
        List<Object> values = LongStream.concat(
                        LongStream.rangeClosed(-150, 150),
                        LongStream.of(min, min + 1, max - 1, max))
                .filter(value -> value >= min && value <= max)
                .distinct()
                .sorted()
                .boxed()
                .map(Object.class::cast)
                .toList();
        return new ValueGenerator(type, values, ImmutableList.of());
    }

    private static List<Object> varcharValues()
    {
        return stringValues(Slices.utf8Slice("\u00e9"));
    }

    private static List<Object> stringValues()
    {
        return stringValues(Slices.utf8Slice("z"));
    }

    private static List<Object> stringValues(Slice extra)
    {
        List<Slice> values = new ArrayList<>();
        values.add(Slices.utf8Slice(""));
        values.add(extra);
        values.add(Slices.utf8Slice("~"));
        for (int i = 0; i < 300; i++) {
            values.add(Slices.utf8Slice("key" + i));
        }
        values.sort(Comparator.naturalOrder());
        return ImmutableList.copyOf(values);
    }

    // bytes above 0x7f must be ordered after the lower ones
    private static List<Object> binaryValues()
    {
        List<Slice> values = new ArrayList<>();
        for (int first = 0; first < 256; first += 5) {
            values.add(Slices.wrappedBuffer((byte) first));
            values.add(Slices.wrappedBuffer((byte) first, (byte) 0));
            values.add(Slices.wrappedBuffer((byte) first, (byte) 0xFF));
        }
        values.add(Slices.EMPTY_SLICE);
        values.sort(Comparator.naturalOrder());
        return ImmutableList.copyOf(values);
    }

    /**
     * Generates native values, domains and blocks of one type.
     *
     * @param orderedValues distinct values in ascending order, used for the domain bounds and in the blocks
     * @param unorderedValues values that are in the blocks, but cannot be used as a range bound (NaN)
     */
    private record ValueGenerator(Type type, List<Object> orderedValues, List<Object> unorderedValues)
    {
        List<Domain> domains(Random random)
        {
            List<Domain> domains = new ArrayList<>();
            domains.add(Domain.none(type));
            domains.add(Domain.all(type));
            domains.add(Domain.onlyNull(type));
            domains.add(Domain.notNull(type));
            for (boolean nullAllowed : new boolean[] {false, true}) {
                domains.add(Domain.create(ValueSet.all(type), nullAllowed));
                domains.add(Domain.create(ValueSet.none(type), nullAllowed));
                for (int i = 0; i < 3; i++) {
                    domains.add(Domain.create(ValueSet.of(type, randomValue(random)), nullAllowed));
                }
                for (int size : new int[] {2, 3, 5, 20, 200}) {
                    List<Object> values = new ArrayList<>();
                    for (int i = 0; i < size; i++) {
                        values.add(randomValue(random));
                    }
                    if (!unorderedValues.isEmpty() && random.nextBoolean()) {
                        values.add(unorderedValues.get(random.nextInt(unorderedValues.size())));
                    }
                    ValueSet discrete = ValueSet.copyOf(type, values);
                    domains.add(Domain.create(discrete, nullAllowed));
                    domains.add(Domain.create(discrete.complement(), nullAllowed));
                }
                for (int i = 0; i < 4; i++) {
                    Object bound = randomValue(random);
                    domains.add(Domain.create(ValueSet.ofRanges(Range.greaterThan(type, bound)), nullAllowed));
                    domains.add(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, bound)), nullAllowed));
                    domains.add(Domain.create(ValueSet.ofRanges(Range.lessThan(type, bound)), nullAllowed));
                    domains.add(Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, bound)), nullAllowed));
                    domains.add(Domain.create(ValueSet.ofRanges(Range.all(type)), nullAllowed));
                    domains.add(Domain.create(randomRanges(random, 1 + random.nextInt(2)), nullAllowed));
                    domains.add(Domain.create(randomRanges(random, 3 + random.nextInt(10)), nullAllowed));
                }
            }
            return domains;
        }

        private ValueSet randomRanges(Random random, int count)
        {
            int[] indexes = random.ints(2L * count, 0, orderedValues.size()).sorted().toArray();
            List<Range> ranges = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                Object low = orderedValues.get(indexes[2 * i]);
                Object high = orderedValues.get(indexes[2 * i + 1]);
                if (indexes[2 * i] == indexes[2 * i + 1]) {
                    ranges.add(Range.equal(type, low));
                }
                else {
                    ranges.add(Range.range(type, low, random.nextBoolean(), high, random.nextBoolean()));
                }
            }
            // an unbounded end on the first and the last range
            if (random.nextInt(4) == 0) {
                ranges.add(Range.lessThan(type, orderedValues.get(indexes[0])));
            }
            if (random.nextInt(4) == 0) {
                ranges.add(Range.greaterThan(type, orderedValues.get(indexes[indexes.length - 1])));
            }
            return ValueSet.ofRanges(ranges);
        }

        private Object randomValue(Random random)
        {
            return orderedValues.get(random.nextInt(orderedValues.size()));
        }

        List<Block> blocks(Random random)
        {
            List<Block> blocks = new ArrayList<>();
            for (int shape = 0; shape < 5; shape++) {
                for (int positionCount : new int[] {0, 1, 7, 100, 500}) {
                    blocks.add(block(random, positionCount, shape));
                }
            }
            return blocks;
        }

        /**
         * @param shape 0 flat, 1 flat with nulls, 2 dictionary smaller than the positions, 3 dictionary larger than the positions, 4 run length encoded
         */
        Block block(Random random, int positionCount, int shape)
        {
            return switch (shape) {
                case 0 -> flatBlock(random, positionCount, 0);
                case 1 -> flatBlock(random, positionCount, 0.3);
                case 2 -> dictionaryBlock(random, positionCount, Math.max(1, positionCount / 4));
                case 3 -> dictionaryBlock(random, positionCount, positionCount * 2 + 1);
                default -> {
                    List<Object> value = new ArrayList<>();
                    value.add(random.nextInt(4) == 0 ? null : randomValue(random));
                    yield RunLengthEncodedBlock.create(createBlock(type, value), positionCount);
                }
            };
        }

        private Block flatBlock(Random random, int positionCount, double nullRate)
        {
            // a region of a larger block, to have a block with an offset
            int offset = random.nextInt(4);
            Block block = createBlock(type, randomValues(random, positionCount + offset + 1, nullRate));
            return block.getRegion(offset, positionCount);
        }

        private Block dictionaryBlock(Random random, int positionCount, int dictionarySize)
        {
            Block dictionary = createBlock(type, randomValues(random, dictionarySize, 0.2));
            int[] ids = new int[positionCount + 3];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = random.nextInt(dictionarySize);
            }
            return DictionaryBlock.create(ids.length, dictionary, ids).getRegion(2, positionCount);
        }

        private List<Object> randomValues(Random random, int count, double nullRate)
        {
            List<Object> values = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                if (random.nextDouble() < nullRate) {
                    values.add(null);
                }
                else if (!unorderedValues.isEmpty() && random.nextInt(10) == 0) {
                    values.add(unorderedValues.get(random.nextInt(unorderedValues.size())));
                }
                else {
                    values.add(randomValue(random));
                }
            }
            return values;
        }
    }
}
