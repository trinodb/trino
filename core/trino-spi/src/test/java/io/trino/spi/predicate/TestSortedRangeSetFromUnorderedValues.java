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
package io.trino.spi.predicate;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.predicate.Utils.TUPLE_DOMAIN_TYPE_OPERATORS;
import static io.trino.spi.predicate.Utils.handleThrowable;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that building a {@link SortedRangeSet} from unordered values gives the same
 * result as the original list based sorting algorithm, which is kept here as a reference.
 */
public class TestSortedRangeSetFromUnorderedValues
{
    private static final long SEED = 20260503;
    private static final List<Integer> SIZES = List.of(1, 2, 3, 10, 17, 33, 100, 1000, 20000);
    private static final List<String> TIME_ZONES = List.of("UTC", "Europe/Warsaw", "America/Los_Angeles", "Asia/Kolkata");

    private enum Pattern
    {
        RANDOM,
        FEW_DISTINCT,
        SORTED,
        SORTED_WITH_DUPLICATES,
        REVERSE_SORTED,
        REVERSE_SORTED_WITH_DUPLICATES,
        ALL_EQUAL,
    }

    private record TypeCase(Type type, Function<Random, Object> generator) {}

    private static List<TypeCase> typeCases()
    {
        return List.of(
                new TypeCase(BIGINT, random -> random.nextLong(-100_000, 100_000)),
                new TypeCase(BIGINT, Random::nextLong),
                new TypeCase(INTEGER, random -> (long) random.nextInt(-50_000, 50_000)),
                new TypeCase(INTEGER, random -> (long) random.nextInt()),
                new TypeCase(SMALLINT, random -> (long) random.nextInt(Short.MIN_VALUE, Short.MAX_VALUE + 1)),
                new TypeCase(TINYINT, random -> (long) random.nextInt(Byte.MIN_VALUE, Byte.MAX_VALUE + 1)),
                new TypeCase(DATE, random -> (long) random.nextInt(-10_000, 30_000)),
                new TypeCase(BOOLEAN, Random::nextBoolean),
                new TypeCase(VARCHAR, random -> randomSlice(random, 12)),
                new TypeCase(VARCHAR, random -> randomSlice(random, 2)),
                new TypeCase(VARBINARY, random -> randomSlice(random, 12)),
                // bytes above 0x7F order after the ASCII ones, as they are compared unsigned
                new TypeCase(VARBINARY, random -> randomBinary(random, 5)),
                new TypeCase(VARCHAR, TestSortedRangeSetFromUnorderedValues::randomUnicodeSlice),
                new TypeCase(UUID, random -> javaUuidToTrinoUuid(new java.util.UUID(random.nextLong(), random.nextLong()))),
                // a few distinct bytes, some above 0x7F, to get equal values that differ in a single byte
                new TypeCase(UUID, TestSortedRangeSetFromUnorderedValues::randomUuidWithFewDistinctBytes),
                new TypeCase(DOUBLE, TestSortedRangeSetFromUnorderedValues::randomDouble),
                new TypeCase(REAL, random -> (long) floatToRawIntBits((float) randomDouble(random))),
                new TypeCase(TIME_MILLIS, random -> random.nextLong(0, 86_400_000) * 1_000_000_000L),
                new TypeCase(createDecimalType(10, 2), random -> random.nextLong(-1_000_000, 1_000_000)),
                new TypeCase(createDecimalType(30, 2), random -> Int128.valueOf(random.nextLong(-1_000, 1_000), random.nextLong(-100, 100))),
                new TypeCase(TIMESTAMP_MILLIS, random -> random.nextLong(-1_000, 1_000) * 1_000),
                new TypeCase(TIMESTAMP_NANOS, random -> new LongTimestamp(random.nextLong(-1_000, 1_000), random.nextInt(1_000) * 1_000)),
                // values with the same instant in different zones compare as equal
                new TypeCase(TIMESTAMP_TZ_MILLIS, random -> packDateTimeWithZone(random.nextLong(-1_000, 1_000), TimeZoneKey.getTimeZoneKey(TIME_ZONES.get(random.nextInt(TIME_ZONES.size()))))));
    }

    @Test
    public void testCollection()
    {
        Random random = new Random(SEED);
        for (TypeCase typeCase : typeCases()) {
            for (int size : SIZES) {
                for (Pattern pattern : Pattern.values()) {
                    long seed = random.nextLong();
                    List<Object> values = generate(typeCase, size, pattern, new Random(seed));
                    String description = "type=%s, size=%s, pattern=%s, seed=%s".formatted(typeCase.type(), size, pattern, seed);

                    assertSameAsReference(typeCase.type(), SortedRangeSet.of(typeCase.type(), values), values, description);
                }
            }
        }
    }

    @Test
    public void testVarargs()
    {
        Random random = new Random(SEED);
        for (TypeCase typeCase : typeCases()) {
            for (int size : List.of(1, 2, 3, 10, 100)) {
                for (Pattern pattern : Pattern.values()) {
                    long seed = random.nextLong();
                    List<Object> values = generate(typeCase, size, pattern, new Random(seed));
                    String description = "type=%s, size=%s, pattern=%s, seed=%s".formatted(typeCase.type(), size, pattern, seed);

                    SortedRangeSet actual = SortedRangeSet.of(typeCase.type(), values.getFirst(), values.subList(1, values.size()).toArray());
                    assertSameAsReference(typeCase.type(), actual, values, description);
                }
            }
        }
    }

    @Test
    public void testNegativeZero()
    {
        // zeros of different sign are distinct values
        List<Object> values = List.of(0.0, -0.0, 1.0, -0.0, 0.0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        SortedRangeSet rangeSet = SortedRangeSet.of(DOUBLE, values);
        assertSameAsReference(DOUBLE, rangeSet, values, "doubles");
        assertThat(rangeSet.getDiscreteSet()).isEqualTo(referenceDistinctValues(DOUBLE, values));
    }

    @Test
    public void testKeepsFirstOfEqualValues()
    {
        long warsaw = packDateTimeWithZone(1000, "Europe/Warsaw");
        long utc = packDateTimeWithZone(1000, "UTC");
        long later = packDateTimeWithZone(2000, "UTC");
        long earlier = packDateTimeWithZone(500, "Asia/Kolkata");

        assertThat(SortedRangeSet.of(TIMESTAMP_TZ_MILLIS, List.of(warsaw, utc, later, earlier, utc, warsaw)).getDiscreteSet())
                .containsExactly(earlier, warsaw, later);
        assertThat(SortedRangeSet.of(TIMESTAMP_TZ_MILLIS, List.of(utc, warsaw)).getDiscreteSet())
                .containsExactly(utc);
    }

    private static void assertSameAsReference(Type type, SortedRangeSet actual, List<Object> values, String description)
    {
        List<Object> expectedValues = referenceDistinctValues(type, values);
        assertThat(actual.getDiscreteSet()).as(description).isEqualTo(expectedValues);
        assertThat(actual.getRangeCount()).as(description).isEqualTo(expectedValues.size());
        assertThat(actual.getDiscreteSetMarker()).as(description).isEqualTo(SortedRangeSet.DiscreteSetMarker.DISCRETE);

        // an independently built set must be equal
        List<Range> ranges = expectedValues.stream()
                .map(value -> Range.equal(type, value))
                .toList();
        SortedRangeSet expected = SortedRangeSet.copyOf(type, ranges);
        assertThat(actual).as(description).isEqualTo(expected);
        assertThat(expected).as(description).isEqualTo(actual);
        assertThat(actual.hashCode()).as(description).isEqualTo(expected.hashCode());
        assertThat(actual.toString()).as(description).isEqualTo(expected.toString());
        for (Object value : values) {
            assertThat(actual.containsValue(value)).as(description).isTrue();
        }
    }

    private static List<Object> generate(TypeCase typeCase, int size, Pattern pattern, Random random)
    {
        Type type = typeCase.type();
        return switch (pattern) {
            case RANDOM -> generateValues(typeCase, size, random);
            case FEW_DISTINCT -> {
                List<Object> pool = generateValues(typeCase, Math.max(1, Math.min(size, 3) + size / 50), random);
                List<Object> values = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    values.add(pool.get(random.nextInt(pool.size())));
                }
                yield values;
            }
            case SORTED -> referenceDistinctValues(type, generateValues(typeCase, size, random));
            case SORTED_WITH_DUPLICATES -> repeatRandomly(referenceDistinctValues(type, generateValues(typeCase, size, random)), random);
            case REVERSE_SORTED -> reverse(referenceDistinctValues(type, generateValues(typeCase, size, random)));
            case REVERSE_SORTED_WITH_DUPLICATES -> reverse(repeatRandomly(referenceDistinctValues(type, generateValues(typeCase, size, random)), random));
            case ALL_EQUAL -> Collections.nCopies(size, generateValues(typeCase, 1, random).getFirst());
        };
    }

    private static List<Object> generateValues(TypeCase typeCase, int size, Random random)
    {
        List<Object> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(typeCase.generator().apply(random));
        }
        return values;
    }

    private static List<Object> repeatRandomly(List<Object> values, Random random)
    {
        List<Object> result = new ArrayList<>();
        for (Object value : values) {
            int repeats = 1 + random.nextInt(3);
            for (int i = 0; i < repeats; i++) {
                result.add(value);
            }
        }
        return result;
    }

    private static List<Object> reverse(List<Object> values)
    {
        List<Object> result = new ArrayList<>(values);
        Collections.reverse(result);
        return result;
    }

    private static Slice randomSlice(Random random, int maxLength)
    {
        byte[] bytes = new byte[random.nextInt(maxLength + 1)];
        for (int i = 0; i < bytes.length; i++) {
            // printable ASCII keeps values valid for varchar
            bytes[i] = (byte) ('a' + random.nextInt(26));
        }
        return Slices.wrappedBuffer(bytes);
    }

    private static Slice randomUuidWithFewDistinctBytes(Random random)
    {
        byte[] bytes = new byte[16];
        for (int position : new int[] {0, 7, 8, 15}) {
            bytes[position] = (byte) switch (random.nextInt(4)) {
                case 0 -> 0x00;
                case 1 -> 0x01;
                case 2 -> 0x80;
                default -> 0xFF;
            };
        }
        return Slices.wrappedBuffer(bytes);
    }

    private static Slice randomBinary(Random random, int maxLength)
    {
        byte[] bytes = new byte[random.nextInt(maxLength + 1)];
        for (int i = 0; i < bytes.length; i++) {
            // a small set of bytes, to get equal values and values that are a prefix of another
            bytes[i] = (byte) switch (random.nextInt(5)) {
                case 0 -> 0x00;
                case 1 -> 0x01;
                case 2 -> 0x7F;
                case 3 -> 0x80;
                default -> 0xFF;
            };
        }
        return Slices.wrappedBuffer(bytes);
    }

    private static Slice randomUnicodeSlice(Random random)
    {
        String[] characters = {"a", "b", "z", "é", "ÿ", "€", "中", "😀"};
        StringBuilder builder = new StringBuilder();
        int length = random.nextInt(5);
        for (int i = 0; i < length; i++) {
            builder.append(characters[random.nextInt(characters.length)]);
        }
        return Slices.utf8Slice(builder.toString());
    }

    private static double randomDouble(Random random)
    {
        return switch (random.nextInt(20)) {
            case 0 -> 0.0;
            case 1 -> -0.0;
            case 2 -> Double.POSITIVE_INFINITY;
            case 3 -> Double.NEGATIVE_INFINITY;
            case 4, 5, 6 -> random.nextInt(10);
            default -> random.nextDouble() * 2000 - 1000;
        };
    }

    /**
     * The original implementation of unordered values handling in {@link SortedRangeSet},
     * which sorts boxed positions with a list sort and then removes adjacent equal values.
     * Returns the retained values in order.
     */
    private static List<Object> referenceDistinctValues(Type type, List<?> values)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, values.size());
        for (Object value : values) {
            writeNativeValue(type, blockBuilder, value);
        }
        Block block = blockBuilder.build();

        MethodHandle comparisonOperator = TUPLE_DOMAIN_TYPE_OPERATORS.getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));

        List<Integer> indexes = new ArrayList<>(block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            indexes.add(position);
        }
        indexes.sort((left, right) -> compareValues(comparisonOperator, block, left, right));

        List<Object> result = new ArrayList<>();
        result.add(readNativeValue(type, block, indexes.getFirst()));
        for (int i = 1; i < indexes.size(); i++) {
            int compare = compareValues(comparisonOperator, block, indexes.get(i - 1), indexes.get(i));
            assertThat(compare).isLessThanOrEqualTo(0);
            if (compare != 0) {
                result.add(readNativeValue(type, block, indexes.get(i)));
            }
        }
        return result;
    }

    private static int compareValues(MethodHandle comparisonOperator, Block block, int left, int right)
    {
        try {
            return (int) (long) comparisonOperator.invokeExact(block, left, block, right);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }
    }
}
