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

import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRangeSetOperations
{
    @Test
    public void testSubtractionBoundaries()
    {
        for (Type type : List.of(BIGINT, VARCHAR, DOUBLE, REAL)) {
            List<ValueSet> sets = new ArrayList<>();
            sets.add(ValueSet.none(type));
            sets.add(ValueSet.all(type));
            for (int low = -2; low <= 2; low++) {
                sets.add(ValueSet.ofRanges(Range.equal(type, value(type, low))));
                sets.add(ValueSet.ofRanges(Range.lessThan(type, value(type, low))));
                sets.add(ValueSet.ofRanges(Range.lessThanOrEqual(type, value(type, low))));
                sets.add(ValueSet.ofRanges(Range.greaterThan(type, value(type, low))));
                sets.add(ValueSet.ofRanges(Range.greaterThanOrEqual(type, value(type, low))));
                for (int high = low + 1; high <= 2; high++) {
                    for (boolean lowInclusive : List.of(false, true)) {
                        for (boolean highInclusive : List.of(false, true)) {
                            sets.add(ValueSet.ofRanges(Range.range(type, value(type, low), lowInclusive, value(type, high), highInclusive)));
                        }
                    }
                }
            }
            sets.add(ValueSet.of(type, value(type, -1), value(type, 1)));
            for (boolean lowInclusive : List.of(false, true)) {
                for (boolean highInclusive : List.of(false, true)) {
                    sets.add(ValueSet.ofRanges(
                            Range.range(type, value(type, -2), lowInclusive, value(type, -1), highInclusive),
                            Range.range(type, value(type, 1), lowInclusive, value(type, 2), highInclusive)));
                    sets.add(ValueSet.ofRanges(
                            lowInclusive ? Range.lessThanOrEqual(type, value(type, -1)) : Range.lessThan(type, value(type, -1)),
                            highInclusive ? Range.greaterThanOrEqual(type, value(type, 1)) : Range.greaterThan(type, value(type, 1))));
                }
            }
            if (type.equals(DOUBLE) || type.equals(REAL)) {
                ValueSet nan = ValueSet.of(type, FloatingPointValueSet.nanValue(type));
                sets.add(nan);
                sets.add(nan.complement());
                sets.add(nan.union(ValueSet.of(type, value(type, 0))));
                sets.add(ValueSet.of(type, floatingValue(type, Double.NEGATIVE_INFINITY), floatingValue(type, Double.POSITIVE_INFINITY), floatingValue(type, -0.0)));
            }
            for (ValueSet left : sets) {
                for (ValueSet right : sets) {
                    assertThat(left.subtract(right)).as("%s minus %s", left, right)
                            .isEqualTo(left.intersect(right.complement()));
                }
            }
        }
    }

    @Test
    public void testSkewedContainmentBoundaries()
    {
        for (Type type : List.of(BIGINT, VARCHAR, DOUBLE)) {
            ValueSet large = ValueSet.copyOfRanges(type, IntStream.range(0, 1000)
                    .mapToObj(index -> Range.range(type, value(type, index * 4), index % 2 == 0, value(type, index * 4 + 2), index % 3 == 0))
                    .toList());
            for (int index : List.of(0, 1, 500, 999, 1000)) {
                for (int offset = -1; offset <= 3; offset++) {
                    Object low = value(type, index * 4 + offset);
                    Object high = value(type, index * 4 + offset + 1);
                    for (ValueSet candidate : List.of(
                            ValueSet.of(type, low),
                            ValueSet.ofRanges(Range.range(type, low, false, high, false)),
                            ValueSet.ofRanges(Range.range(type, low, true, high, true)),
                            ValueSet.ofRanges(Range.lessThan(type, low)),
                            ValueSet.ofRanges(Range.greaterThan(type, low)))) {
                        assertThat(large.contains(candidate)).as("%s contains %s", type, candidate)
                                .isEqualTo(large.intersect(candidate).equals(candidate));
                    }
                }
            }
            ValueSet several = ValueSet.copyOfRanges(type, List.of(
                    Range.equal(type, value(type, 0)),
                    Range.range(type, value(type, 2000), false, value(type, 2002), false),
                    Range.equal(type, value(type, 3997))));
            assertThat(large.contains(several)).isTrue();
        }
    }

    @Test
    public void testRandomizedOperations()
    {
        Random random = new Random(260618);
        for (Type type : List.of(BIGINT, VARCHAR, DOUBLE, REAL)) {
            for (int iteration = 0; iteration < 200; iteration++) {
                List<ValueSet> sets = new ArrayList<>();
                List<Range> allRanges = new ArrayList<>();
                int setCount = 3 + random.nextInt(8);
                for (int setIndex = 0; setIndex < setCount; setIndex++) {
                    List<Range> ranges = new ArrayList<>();
                    int rangeCount = random.nextInt(12);
                    for (int rangeIndex = 0; rangeIndex < rangeCount; rangeIndex++) {
                        int low = random.nextInt(40);
                        int high = low + 1 + random.nextInt(8);
                        Range range = switch (random.nextInt(12)) {
                            case 0 -> Range.lessThan(type, value(type, high));
                            case 1 -> Range.greaterThan(type, value(type, low));
                            case 2 -> Range.equal(type, value(type, low));
                            default -> Range.range(type, value(type, low), random.nextBoolean(), value(type, high), random.nextBoolean());
                        };
                        ranges.add(range);
                    }
                    sets.add(ValueSet.copyOfRanges(type, ranges));
                    allRanges.addAll(ranges);
                }
                ValueSet actual = sets.getFirst().union(sets.subList(1, sets.size()));
                assertThat(actual).isEqualTo(ValueSet.copyOfRanges(type, allRanges));
                ValueSet left = sets.get(0);
                ValueSet right = sets.get(1);
                assertThat(left.subtract(right)).isEqualTo(left.intersect(right.complement()));
            }
        }
    }

    @Test
    public void testGrowingAndCompactedUnionResults()
    {
        List<ValueSet> sets = new ArrayList<>();
        List<Range> expected = new ArrayList<>();
        for (int setIndex = 0; setIndex < 7; setIndex++) {
            int offset = setIndex;
            List<Range> ranges = IntStream.range(0, 1000)
                    .mapToObj(index -> Range.range(BIGINT, (index * 7L + offset) * 4, true, (index * 7L + offset) * 4 + 2, true)).toList();
            sets.add(ValueSet.copyOfRanges(BIGINT, ranges));
            expected.addAll(ranges);
        }
        assertThat(sets.getFirst().union(sets.subList(1, sets.size())))
                .isEqualTo(ValueSet.copyOfRanges(BIGINT, expected));

        ValueSet bridge = ValueSet.ofRanges(Range.range(BIGINT, 0L, true, 28_000L, true));
        sets.add(bridge);
        ValueSet compacted = sets.getFirst().union(sets.subList(1, sets.size()));
        assertThat(compacted).isEqualTo(bridge);
        assertThat(compacted.getRetainedSizeInBytes()).isLessThan(1024);
    }

    @Test
    public void testLargeSubtractionResults()
    {
        ValueSet points = ValueSet.copyOf(BIGINT, IntStream.range(0, 10_000).mapToObj(index -> index * 4L + 1).toList());
        ValueSet range = ValueSet.ofRanges(Range.range(BIGINT, 0L, true, 40_000L, true));
        assertThat(range.subtract(points)).isEqualTo(range.intersect(points.complement()));
        ValueSet ranges = ValueSet.ofRanges(
                Range.range(BIGINT, 0L, true, 19_998L, true),
                Range.range(BIGINT, 20_000L, true, 40_000L, true));
        assertThat(ranges.subtract(points)).isEqualTo(ranges.intersect(points.complement()));
    }

    private static Object value(Type type, int value)
    {
        if (type.equals(BIGINT)) {
            return (long) value;
        }
        if (type.equals(VARCHAR)) {
            return utf8Slice("%05d".formatted(value + 1000));
        }
        return floatingValue(type, value);
    }

    private static Object floatingValue(Type type, double value)
    {
        if (type.equals(DOUBLE)) {
            return value;
        }
        return (long) floatToRawIntBits((float) value);
    }
}
