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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.block.TestingBlockJsonSerde;
import io.trino.spi.type.TestingTypeDeserializer;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import org.assertj.core.api.AssertProvider;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.block.BlockTestUtils.assertBlockEquals;
import static io.trino.spi.predicate.SortedRangeSet.DiscreteSetMarker.UNKNOWN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSortedRangeSet
{
    @Test
    public void testEmptySet()
    {
        SortedRangeSet rangeSet = SortedRangeSet.none(BIGINT);
        assertThat(rangeSet.getType()).isEqualTo(BIGINT);
        assertThat(rangeSet.isNone()).isTrue();
        assertThat(rangeSet.isAll()).isFalse();
        assertThat(rangeSet.isSingleValue()).isFalse();
        assertThat(rangeSet.isDiscreteSet()).isFalse();
        assertThat(rangeSet.getOrderedRanges().isEmpty()).isTrue();
        assertThat(rangeSet.getRangeCount()).isEqualTo(0);
        assertThat(rangeSet.complement()).isEqualTo(SortedRangeSet.all(BIGINT));
        assertThat(rangeSet.containsValue(0L)).isFalse();
        assertThat(rangeSet.toString()).isEqualTo("SortedRangeSet[type=bigint, ranges=0, {}]");
    }

    @Test
    public void testEntireSet()
    {
        SortedRangeSet rangeSet = SortedRangeSet.all(BIGINT);
        assertThat(rangeSet.getType()).isEqualTo(BIGINT);
        assertThat(rangeSet.isNone()).isFalse();
        assertThat(rangeSet.isAll()).isTrue();
        assertThat(rangeSet.isSingleValue()).isFalse();
        assertThat(rangeSet.isDiscreteSet()).isFalse();
        assertThat(rangeSet.getRangeCount()).isEqualTo(1);
        assertThat(rangeSet.complement()).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(rangeSet.containsValue(0L)).isTrue();
        assertThat(rangeSet.toString()).isEqualTo("SortedRangeSet[type=bigint, ranges=1, {(<min>,<max>)}]");
    }

    @Test
    public void testSingleValue()
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(BIGINT, 10L);

        SortedRangeSet complement = SortedRangeSet.of(Range.greaterThan(BIGINT, 10L), Range.lessThan(BIGINT, 10L));

        assertThat(rangeSet.getType()).isEqualTo(BIGINT);
        assertThat(rangeSet.isNone()).isFalse();
        assertThat(rangeSet.isAll()).isFalse();
        assertThat(rangeSet.isSingleValue()).isTrue();
        assertThat(rangeSet.isDiscreteSet()).isTrue();
        assertThat(rangeSet.getOrderedRanges()).isEqualTo(ImmutableList.of(Range.equal(BIGINT, 10L)));
        assertThat(rangeSet.getRangeCount()).isEqualTo(1);
        assertThat(rangeSet.complement()).isEqualTo(complement);
        assertThat(rangeSet.containsValue(10L)).isTrue();
        assertThat(rangeSet.containsValue(9L)).isFalse();
        assertThat(rangeSet.toString()).isEqualTo("SortedRangeSet[type=bigint, ranges=1, {[10]}]");

        assertThat(SortedRangeSet.of(Range.equal(VARCHAR, utf8Slice("LARGE PLATED NICKEL"))).toString()).isEqualTo("SortedRangeSet[type=varchar, ranges=1, {[LARGE PLATED NICKEL]}]");
    }

    @Test
    public void testBoundedSet()
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(
                Range.equal(BIGINT, 10L),
                Range.equal(BIGINT, 0L),
                Range.range(BIGINT, 9L, true, 11L, false),
                Range.equal(BIGINT, 0L),
                Range.range(BIGINT, 2L, true, 4L, true),
                Range.range(BIGINT, 4L, false, 5L, true));

        ImmutableList<Range> normalizedResult = ImmutableList.of(
                Range.equal(BIGINT, 0L),
                Range.range(BIGINT, 2L, true, 5L, true),
                Range.range(BIGINT, 9L, true, 11L, false));

        SortedRangeSet complement = SortedRangeSet.of(
                Range.lessThan(BIGINT, 0L),
                Range.range(BIGINT, 0L, false, 2L, false),
                Range.range(BIGINT, 5L, false, 9L, false),
                Range.greaterThanOrEqual(BIGINT, 11L));

        assertThat(rangeSet.getType()).isEqualTo(BIGINT);
        assertThat(rangeSet.isNone()).isFalse();
        assertThat(rangeSet.isAll()).isFalse();
        assertThat(rangeSet.isSingleValue()).isFalse();
        assertThat(rangeSet.isDiscreteSet()).isFalse();
        assertThat(rangeSet.getOrderedRanges()).isEqualTo(normalizedResult);
        assertThat(rangeSet).isEqualTo(SortedRangeSet.copyOf(BIGINT, normalizedResult));
        assertThat(rangeSet.getRangeCount()).isEqualTo(3);
        assertThat(rangeSet.complement()).isEqualTo(complement);
        assertThat(rangeSet.containsValue(0L)).isTrue();
        assertThat(rangeSet.containsValue(1L)).isFalse();
        assertThat(rangeSet.containsValue(7L)).isFalse();
        assertThat(rangeSet.containsValue(9L)).isTrue();
        assertThat(rangeSet.toString()).isEqualTo("SortedRangeSet[type=bigint, ranges=3, {[0], [2,5], [9,11)}]");
        assertThat(rangeSet.toString(ToStringSession.INSTANCE, 2)).isEqualTo("SortedRangeSet[type=bigint, ranges=3, {[0], ..., [9,11)}]");
        assertThat(rangeSet.toString(ToStringSession.INSTANCE, 1)).isEqualTo("SortedRangeSet[type=bigint, ranges=3, {[0], ...}]");
    }

    @Test
    public void testUnboundedSet()
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(
                Range.greaterThan(BIGINT, 10L),
                Range.lessThanOrEqual(BIGINT, 0L),
                Range.range(BIGINT, 2L, true, 4L, false),
                Range.range(BIGINT, 4L, true, 6L, false),
                Range.range(BIGINT, 1L, false, 2L, false),
                Range.range(BIGINT, 9L, false, 11L, false));

        ImmutableList<Range> normalizedResult = ImmutableList.of(
                Range.lessThanOrEqual(BIGINT, 0L),
                Range.range(BIGINT, 1L, false, 6L, false),
                Range.greaterThan(BIGINT, 9L));

        SortedRangeSet complement = SortedRangeSet.of(
                Range.range(BIGINT, 0L, false, 1L, true),
                Range.range(BIGINT, 6L, true, 9L, true));

        assertThat(rangeSet.getType()).isEqualTo(BIGINT);
        assertThat(rangeSet.isNone()).isFalse();
        assertThat(rangeSet.isAll()).isFalse();
        assertThat(rangeSet.isSingleValue()).isFalse();
        assertThat(rangeSet.isDiscreteSet()).isFalse();
        assertThat(rangeSet.getOrderedRanges()).isEqualTo(normalizedResult);
        assertThat(rangeSet).isEqualTo(SortedRangeSet.copyOf(BIGINT, normalizedResult));
        assertThat(rangeSet.getRangeCount()).isEqualTo(3);
        assertThat(rangeSet.complement()).isEqualTo(complement);
        assertThat(rangeSet.containsValue(0L)).isTrue();
        assertThat(rangeSet.containsValue(4L)).isTrue();
        assertThat(rangeSet.containsValue(7L)).isFalse();
        assertThat(rangeSet.toString()).isEqualTo("SortedRangeSet[type=bigint, ranges=3, {(<min>,0], (1,6), (9,<max>)}]");
    }

    @Test
    public void testCreateWithRanges()
    {
        // two low-unbounded, first shorter
        assertThat(SortedRangeSet.of(Range.lessThan(BIGINT, 5L), Range.lessThan(BIGINT, 10L)).getOrderedRanges())
                .containsExactly(Range.lessThan(BIGINT, 10L));
        assertThat(SortedRangeSet.of(Range.lessThan(BIGINT, 10L), Range.lessThanOrEqual(BIGINT, 10L)).getOrderedRanges())
                .containsExactly(Range.lessThanOrEqual(BIGINT, 10L));

        // two low-unbounded, second shorter
        assertThat(SortedRangeSet.of(Range.lessThan(BIGINT, 10L), Range.lessThan(BIGINT, 5L)).getOrderedRanges())
                .containsExactly(Range.lessThan(BIGINT, 10L));
        assertThat(SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 10L), Range.lessThan(BIGINT, 10L)).getOrderedRanges())
                .containsExactly(Range.lessThanOrEqual(BIGINT, 10L));

        // two high-unbounded, first shorter
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 10L), Range.greaterThan(BIGINT, 5L)).getOrderedRanges())
                .containsExactly(Range.greaterThan(BIGINT, 5L));
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 10L), Range.greaterThanOrEqual(BIGINT, 10L)).getOrderedRanges())
                .containsExactly(Range.greaterThanOrEqual(BIGINT, 10L));

        // two high-unbounded, second shorter
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 5L), Range.greaterThan(BIGINT, 10L)).getOrderedRanges())
                .containsExactly(Range.greaterThan(BIGINT, 5L));
        assertThat(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 10L), Range.greaterThan(BIGINT, 10L)).getOrderedRanges())
                .containsExactly(Range.greaterThanOrEqual(BIGINT, 10L));
    }

    @Test
    public void testGetSingleValue()
    {
        assertThat(SortedRangeSet.of(BIGINT, 0L).getSingleValue()).isEqualTo(0L);
        assertThatThrownBy(() -> SortedRangeSet.all(BIGINT).getSingleValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("SortedRangeSet does not have just a single value");
    }

    @Test
    public void testSpan()
    {
        assertThatThrownBy(() -> SortedRangeSet.none(BIGINT).getSpan())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot get span if no ranges exist");

        assertThat(SortedRangeSet.all(BIGINT).getSpan()).isEqualTo(Range.all(BIGINT));
        assertThat(SortedRangeSet.of(BIGINT, 0L).getSpan()).isEqualTo(Range.equal(BIGINT, 0L));
        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).getSpan()).isEqualTo(Range.range(BIGINT, 0L, true, 1L, true));
        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.greaterThan(BIGINT, 1L)).getSpan()).isEqualTo(Range.greaterThanOrEqual(BIGINT, 0L));
        assertThat(SortedRangeSet.of(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 1L)).getSpan()).isEqualTo(Range.all(BIGINT));
    }

    @Test
    public void testOverlaps()
    {
        assertOverlaps(SortedRangeSet.all(BIGINT), SortedRangeSet.all(BIGINT));
        assertDoesNotOverlap(SortedRangeSet.all(BIGINT), SortedRangeSet.none(BIGINT));
        assertOverlaps(SortedRangeSet.all(BIGINT), SortedRangeSet.of(BIGINT, 0L));
        assertOverlaps(SortedRangeSet.all(BIGINT), SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)));
        assertOverlaps(SortedRangeSet.all(BIGINT), SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)));
        assertOverlaps(SortedRangeSet.all(BIGINT), SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L)));

        assertDoesNotOverlap(SortedRangeSet.none(BIGINT), SortedRangeSet.all(BIGINT));
        assertDoesNotOverlap(SortedRangeSet.none(BIGINT), SortedRangeSet.none(BIGINT));
        assertDoesNotOverlap(SortedRangeSet.none(BIGINT), SortedRangeSet.of(BIGINT, 0L));
        assertDoesNotOverlap(SortedRangeSet.none(BIGINT), SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)));
        assertDoesNotOverlap(SortedRangeSet.none(BIGINT), SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)));
        assertDoesNotOverlap(SortedRangeSet.none(BIGINT), SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L)));

        assertOverlaps(SortedRangeSet.of(BIGINT, 0L), SortedRangeSet.all(BIGINT));
        assertDoesNotOverlap(SortedRangeSet.of(BIGINT, 0L), SortedRangeSet.none(BIGINT));
        assertOverlaps(SortedRangeSet.of(BIGINT, 0L), SortedRangeSet.of(BIGINT, 0L));
        assertOverlaps(SortedRangeSet.of(BIGINT, 0L), SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)));
        assertDoesNotOverlap(SortedRangeSet.of(BIGINT, 0L), SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)));
        assertDoesNotOverlap(SortedRangeSet.of(BIGINT, 0L), SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L)));

        assertOverlaps(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)), SortedRangeSet.of(Range.equal(BIGINT, 1L)));
        assertDoesNotOverlap(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)), SortedRangeSet.of(Range.equal(BIGINT, 2L)));
        assertOverlaps(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)), SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)));
        assertOverlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)), SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)));
        assertDoesNotOverlap(SortedRangeSet.of(Range.lessThan(BIGINT, 0L)), SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)));

        // distinct values
        SortedRangeSet testRangeSet = SortedRangeSet.of(BIGINT, 500L, LongStream.range(501, 1001).boxed().filter(i -> i % 4 == 0).toList().toArray());
        // beginning of set
        assertOverlaps(testRangeSet, SortedRangeSet.of(BIGINT, 500L));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(BIGINT, 499L));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(BIGINT, 400L, LongStream.range(401, 500).boxed().filter(i -> i % 15 == 0).toList().toArray()));
        assertOverlaps(testRangeSet, SortedRangeSet.of(BIGINT, 515L, LongStream.range(530, 550).boxed().filter(i -> i % 15 == 0).toList().toArray()));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(Range.range(BIGINT, 100L, false, 399L, false), Range.range(BIGINT, 400L, false, 499L, false)));
        assertOverlaps(testRangeSet, SortedRangeSet.of(Range.range(BIGINT, 100L, false, 399L, false), Range.range(BIGINT, 400L, false, 501L, false)));
        // end of set
        assertOverlaps(testRangeSet, SortedRangeSet.of(BIGINT, 1000L));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(BIGINT, 1001L));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(BIGINT, 965L, LongStream.range(966, 1100).boxed().filter(i -> i % 15 == 0).toList().toArray()));
        assertOverlaps(testRangeSet, SortedRangeSet.of(BIGINT, 955L, LongStream.range(956, 1100).boxed().filter(i -> i % 15 == 0).toList().toArray()));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(BIGINT, 1001L, LongStream.range(1002L, 1100).boxed().toList().toArray()));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(Range.range(BIGINT, 1000L, false, 1399L, false), Range.range(BIGINT, 1400L, false, 1499L, false)));
        assertOverlaps(testRangeSet, SortedRangeSet.of(Range.range(BIGINT, 999L, false, 1399L, false), Range.range(BIGINT, 1400L, false, 1501L, false)));

        // middle of set
        assertOverlaps(testRangeSet, SortedRangeSet.of(BIGINT, 768L));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(BIGINT, 769L));
        assertOverlaps(testRangeSet, SortedRangeSet.of(BIGINT, 715L, LongStream.range(730, 800).boxed().filter(i -> i % 15 == 0).toList().toArray()));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(BIGINT, 715L, LongStream.range(730, 780).boxed().filter(i -> i % 15 == 0).toList().toArray()));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(Range.range(BIGINT, 768L, false, 772L, false), Range.range(BIGINT, 772L, false, 776L, false)));
        assertOverlaps(testRangeSet, SortedRangeSet.of(Range.range(BIGINT, 764L, false, 770L, false), Range.range(BIGINT, 772L, false, 776L, false)));

        // ranges
        testRangeSet = SortedRangeSet.of(
                Range.range(BIGINT, 499L, false, 505L, false), LongStream.range(100, 201).filter(i -> i % 10 == 0)
                        .mapToObj(i -> Range.range(BIGINT, i * 5, false, (i + 1) * 5, false)).toList().toArray(Range[]::new));
        // beginning of set
        assertOverlaps(testRangeSet, SortedRangeSet.of(BIGINT, 500L));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(BIGINT, 499L));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(BIGINT, 400L, LongStream.range(401, 500).boxed().toList().toArray()));
        assertOverlaps(testRangeSet, SortedRangeSet.of(BIGINT, 520L, 540L, 551L));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(Range.range(BIGINT, 100L, false, 399L, false), Range.range(BIGINT, 400L, false, 499L, false)));
        assertOverlaps(testRangeSet, SortedRangeSet.of(Range.range(BIGINT, 100L, false, 399L, false), Range.range(BIGINT, 400L, false, 501L, false)));
        // end of set
        assertOverlaps(testRangeSet, SortedRangeSet.of(BIGINT, 1001L));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(BIGINT, 1005L));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(BIGINT, 965L, LongStream.range(966, 1100).boxed().filter(i -> i % 15 == 0).toList().toArray()));
        assertOverlaps(testRangeSet, SortedRangeSet.of(BIGINT, 950L, 970L, 999L, 1001L, 1006L));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(BIGINT, 1005L, LongStream.range(1006L, 1100).boxed().toList().toArray()));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(Range.range(BIGINT, 1005L, false, 1399L, false), Range.range(BIGINT, 1400L, false, 1499L, false)));
        assertOverlaps(testRangeSet, SortedRangeSet.of(Range.range(BIGINT, 999L, false, 1399L, false), Range.range(BIGINT, 1400L, false, 1501L, false)));

        // middle of set
        assertOverlaps(testRangeSet, SortedRangeSet.of(BIGINT, 754L));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(BIGINT, 755L));
        assertOverlaps(testRangeSet, SortedRangeSet.of(BIGINT, 715L, LongStream.range(730, 800).boxed().toList().toArray()));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(BIGINT, 715L, LongStream.range(730, 780).boxed().filter(i -> i % 15 == 0).toList().toArray()));
        assertDoesNotOverlap(testRangeSet, SortedRangeSet.of(Range.range(BIGINT, 768L, false, 772L, false), Range.range(BIGINT, 772L, false, 776L, false)));
        assertOverlaps(testRangeSet, SortedRangeSet.of(Range.range(BIGINT, 750L, false, 770L, false), Range.range(BIGINT, 772L, false, 776L, false)));
    }

    private void assertOverlaps(SortedRangeSet first, SortedRangeSet second)
    {
        assertThat(first.overlaps(second)).isTrue();
        assertThat(first.linearSearchOverlaps(second)).isTrue();
        assertThat(first.binarySearchOverlaps(second)).isTrue();
        assertThat(second.overlaps(first)).isTrue();
        assertThat(second.linearSearchOverlaps(first)).isTrue();
        assertThat(second.binarySearchOverlaps(first)).isTrue();
    }

    private void assertDoesNotOverlap(SortedRangeSet first, SortedRangeSet second)
    {
        assertThat(first.overlaps(second)).isFalse();
        assertThat(first.linearSearchOverlaps(second)).isFalse();
        assertThat(first.binarySearchOverlaps(second)).isFalse();
        assertThat(second.overlaps(first)).isFalse();
        assertThat(second.linearSearchOverlaps(first)).isFalse();
        assertThat(second.binarySearchOverlaps(first)).isFalse();
    }

    @Test
    public void testContains()
    {
        assertThat(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.all(BIGINT))).isTrue();
        assertThat(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.none(BIGINT))).isTrue();
        assertThat(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.of(BIGINT, 0L))).isTrue();
        assertThat(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isTrue();
        assertThat(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isTrue();
        assertThat(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L)))).isTrue();

        assertThat(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.all(BIGINT))).isFalse();
        assertThat(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.none(BIGINT))).isTrue();
        assertThat(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.of(BIGINT, 0L))).isFalse();
        assertThat(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isFalse();
        assertThat(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isFalse();
        assertThat(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L)))).isFalse();

        ValueSet valueSet = SortedRangeSet.of(BIGINT, 0L);
        assertThat(valueSet.contains(valueSet)).isTrue();

        assertThat(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.all(BIGINT))).isFalse();
        assertThat(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.none(BIGINT))).isTrue();
        assertThat(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.of(BIGINT, 0L))).isTrue();
        assertThat(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isFalse();
        assertThat(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isFalse();
        assertThat(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L)))).isFalse();

        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).contains(SortedRangeSet.of(Range.equal(BIGINT, 1L)))).isTrue();
        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).contains(SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)))).isFalse();
        assertThat(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isTrue();
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).contains(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)))).isFalse();
        assertThat(SortedRangeSet.of(Range.lessThan(BIGINT, 0L)).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isFalse();

        Range rangeA = Range.range(BIGINT, 0L, true, 2L, true);
        Range rangeB = Range.range(BIGINT, 4L, true, 6L, true);
        Range rangeC = Range.range(BIGINT, 8L, true, 10L, true);
        assertThat(SortedRangeSet.of(rangeA, rangeB).contains(SortedRangeSet.of(rangeC))).isFalse();
        assertThat(SortedRangeSet.of(rangeB, rangeC).contains(SortedRangeSet.of(rangeA))).isFalse();
        assertThat(SortedRangeSet.of(rangeA, rangeC).contains(SortedRangeSet.of(rangeB))).isFalse();
        assertThat(SortedRangeSet.of(rangeA, rangeB).contains(SortedRangeSet.of(rangeB, rangeC))).isFalse();
        assertThat(SortedRangeSet.of(rangeA, rangeB, rangeC).contains(SortedRangeSet.of(rangeA))).isTrue();
        assertThat(SortedRangeSet.of(rangeA, rangeB, rangeC).contains(SortedRangeSet.of(rangeB))).isTrue();
        assertThat(SortedRangeSet.of(rangeA, rangeB, rangeC).contains(SortedRangeSet.of(rangeC))).isTrue();
        assertThat(SortedRangeSet.of(rangeA, rangeB, rangeC).contains(
                SortedRangeSet.of(Range.equal(BIGINT, 4L), Range.equal(BIGINT, 6L), Range.equal(BIGINT, 9L)))).isTrue();
        assertThat(SortedRangeSet.of(rangeA, rangeB, rangeC).contains(
                SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.range(BIGINT, 6L, true, 10L, true)))).isFalse();
    }

    @Test
    public void testContainsValue()
    {
        // BIGINT all
        assertSortedRangeSet(SortedRangeSet.all(BIGINT))
                .containsValue(Long.MIN_VALUE)
                .containsValue(0L)
                .containsValue(42L)
                .containsValue(Long.MAX_VALUE);

        // BIGINT range
        assertSortedRangeSet(SortedRangeSet.of(Range.range(BIGINT, 10L, true, 41L, true)))
                .doesNotContainValue(9L)
                .containsValue(10L)
                .containsValue(11L)
                .containsValue(30L)
                .containsValue(41L)
                .doesNotContainValue(42L);

        assertSortedRangeSet(SortedRangeSet.of(Range.range(BIGINT, 10L, false, 41L, false)))
                .doesNotContainValue(10L)
                .containsValue(11L)
                .containsValue(40L)
                .doesNotContainValue(41L);

        // REAL all
        assertSortedRangeSet(SortedRangeSet.all(REAL))
                .containsValue((long) floatToRawIntBits(42.0f))
                .containsValue((long) floatToRawIntBits(Float.NaN));

        // REAL range
        assertSortedRangeSet(SortedRangeSet.of(Range.range(REAL, (long) floatToRawIntBits(10.0f), true, (long) floatToRawIntBits(41.0f), true)))
                .doesNotContainValue((long) floatToRawIntBits(9.999999f))
                .containsValue((long) floatToRawIntBits(10.0f))
                .containsValue((long) floatToRawIntBits(41.0f))
                .doesNotContainValue((long) floatToRawIntBits(41.00001f))
                .doesNotContainValue((long) floatToRawIntBits(Float.NaN));

        assertSortedRangeSet(SortedRangeSet.of(Range.range(REAL, (long) floatToRawIntBits(10.0f), false, (long) floatToRawIntBits(41.0f), false)))
                .doesNotContainValue((long) floatToRawIntBits(10.0f))
                .containsValue((long) floatToRawIntBits(10.00001f))
                .containsValue((long) floatToRawIntBits(40.99999f))
                .doesNotContainValue((long) floatToRawIntBits(41.0f))
                .doesNotContainValue((long) floatToRawIntBits(Float.NaN));

        // DOUBLE all
        assertSortedRangeSet(SortedRangeSet.all(DOUBLE))
                .containsValue(42.0)
                .containsValue(Double.NaN);

        // DOUBLE range
        assertSortedRangeSet(SortedRangeSet.of(Range.range(DOUBLE, 10.0, true, 41.0, true)))
                .doesNotContainValue(9.999999999999999)
                .containsValue(10.0)
                .containsValue(41.0)
                .doesNotContainValue(41.00000000000001)
                .doesNotContainValue(Double.NaN);

        assertSortedRangeSet(SortedRangeSet.of(Range.range(DOUBLE, 10.0, false, 41.0, false)))
                .doesNotContainValue(10.0)
                .containsValue(10.00000000000001)
                .containsValue(40.99999999999999)
                .doesNotContainValue(41.0)
                .doesNotContainValue(Double.NaN);
    }

    @Test
    public void testContainsValueRejectNull()
    {
        SortedRangeSet all = SortedRangeSet.all(BIGINT);
        SortedRangeSet none = SortedRangeSet.none(BIGINT);
        SortedRangeSet someRange = SortedRangeSet.of(Range.range(BIGINT, 10L, false, 41L, false));

        assertThatThrownBy(() -> all.containsValue(null))
                .hasMessage("value is null");
        assertThatThrownBy(() -> none.containsValue(null))
                .hasMessage("value is null");
        assertThatThrownBy(() -> someRange.containsValue(null))
                .hasMessage("value is null");
    }

    @Test
    public void testIntersect()
    {
        assertIntersect(
                SortedRangeSet.none(BIGINT),
                SortedRangeSet.none(BIGINT),
                SortedRangeSet.none(BIGINT));

        assertIntersect(
                SortedRangeSet.all(BIGINT),
                SortedRangeSet.all(BIGINT),
                SortedRangeSet.all(BIGINT));

        assertIntersect(
                SortedRangeSet.none(BIGINT),
                SortedRangeSet.all(BIGINT),
                SortedRangeSet.none(BIGINT));

        assertIntersect(
                SortedRangeSet.of(Range.equal(BIGINT, 1L)),
                SortedRangeSet.of(Range.equal(BIGINT, 2L)),
                SortedRangeSet.none(BIGINT));

        assertIntersect(
                SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)),
                SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.equal(BIGINT, 4L)),
                SortedRangeSet.of(Range.equal(BIGINT, 2L)));

        assertIntersect(
                SortedRangeSet.all(BIGINT),
                SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.equal(BIGINT, 4L)),
                SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.equal(BIGINT, 4L)));

        assertIntersect(
                SortedRangeSet.of(Range.range(BIGINT, 0L, true, 4L, false)),
                SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.greaterThan(BIGINT, 3L)),
                SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.range(BIGINT, 3L, false, 4L, false)));

        assertIntersect(
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)),
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 0L)),
                SortedRangeSet.of(Range.equal(BIGINT, 0L)));

        assertIntersect(
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, -1L)),
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 1L)),
                SortedRangeSet.of(Range.range(BIGINT, -1L, true, 1L, true)));

        assertIntersect(
                SortedRangeSet.of(Range.range(BIGINT, 1L, true, 9L, true), Range.range(BIGINT, 12L, true, 18L, true)),
                SortedRangeSet.of(Range.range(BIGINT, 7L, true, 15L, true), Range.range(BIGINT, 17L, true, 21L, true)),
                SortedRangeSet.of(
                        Range.range(BIGINT, 7L, true, 9L, true),
                        Range.range(BIGINT, 12L, true, 15L, true),
                        Range.range(BIGINT, 17L, true, 18L, true)));

        assertIntersect(
                SortedRangeSet.of(Range.range(BIGINT, 1L, true, 9L, true), Range.range(BIGINT, 12L, true, 18L, true)),
                SortedRangeSet.of(BIGINT, 0L, LongStream.rangeClosed(1L, 25L).boxed().toArray()),
                SortedRangeSet.of(BIGINT, 1L, Stream.concat(LongStream.rangeClosed(2, 9).boxed(), LongStream.rangeClosed(12, 18).boxed()).toArray()));

        assertIntersect(
                SortedRangeSet.of(Range.range(BIGINT, 2L, false, 9L, false), Range.range(BIGINT, 12L, false, 18L, false)),
                SortedRangeSet.of(BIGINT, 0L, LongStream.rangeClosed(1L, 25L).boxed().toArray()),
                SortedRangeSet.of(BIGINT, 3L, Stream.concat(LongStream.rangeClosed(4, 8).boxed(), LongStream.rangeClosed(13, 17).boxed()).toArray()));

        assertIntersect(
                SortedRangeSet.of(Range.range(BIGINT, 0L, true, 50L, true)),
                SortedRangeSet.of(
                        Range.range(BIGINT, 50L, false, 100L, true),
                        Range.range(BIGINT, 150L, true, 200L, true)),
                SortedRangeSet.none(BIGINT));

        assertIntersect(
                SortedRangeSet.of(Range.range(BIGINT, 50L, true, 100L, true)),
                SortedRangeSet.of(
                        Range.range(BIGINT, 0L, true, 50L, false),
                        Range.range(BIGINT, 150L, true, 200L, true)),
                SortedRangeSet.none(BIGINT));

        assertIntersect(
                SortedRangeSet.of(Range.range(BIGINT, 200L, true, 300L, true)),
                SortedRangeSet.of(
                        Range.range(BIGINT, 0L, true, 50L, true),
                        Range.range(BIGINT, 150L, true, 200L, false)),
                SortedRangeSet.none(BIGINT));

        assertIntersect(
                SortedRangeSet.of(Range.range(BIGINT, 10L, true, 20L, true)),
                SortedRangeSet.of(
                        Range.range(BIGINT, 8L, true, 12L, true),
                        Range.range(BIGINT, 18L, true, 22L, true)),
                SortedRangeSet.of(
                        Range.range(BIGINT, 10L, true, 12L, true),
                        Range.range(BIGINT, 18L, true, 20L, true)));

        assertIntersect(
                SortedRangeSet.of(Range.range(BIGINT, 10L, true, 20L, true)),
                SortedRangeSet.of(
                        Range.range(BIGINT, 12L, true, 18L, true)),
                SortedRangeSet.of(
                        Range.range(BIGINT, 12L, true, 18L, true)));

        assertIntersect(
                SortedRangeSet.of(Range.range(BIGINT, 10L, true, 20L, true)),
                SortedRangeSet.of(
                        Range.range(BIGINT, 12L, true, 14L, true),
                        Range.range(BIGINT, 16L, true, 18L, true),
                        Range.range(BIGINT, 22L, true, 24L, true)),
                SortedRangeSet.of(
                        Range.range(BIGINT, 12L, true, 14L, true),
                        Range.range(BIGINT, 16L, true, 18L, true)));

        assertIntersect(
                SortedRangeSet.of(
                        Range.equal(BIGINT, 1L),
                        LongStream.range(1L, 5L).mapToObj(l -> Range.range(BIGINT, l * 10, l % 2 == 0, (l + 1) * 10 - 1, l % 2 == 1)).toList().toArray(Range[]::new)),
                SortedRangeSet.of(
                        Range.range(BIGINT, 9L, true, 30L, true),
                        LongStream.rangeClosed(4L, 10L).mapToObj(l -> Range.range(BIGINT, l * 10, l % 2 == 1, (l + 1) * 10 - 1, l % 2 == 0)).toList().toArray(Range[]::new)),
                SortedRangeSet.of(
                        Range.range(BIGINT, 10L, false, 19L, true),
                        Range.range(BIGINT, 20L, true, 29L, false),
                        Range.range(BIGINT, 40L, false, 49L, false)));

        // ValueBlock
        List<Slice> slices = IntStream.rangeClosed(0, 500)
                .mapToObj(String::valueOf)
                .map(Slices::utf8Slice)
                .sorted()
                .toList();
        assertIntersect(
                SortedRangeSet.copyOf(VARCHAR,
                        Stream.concat(
                                IntStream.range(0, 2).mapToObj(l -> Range.range(VARCHAR, slices.get(l * 50), l % 2 == 0, slices.get((l + 1) * 50 - 1), l % 2 == 1)),
                                IntStream.range(3, 5).mapToObj(l -> Range.range(VARCHAR, slices.get(l * 50), l % 2 == 0, slices.get((l + 1) * 50 - 1), l % 2 == 1))).toList()),
                SortedRangeSet.copyOf(
                        VARCHAR,
                        IntStream.rangeClosed(1, 50).mapToObj(l -> Range.range(VARCHAR, slices.get(l * 5), l % 2 == 1, slices.get((l + 1) * 5 - 1), l % 2 == 0)).toList()),
                SortedRangeSet.copyOf(
                        VARCHAR,
                        Stream.concat(
                                IntStream.rangeClosed(1, 19).mapToObj(l -> Range.range(VARCHAR, slices.get(l * 5), l % 2 == 1, slices.get((l + 1) * 5 - 1), l % 2 == 0)),
                                IntStream.rangeClosed(30, 49).mapToObj(l -> Range.range(VARCHAR, slices.get(l * 5), l % 2 == 1, slices.get((l + 1) * 5 - 1), l % 2 == 0))).toList()));
    }

    @Test
    public void testLinearDiscreteSetIntersect()
    {
        SortedRangeSet result = SortedRangeSet.of(BIGINT, 1L, 2L, 10L, 11L, 20L, 21L)
                .linearDiscreteSetIntersect(SortedRangeSet.of(BIGINT, 1L, 2L, 20L, 21L, 30L));
        assertThat(result).isEqualTo(SortedRangeSet.of(BIGINT, 1L, 2L, 20L, 21L));
        assertThat(result.isDiscreteSet()).isTrue();

        result = SortedRangeSet.of(BIGINT, 1L, 2L, 10L)
                .linearDiscreteSetIntersect(SortedRangeSet.of(BIGINT, 1L, 2L, 11L));
        assertThat(result).isEqualTo(SortedRangeSet.of(BIGINT, 1L, 2L));
        assertThat(result.isDiscreteSet()).isTrue();

        result = SortedRangeSet.of(BIGINT, 1L, 2L, 10L)
                .linearDiscreteSetIntersect(SortedRangeSet.of(BIGINT, 42L));
        assertThat(result).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(result.isDiscreteSet()).isFalse();
    }

    private void assertIntersect(SortedRangeSet first, SortedRangeSet second, SortedRangeSet result)
    {
        assertThat(first.intersect(second)).isEqualTo(result);
        assertThat(first.linearSearchIntersect(second)).isEqualTo(result);
        assertThat(first.binarySearchIntersect(second)).isEqualTo(result);
        assertThat(second.intersect(first)).isEqualTo(result);
        assertThat(second.linearSearchIntersect(first)).isEqualTo(result);
        assertThat(second.binarySearchIntersect(first)).isEqualTo(result);

        // force discrete set to be evaluated
        first.isDiscreteSet();
        second.isDiscreteSet();
        assertThat(first.getDiscreteSetMarker()).isNotEqualTo(UNKNOWN);
        assertThat(second.getDiscreteSetMarker()).isNotEqualTo(UNKNOWN);
        assertThat(first.intersect(second).isDiscreteSet()).isEqualTo(result.isDiscreteSet());
    }

    @Test
    public void testUnion()
    {
        assertUnion(SortedRangeSet.none(BIGINT), SortedRangeSet.none(BIGINT), SortedRangeSet.none(BIGINT));
        assertUnion(SortedRangeSet.all(BIGINT), SortedRangeSet.all(BIGINT), SortedRangeSet.all(BIGINT));
        assertUnion(SortedRangeSet.none(BIGINT), SortedRangeSet.all(BIGINT), SortedRangeSet.all(BIGINT));
        assertUnion(SortedRangeSet.none(BIGINT), SortedRangeSet.of(Range.equal(BIGINT, 2L)), SortedRangeSet.of(Range.equal(BIGINT, 2L)));

        assertUnion(
                SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)),
                SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)),
                SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)));

        assertUnion(SortedRangeSet.all(BIGINT), SortedRangeSet.of(Range.equal(BIGINT, 0L)), SortedRangeSet.all(BIGINT));

        assertUnion(
                SortedRangeSet.of(Range.range(BIGINT, 0L, true, 4L, false)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 3L)),
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)));

        assertUnion(
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)),
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 0L)),
                SortedRangeSet.of(Range.all(BIGINT)));

        assertUnion(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)),
                SortedRangeSet.of(Range.lessThan(BIGINT, 0L)),
                SortedRangeSet.of(BIGINT, 0L).complement());

        assertUnion(
                SortedRangeSet.of(Range.range(BIGINT, 0L, true, 10L, false)),
                SortedRangeSet.of(Range.equal(BIGINT, 9L)),
                SortedRangeSet.of(Range.range(BIGINT, 0L, true, 10L, false)));

        // two low-unbounded, first shorter
        assertUnion(
                SortedRangeSet.of(Range.lessThan(BIGINT, 5L)),
                SortedRangeSet.of(Range.lessThan(BIGINT, 10L)),
                SortedRangeSet.of(Range.lessThan(BIGINT, 10L)));
        assertUnion(
                SortedRangeSet.of(Range.lessThan(BIGINT, 10L)),
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 10L)),
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 10L)));

        // two low-unbounded, second shorter
        assertUnion(
                SortedRangeSet.of(Range.lessThan(BIGINT, 10L)),
                SortedRangeSet.of(Range.lessThan(BIGINT, 5L)),
                SortedRangeSet.of(Range.lessThan(BIGINT, 10L)));
        assertUnion(
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 10L)),
                SortedRangeSet.of(Range.lessThan(BIGINT, 10L)),
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 10L)));

        // two high-unbounded, first shorter
        assertUnion(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 10L)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 5L)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 5L)));
        assertUnion(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 10L)),
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 10L)),
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 10L)));

        // two high-unbounded, second shorter
        assertUnion(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 5L)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 10L)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 5L)));
        assertUnion(
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 10L)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 10L)),
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 10L)));

        assertUnion(
                SortedRangeSet.of(Range.range(createVarcharType(25), utf8Slice("LARGE PLATED "), true, utf8Slice("LARGE PLATED!"), false)),
                SortedRangeSet.of(Range.equal(createVarcharType(25), utf8Slice("LARGE PLATED NICKEL"))),
                SortedRangeSet.of(Range.range(createVarcharType(25), utf8Slice("LARGE PLATED "), true, utf8Slice("LARGE PLATED!"), false)));
    }

    @Test
    public void testLinearDiscreteSetUnion()
    {
        SortedRangeSet result = SortedRangeSet.of(BIGINT, 1L, 2L, 3L, 10L, 20L, 30L)
                .linearDiscreteSetUnion(SortedRangeSet.of(BIGINT, 100L, 101L, 102L, 103L));
        assertThat(result).isEqualTo(SortedRangeSet.of(BIGINT, 1L, 2L, 3L, 10L, 20L, 30L, 100L, 101L, 102L, 103L));
        assertThat(result.isDiscreteSet()).isTrue();

        result = SortedRangeSet.of(BIGINT, 100L, 101L, 102L, 103L)
                .linearDiscreteSetUnion(SortedRangeSet.of(BIGINT, 1L, 2L, 3L, 10L, 20L, 30L));
        assertThat(result).isEqualTo(SortedRangeSet.of(BIGINT, 1L, 2L, 3L, 10L, 20L, 30L, 100L, 101L, 102L, 103L));
        assertThat(result.isDiscreteSet()).isTrue();

        result = SortedRangeSet.of(BIGINT, 1L, 3L, 5L, 7L, 9L)
                .linearDiscreteSetUnion(SortedRangeSet.of(BIGINT, 2L, 4L, 6L, 8L, 10L));
        assertThat(result).isEqualTo(SortedRangeSet.of(BIGINT, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L));
        assertThat(result.isDiscreteSet()).isTrue();

        result = SortedRangeSet.of(BIGINT, 1L)
                .linearDiscreteSetUnion(SortedRangeSet.of(BIGINT, 2L, 4L, 6L, 8L, 10L));
        assertThat(result).isEqualTo(SortedRangeSet.of(BIGINT, 1L, 2L, 4L, 6L, 8L, 10L));
        assertThat(result.isDiscreteSet()).isTrue();

        result = SortedRangeSet.of(BIGINT, 1L, 3L, 5L, 7L, 9L)
                .linearDiscreteSetUnion(SortedRangeSet.of(BIGINT, 2L));
        assertThat(result).isEqualTo(SortedRangeSet.of(BIGINT, 1L, 2L, 3L, 5L, 7L, 9L));
        assertThat(result.isDiscreteSet()).isTrue();

        result = SortedRangeSet.of(BIGINT, 1L, 2L, 3L, 5L, 7L)
                .linearDiscreteSetUnion(SortedRangeSet.of(BIGINT, 2L, 3L, 5L));
        assertThat(result).isEqualTo(SortedRangeSet.of(BIGINT, 1L, 2L, 3L, 5L, 7L));
        assertThat(result.isDiscreteSet()).isTrue();

        result = SortedRangeSet.of(BIGINT, 1L, 2L, 10L, 11L, 20L, 21L)
                .linearDiscreteSetUnion(SortedRangeSet.of(BIGINT, 1L, 2L, 12L, 20L, 21L, 30L));
        assertThat(result).isEqualTo(SortedRangeSet.of(BIGINT, 1L, 2L, 10L, 11L, 12L, 20L, 21L, 30L));
        assertThat(result.isDiscreteSet()).isTrue();
    }

    @Test
    public void testSubtract()
    {
        assertThat(SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.all(BIGINT))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.none(BIGINT))).isEqualTo(SortedRangeSet.all(BIGINT));
        assertThat(SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.of(BIGINT, 0L))).isEqualTo(SortedRangeSet.of(BIGINT, 0L).complement());
        assertThat(SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isEqualTo(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).complement());
        assertThat(SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isEqualTo(SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 0L)));

        assertThat(SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.all(BIGINT))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.none(BIGINT))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.of(BIGINT, 0L))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isEqualTo(SortedRangeSet.none(BIGINT));

        assertThat(SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.all(BIGINT))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.none(BIGINT))).isEqualTo(SortedRangeSet.of(BIGINT, 0L));
        assertThat(SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.of(BIGINT, 0L))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isEqualTo(SortedRangeSet.of(BIGINT, 0L));

        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.all(BIGINT))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.none(BIGINT))).isEqualTo(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)));
        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.of(BIGINT, 0L))).isEqualTo(SortedRangeSet.of(BIGINT, 1L));
        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isEqualTo(SortedRangeSet.of(Range.equal(BIGINT, 0L)));

        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.all(BIGINT))).isEqualTo(SortedRangeSet.none(BIGINT));
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.none(BIGINT))).isEqualTo(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)));
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.of(BIGINT, 0L))).isEqualTo(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)));
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)))).isEqualTo(SortedRangeSet.of(Range.range(BIGINT, 0L, false, 1L, false), Range.greaterThan(BIGINT, 1L)));
        assertThat(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)))).isEqualTo(SortedRangeSet.none(BIGINT));
    }

    @Test
    public void testNormalizeDictionarySortedRanges()
    {
        SortedRangeSet values = (SortedRangeSet) ValueSet.of(BIGINT, 0L, -1L);

        // make sure normalization preserves equality of TupleDomains
        SortedRangeSet normalizedValues = values.normalize();
        assertThat(normalizedValues).isEqualTo(values);
        assertBlockEquals(BIGINT, normalizedValues.getSortedRanges(), values.getSortedRanges());
        assertThat(values.getSortedRanges()).isInstanceOf(DictionaryBlock.class);
        assertThat(normalizedValues.getSortedRanges()).isInstanceOf(LongArrayBlock.class);

        // further normalization shouldn't change SortedRangeSet underlying block
        SortedRangeSet doubleNormalizedValues = normalizedValues.normalize();
        assertThat(doubleNormalizedValues.getSortedRanges()).isInstanceOf(LongArrayBlock.class);
        assertBlockEquals(BIGINT, doubleNormalizedValues.getSortedRanges(), normalizedValues.getSortedRanges());
    }

    @Test
    public void testNormalizeRleSortedRanges()
    {
        SortedRangeSet values = (SortedRangeSet) ValueSet.of(BIGINT, 0L);

        // make sure normalization preserves equality of TupleDomains
        SortedRangeSet normalizedValues = values.normalize();
        assertThat(normalizedValues).isEqualTo(values);
        assertBlockEquals(BIGINT, normalizedValues.getSortedRanges(), values.getSortedRanges());
        assertThat(values.getSortedRanges()).isInstanceOf(RunLengthEncodedBlock.class);
        assertThat(normalizedValues.getSortedRanges()).isInstanceOf(LongArrayBlock.class);
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

        ObjectMapper mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)));

        SortedRangeSet set = SortedRangeSet.all(BIGINT);
        assertThat(set).isEqualTo(mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));

        set = SortedRangeSet.none(DOUBLE);
        assertThat(set).isEqualTo(mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));

        set = SortedRangeSet.of(VARCHAR, utf8Slice("abc"));
        assertThat(set).isEqualTo(mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));

        set = SortedRangeSet.of(Range.equal(BOOLEAN, true), Range.equal(BOOLEAN, false));
        assertThat(set).isEqualTo(mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));
    }

    @Test
    public void testExpandRangesForDenseType()
    {
        for (Type type : Arrays.asList(BIGINT, INTEGER, SMALLINT, TINYINT, createDecimalType(2))) {
            assertThat(ValueSet.ofRanges(Range.equal(type, 1L))
                    .tryExpandRanges(0))
                    .isEqualTo(Optional.empty());

            assertThat(ValueSet.none(type)
                    .tryExpandRanges(0))
                    .isEqualTo(Optional.of(List.of()));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, true, 5L, true))
                    .tryExpandRanges(10))
                    .isEqualTo(Optional.of(List.of(1L, 2L, 3L, 4L, 5L)));

            assertThat(ValueSet.of(type, 1L, 2L, 3L, 4L, 5L)
                    .tryExpandRanges(10))
                    .isEqualTo(Optional.of(List.of(1L, 2L, 3L, 4L, 5L)));

            type.getRange().ifPresent(range -> {
                long min = (long) range.getMin();

                assertThat(ValueSet.ofRanges(Range.range(type, min, true, min + 3, true))
                        .tryExpandRanges(10))
                        .isEqualTo(Optional.of(List.of(min, min + 1, min + 2, min + 3)));
                assertThat(ValueSet.ofRanges(Range.lessThan(type, min + 4))
                        .tryExpandRanges(10))
                        .isEqualTo(Optional.of(List.of(min, min + 1, min + 2, min + 3)));
                assertThat(ValueSet.ofRanges(Range.lessThanOrEqual(type, min + 3))
                        .tryExpandRanges(10))
                        .isEqualTo(Optional.of(List.of(min, min + 1, min + 2, min + 3)));

                long max = (long) range.getMax();

                assertThat(ValueSet.ofRanges(Range.range(type, max - 3, true, max, true))
                        .tryExpandRanges(10))
                        .isEqualTo(Optional.of(List.of(max - 3, max - 2, max - 1, max)));
                assertThat(ValueSet.ofRanges(Range.greaterThan(type, max - 4))
                        .tryExpandRanges(10))
                        .isEqualTo(Optional.of(List.of(max - 3, max - 2, max - 1, max)));
                assertThat(ValueSet.ofRanges(Range.greaterThanOrEqual(type, max - 3))
                        .tryExpandRanges(10))
                        .isEqualTo(Optional.of(List.of(max - 3, max - 2, max - 1, max)));
            });

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, true, 5L, true))
                    .tryExpandRanges(10))
                    .isEqualTo(Optional.of(List.of(1L, 2L, 3L, 4L, 5L)));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, true, 5L, true))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.of(List.of(1L, 2L, 3L, 4L, 5L)));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, true, 5L, false))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.of(List.of(1L, 2L, 3L, 4L)));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, true, 6L, true))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.empty());

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, true, 6L, false))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.of(List.of(1L, 2L, 3L, 4L, 5L)));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, false, 5L, true))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.of(List.of(2L, 3L, 4L, 5L)));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, true, 5L, false))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.of(List.of(1L, 2L, 3L, 4L)));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, false, 5L, false))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.of(List.of(2L, 3L, 4L)));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, false, 2L, false))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.of(List.of()));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, false, 3L, false))
                    .tryExpandRanges(5))
                    .isEqualTo(Optional.of(List.of(2L)));

            assertThat(ValueSet.ofRanges(Range.range(type, 1L, true, 5L, true))
                    .tryExpandRanges(3))
                    .isEqualTo(Optional.empty());

            assertThat(ValueSet.of(type, 1L, 2L, 3L, 4L, 5L)
                    .tryExpandRanges(3))
                    .isEqualTo(Optional.empty());

            assertThat(ValueSet.ofRanges(Range.greaterThan(type, 1L))
                    .tryExpandRanges(3))
                    .isEqualTo(Optional.empty());

            assertThat(ValueSet.ofRanges(Range.greaterThanOrEqual(type, 1L))
                    .tryExpandRanges(3))
                    .isEqualTo(Optional.empty());

            assertThat(ValueSet.ofRanges(Range.lessThan(type, 1L))
                    .tryExpandRanges(3))
                    .isEqualTo(Optional.empty());

            assertThat(ValueSet.ofRanges(Range.lessThanOrEqual(type, 1L))
                    .tryExpandRanges(3))
                    .isEqualTo(Optional.empty());
        }
    }

    @Test
    public void testRangeSetHashcode()
    {
        assertThat(ValueSet.ofRanges(Range.lessThan(INTEGER, 0L)).hashCode()).isNotEqualTo(ValueSet.ofRanges(Range.greaterThan(INTEGER, 0L)).hashCode());
        assertThat(ValueSet.ofRanges(Range.lessThan(INTEGER, 1L)).hashCode()).isNotEqualTo(ValueSet.ofRanges(Range.range(INTEGER, 0L, false, 1L, false)).hashCode());
    }

    private void assertUnion(SortedRangeSet first, SortedRangeSet second, SortedRangeSet expected)
    {
        assertThat(first.union(second)).isEqualTo(expected);
        assertThat(first.union(ImmutableList.of(first, second))).isEqualTo(expected);

        // force discrete set to be evaluated
        first.isDiscreteSet();
        second.isDiscreteSet();
        assertThat(first.getDiscreteSetMarker()).isNotEqualTo(UNKNOWN);
        assertThat(second.getDiscreteSetMarker()).isNotEqualTo(UNKNOWN);
        assertThat(first.union(second).isDiscreteSet()).isEqualTo(expected.isDiscreteSet());
    }

    private static SortedRangeSetAssert assertSortedRangeSet(SortedRangeSet sortedRangeSet)
    {
        return assertThat((AssertProvider<SortedRangeSetAssert>) () -> new SortedRangeSetAssert(sortedRangeSet));
    }

    private static class SortedRangeSetAssert
    {
        private final SortedRangeSet sortedRangeSet;

        public SortedRangeSetAssert(SortedRangeSet sortedRangeSet)
        {
            this.sortedRangeSet = requireNonNull(sortedRangeSet, "sortedRangeSet is null");
        }

        public SortedRangeSetAssert containsValue(Object value)
        {
            if (!sortedRangeSet.containsValue(value)) {
                throw new AssertionError(format("Expected %s to contain %s", sortedRangeSet, value));
            }
            return this;
        }

        public SortedRangeSetAssert doesNotContainValue(Object value)
        {
            if (sortedRangeSet.containsValue(value)) {
                throw new AssertionError(format("Expected %s not to contain %s", sortedRangeSet, value));
            }
            return this;
        }
    }
}
