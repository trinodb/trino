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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.DynamicFilterDomain.fromBloomFilter;
import static io.trino.sql.planner.DynamicFilterDomain.fromDomain;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDynamicFilterDomain
{
    @Test
    void testCreateWithDomain()
    {
        DynamicFilterDomain dynamicFilterDomain = fromDomain(Domain.multipleValues(BIGINT, ImmutableList.of(1L, 4L, 7L)));
        assertThat(dynamicFilterDomain.getDomain()).isPresent();
        assertThat(dynamicFilterDomain.getBloomfilterWithRange()).isEmpty();
        assertThat(dynamicFilterDomain.isAll()).isFalse();
        assertThat(dynamicFilterDomain.isNone()).isFalse();
        assertThat(dynamicFilterDomain.getSpan()).isEqualTo(Range.range(BIGINT, 1L, true, 7L, true));

        assertThat(dynamicFilterDomain.withNullsAllowed()).isEqualTo(
                fromDomain(Domain.multipleValues(BIGINT, ImmutableList.of(1L, 4L, 7L), true)));
    }

    @Test
    void testCreateWithBloomFilter()
    {
        LongBloomFilter bloomFilter = createBloomFilter(1L, 4L, 7L);
        ValueSet ranges = ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 4L, true), Range.equal(BIGINT, 7L));
        BloomFilterWithRange bloomFilterWithRange = new BloomFilterWithRange(bloomFilter, ranges, BIGINT, false);
        DynamicFilterDomain dynamicFilterDomain = fromBloomFilter(bloomFilterWithRange);
        assertThat(dynamicFilterDomain.getDomain()).isEmpty();
        assertThat(dynamicFilterDomain.getBloomfilterWithRange()).isPresent();
        assertThat(dynamicFilterDomain.isAll()).isFalse();
        assertThat(dynamicFilterDomain.isNone()).isFalse();
        assertThat(dynamicFilterDomain.toDomain()).isEqualTo(Domain.create(ranges, false));
        assertThat(dynamicFilterDomain.getSpan()).isEqualTo(Range.range(BIGINT, 1L, true, 7L, true));

        assertThat(dynamicFilterDomain.withNullsAllowed()).isEqualTo(
                fromBloomFilter(new BloomFilterWithRange(bloomFilter, ranges, BIGINT, true)));
    }

    @Test
    void testSimplify()
    {
        assertThat(DynamicFilterDomain.all(BIGINT).simplifyDomains(1)).isEqualTo(DynamicFilterDomain.all(BIGINT));
        assertThat(DynamicFilterDomain.none(BIGINT).simplifyDomains(1)).isEqualTo(DynamicFilterDomain.none(BIGINT));

        DynamicFilterDomain dynamicFilterDomain = fromDomain(Domain.multipleValues(BIGINT, ImmutableList.of(1L, 4L, 7L)));
        LongBloomFilter bloomFilter = createBloomFilter(1L, 4L, 7L);
        assertThat(dynamicFilterDomain.simplifyDomains(1)).isEqualTo(
                fromBloomFilter(new BloomFilterWithRange(bloomFilter, createRange(1L, 7L), BIGINT, false)));

        ValueSet ranges = ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 4L, true), Range.equal(BIGINT, 7L));
        DynamicFilterDomain simplified = fromDomain(Domain.create(createRange(1L, 7L), false));
        assertThat(fromDomain(Domain.create(ranges, false)).simplifyDomains(1))
                .isEqualTo(simplified);
        assertThat(simplified.simplifyDomains(1)).isEqualTo(simplified);

        BloomFilterWithRange bloomFilterWithRange = new BloomFilterWithRange(bloomFilter, ranges, BIGINT, false);
        dynamicFilterDomain = fromBloomFilter(bloomFilterWithRange);
        simplified = fromBloomFilter(new BloomFilterWithRange(bloomFilter, createRange(1L, 7L), BIGINT, false));
        assertThat(dynamicFilterDomain.simplifyDomains(1)).isEqualTo(simplified);
        assertThat(simplified.simplifyDomains(1)).isEqualTo(simplified);
    }

    @Test
    void testUnion()
    {
        assertUnion(DynamicFilterDomain.all(BIGINT), DynamicFilterDomain.all(BIGINT), DynamicFilterDomain.all(BIGINT));
        assertUnion(DynamicFilterDomain.none(BIGINT), DynamicFilterDomain.none(BIGINT), DynamicFilterDomain.none(BIGINT));
        assertUnion(DynamicFilterDomain.all(BIGINT), DynamicFilterDomain.none(BIGINT), DynamicFilterDomain.all(BIGINT));
        assertUnion(fromDomain(Domain.notNull(BIGINT)), DynamicFilterDomain.onlyNull(BIGINT), DynamicFilterDomain.all(BIGINT));
        assertUnion(DynamicFilterDomain.singleValue(BIGINT, 0L), DynamicFilterDomain.all(BIGINT), DynamicFilterDomain.all(BIGINT));
        assertUnion(DynamicFilterDomain.singleValue(BIGINT, 0L), fromDomain(Domain.notNull(BIGINT)), fromDomain(Domain.notNull(BIGINT)));
        assertUnion(
                DynamicFilterDomain.singleValue(BIGINT, 0L),
                DynamicFilterDomain.onlyNull(BIGINT),
                fromDomain(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 0L)), true)));

        assertUnion(
                fromDomain(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true)),
                fromDomain(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), true)),
                fromDomain(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), true)));

        assertUnion(
                fromDomain(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 9L)), true)),
                fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1L, 4L, 7L), createRange(1L, 7L), BIGINT, false)),
                fromBloomFilter(new BloomFilterWithRange(createBloomFilter(0L, 1L, 4L, 7L, 9L), createRange(0L, 9L), BIGINT, true)));

        assertUnion(
                fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1L, 4L, 7L), createRange(1L, 7L), BIGINT, false)),
                fromDomain(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 10L)), true)),
                fromDomain(Domain.create(
                        ValueSet.ofRanges(
                                Range.range(BIGINT, 1L, true, 7L, true),
                                Range.greaterThanOrEqual(BIGINT, 10L)),
                        true)));

        assertUnion(
                fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1L, 4L, 7L, 10L), createRange(1L, 10L), BIGINT, false)),
                fromBloomFilter(new BloomFilterWithRange(createBloomFilter(0L, 1L, 4L, 7L, 9L), createRange(0L, 9L), BIGINT, true)),
                fromBloomFilter(new BloomFilterWithRange(createBloomFilter(0L, 1L, 4L, 7L, 9L, 10L), createRange(0L, 10L), BIGINT, true)));
    }

    @Test
    void testIntersect()
    {
        assertIntersect(DynamicFilterDomain.all(BIGINT), DynamicFilterDomain.all(BIGINT), DynamicFilterDomain.all(BIGINT));
        assertIntersect(DynamicFilterDomain.none(BIGINT), DynamicFilterDomain.none(BIGINT), DynamicFilterDomain.none(BIGINT));
        assertIntersect(DynamicFilterDomain.all(BIGINT), DynamicFilterDomain.none(BIGINT), DynamicFilterDomain.none(BIGINT));
        assertIntersect(fromDomain(Domain.notNull(BIGINT)), DynamicFilterDomain.onlyNull(BIGINT), DynamicFilterDomain.none(BIGINT));
        assertIntersect(DynamicFilterDomain.singleValue(BIGINT, 0L), DynamicFilterDomain.all(BIGINT), DynamicFilterDomain.singleValue(BIGINT, 0L));
        assertIntersect(
                DynamicFilterDomain.singleValue(BIGINT, 0L),
                fromDomain(Domain.notNull(BIGINT)),
                DynamicFilterDomain.singleValue(BIGINT, 0L));
        assertIntersect(
                DynamicFilterDomain.singleValue(BIGINT, 0L),
                DynamicFilterDomain.onlyNull(BIGINT),
                DynamicFilterDomain.none(BIGINT));

        assertIntersect(
                fromDomain(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true)),
                fromDomain(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), true)),
                DynamicFilterDomain.onlyNull(BIGINT));

        assertIntersect(
                fromDomain(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 9L)), true)),
                fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1L, 4L, 7L), createRange(1L, 7L), BIGINT, false)),
                DynamicFilterDomain.none(BIGINT));

        assertIntersect(
                fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1L, 4L, 7L), createRange(1L, 7L), BIGINT, false)),
                fromDomain(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 10L)), true)),
                DynamicFilterDomain.none(BIGINT));

        assertIntersect(
                fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1L, 4L, 7L, 10L), createRange(1L, 10L), BIGINT, false)),
                fromBloomFilter(new BloomFilterWithRange(createBloomFilter(0L, 1L, 4L, 7L, 9L), createRange(0L, 9L), BIGINT, true)),
                fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1L, 4L, 7L), createRange(1L, 9L), BIGINT, false)));
    }

    private static LongBloomFilter createBloomFilter(long... values)
    {
        LongBloomFilter bloomFilter = new LongBloomFilter();
        for (long value : values) {
            bloomFilter.insert(value);
        }
        return bloomFilter;
    }

    private static ValueSet createRange(long low, long high)
    {
        return ValueSet.ofRanges(Range.range(BIGINT, low, true, high, true));
    }

    private static void assertUnion(DynamicFilterDomain first, DynamicFilterDomain second, DynamicFilterDomain expected)
    {
        assertThat(first.union(second)).isEqualTo(expected);
        assertThat(second.union(first)).isEqualTo(expected);
        assertThat(DynamicFilterDomain.union(ImmutableList.of(first, second))).isEqualTo(expected);
    }

    private static void assertIntersect(DynamicFilterDomain first, DynamicFilterDomain second, DynamicFilterDomain expected)
    {
        assertThat(first.intersect(second)).isEqualTo(expected);
        assertThat(second.intersect(first)).isEqualTo(expected);
    }
}
