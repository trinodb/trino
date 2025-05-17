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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.DynamicFilterDomain.fromBloomFilter;
import static io.trino.sql.planner.DynamicFilterDomain.fromDomain;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDynamicFilterTupleDomain
{
    private static final ColumnHandle A = new TestingColumnHandle("a");
    private static final ColumnHandle B = new TestingColumnHandle("b");
    private static final ColumnHandle C = new TestingColumnHandle("c");
    private static final ColumnHandle D = new TestingColumnHandle("d");
    private static final ColumnHandle E = new TestingColumnHandle("e");
    private static final ColumnHandle F = new TestingColumnHandle("f");

    @Test
    void testNone()
    {
        assertThat(DynamicFilterTupleDomain.none().isNone()).isTrue();
        assertThat(DynamicFilterTupleDomain.<ColumnHandle>none()).isEqualTo(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                A, DynamicFilterDomain.none(BIGINT))));
        assertThat(DynamicFilterTupleDomain.<ColumnHandle>none()).isEqualTo(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                A, DynamicFilterDomain.all(BIGINT),
                B, DynamicFilterDomain.none(VARCHAR))));
    }

    @Test
    void testAll()
    {
        assertThat(DynamicFilterTupleDomain.all().isAll()).isTrue();
        assertThat(DynamicFilterTupleDomain.<ColumnHandle>all()).isEqualTo(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                A, DynamicFilterDomain.all(BIGINT))));
        assertThat(DynamicFilterTupleDomain.<ColumnHandle>all()).isEqualTo(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, DynamicFilterDomain>of()));
    }

    @Test
    void testIntersection()
    {
        DynamicFilterTupleDomain<ColumnHandle> tupleDomain1 = DynamicFilterTupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, DynamicFilterDomain>builder()
                        .put(A, DynamicFilterDomain.all(VARCHAR))
                        .put(B, fromDomain(Domain.notNull(DOUBLE)))
                        .put(C, DynamicFilterDomain.singleValue(BIGINT, 1L))
                        .put(D, fromDomain(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE, 0.0)), true)))
                        .buildOrThrow());

        DynamicFilterTupleDomain<ColumnHandle> tupleDomain2 = DynamicFilterTupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, DynamicFilterDomain>builder()
                        .put(A, DynamicFilterDomain.singleValue(VARCHAR, utf8Slice("value")))
                        .put(B, DynamicFilterDomain.singleValue(DOUBLE, 0.0))
                        .put(C, DynamicFilterDomain.singleValue(BIGINT, 1L))
                        .put(D, fromDomain(Domain.create(ValueSet.ofRanges(Range.lessThan(DOUBLE, 10.0)), false)))
                        .buildOrThrow());

        DynamicFilterTupleDomain<ColumnHandle> expectedTupleDomain = DynamicFilterTupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, DynamicFilterDomain>builder()
                        .put(A, DynamicFilterDomain.singleValue(VARCHAR, utf8Slice("value")))
                        .put(B, DynamicFilterDomain.singleValue(DOUBLE, 0.0))
                        .put(C, DynamicFilterDomain.singleValue(BIGINT, 1L))
                        .put(D, fromDomain(Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 0.0, true, 10.0, false)), false)))
                        .buildOrThrow());

        assertThat(tupleDomain1.intersect(tupleDomain2)).isEqualTo(expectedTupleDomain);
        assertThat(tupleDomain2.intersect(tupleDomain1)).isEqualTo(expectedTupleDomain);

        assertThat(DynamicFilterTupleDomain.intersect(ImmutableList.of())).isEqualTo(DynamicFilterTupleDomain.all());
        assertThat(DynamicFilterTupleDomain.intersect(ImmutableList.of(tupleDomain1))).isEqualTo(tupleDomain1);
        assertThat(DynamicFilterTupleDomain.intersect(ImmutableList.of(tupleDomain1, tupleDomain2))).isEqualTo(expectedTupleDomain);

        DynamicFilterTupleDomain<ColumnHandle> tupleDomain3 = DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                C, DynamicFilterDomain.singleValue(BIGINT, 1L),
                D, fromDomain(Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 5.0, true, 100.0, true)), true))));
        expectedTupleDomain = DynamicFilterTupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, DynamicFilterDomain.singleValue(VARCHAR, utf8Slice("value")),
                        B, DynamicFilterDomain.singleValue(DOUBLE, 0.0),
                        C, DynamicFilterDomain.singleValue(BIGINT, 1L),
                        D, fromDomain(Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 5.0, true, 10.0, false)), false))));
        assertThat(DynamicFilterTupleDomain.intersect(ImmutableList.of(tupleDomain1, tupleDomain2, tupleDomain3)))
                .isEqualTo(expectedTupleDomain);
    }

    @Test
    void testNoneIntersection()
    {
        assertThat(DynamicFilterTupleDomain.none().intersect(DynamicFilterTupleDomain.all())).isEqualTo(DynamicFilterTupleDomain.none());
        assertThat(DynamicFilterTupleDomain.all().intersect(DynamicFilterTupleDomain.none())).isEqualTo(DynamicFilterTupleDomain.none());
        assertThat(DynamicFilterTupleDomain.none().intersect(DynamicFilterTupleDomain.none())).isEqualTo(DynamicFilterTupleDomain.none());
        assertThat(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(A, DynamicFilterDomain.onlyNull(BIGINT)))
                .intersect(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(A, fromDomain(Domain.notNull(BIGINT))))))
                .isEqualTo(DynamicFilterTupleDomain.<ColumnHandle>none());
    }

    @Test
    void mismatchedColumnIntersection()
    {
        DynamicFilterTupleDomain<ColumnHandle> tupleDomain1 = DynamicFilterTupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, DynamicFilterDomain.all(DOUBLE),
                        B, DynamicFilterDomain.singleValue(VARCHAR, utf8Slice("value"))));

        DynamicFilterTupleDomain<ColumnHandle> tupleDomain2 = DynamicFilterTupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, fromDomain(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE, 0.0)), true)),
                        C, DynamicFilterDomain.singleValue(BIGINT, 1L)));

        DynamicFilterTupleDomain<ColumnHandle> expectedTupleDomain = DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(
                A, fromDomain(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE, 0.0)), true)),
                B, DynamicFilterDomain.singleValue(VARCHAR, utf8Slice("value")),
                C, DynamicFilterDomain.singleValue(BIGINT, 1L)));

        assertThat(tupleDomain1.intersect(tupleDomain2)).isEqualTo(expectedTupleDomain);
    }

    @Test
    void testColumnWiseUnion()
    {
        DynamicFilterTupleDomain<ColumnHandle> tupleDomain1 = DynamicFilterTupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, DynamicFilterDomain>builder()
                        .put(A, DynamicFilterDomain.all(VARCHAR))
                        .put(B, fromDomain(Domain.notNull(DOUBLE)))
                        .put(C, DynamicFilterDomain.onlyNull(BIGINT))
                        .put(D, DynamicFilterDomain.singleValue(BIGINT, 1L))
                        .put(E, fromDomain(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE, 0.0)), true)))
                        .buildOrThrow());

        DynamicFilterTupleDomain<ColumnHandle> tupleDomain2 = DynamicFilterTupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, DynamicFilterDomain>builder()
                        .put(A, DynamicFilterDomain.singleValue(VARCHAR, utf8Slice("value")))
                        .put(B, DynamicFilterDomain.singleValue(DOUBLE, 0.0))
                        .put(C, fromDomain(Domain.notNull(BIGINT)))
                        .put(D, DynamicFilterDomain.singleValue(BIGINT, 1L))
                        .put(E, fromDomain(Domain.create(ValueSet.ofRanges(Range.lessThan(DOUBLE, 10.0)), false)))
                        .buildOrThrow());

        DynamicFilterTupleDomain<ColumnHandle> expectedTupleDomain = DynamicFilterTupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, DynamicFilterDomain>builder()
                        .put(A, DynamicFilterDomain.all(VARCHAR))
                        .put(B, fromDomain(Domain.notNull(DOUBLE)))
                        .put(C, DynamicFilterDomain.all(BIGINT))
                        .put(D, DynamicFilterDomain.singleValue(BIGINT, 1L))
                        .put(E, DynamicFilterDomain.all(DOUBLE))
                        .buildOrThrow());

        assertThat(DynamicFilterTupleDomain.columnWiseUnion(ImmutableList.of(tupleDomain1, tupleDomain2))).isEqualTo(expectedTupleDomain);
    }

    @Test
    void testNoneColumnWiseUnion()
    {
        assertThat(DynamicFilterTupleDomain.columnWiseUnion(ImmutableList.of(DynamicFilterTupleDomain.none(), DynamicFilterTupleDomain.all())))
                .isEqualTo(DynamicFilterTupleDomain.all());
        assertThat(DynamicFilterTupleDomain.columnWiseUnion(ImmutableList.of(DynamicFilterTupleDomain.all(), DynamicFilterTupleDomain.none())))
                .isEqualTo(DynamicFilterTupleDomain.all());
        assertThat(DynamicFilterTupleDomain.columnWiseUnion(ImmutableList.of(DynamicFilterTupleDomain.none(), DynamicFilterTupleDomain.none())))
                .isEqualTo(DynamicFilterTupleDomain.none());
        assertThat(DynamicFilterTupleDomain.columnWiseUnion(
                ImmutableList.of(
                        DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(A, DynamicFilterDomain.onlyNull(BIGINT))),
                        DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(A, fromDomain(Domain.notNull(BIGINT)))))))
                .isEqualTo(DynamicFilterTupleDomain.<ColumnHandle>all());
    }

    @Test
    void testMismatchedColumnWiseUnion()
    {
        DynamicFilterTupleDomain<ColumnHandle> tupleDomain1 = DynamicFilterTupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, DynamicFilterDomain.all(DOUBLE),
                        B, DynamicFilterDomain.singleValue(VARCHAR, utf8Slice("value"))));

        DynamicFilterTupleDomain<ColumnHandle> tupleDomain2 = DynamicFilterTupleDomain.withColumnDomains(
                ImmutableMap.of(
                        A, fromDomain(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(DOUBLE, 0.0)), true)),
                        C, DynamicFilterDomain.singleValue(BIGINT, 1L)));

        DynamicFilterTupleDomain<ColumnHandle> expectedTupleDomain = DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(A, DynamicFilterDomain.all(DOUBLE)));

        assertThat(DynamicFilterTupleDomain.columnWiseUnion(ImmutableList.of(tupleDomain1, tupleDomain2))).isEqualTo(expectedTupleDomain);
    }

    @Test
    void testIsNone()
    {
        assertThat(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, DynamicFilterDomain>of()).isNone()).isFalse();
        assertThat(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(A, DynamicFilterDomain.singleValue(BIGINT, 0L))).isNone()).isFalse();
        assertThat(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(A, DynamicFilterDomain.none(BIGINT))).isNone()).isTrue();
        assertThat(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(A, DynamicFilterDomain.all(BIGINT))).isNone()).isFalse();
        assertThat(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(A, DynamicFilterDomain.all(BIGINT), B, DynamicFilterDomain.none(BIGINT))).isNone()).isTrue();
    }

    @Test
    void testIsAll()
    {
        assertThat(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, DynamicFilterDomain>of()).isAll()).isTrue();
        assertThat(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(A, DynamicFilterDomain.singleValue(BIGINT, 0L))).isAll()).isFalse();
        assertThat(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(A, DynamicFilterDomain.all(BIGINT))).isAll()).isTrue();
        assertThat(DynamicFilterTupleDomain.withColumnDomains(ImmutableMap.of(A, DynamicFilterDomain.singleValue(BIGINT, 0L), B, DynamicFilterDomain.all(BIGINT))).isAll()).isFalse();
    }

    @Test
    void testToTupleDomain()
    {
        assertThat(DynamicFilterTupleDomain.none().toTupleDomain()).isEqualTo(TupleDomain.none());
        assertThat(DynamicFilterTupleDomain.all().toTupleDomain()).isEqualTo(TupleDomain.all());

        LongBloomFilter bloomFilter = createBloomFilter(1, 2, 3);
        DynamicFilterTupleDomain<ColumnHandle> dynamicFilterTupleDomain = DynamicFilterTupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, DynamicFilterDomain>builder()
                        .put(A, DynamicFilterDomain.all(VARCHAR))
                        .put(B, fromDomain(Domain.notNull(DOUBLE)))
                        .put(C, fromBloomFilter(new BloomFilterWithRange(bloomFilter, createRange(1, 3), BIGINT, false)))
                        .put(D, DynamicFilterDomain.singleValue(BIGINT, 1L))
                        .put(E, DynamicFilterDomain.all(DOUBLE))
                        .buildOrThrow());
        assertThat(dynamicFilterTupleDomain.toTupleDomain()).isEqualTo(
                TupleDomain.withColumnDomains(
                        ImmutableMap.<ColumnHandle, Domain>builder()
                                .put(A, Domain.all(VARCHAR))
                                .put(B, Domain.notNull(DOUBLE))
                                .put(C, Domain.create(createRange(1, 3), false))
                                .put(D, Domain.singleValue(BIGINT, 1L))
                                .put(E, Domain.all(DOUBLE))
                                .buildOrThrow()));
    }

    @Test
    void testSimplify()
    {
        assertThat(DynamicFilterTupleDomain.none().simplifyDomains(1)).isEqualTo(DynamicFilterTupleDomain.none());
        assertThat(DynamicFilterTupleDomain.all().simplifyDomains(1)).isEqualTo(DynamicFilterTupleDomain.all());

        ValueSet ranges = ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 4L, true), Range.equal(BIGINT, 7L));
        DynamicFilterTupleDomain<ColumnHandle> dynamicFilterTupleDomain = DynamicFilterTupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, DynamicFilterDomain>builder()
                        .put(A, fromDomain(Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L), true)))
                        .put(B, fromDomain(Domain.notNull(DOUBLE)))
                        .put(C, fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1, 4, 7), ranges, BIGINT, false)))
                        .buildOrThrow());

        assertThat(dynamicFilterTupleDomain.simplifyDomains(1)).isEqualTo(
                DynamicFilterTupleDomain.withColumnDomains(
                        ImmutableMap.<ColumnHandle, DynamicFilterDomain>builder()
                                .put(A, fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1, 2, 3), createRange(1, 3), BIGINT, true)))
                                .put(B, fromDomain(Domain.notNull(DOUBLE)))
                                .put(C, fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1, 4, 7), createRange(1, 7), BIGINT, false)))
                                .buildOrThrow()));

        dynamicFilterTupleDomain = DynamicFilterTupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, DynamicFilterDomain>builder()
                        .put(A, fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1, 2, 3), createRange(1, 3), BIGINT, true)))
                        .put(B, fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1, 2, 3, 4, 5), createRange(1, 5), BIGINT, true)))
                        .put(C, fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1, 4, 7), createRange(1, 7), BIGINT, false)))
                        .put(D, fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1, 2, 3, 4, 5, 6, 7), createRange(1, 7), BIGINT, true)))
                        .put(E, fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1, 7), createRange(1, 7), BIGINT, false)))
                        .put(F, fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1, 7), createRange(1, 7), BIGINT, false)))
                        .buildOrThrow());

        assertThat(dynamicFilterTupleDomain.simplifyDomains(1)).isEqualTo(
                DynamicFilterTupleDomain.withColumnDomains(
                        ImmutableMap.<ColumnHandle, DynamicFilterDomain>builder()
                                .put(A, fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1, 2, 3), createRange(1, 3), BIGINT, true)))
                                .put(B, fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1, 2, 3, 4, 5), createRange(1, 5), BIGINT, true)))
                                .put(C, fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1, 4, 7), createRange(1, 7), BIGINT, false)))
                                .put(D, fromDomain(Domain.create(createRange(1, 7), true)))
                                .put(E, fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1, 7), createRange(1, 7), BIGINT, false)))
                                .put(F, fromBloomFilter(new BloomFilterWithRange(createBloomFilter(1, 7), createRange(1, 7), BIGINT, false)))
                                .buildOrThrow()));
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
}
