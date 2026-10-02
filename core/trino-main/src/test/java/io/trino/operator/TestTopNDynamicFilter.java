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
package io.trino.operator;

import com.google.common.collect.ImmutableMap;
import io.trino.connector.TestingColumnHandle;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTopNDynamicFilter
{
    private static final ColumnHandle COLUMN = new TestingColumnHandle("column");

    @Test
    public void testAscending()
    {
        TopNDynamicFilter dynamicFilter = createDynamicFilter(BIGINT, ASC_NULLS_LAST, false);
        assertThat(dynamicFilter.snapshot()).isSameAs(DynamicFilter.EMPTY);

        dynamicFilter.tighten(createLongsBlock(10L), 0);
        DynamicFilter snapshot = dynamicFilter.snapshot();
        // a table scan must not wait for a TopNDynamicFilter
        assertThat(snapshot.isComplete()).isTrue();
        assertThat(snapshot.isAwaitable()).isFalse();
        assertThat(snapshot.isBlocked()).isDone();
        assertThat(snapshot.getColumnsCovered()).containsExactly(COLUMN);
        assertThat(snapshot.getCurrentPredicate()).isEqualTo(predicate(Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 10L)), false)));

        // a bound ranked below the current bound is ignored
        dynamicFilter.tighten(createLongsBlock(20L), 0);
        dynamicFilter.tighten(createLongsBlock(10L), 0);
        assertThat(dynamicFilter.snapshot()).isSameAs(snapshot);

        dynamicFilter.tighten(createLongsBlock(5L), 0);
        // a snapshot does not change when the filter is tightened
        assertThat(snapshot.getCurrentPredicate()).isEqualTo(predicate(Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 10L)), false)));
        assertThat(dynamicFilter.snapshot().getCurrentPredicate()).isEqualTo(predicate(Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 5L)), false)));
    }

    @Test
    public void testDescendingInclusiveWithNullsFirst()
    {
        TopNDynamicFilter dynamicFilter = createDynamicFilter(VARCHAR, DESC_NULLS_FIRST, true);
        dynamicFilter.tighten(createStringsBlock("b"), 0);
        assertThat(dynamicFilter.snapshot().getCurrentPredicate()).isEqualTo(predicate(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(VARCHAR, utf8Slice("b"))), true)));

        dynamicFilter.tighten(createStringsBlock("a"), 0);
        assertThat(dynamicFilter.snapshot().getCurrentPredicate()).isEqualTo(predicate(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(VARCHAR, utf8Slice("b"))), true)));

        dynamicFilter.tighten(createStringsBlock("c"), 0);
        assertThat(dynamicFilter.snapshot().getCurrentPredicate()).isEqualTo(predicate(Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(VARCHAR, utf8Slice("c"))), true)));
    }

    @Test
    public void testNullBoundIsIgnored()
    {
        TopNDynamicFilter dynamicFilter = createDynamicFilter(BIGINT, ASC_NULLS_LAST, false);
        Block block = createLongsBlock((Long) null);
        dynamicFilter.tighten(block, 0);
        assertThat(dynamicFilter.snapshot()).isSameAs(DynamicFilter.EMPTY);
    }

    @Test
    public void testOrderableType()
    {
        CharType type = createCharType(3);
        TopNDynamicFilter dynamicFilter = createDynamicFilter(type, ASC_NULLS_FIRST, false);
        dynamicFilter.tighten(writeNativeValue(type, utf8Slice("b")), 0);
        assertThat(dynamicFilter.snapshot().getCurrentPredicate()).isEqualTo(predicate(Domain.create(ValueSet.ofRanges(Range.lessThan(type, utf8Slice("b"))), true)));

        dynamicFilter.tighten(writeNativeValue(type, utf8Slice("a")), 0);
        assertThat(dynamicFilter.snapshot().getCurrentPredicate()).isEqualTo(predicate(Domain.create(ValueSet.ofRanges(Range.lessThan(type, utf8Slice("a"))), true)));
    }

    @Test
    public void testSupportedTypes()
    {
        assertThat(TopNDynamicFilter.isSupportedType(BIGINT)).isTrue();
        assertThat(TopNDynamicFilter.isSupportedType(VARCHAR)).isTrue();
        assertThat(TopNDynamicFilter.isSupportedType(TIMESTAMP_MILLIS)).isTrue();
        assertThat(TopNDynamicFilter.isSupportedType(BOOLEAN)).isTrue();
        assertThat(TopNDynamicFilter.isSupportedType(createCharType(3))).isTrue();
        assertThat(TopNDynamicFilter.isSupportedType(new ArrayType(BIGINT))).isTrue();
        assertThat(TopNDynamicFilter.isSupportedType(HYPER_LOG_LOG)).isFalse();
        // NaN is ordered, but cannot be represented in a range
        assertThat(TopNDynamicFilter.isSupportedType(DOUBLE)).isFalse();
        assertThat(TopNDynamicFilter.isSupportedType(REAL)).isFalse();
        assertThat(TopNDynamicFilter.isSupportedType(NUMBER)).isFalse();
        // NaN in a nested value is ordered depending on the null ordering, while ranges always order it last
        assertThat(TopNDynamicFilter.isSupportedType(new ArrayType(DOUBLE))).isFalse();
        assertThat(TopNDynamicFilter.isSupportedType(RowType.anonymousRow(BIGINT, REAL))).isFalse();
    }

    private static TopNDynamicFilter createDynamicFilter(Type type, SortOrder sortOrder, boolean inclusive)
    {
        return new TopNDynamicFilter(COLUMN, type, sortOrder, inclusive, new TypeOperators());
    }

    private static TupleDomain<ColumnHandle> predicate(Domain domain)
    {
        return TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN, domain));
    }
}
