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

import com.google.common.collect.ImmutableMap;
import io.trino.connector.TestingColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.util.DynamicFiltersTestUtil.TestingDynamicFilter;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.IntersectionDynamicFilter.intersect;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIntersectionDynamicFilter
{
    private static final ColumnHandle COLUMN_A = new TestingColumnHandle("a");
    private static final ColumnHandle COLUMN_B = new TestingColumnHandle("b");

    @Test
    public void testIntersectWithEmpty()
    {
        TestingDynamicFilter dynamicFilter = new TestingDynamicFilter(1);
        assertThat(intersect(dynamicFilter, DynamicFilter.EMPTY)).isSameAs(dynamicFilter);
        assertThat(intersect(DynamicFilter.EMPTY, dynamicFilter)).isSameAs(dynamicFilter);
    }

    @Test
    public void testAwaitableAndComplete()
    {
        TestingDynamicFilter awaitable = new TestingDynamicFilter(1);
        DynamicFilter complete = new TestingDynamicFilter(0);
        DynamicFilter dynamicFilter = intersect(awaitable, complete);

        assertThat(dynamicFilter.isAwaitable()).isTrue();
        assertThat(dynamicFilter.isComplete()).isFalse();
        CompletableFuture<?> isBlocked = dynamicFilter.isBlocked();
        assertThat(isBlocked).isNotDone();

        awaitable.update(TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN_A, Domain.singleValue(BIGINT, 1L))));
        assertThat(isBlocked).isDone();
        assertThat(dynamicFilter.isAwaitable()).isFalse();
        assertThat(dynamicFilter.isComplete()).isTrue();
        assertThat(dynamicFilter.isBlocked()).isDone();
    }

    @Test
    public void testPredicate()
    {
        TestingDynamicFilter first = new TestingDynamicFilter(1);
        TestingDynamicFilter second = new TestingDynamicFilter(1);
        first.update(TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN_A, Domain.singleValue(BIGINT, 1L))));
        second.update(TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN_B, Domain.singleValue(BIGINT, 2L))));

        DynamicFilter dynamicFilter = intersect(first, second);
        assertThat(dynamicFilter.getColumnsCovered()).containsExactlyInAnyOrder(COLUMN_A, COLUMN_B);
        assertThat(dynamicFilter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                COLUMN_A, Domain.singleValue(BIGINT, 1L),
                COLUMN_B, Domain.singleValue(BIGINT, 2L))));
    }

    @Test
    public void testEquality()
    {
        // consumers caching work per dynamic filter, like the dynamic row filter, reuse it for intersections of the same filters
        DynamicFilter first = new TestingDynamicFilter(1);
        DynamicFilter second = new TestingDynamicFilter(1);
        assertThat(intersect(first, second)).isEqualTo(intersect(first, second));
        assertThat(intersect(first, second)).isNotEqualTo(intersect(first, new TestingDynamicFilter(1)));
    }
}
