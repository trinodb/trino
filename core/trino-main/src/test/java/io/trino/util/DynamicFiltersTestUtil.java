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
package io.trino.util;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.gen.columnar.ColumnarFilterCompiler;
import io.trino.sql.gen.columnar.DynamicPageFilter;
import io.trino.sql.gen.columnar.FilterEvaluator;
import io.trino.sql.planner.Symbol;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public final class DynamicFiltersTestUtil
{
    private DynamicFiltersTestUtil() {}

    public static String getSimplifiedDomainString(long low, long high, int rangeCount, Type type)
    {
        /* Expected simplified domain string is generated explicitly here instead of
         * directly relying on output of Domain#toString so that we know when formatting or content
         * of the output changes in some unexpected way. It is necessary to ensure that the output
         * remains readable and compact while providing sufficient information.
         * This information is provided to user as part of EXPLAIN ANALYZE.
         */
        String formattedValues;
        if (rangeCount == 1) {
            formattedValues = format("{[%d]}", low);
        }
        else if (rangeCount == 2) {
            formattedValues = LongStream.of(low, high)
                    .mapToObj(value -> "[" + value + "]")
                    .collect(Collectors.joining(", ", "{", "}"));
        }
        else {
            formattedValues = format("{[%d], ..., [%d]}", low, high);
        }
        return "[ " +
                new StringJoiner(", ", SortedRangeSet.class.getSimpleName() + "[", "]")
                        .add("type=" + type)
                        .add("ranges=" + rangeCount)
                        .add(formattedValues) +
                " ]";
    }

    public static FilterEvaluator createDynamicFilterEvaluator(
            TupleDomain<ColumnHandle> tupleDomain,
            Map<ColumnHandle, Integer> channels)
    {
        return createDynamicFilterEvaluator(tupleDomain, channels, 1);
    }

    public static FilterEvaluator createDynamicFilterEvaluator(
            TupleDomain<ColumnHandle> tupleDomain,
            Map<ColumnHandle, Integer> channels,
            double selectivityThreshold)
    {
        TestingDynamicFilter dynamicFilter = new TestingDynamicFilter(1);
        dynamicFilter.update(tupleDomain);
        Map<ColumnHandle, Type> types = tupleDomain.getDomains().orElse(ImmutableMap.of())
                .entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getType()));
        int index = 0;
        ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, Integer> layout = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Integer> entry : channels.entrySet()) {
            ColumnHandle column = entry.getKey();
            Symbol symbol = new Symbol(types.get(column), "col" + index++);
            columns.put(symbol, column);
            int channel = entry.getValue();
            layout.put(symbol, channel);
        }
        return new DynamicPageFilter(
                PLANNER_CONTEXT,
                testSessionBuilder().build(),
                columns.buildOrThrow(),
                layout.buildOrThrow(),
                selectivityThreshold)
                .createDynamicPageFilterEvaluator(new ColumnarFilterCompiler(createTestingFunctionManager(), 0), dynamicFilter)
                .get();
    }

    public static class TestingDynamicFilter
            implements DynamicFilter
    {
        private CompletableFuture<?> isBlocked;
        private TupleDomain<ColumnHandle> currentPredicate;
        private int futuresLeft;

        public TestingDynamicFilter(int expectedFilters)
        {
            this.futuresLeft = expectedFilters;
            this.isBlocked = expectedFilters == 0 ? NOT_BLOCKED : new CompletableFuture<>();
            this.currentPredicate = TupleDomain.all();
        }

        public void update(TupleDomain<ColumnHandle> predicate)
        {
            futuresLeft -= 1;
            verify(futuresLeft >= 0);
            currentPredicate = currentPredicate.intersect(predicate);
            CompletableFuture<?> currentFuture = isBlocked;
            // create next blocking future (if needed)
            isBlocked = isComplete() ? NOT_BLOCKED : new CompletableFuture<>();
            verify(currentFuture.complete(null));
        }

        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return currentPredicate.getDomains().orElseThrow().keySet();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return unmodifiableFuture(isBlocked);
        }

        @Override
        public boolean isComplete()
        {
            return futuresLeft == 0;
        }

        @Override
        public boolean isAwaitable()
        {
            return futuresLeft > 0;
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return currentPredicate;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestingDynamicFilter that = (TestingDynamicFilter) o;
            return futuresLeft == that.futuresLeft
                    && Objects.equals(isBlocked, that.isBlocked)
                    && Objects.equals(currentPredicate, that.currentPredicate);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(isBlocked, currentPredicate, futuresLeft);
        }
    }
}
