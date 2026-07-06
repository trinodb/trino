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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;
import io.trino.util.MergeSortedPages.PageWithPosition;

import java.io.Closeable;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.util.MergeSortedPages.mergeSortedPages;

/**
 * This class performs merge of previously hash sorted pages streams.
 * <p>
 * Positions are compared using their hash value. It is possible
 * that two distinct values to have same hash value, thus returned
 * stream of Pages can have interleaved positions with same hash value.
 */
public class MergeHashSort
        implements Closeable
{
    private final AggregatedMemoryContext memoryContext;

    public MergeHashSort(AggregatedMemoryContext memoryContext)
    {
        this.memoryContext = memoryContext;
    }

    /**
     * Rows with same hash value are guaranteed to be in the same result page.
     */
    public WorkProcessor<Page> merge(List<Type> allTypes, List<WorkProcessor<Page>> channels, DriverYieldSignal driverYieldSignal, int hashChannel)
    {
        HashGenerator hashGenerator = new PrecomputedHashGenerator(hashChannel);
        return mergeSortedPages(
                channels,
                // the hash prefix is the whole merge key, so equal prefixes are true ties
                (_, _, _, _) -> 0,
                ImmutableList.of(hashPrefixFiller(hashGenerator)),
                IntStream.range(0, allTypes.size()).boxed().collect(toImmutableList()),
                allTypes,
                keepSameHashValuesWithinSinglePage(),
                true,
                memoryContext,
                driverYieldSignal);
    }

    @VisibleForTesting
    public WorkProcessor<Page> merge(List<Type> allTypes, List<WorkProcessor<Page>> channels, DriverYieldSignal driverYieldSignal, HashGenerator hashGenerator)
    {
        return mergeSortedPages(
                channels,
                // the hash prefix is the whole merge key, so equal prefixes are true ties
                (_, _, _, _) -> 0,
                ImmutableList.of(hashPrefixFiller(hashGenerator)),
                IntStream.range(0, allTypes.size()).boxed().collect(toImmutableList()),
                allTypes,
                keepSameHashValuesWithinSinglePage(),
                true,
                memoryContext,
                driverYieldSignal);
    }

    @Override
    public void close()
    {
        memoryContext.close();
    }

    private static BiPredicate<PageBuilder, PageWithPosition> keepSameHashValuesWithinSinglePage()
    {
        return new BiPredicate<>()
        {
            private long lastPrefix;

            @Override
            public boolean test(PageBuilder pageBuilder, PageWithPosition pageWithPosition)
            {
                // set the last bit on the prefix, so that zero is never produced
                long prefix = pageWithPosition.getPrefix() | 1;
                boolean samePrefix = prefix == lastPrefix;
                lastPrefix = prefix;

                return !pageBuilder.isEmpty() && !samePrefix && pageBuilder.isFull();
            }
        };
    }

    /**
     * The merge key is the 64-bit hash itself, so the sort key prefix is the whole key and fully
     * decides the merge order: the sign bit is flipped so that the unsigned prefix order matches
     * the signed hash order, and the hash is computed once per row instead of on every merge
     * comparison.
     */
    private static PageSortKeyPrefixFiller hashPrefixFiller(HashGenerator hashGenerator)
    {
        return (page, prefixes) -> {
            for (int position = 0; position < page.getPositionCount(); position++) {
                prefixes[position] = hashGenerator.hashPosition(position, page) ^ Long.MIN_VALUE;
            }
        };
    }
}
