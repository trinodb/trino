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
                createHashPageWithPositionComparator(hashGenerator),
                IntStream.range(0, allTypes.size()).boxed().collect(toImmutableList()),
                allTypes,
                keepSameHashValuesWithinSinglePage(hashGenerator),
                true,
                memoryContext,
                driverYieldSignal);
    }

    @VisibleForTesting
    public WorkProcessor<Page> merge(List<Type> allTypes, List<WorkProcessor<Page>> channels, DriverYieldSignal driverYieldSignal, HashGenerator hashGenerator)
    {
        return mergeSortedPages(
                channels,
                createHashPageWithPositionComparator(hashGenerator),
                IntStream.range(0, allTypes.size()).boxed().collect(toImmutableList()),
                allTypes,
                keepSameHashValuesWithinSinglePage(hashGenerator),
                true,
                memoryContext,
                driverYieldSignal);
    }

    @Override
    public void close()
    {
        memoryContext.close();
    }

    private static BiPredicate<PageBuilder, PageWithPosition> keepSameHashValuesWithinSinglePage(HashGenerator hashGenerator)
    {
        return new BiPredicate<>()
        {
            private long lastHash;

            @Override
            public boolean test(PageBuilder pageBuilder, PageWithPosition pageWithPosition)
            {
                // set the last bit on the hash, so that zero is never produced
                long hash = hashGenerator.hashPosition(pageWithPosition.getPosition(), pageWithPosition.getPage()) | 1;
                boolean sameHash = hash == lastHash;
                lastHash = hash;

                return !pageBuilder.isEmpty() && !sameHash && pageBuilder.isFull();
            }
        };
    }

    private static PageWithPositionComparator createHashPageWithPositionComparator(HashGenerator hashGenerator)
    {
        return (Page leftPage, int leftPosition, Page rightPage, int rightPosition) -> {
            long leftHash = hashGenerator.hashPosition(leftPosition, leftPage);
            long rightHash = hashGenerator.hashPosition(rightPosition, rightPage);

            return Long.compare(leftHash, rightHash);
        };
    }
}
