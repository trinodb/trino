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
package io.trino.sql.gen.columnar;

import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static it.unimi.dsi.fastutil.ints.IntArrays.quickSort;

public final class FilterReorderingProfiler
{
    private static final long MIN_SAMPLE_POSITIONS_REORDERING = 4095;

    private final boolean filterReorderingEnabled;
    private final int[] filterOrder;
    private final List<FilterMetrics> filterMetrics;
    private long sampledPositions;

    public FilterReorderingProfiler(int filterCount, boolean filterReorderingEnabled)
    {
        this.filterReorderingEnabled = filterReorderingEnabled;
        this.filterOrder = new int[filterCount];
        for (int i = 0; i < filterCount; i++) {
            this.filterOrder[i] = i;
        }
        this.filterMetrics = IntStream.range(0, filterCount)
                .mapToObj(_ -> new FilterMetrics())
                .collect(toImmutableList());
    }

    public int[] getFilterOrder()
    {
        return filterOrder;
    }

    public void addFilterMetrics(int filterIndex, long filterTimeNanos, long filteredPositions)
    {
        filterMetrics.get(filterIndex).addFilterMetrics(filterTimeNanos, filteredPositions);
    }

    public void reorderFilters(int inputPositions)
    {
        sampledPositions += inputPositions;
        // Don't reorder too often due to small pages
        if (!filterReorderingEnabled || sampledPositions < MIN_SAMPLE_POSITIONS_REORDERING) {
            return;
        }
        quickSort(
                filterOrder,
                (filterIndex1, filterIndex2) ->
                        Double.compare(filterMetrics.get(filterIndex1).score(), filterMetrics.get(filterIndex2).score()));
        sampledPositions = 0;
    }

    private static class FilterMetrics
    {
        private long totalFilterTimeNanos;
        private long totalFilteredPositions;

        void addFilterMetrics(long filterTimeNanos, long filteredPositions)
        {
            totalFilterTimeNanos += filterTimeNanos;
            totalFilteredPositions += filteredPositions;
        }

        double score()
        {
            return totalFilterTimeNanos / (1.0 + totalFilteredPositions);
        }
    }
}
