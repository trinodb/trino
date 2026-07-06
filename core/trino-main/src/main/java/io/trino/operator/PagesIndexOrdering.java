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

import io.trino.memory.context.LocalMemoryContext;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import jakarta.annotation.Nullable;

import java.util.List;

import static io.airlift.slice.SizeOf.sizeOfLongArray;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PagesIndexOrdering
{
    private static final int SMALL = 7;
    private static final int MEDIUM = 40;

    // below this size the sort key prefix array is not worth building
    private static final int MIN_SORT_KEY_PREFIX_POSITIONS = 64;

    // number of leading positions used to estimate the prefix cardinality
    private static final int CARDINALITY_SAMPLE_POSITIONS = 4096;
    // abandon the prefixes when fewer than this fraction of the sampled prefixes are distinct
    private static final double MIN_PREFIX_CARDINALITY_RATIO = 0.1;

    private final PagesIndexComparator comparator;
    private final List<SortKeyPrefixFiller> sortKeyPrefixFillers;
    // whether equal prefixes imply that the positions are equal under the comparator: all sort
    // channels are packed and every field is exact within its bits
    private final boolean sortKeyPrefixDecidesTies;

    public PagesIndexOrdering(
            PagesIndexComparator comparator,
            List<SortKeyPrefixFiller> sortKeyPrefixFillers,
            boolean sortKeyPrefixDecidesTies)
    {
        this.comparator = requireNonNull(comparator, "comparator is null");
        this.sortKeyPrefixFillers = List.copyOf(requireNonNull(sortKeyPrefixFillers, "sortKeyPrefixFillers is null"));
        this.sortKeyPrefixDecidesTies = sortKeyPrefixDecidesTies;
    }

    public PagesIndexComparator getComparator()
    {
        return comparator;
    }

    /**
     * Sorts the given range of positions. The transient working memory allocated by the sort
     * (the sort key prefix array) is accounted in the given memory context for the duration of
     * the sort.
     */
    public void sort(PagesIndex pagesIndex, int startPosition, int endPosition, LocalMemoryContext memoryContext)
    {
        long workingMemory = sortWorkingMemory(startPosition, endPosition);
        if (workingMemory == 0) {
            sortRange(pagesIndex, startPosition, endPosition);
            return;
        }
        memoryContext.setBytes(memoryContext.getBytes() + workingMemory);
        try {
            sortRange(pagesIndex, startPosition, endPosition);
        }
        finally {
            memoryContext.setBytes(memoryContext.getBytes() - workingMemory);
        }
    }

    private void sortRange(PagesIndex pagesIndex, int startPosition, int endPosition)
    {
        SortKeyPrefixes sortKeyPrefixes = null;
        if (useSortKeyPrefixes(endPosition - startPosition)) {
            sortKeyPrefixes = buildSortKeyPrefixes(pagesIndex, startPosition, endPosition);
        }
        quickSort(pagesIndex, sortKeyPrefixes, startPosition, endPosition);
    }

    private long sortWorkingMemory(int startPosition, int endPosition)
    {
        int positionCount = endPosition - startPosition;
        if (useSortKeyPrefixes(positionCount)) {
            return sizeOfLongArray(positionCount);
        }
        return 0;
    }

    private boolean useSortKeyPrefixes(int positionCount)
    {
        return !sortKeyPrefixFillers.isEmpty() && positionCount >= MIN_SORT_KEY_PREFIX_POSITIONS;
    }

    /**
     * The leading sort channels of each position, packed into a 64-bit prefix by the types' sort
     * key prefix operators (see {@link io.trino.spi.function.OperatorType#SORT_KEY_PREFIX_UNORDERED_LAST}),
     * so most quicksort comparisons are a single primitive comparison instead of indirect block
     * accesses through the comparator. Descending order inverts the bits of a field. Positions
     * with equal prefixes are compared with the full comparator, unless the prefix decides ties.
     */
    private record SortKeyPrefixes(long[] keys, int offset, boolean decidesTies)
    {
        long key(int position)
        {
            return keys[position - offset];
        }

        void swap(int a, int b)
        {
            long temp = keys[a - offset];
            keys[a - offset] = keys[b - offset];
            keys[b - offset] = temp;
        }
    }

    /**
     * Returns null when the leading sort key values collide too much for the prefixes to pay
     * off: when the prefix can not decide ties, every colliding comparison costs a wasted key
     * comparison on top of the full comparison, so low-cardinality prefixes (for example
     * varchars sharing a long common prefix) would make the sort slower than not using
     * prefixes at all. Cardinality is estimated from the first positions and the build is
     * abandoned early, the same way PostgreSQL aborts abbreviated keys.
     */
    @Nullable
    private SortKeyPrefixes buildSortKeyPrefixes(PagesIndex pagesIndex, int startPosition, int endPosition)
    {
        long[] keys = new long[endPosition - startPosition];

        int samplePosition = toIntExact(Math.min((long) startPosition + CARDINALITY_SAMPLE_POSITIONS, endPosition));
        boolean collidingNulls = fillSortKeyPrefixes(pagesIndex, startPosition, samplePosition, keys, 0);

        boolean decidesTies = sortKeyPrefixDecidesTies && !collidingNulls;
        if (!decidesTies && sampleCardinalityTooLow(keys, samplePosition - startPosition)) {
            return null;
        }

        collidingNulls |= fillSortKeyPrefixes(pagesIndex, samplePosition, endPosition, keys, samplePosition - startPosition);
        decidesTies = sortKeyPrefixDecidesTies && !collidingNulls;
        return new SortKeyPrefixes(keys, startPosition, decidesTies);
    }

    private static boolean sampleCardinalityTooLow(long[] keys, int sampleSize)
    {
        LongOpenHashSet distinctKeys = new LongOpenHashSet(sampleSize);
        for (int i = 0; i < sampleSize; i++) {
            distinctKeys.add(keys[i]);
        }
        return distinctKeys.size() < sampleSize * MIN_PREFIX_CARDINALITY_RATIO;
    }

    private boolean fillSortKeyPrefixes(PagesIndex pagesIndex, int startPosition, int endPosition, long[] keys, int keysOffset)
    {
        boolean collidingNulls = false;
        for (SortKeyPrefixFiller filler : sortKeyPrefixFillers) {
            collidingNulls |= filler.fill(pagesIndex, startPosition, endPosition, keys, keysOffset);
        }
        return collidingNulls;
    }

    private int compare(PagesIndex pagesIndex, @Nullable SortKeyPrefixes sortKeyPrefixes, int leftPosition, int rightPosition)
    {
        if (sortKeyPrefixes != null) {
            int compare = Long.compareUnsigned(sortKeyPrefixes.key(leftPosition), sortKeyPrefixes.key(rightPosition));
            if (compare != 0 || sortKeyPrefixes.decidesTies()) {
                return compare;
            }
        }
        return comparator.compareTo(pagesIndex, leftPosition, rightPosition);
    }

    private static void swap(PagesIndex pagesIndex, @Nullable SortKeyPrefixes sortKeyPrefixes, int a, int b)
    {
        if (sortKeyPrefixes != null) {
            sortKeyPrefixes.swap(a, b);
        }
        pagesIndex.swap(a, b);
    }

    /**
     * Sorts the specified range of elements using the specified swapper and according to the order induced by the specified
     * comparator using quickSort.
     * <p>The sorting algorithm is a tuned quickSort adapted from Jon L. Bentley and M. Douglas
     * McIlroy, &ldquo;Engineering a Sort Function&rdquo;, <i>Software: Practice and Experience</i>, 23(11), pages
     * 1249&minus;1265, 1993.
     *
     * @param from the index of the first element (inclusive) to be sorted.
     * @param to the index of the last element (exclusive) to be sorted.
     */
    // note this code was forked from Fastutils
    @SuppressWarnings("InnerAssignment")
    private void quickSort(PagesIndex pagesIndex, @Nullable SortKeyPrefixes sortKeyPrefixes, int from, int to)
    {
        int len = to - from;
        // Insertion sort on smallest arrays
        if (len < SMALL) {
            for (int i = from; i < to; i++) {
                for (int j = i; j > from && (compare(pagesIndex, sortKeyPrefixes, j - 1, j) > 0); j--) {
                    swap(pagesIndex, sortKeyPrefixes, j, j - 1);
                }
            }
            return;
        }

        // Choose a partition element, v
        int m = from + len / 2; // Small arrays, middle element
        if (len > SMALL) {
            int l = from;
            int n = to - 1;
            if (len > MEDIUM) { // Big arrays, pseudomedian of 9
                int s = len / 8;
                l = median3(pagesIndex, sortKeyPrefixes, l, l + s, l + 2 * s);
                m = median3(pagesIndex, sortKeyPrefixes, m - s, m, m + s);
                n = median3(pagesIndex, sortKeyPrefixes, n - 2 * s, n - s, n);
            }
            m = median3(pagesIndex, sortKeyPrefixes, l, m, n); // Mid-size, med of 3
        }

        int a = from;
        int b = a;
        int c = to - 1;
        // Establish Invariant: v* (<v)* (>v)* v*
        int d = c;
        while (true) {
            int comparison;
            while (b <= c && ((comparison = compare(pagesIndex, sortKeyPrefixes, b, m)) <= 0)) {
                if (comparison == 0) {
                    if (a == m) {
                        m = b; // moving target; DELTA to JDK !!!
                    }
                    else if (b == m) {
                        m = a; // moving target; DELTA to JDK !!!
                    }
                    swap(pagesIndex, sortKeyPrefixes, a++, b);
                }
                b++;
            }
            while (c >= b && ((comparison = compare(pagesIndex, sortKeyPrefixes, c, m)) >= 0)) {
                if (comparison == 0) {
                    if (c == m) {
                        m = d; // moving target; DELTA to JDK !!!
                    }
                    else if (d == m) {
                        m = c; // moving target; DELTA to JDK !!!
                    }
                    swap(pagesIndex, sortKeyPrefixes, c, d--);
                }
                c--;
            }
            if (b > c) {
                break;
            }
            if (b == m) {
                m = d; // moving target; DELTA to JDK !!!
            }
            else if (c == m) {
                m = c; // moving target; DELTA to JDK !!!
            }
            swap(pagesIndex, sortKeyPrefixes, b++, c--);
        }

        // Swap partition elements back to middle
        int s;
        int n = to;
        s = Math.min(a - from, b - a);
        vectorSwap(pagesIndex, sortKeyPrefixes, from, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vectorSwap(pagesIndex, sortKeyPrefixes, b, n - s, s);

        // Recursively sort non-partition-elements
        if ((s = b - a) > 1) {
            quickSort(pagesIndex, sortKeyPrefixes, from, from + s);
        }
        if ((s = d - c) > 1) {
            quickSort(pagesIndex, sortKeyPrefixes, n - s, n);
        }
    }

    /**
     * Returns the index of the median of the three positions.
     */
    private int median3(PagesIndex pagesIndex, @Nullable SortKeyPrefixes sortKeyPrefixes, int a, int b, int c)
    {
        int ab = compare(pagesIndex, sortKeyPrefixes, a, b);
        int ac = compare(pagesIndex, sortKeyPrefixes, a, c);
        int bc = compare(pagesIndex, sortKeyPrefixes, b, c);
        return (ab < 0 ?
                (bc < 0 ? b : ac < 0 ? c : a) :
                (bc > 0 ? b : ac > 0 ? c : a));
    }

    /**
     * Swaps x[a .. (a+n-1)] with x[b .. (b+n-1)].
     */
    private static void vectorSwap(PagesIndex pagesIndex, @Nullable SortKeyPrefixes sortKeyPrefixes, int from, int l, int s)
    {
        for (int i = 0; i < s; i++, from++, l++) {
            swap(pagesIndex, sortKeyPrefixes, from, l);
        }
    }
}
