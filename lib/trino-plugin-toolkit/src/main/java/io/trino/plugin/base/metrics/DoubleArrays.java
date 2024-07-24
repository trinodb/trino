/*
 * Copyright (C) 2002-2019 Sebastiano Vigna
 *
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
 *
 *
 * For the sorting and binary search code:
 *
 * Copyright (C) 1999 CERN - European Organization for Nuclear Research.
 *
 * Permission to use, copy, modify, distribute and sell this software and
 * its documentation for any purpose is hereby granted without fee,
 * provided that the above copyright notice appear in all copies and that
 * both that copyright notice and this permission notice appear in
 * supporting documentation. CERN makes no representations about the
 * suitability of this software for any purpose. It is provided "as is"
 * without expressed or implied warranty.
 */
package io.trino.plugin.base.metrics;

// Note: this code was forked from fastutil (http://fastutil.di.unimi.it/)
final class DoubleArrays
{
    private DoubleArrays() {}

    private static final int QUICKSORT_NO_REC = 16;
    private static final int QUICKSORT_MEDIAN_OF_9 = 128;

    /**
     * Sorts the specified range of elements according to the natural ascending
     * order using indirect quicksort.
     *
     * <p>
     * The sorting algorithm is a tuned quicksort adapted from Jon L. Bentley and M.
     * Douglas McIlroy, &ldquo;Engineering a Sort Function&rdquo;, <i>Software:
     * Practice and Experience</i>, 23(11), pages 1249&minus;1265, 1993.
     *
     * <p>
     * This method implement an <em>indirect</em> sort. The elements of {@code perm}
     * (which must be exactly the numbers in the interval {@code [0..perm.length)})
     * will be permuted so that {@code x[perm[i]] &le; x[perm[i + 1]]}.
     *
     * <p>
     * Note that this implementation does not allocate any object, contrarily to the
     * implementation used to sort primitive types in {@link java.util.Arrays},
     * which switches to mergesort on large inputs.
     *
     * @param perm a permutation array indexing {@code x}.
     * @param x the array to be sorted.
     * @param from the index of the first element (inclusive) to be sorted.
     * @param to the index of the last element (exclusive) to be sorted.
     */
    @SuppressWarnings("checkstyle:InnerAssignment")
    public static void quickSortIndirect(final int[] perm, final double[] x, final int from, final int to)
    {
        final int len = to - from;
        // Selection sort on smallest arrays
        if (len < QUICKSORT_NO_REC) {
            insertionSortIndirect(perm, x, from, to);
            return;
        }
        // Choose a partition element, v
        int m = from + len / 2;
        int l = from;
        int n = to - 1;
        if (len > QUICKSORT_MEDIAN_OF_9) { // Big arrays, pseudomedian of 9
            int s = len / 8;
            l = med3Indirect(perm, x, l, l + s, l + 2 * s);
            m = med3Indirect(perm, x, m - s, m, m + s);
            n = med3Indirect(perm, x, n - 2 * s, n - s, n);
        }
        m = med3Indirect(perm, x, l, m, n); // Mid-size, med of 3
        final double v = x[perm[m]];
        // Establish Invariant: v* (<v)* (>v)* v*
        int a = from;
        int b = a;
        int c = to - 1;
        int d = c;
        while (true) {
            int comparison;
            while (b <= c && (comparison = (Double.compare((x[perm[b]]), (v)))) <= 0) {
                if (comparison == 0) {
                    swap(perm, a++, b);
                }
                b++;
            }
            while (c >= b && (comparison = (Double.compare((x[perm[c]]), (v)))) >= 0) {
                if (comparison == 0) {
                    swap(perm, c, d--);
                }
                c--;
            }
            if (b > c) {
                break;
            }
            swap(perm, b++, c--);
        }
        // Swap partition elements back to middle
        int s;
        s = Math.min(a - from, b - a);
        swap(perm, from, b - s, s);
        s = Math.min(d - c, to - d - 1);
        swap(perm, b, to - s, s);
        // Recursively sort non-partition-elements
        if ((s = b - a) > 1) {
            quickSortIndirect(perm, x, from, from + s);
        }
        if ((s = d - c) > 1) {
            quickSortIndirect(perm, x, to - s, to);
        }
    }

    private static void insertionSortIndirect(final int[] perm, final double[] a, final int from, final int to)
    {
        for (int i = from; ++i < to; ) {
            int t = perm[i];
            int j = i;
            for (int u = perm[j - 1]; (Double.compare((a[t]), (a[u])) < 0); u = perm[--j - 1]) {
                perm[j] = u;
                if (from == j - 1) {
                    --j;
                    break;
                }
            }
            perm[j] = t;
        }
    }

    private static int med3Indirect(final int[] perm, final double[] x, final int a, final int b, final int c)
    {
        final double aa = x[perm[a]];
        final double bb = x[perm[b]];
        final double cc = x[perm[c]];
        final int ab = (Double.compare((aa), (bb)));
        final int ac = (Double.compare((aa), (cc)));
        final int bc = (Double.compare((bb), (cc)));
        return (ab < 0 ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
    }

    private static void swap(final int[] x, final int a, final int b)
    {
        final int t = x[a];
        x[a] = x[b];
        x[b] = t;
    }

    private static void swap(final int[] x, int a, int b, final int n)
    {
        for (int i = 0; i < n; i++, a++, b++) {
            swap(x, a, b);
        }
    }
}
