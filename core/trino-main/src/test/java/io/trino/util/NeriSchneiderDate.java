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

/**
 * Reference implementation of the Neri-Schneider civil-from-days algorithm
 * (Cassio Neri & Lorenz Schneider, "Euclidean Affine Functions and Applications to Calendar
 * Algorithms", 2021, <a href="https://arxiv.org/abs/2102.06959">arXiv:2102.06959</a>).
 * <p>
 * Used by {@link BenchmarkDateExtraction} to back the algorithm-choice comparison referenced
 * in the corresponding PR. Not on any production code path.
 */
public final class NeriSchneiderDate
{
    private NeriSchneiderDate() {}

    // Shifts sized to keep the algorithm's internal counter non-negative across the full int
    // days range (matches the era cycle used by FastDate).
    private static final long K = 146097L * 14704L + 719468L;
    private static final long L = 400L * 14704L;

    /**
     * Returns {@code (year << 32) | (month << 8) | day} for the given days-since-1970-01-01.
     */
    public static long ymdFromEpochDay(int days)
    {
        long n = days + K;
        long n1 = 4L * n + 3L;
        long c = n1 / 146097L;
        long nC = (n1 % 146097L) >>> 2;
        long n2 = 4L * nC + 3L;
        long p2 = 2939745L * n2;
        long z = p2 >>> 32;
        long nY = ((p2 & 0xFFFFFFFFL) / 2939745L) >>> 2;
        long y = 100L * c + z;
        long n3 = 2141L * nY + 197913L;
        long m = n3 >>> 16;
        long d = (n3 & 0xFFFFL) / 2141L;
        long j = nY >= 306L ? 1L : 0L;
        long year = (y - L) + j;
        long month = j != 0L ? m - 12L : m;
        long day = d + 1L;
        return (year << 32) | (month << 8) | day;
    }
}
