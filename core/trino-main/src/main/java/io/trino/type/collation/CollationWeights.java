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
package io.trino.type.collation;

import com.ibm.icu.text.CollationElementIterator;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.ibm.icu.text.CollationElementIterator.NULLORDER;

/// The weights of a string under an ICU collator, one sequence for each comparison level: the
/// non-zero primary, secondary, and tertiary weights of the string's collation elements, in
/// order. For a collator without case level, case first, French secondary, or shifted alternate
/// handling, comparing the sequences level by level gives the collator's order.
///
/// The iterator returns a collation element that does not fit 32 bits as two halves. The first
/// holds the high 16 bits of the primary weight, the high byte of the secondary weight, and the
/// high byte of the tertiary weight with its case bits. The second, marked by `0xC0` in its low
/// byte, holds the low 16 bits of the primary weight and the low bytes of the secondary and
/// tertiary weights.
final class CollationWeights
{
    static final int PRIMARY = 0;
    static final int SECONDARY = 1;
    static final int TERTIARY = 2;

    private static final int CONTINUATION = 0xC0;
    private static final int TERTIARY_MASK = 0x3F;

    private final int[][] weights;
    private final int[] counts;

    private CollationWeights(int levels)
    {
        weights = new int[levels][];
        counts = new int[levels];
        for (int level = 0; level < levels; level++) {
            weights[level] = new int[8];
        }
    }

    /// The weights of `value` at the first `levels` levels, read with `iterator`.
    static CollationWeights of(CollationElementIterator iterator, String value, int levels)
    {
        checkArgument(levels >= 1 && levels <= 3, "levels must be between 1 and 3: %s", levels);
        CollationWeights result = new CollationWeights(levels);
        iterator.setText(value);
        int element = iterator.next();
        while (element != NULLORDER) {
            int next = iterator.next();
            int primary = element & 0xFFFF0000;
            int secondary = (element >>> 8 & 0xFF) << 8;
            int tertiary = (element & TERTIARY_MASK) << 8;
            if (next != NULLORDER && (next & CONTINUATION) == CONTINUATION) {
                primary |= next >>> 16;
                secondary |= next >>> 8 & 0xFF;
                tertiary |= next & TERTIARY_MASK;
                next = iterator.next();
            }
            result.add(PRIMARY, primary);
            if (levels > SECONDARY) {
                result.add(SECONDARY, secondary);
            }
            if (levels > TERTIARY) {
                result.add(TERTIARY, tertiary);
            }
            element = next;
        }
        return result;
    }

    /// The weights at `level`; only the first [#count(int)] are meaningful.
    int[] weights(int level)
    {
        return weights[level];
    }

    int count(int level)
    {
        return counts[level];
    }

    private void add(int level, int weight)
    {
        if (weight == 0) {
            return;
        }
        if (counts[level] == weights[level].length) {
            weights[level] = Arrays.copyOf(weights[level], counts[level] * 2);
        }
        weights[level][counts[level]++] = weight;
    }
}
