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
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.spi.type.CollationComparison;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.type.collation.CollationWeights.PRIMARY;
import static io.trino.type.collation.PadSpaceKeys.appendLevel;
import static io.trino.type.collation.PadSpaceKeys.comparePadded;

/// A `PAD SPACE` collation implemented by an ICU collator.
///
/// The collator compares strings by the weights of their collation elements, level by level. The
/// padding applies to those weights: at each level, the shorter weight sequence is compared as
/// though it were extended with the space's weight at that level. A character the collator
/// ignores at a level contributes no weight there, so it does not count toward the padding, and a
/// trailing character with the space's weights at every compared level, such as a no-break space
/// under a case-insensitive collation, is padding. ICU's own sort keys compress weights, so the
/// collation key is built from the weights (see [PadSpaceKeys]).
final class IcuPadSpaceCollation
        implements CollationComparison
{
    private static final int PRIMARY_WIDTH = 4;
    private static final int SECONDARY_AND_TERTIARY_WIDTH = 2;

    private final int levels;
    private final int[] padWeights;
    private final ThreadLocal<CollationElementIterator> threadIterator;

    IcuPadSpaceCollation(RuleBasedCollator collator)
    {
        checkArgument(collator.isFrozen(), "Collator must be frozen");
        checkArgument(collator.getStrength() <= Collator.TERTIARY, "Collator strength must be at most tertiary");
        checkArgument(
                !collator.isCaseLevel() && !collator.isUpperCaseFirst() && !collator.isLowerCaseFirst() && !collator.isFrenchCollation() && !collator.isAlternateHandlingShifted(),
                "Collator must compare only primary, secondary, and tertiary weights");
        // each thread reads collation elements with its own iterator over its own collator copy
        this.threadIterator = ThreadLocal.withInitial(() -> collator.cloneAsThawed().getCollationElementIterator(""));
        this.levels = collator.getStrength() + 1;

        CollationWeights space = CollationWeights.of(threadIterator.get(), " ", levels);
        this.padWeights = new int[levels];
        for (int level = 0; level < levels; level++) {
            checkState(space.count(level) == 1, "A space must have one weight at level %s", level);
            padWeights[level] = space.weights(level)[0];
        }
    }

    @Override
    public int compare(Slice left, Slice right)
    {
        CollationWeights leftWeights = weights(left);
        CollationWeights rightWeights = weights(right);
        for (int level = 0; level < levels; level++) {
            int result = comparePadded(
                    leftWeights.weights(level),
                    leftWeights.count(level),
                    rightWeights.weights(level),
                    rightWeights.count(level),
                    padWeights[level]);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    @Override
    public boolean equal(Slice left, Slice right)
    {
        // identical bytes are equal under every collation; avoid decoding for the common case
        return left.equals(right) || compare(left, right) == 0;
    }

    @Override
    public Slice collationKey(Slice value)
    {
        CollationWeights weights = weights(value);
        DynamicSliceOutput output = new DynamicSliceOutput(value.length() * 8 + 2 * levels);
        for (int level = 0; level < levels; level++) {
            int width = level == PRIMARY ? PRIMARY_WIDTH : SECONDARY_AND_TERTIARY_WIDTH;
            appendLevel(output, weights.weights(level), weights.count(level), padWeights[level], width);
        }
        return output.slice();
    }

    private CollationWeights weights(Slice value)
    {
        return CollationWeights.of(threadIterator.get(), value.toStringUtf8(), levels);
    }
}
