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
package io.trino.operator.scalar;

import io.trino.spi.block.Block;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsIdentical;

/// A per-value occurrence count of the elements of a multiset block, keyed by the element's
/// `IDENTICAL` operator so that null is not distinct from null. Building it costs O(n) and each
/// [#consume] is an amortized O(1) hashed lookup, which lets the multiplicity-matching bag
/// operators (EXCEPT and INTERSECT ALL) run in O(n + m) rather than O(n * m).
public final class MultisetMultiplicities
{
    private final BlockSet distinctValues;
    private final int[] counts;
    private int nullCount;

    public static MultisetMultiplicities of(BlockPositionIsIdentical elementIdentical, BlockPositionHashCode elementHashCode, Block block)
    {
        return new MultisetMultiplicities(elementIdentical, elementHashCode, block);
    }

    private MultisetMultiplicities(BlockPositionIsIdentical elementIdentical, BlockPositionHashCode elementHashCode, Block block)
    {
        // null is tracked separately because BlockSet.positionOf cannot key on a null element
        distinctValues = new BlockSet(elementIdentical, elementHashCode, block.getPositionCount());
        counts = new int[block.getPositionCount()];
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                nullCount++;
            }
            else if (distinctValues.add(block, position)) {
                counts[distinctValues.positionOf(block, position)] = 1;
            }
            else {
                counts[distinctValues.positionOf(block, position)]++;
            }
        }
    }

    /// If the value at `position` still has an unconsumed occurrence, consume one and return
    /// `true`; otherwise return `false`.
    public boolean consume(Block block, int position)
    {
        if (block.isNull(position)) {
            if (nullCount == 0) {
                return false;
            }
            nullCount--;
            return true;
        }

        int slot = distinctValues.positionOf(block, position);
        if (slot < 0 || counts[slot] == 0) {
            return false;
        }
        counts[slot]--;
        return true;
    }
}
