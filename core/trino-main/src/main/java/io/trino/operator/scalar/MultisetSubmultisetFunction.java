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
import io.trino.spi.block.SqlMultiset;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;

/// Implements `left SUBMULTISET OF right`: true when every value's multiplicity in `left` does
/// not exceed its multiplicity in `right` (null is a value, so `left` may hold no more
/// nulls than `right`).
///
/// Both operands carry an `IDENTICAL`-keyed multiplicity index, so a value's two multiplicities
/// are each an O(1) amortized lookup and the whole test is O(n) over the left elements — no per-call
/// multiplicity map and no pairwise scan. The element operators come from the carried index, so the
/// function declares no operator dependencies of its own.
@ScalarFunction(value = "$submultiset", hidden = true, neverFails = true)
public final class MultisetSubmultisetFunction
{
    private MultisetSubmultisetFunction() {}

    @TypeParameter("E")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean submultiset(
            @SqlType("multiset(E)") SqlMultiset left,
            @SqlType("multiset(E)") SqlMultiset right)
    {
        // left is a submultiset of right when no value occurs more often in left than in right. A null
        // element probes each index's null count, so nulls are compared the same way. Re-probing a
        // repeated left value is harmless and keeps the scan O(n) with O(1) amortized lookups.
        Block leftElements = left.getRawElementBlock();
        int offset = left.getRawOffset();
        for (int i = 0; i < left.getSize(); i++) {
            int position = offset + i;
            if (left.multiplicity(leftElements, position) > right.multiplicity(leftElements, position)) {
                return false;
            }
        }
        return true;
    }
}
