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

import io.trino.spi.block.SqlMultiset;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;

/// Implements `x IS A SET`: true when the multiset contains no duplicate elements (null counted as
/// one value, so two nulls are duplicates). Backed by the multiset's hash index, so it is O(1)
/// amortized and reuses the index across calls on the same value.
@ScalarFunction(value = "$is_a_set", hidden = true, neverFails = true)
public final class MultisetIsASetFunction
{
    private MultisetIsASetFunction() {}

    @TypeParameter("E")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isASet(@SqlType("multiset(E)") SqlMultiset multiset)
    {
        return multiset.isSet();
    }
}
