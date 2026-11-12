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

import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

public final class Collations
{
    private Collations() {}

    /// Whether a type contains a character string with a non-default collation.
    ///
    /// Equality under such a collation does not identify the string: `'Alice'` and `'ALICE'` are
    /// equal under a case-insensitive collation and remain distinguishable. Rewrites that replace
    /// a value by an equal one must not apply to these types, and their values cannot be put in
    /// domains, whose type operators have no collation comparisons.
    public static boolean hasNonDefaultCollation(Type type)
    {
        if (type instanceof VarcharType varcharType) {
            return !varcharType.getCollation().isDefault();
        }
        for (Type parameter : type.getTypeParameters()) {
            if (hasNonDefaultCollation(parameter)) {
                return true;
            }
        }
        return false;
    }
}
