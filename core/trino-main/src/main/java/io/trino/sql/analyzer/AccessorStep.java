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
package io.trino.sql.analyzer;

import io.trino.sql.tree.Identifier;

/// One step in a SQL:2023 §6.36 simplified-accessor chain.
///
/// The chain produced by [JsonAccessorChain#walk(io.trino.sql.tree.Expression)]
/// is a sequence of these steps, in surface order. Each step maps to a
/// JSON-path accessor that [JsonAccessorChain#buildPath(java.util.List)]
/// emits.
sealed interface AccessorStep
{
    /// A member-by-name step (`j.foo`, `j."FooBar"`).
    record Member(Identifier name)
            implements AccessorStep {}

    /// An integer-subscript step (`j[3]`).
    record Index(long value)
            implements AccessorStep {}

    /// A `.*` wildcard-member step.
    enum WildcardMember
            implements AccessorStep
    {
        INSTANCE
    }

    /// A `[*]` wildcard-array step.
    enum WildcardArray
            implements AccessorStep
    {
        INSTANCE
    }
}
