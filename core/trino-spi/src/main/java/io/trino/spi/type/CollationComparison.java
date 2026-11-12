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
package io.trino.spi.type;

import io.airlift.slice.Slice;

/// Compares UTF-8 strings under one [Collation].
///
/// A collated [VarcharType] names its collation only. Its operators obtain the comparison from
/// the [TypeOperators] that build them, through [TypeOperators#collationComparison], which
/// resolves it with the [CollationComparisons] the engine supplies.
///
/// Arguments are never null and may hold invalid UTF-8. Implementations are deterministic and
/// thread-safe, and the three methods agree with each other:
///
/// - [#compare] is a total preorder: strings it compares as `0` are equal under the collation
///   even when their bytes differ.
/// - [#equal] holds exactly when [#compare] returns `0`.
/// - [#collationKey] orders, as unsigned bytes, as [#compare] orders the strings, and two keys
///   are equal exactly when the strings are equal.
public interface CollationComparison
{
    /// A negative number, zero, or a positive number as `left` sorts before, equal to, or after
    /// `right`.
    int compare(Slice left, Slice right);

    boolean equal(Slice left, Slice right);

    /// A byte string that sorts and hashes as `value` does under the collation.
    Slice collationKey(Slice value);
}
