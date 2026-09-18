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
package io.trino.type;

/// Defines the `char`/`varchar` implicit coercion direction and the `char`-to-`varchar`
/// cast semantics.
public enum CharVarcharCoercion
{
    /// Coerce CHAR to VARCHAR. Trim trailing spaces when casting from CHAR to VARCHAR.
    SQL_STANDARD,
    /// Coerce VARCHAR to CHAR. Keep trailing spaces when casting from CHAR to VARCHAR.
    LEGACY,
}
