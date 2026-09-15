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
package io.trino.typesolver;

import java.util.Map;

/// A requirement the solver must discharge.
///
/// Constraints are the unit the solver operates on. Each concrete shape expresses a
/// different kind of fact or obligation over [Expression]s — an upper/lower
/// bound ([Subtype]), an exact binding ([ExactType]), a kind-discipline
/// requirement ([RequireKind]), a numeric relation
/// ([NumericRelation]), a disjunction of alternatives ([Choice]),
/// structural type-class membership ([RequireComparable],
/// [RequireOrderable]), or explicit cast reachability
/// ([RequireCastableTo], [RequireCastableFrom]).
public sealed interface Constraint
        permits Choice,
                ExactType,
                NumericRelation,
                RequireCastableFrom,
                RequireCastableTo,
                RequireComparable,
                RequireKind,
                RequireOrderable,
                Subtype
{
    /// Substitute bound variables with their current values. Free (unresolved) variables
    /// are left untouched. Used by the solver each time it dequeues a constraint so the
    /// handler sees the most refined form.
    default Constraint apply(Map<String, Expression> substitutions)
    {
        return this;
    }

    /// Fully rewrite every variable referenced by this constraint using the given mapping.
    /// Unlike [#apply], this form fails loudly if a variable has no mapping — used
    /// when instantiating schemes or coercion rules with fresh variables.
    default Constraint rewrite(Map<String, Expression> mappings)
    {
        return this;
    }
}
