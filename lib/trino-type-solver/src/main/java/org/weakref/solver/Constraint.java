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
package org.weakref.solver;

import java.util.Map;

/**
 * A requirement the solver must discharge.
 * <p>
 * Constraints are the unit the solver operates on. Each concrete shape expresses a
 * different kind of fact or obligation over {@link Expression}s — an upper/lower
 * bound ({@link Subtype}), an exact binding ({@link ExactType}), a kind-discipline
 * requirement ({@link RequireKind}), a numeric relation
 * ({@link NumericRelation}), a disjunction of alternatives ({@link Choice}),
 * structural type-class membership ({@link RequireComparable},
 * {@link RequireOrderable}), or explicit cast reachability
 * ({@link RequireCastableTo}, {@link RequireCastableFrom}).
 */
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
    /**
     * Substitute bound variables with their current values. Free (unresolved) variables
     * are left untouched. Used by the solver each time it dequeues a constraint so the
     * handler sees the most refined form.
     */
    default Constraint apply(Map<String, Expression> substitutions)
    {
        return this;
    }

    /**
     * Fully rewrite every variable referenced by this constraint using the given mapping.
     * Unlike {@link #apply}, this form fails loudly if a variable has no mapping — used
     * when instantiating schemes or coercion rules with fresh variables.
     */
    default Constraint rewrite(Map<String, Expression> mappings)
    {
        return this;
    }
}
