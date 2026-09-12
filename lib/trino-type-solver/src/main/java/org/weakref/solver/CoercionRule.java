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

import java.util.Optional;
import java.util.Set;

/**
 * Coercion rules answer the question: "Can an expression of type 'from' be coerced to type 'to', and if
 * so, under what conditions?"
 * <p>
 * A negative answer means that the coercion is not possible under *any* circumstances.
 */
public interface CoercionRule
{
    /**
     * Attempt to apply this rule to a candidate coercion.
     *
     * @param allocator source of fresh variables for introducing internal bindings
     * @param from the source expression (may contain variables)
     * @param to the target expression (may contain variables)
     * @return {@link Optional#empty()} if this rule does not apply, otherwise a {@link Match}
     *         whose {@code constraints} must be satisfied for the coercion to hold and whose
     *         optional {@code plan} gives a planner-facing description of the runtime conversion
     */
    Optional<Match> matches(VariableAllocator allocator, Expression from, Expression to);

    /**
     * Stable identifier used when classifying an assembled {@link CoercionPlan} (e.g. to mark
     * a step as "primitive widening"). Override when the default class name is ambiguous.
     */
    default String ruleId()
    {
        return getClass().getSimpleName();
    }

    /**
     * Result of a successful {@link #matches} call.
     *
     * @param constraints conditions under which the coercion holds (may be empty for
     *         unconditional rules). Fed back into the solver's worklist.
     * @param plan optional planner-facing description of the conversion. Absent
     *         for rules that only contribute constraints (e.g. covariance
     *         decomposition produces constraints but no direct-step plan).
     */
    record Match(Set<Constraint> constraints, Optional<CoercionPlan> plan)
    {
        public Match
        {
            constraints = Set.copyOf(constraints);
            plan = plan == null ? Optional.empty() : plan;
        }

        public Match(Set<Constraint> constraints)
        {
            this(constraints, Optional.empty());
        }

        public Match(Set<Constraint> constraints, CoercionPlan plan)
        {
            this(constraints, Optional.of(plan));
        }
    }
}
