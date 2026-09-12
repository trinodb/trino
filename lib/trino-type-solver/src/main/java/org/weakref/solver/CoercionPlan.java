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

import java.util.List;
import java.util.Map;

/**
 * Planner-facing description of how a value of {@code sourceType} is converted to
 * {@code targetType}.
 * <p>
 * Produced alongside solver success as part of function-resolution output. A plan is a
 * tree of {@link CoercionStep steps}:
 * <ul>
 *   <li>{@link Kind#EXACT} — no conversion needed; {@code steps} is empty.</li>
 *   <li>{@link Kind#DIRECT} — a leaf conversion driven by one (or a short chain of) registered
 *       rules; each step is a {@link DirectRule} carrying the {@link CoercionRule#ruleId()}
 *       and any residual conditions.</li>
 *   <li>{@link Kind#DERIVED} — a structural conversion (e.g. {@code array(X) → array(Y)});
 *       each child {@link Structural} step carries the nested plan per type argument.</li>
 * </ul>
 * Plans are deliberately data-only: they carry no runtime behavior themselves. An engine
 * embedding the library walks the tree to emit cast operations in its execution IR.
 */
public record CoercionPlan(Expression sourceType, Expression targetType, Kind kind, List<CoercionStep> steps)
{
    public CoercionPlan
    {
        steps = List.copyOf(steps);
    }

    public boolean isExact()
    {
        return kind() == Kind.EXACT;
    }

    public static CoercionPlan exact(Expression sourceType, Expression targetType)
    {
        return new CoercionPlan(sourceType, targetType, Kind.EXACT, List.of());
    }

    public static CoercionPlan direct(Expression sourceType, Expression targetType, List<String> ruleIds)
    {
        return directSteps(
                sourceType,
                targetType,
                ruleIds.stream()
                        .map(ruleId -> new DirectRule(ruleId, List.of()))
                        .toList());
    }

    public static CoercionPlan directSteps(Expression sourceType, Expression targetType, List<DirectRule> steps)
    {
        return new CoercionPlan(
                sourceType,
                targetType,
                Kind.DIRECT,
                steps.stream()
                        .map(CoercionStep.class::cast)
                        .toList());
    }

    public static CoercionPlan derived(Expression sourceType, Expression targetType, List<CoercionStep> steps)
    {
        return new CoercionPlan(sourceType, targetType, Kind.DERIVED, steps);
    }

    public CoercionPlan apply(Map<String, Expression> substitutions)
    {
        return new CoercionPlan(
                Expression.substitute(sourceType, substitutions),
                Expression.substitute(targetType, substitutions),
                kind,
                steps.stream()
                        .map(step -> apply(step, substitutions))
                        .toList());
    }

    private static CoercionStep apply(CoercionStep step, Map<String, Expression> substitutions)
    {
        return switch (step) {
            case DirectRule(String ruleId, List<Constraint> conditions) -> new DirectRule(
                    ruleId,
                    conditions.stream()
                            .map(constraint -> constraint.apply(substitutions))
                            .toList());
            case Structural(String constructor, List<CoercionPlan> children) -> new Structural(
                    constructor,
                    children.stream()
                            .map(child -> child.apply(substitutions))
                            .toList());
        };
    }

    public sealed interface CoercionStep
            permits DirectRule, Structural {}

    public record DirectRule(String ruleId, List<Constraint> conditions)
            implements CoercionStep
    {
        public DirectRule
        {
            conditions = List.copyOf(conditions);
        }
    }

    public record Structural(String constructor, List<CoercionPlan> children)
            implements CoercionStep
    {
        public Structural
        {
            children = List.copyOf(children);
        }
    }

    public enum Kind
    {
        EXACT,
        DIRECT,
        DERIVED,
    }
}
