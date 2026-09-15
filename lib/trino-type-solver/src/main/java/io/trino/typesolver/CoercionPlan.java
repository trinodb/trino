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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/// Planner-facing description of how a value of `sourceType` is converted to
/// `targetType`.
///
/// Produced alongside solver success as part of function-resolution output. A plan is a
/// tree of [steps][CoercionStep]:
///
/// - [Kind#EXACT] — no conversion needed; `steps` is empty.
/// - [Kind#DIRECT] — a leaf conversion driven by one (or a short chain of) registered
///   rules; each step is a [DirectRule] carrying the [CoercionRule#ruleId()]
///   and any residual conditions.
/// - [Kind#DERIVED] — a structural conversion (e.g. `array(X) → array(Y)`);
///   each child [Structural] step carries the nested plan per type argument.
///
/// Plans are deliberately data-only: they carry no runtime behavior themselves. An engine
/// embedding the library walks the tree to emit cast operations in its execution IR.
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

    @SuppressWarnings("ReferenceEquality")
    public CoercionPlan apply(Map<String, Expression> substitutions)
    {
        if (substitutions.isEmpty()) {
            return this;
        }
        Expression source = Expression.substitute(sourceType, substitutions);
        Expression target = Expression.substitute(targetType, substitutions);
        List<CoercionStep> changed = null;
        for (int index = 0; index < steps.size(); index++) {
            CoercionStep step = steps.get(index);
            CoercionStep replacement = apply(step, substitutions);
            if (replacement != step && changed == null) {
                changed = new ArrayList<>(steps.subList(0, index));
            }
            if (changed != null) {
                changed.add(replacement);
            }
        }
        if (source == sourceType && target == targetType && changed == null) {
            return this;
        }
        return new CoercionPlan(source, target, kind, changed == null ? steps : changed);
    }

    private static CoercionStep apply(CoercionStep step, Map<String, Expression> substitutions)
    {
        return switch (step) {
            case DirectRule(String ruleId, List<Constraint> conditions) -> {
                if (conditions.isEmpty()) {
                    yield step;
                }
                List<Constraint> updated = conditions.stream().map(constraint -> constraint.apply(substitutions)).toList();
                yield updated.equals(conditions) ? step : new DirectRule(ruleId, updated);
            }
            case Structural(String constructor, List<CoercionPlan> children) -> {
                List<CoercionPlan> updated = children.stream().map(child -> child.apply(substitutions)).toList();
                yield updated.equals(children) ? step : new Structural(constructor, updated);
            }
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
