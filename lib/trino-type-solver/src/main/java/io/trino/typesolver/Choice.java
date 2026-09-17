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

import java.util.List;
import java.util.Map;

/// A deferred disjunction — exactly one alternative must hold, but the solver can't tell
/// which yet.
///
/// Produced when multiple coercion rules match a [Subtype] constraint (e.g. a value
/// could widen to any of several targets). The solver parks the choice as pending,
/// then:
///
/// - as other constraints resolve, infeasible alternatives (whose guards become false)
///   are dropped;
/// - dominated alternatives are pruned by [ChoiceSimplifier];
/// - when one alternative remains, it's committed.
public record Choice(List<Alternative> alternatives)
        implements Constraint
{
    @Override
    public Constraint apply(Map<String, Expression> substitutions)
    {
        return new Choice(alternatives.stream()
                .map(alternative -> alternative.apply(substitutions))
                .toList());
    }

    @Override
    public Constraint rewrite(Map<String, Expression> mappings)
    {
        return new Choice(alternatives.stream()
                .map(alternative -> alternative.rewrite(mappings))
                .toList());
    }
}
