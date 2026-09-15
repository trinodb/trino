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

/// Requires that the type bound to `variable` supports equality comparison.
///
/// Used to type-class constrain polymorphic signatures like `=<T>(T, T) -> boolean`
/// or `array_distinct<T>(array(T)) -> array(T)`. Once `variable` resolves to a
/// concrete type, the solver validates it against
/// [TypeSystem#isComparable(Expression)]; a non-comparable binding (e.g. `json`)
/// makes the overload unsatisfiable. Comparability is declared per type via
/// [TypeConstructor#comparable()]. Related to
/// [RequireOrderable] but weaker — every orderable type is comparable, not vice versa
/// (e.g. `map` is comparable but not orderable).
public record RequireComparable(String variable)
        implements Constraint
{
    @Override
    public Constraint apply(Map<String, Expression> substitutions)
    {
        Expression replacement = substitutions.get(variable);
        if (replacement instanceof Expression.Variable(String name)) {
            return new RequireComparable(name);
        }
        return this;
    }

    @Override
    public Constraint rewrite(Map<String, Expression> mappings)
    {
        return apply(mappings);
    }
}
