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
 * Requires that the type bound to {@code variable} supports ordering
 * ({@code <}, {@code <=}, etc.).
 * <p>
 * Used by signatures like {@code array_sort<T>(array(T)) -> array(T)} and the order
 * operators. Once {@code variable} resolves to a concrete type, the solver validates it
 * against {@link TypeSystem#isOrderable(Expression)}; orderability is declared per type via
 * {@link org.weakref.solver.type.TypeConstructor#orderable()}. Related to
 * {@link RequireComparable} but stricter.
 */
public record RequireOrderable(String variable)
        implements Constraint
{
    @Override
    public Constraint apply(Map<String, Expression> substitutions)
    {
        Expression replacement = substitutions.get(variable);
        if (replacement instanceof Expression.Variable(String name)) {
            return new RequireOrderable(name);
        }
        return this;
    }

    @Override
    public Constraint rewrite(Map<String, Expression> mappings)
    {
        return apply(mappings);
    }
}
