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
 * Asserts that {@code subtype} can be coerced to {@code supertype}.
 * <p>
 * This is the solver's primary way of expressing "a value of one type flowing into a
 * position expecting another." Examples:
 * <ul>
 *   <li>{@code Subtype(integer, bigint)} — concrete widening; checked via the coercion rule list.</li>
 *   <li>{@code Subtype(@T, bigint)} — upper bound on {@code @T}; recorded and later materialized.</li>
 *   <li>{@code Subtype(integer, @T)} — lower bound on {@code @T}.</li>
 *   <li>{@code Subtype(array(@T), array(integer))} — structural decomposition into element subtyping.</li>
 * </ul>
 * When neither side has a variable, the solver looks for a matching coercion rule; if
 * multiple rules apply, the result is deferred as a {@link Choice}.
 */
public record Subtype(Expression subtype, Expression supertype)
        implements Constraint
{
    @Override
    public Constraint apply(Map<String, Expression> substitutions)
    {
        return new Subtype(Expression.substitute(subtype, substitutions), Expression.substitute(supertype, substitutions));
    }

    @Override
    public Constraint rewrite(Map<String, Expression> mappings)
    {
        return new Subtype(Expression.rewrite(subtype, mappings), Expression.rewrite(supertype, mappings));
    }
}
