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
 * Requires that {@code source} can be cast to {@code target}. Mirror of {@link RequireCastableTo}
 * with argument order swapped for declaration-site readability — often used when the variable
 * sits on the target side (e.g., {@code json castable to @T}).
 */
public record RequireCastableFrom(Expression target, Expression source)
        implements Constraint
{
    @Override
    public Constraint apply(Map<String, Expression> substitutions)
    {
        return new RequireCastableFrom(
                Expression.substitute(target, substitutions),
                Expression.substitute(source, substitutions));
    }

    @Override
    public Constraint rewrite(Map<String, Expression> mappings)
    {
        return apply(mappings);
    }
}
