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
 * Requires that {@code source} can be cast to {@code target}. Either side may be a variable
 * or a concrete expression. The cast registry (implicit + explicit rules) is consulted once
 * both sides are resolvable to concrete types.
 */
public record RequireCastableTo(Expression source, Expression target)
        implements Constraint
{
    @Override
    public Constraint apply(Map<String, Expression> substitutions)
    {
        return new RequireCastableTo(
                Expression.substitute(source, substitutions),
                Expression.substitute(target, substitutions));
    }

    @Override
    public Constraint rewrite(Map<String, Expression> mappings)
    {
        return apply(mappings);
    }
}
