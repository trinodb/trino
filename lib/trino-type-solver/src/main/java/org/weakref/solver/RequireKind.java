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
 * Restricts a variable to a specific {@link Kind} (type vs numeric).
 * <p>
 * Used by parametric type constructors like {@code decimal(@p, @s)} to declare that
 * their arguments must be numeric, and by signatures that walk generic arguments
 * through the kind-aware decomposition in {@link TypeScheme}.
 */
public record RequireKind(String variable, Kind kind)
        implements Constraint
{
    @Override
    public Constraint apply(Map<String, Expression> substitutions)
    {
        Expression replacement = substitutions.get(variable);
        if (replacement instanceof Expression.Variable(String name)) {
            return new RequireKind(name, kind);
        }
        return this;
    }

    @Override
    public Constraint rewrite(Map<String, Expression> mappings)
    {
        return apply(mappings);
    }
}
