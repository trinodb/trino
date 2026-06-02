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

import org.weakref.solver.Expression.Variable;

import java.util.Map;

/**
 * Forces a type variable to a specific expression.
 * <p>
 * Unlike {@link Subtype}, which records a bound, this commits the variable outright.
 * When the variable is already bound to something else, the solver throws
 * {@link UnsatisfiableException}.
 */
public record ExactType(String variable, Expression type)
        implements Constraint
{
    @Override
    public Constraint apply(Map<String, Expression> substitutions)
    {
        String rewrittenVariable = variable;
        Expression replacement = substitutions.get(variable);
        if (replacement instanceof Variable(String name)) {
            rewrittenVariable = name;
        }
        return new ExactType(rewrittenVariable, Expression.substitute(type, substitutions));
    }

    @Override
    public Constraint rewrite(Map<String, Expression> mappings)
    {
        return apply(mappings);
    }
}
