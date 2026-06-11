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
package io.trino.lib.type;

import org.weakref.solver.NumericRelation;
import org.weakref.solver.RequireKind;
import org.weakref.solver.type.ParametricTypeConstructor;
import org.weakref.solver.type.Type;
import org.weakref.solver.type.TypeConstructor;
import org.weakref.solver.type.TypeConstructor.Argument;
import org.weakref.solver.type.TypeConstructor.NumericArgument;

import java.util.List;

import static org.weakref.solver.Expression.BinaryOperator.GREATER_THAN_OR_EQUAL;
import static org.weakref.solver.Expression.BinaryOperator.LESS_THAN_OR_EQUAL;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.operation;
import static org.weakref.solver.Expression.variable;
import static org.weakref.solver.Kind.NUMBER;

public record VarcharType(long length)
        implements Type
{
    public static final TypeConstructor CONSTRUCTOR = new VarcharTypeConstructor();

    static class VarcharTypeConstructor
            extends ParametricTypeConstructor
    {
        public VarcharTypeConstructor()
        {
            super("varchar",
                    List.of("@n"),
                    List.of(
                            new RequireKind("@n", NUMBER),
                            new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("@n"), literal(0))),
                            new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("@n"), literal(Integer.MAX_VALUE)))));
        }

        @Override
        public Type newInstance(List<Argument> arguments)
        {
            return new VarcharType(((NumericArgument) arguments.getFirst()).value());
        }
    }
}
