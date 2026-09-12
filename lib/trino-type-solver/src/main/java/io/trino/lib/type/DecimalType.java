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

public record DecimalType(int precision, int scale)
        implements Type
{
    public static final TypeConstructor CONSTRUCTOR = new DecimalTypeConstructor();

    static class DecimalTypeConstructor
            extends ParametricTypeConstructor
    {
        public DecimalTypeConstructor()
        {
            super("decimal",
                    List.of("@p", "@s"),
                    List.of(
                            new RequireKind("@p", NUMBER),
                            new RequireKind("@s", NUMBER),
                            new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("@p"), literal(1))),
                            new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("@p"), literal(38))),
                            new NumericRelation(operation(GREATER_THAN_OR_EQUAL, variable("@s"), literal(0))),
                            new NumericRelation(operation(LESS_THAN_OR_EQUAL, variable("@s"), variable("@p")))));
        }

        @Override
        public Type newInstance(List<Argument> arguments)
        {
            int precision = ((NumericArgument) arguments.get(0)).value();
            int scale = ((NumericArgument) arguments.get(1)).value();

            if (precision > 38) {
                throw new IllegalArgumentException("Decimal precision cannot be greater than 38: " + precision);
            }
            if (scale < 0) {
                throw new IllegalArgumentException("Decimal scale cannot be negative: " + scale);
            }
            if (scale > precision) {
                throw new IllegalArgumentException("Decimal scale cannot be greater than precision: " + scale + " > " + precision);
            }

            return new DecimalType(precision, scale);
        }
    }
}
