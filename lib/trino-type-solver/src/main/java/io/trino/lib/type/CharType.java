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

import org.weakref.solver.RequireKind;
import org.weakref.solver.type.ParametricTypeConstructor;
import org.weakref.solver.type.Type;
import org.weakref.solver.type.TypeConstructor;
import org.weakref.solver.type.TypeConstructor.Argument;
import org.weakref.solver.type.TypeConstructor.NumericArgument;

import java.util.List;

import static org.weakref.solver.Kind.NUMBER;

public record CharType(long length)
        implements Type
{
    /**
     * Maximum {@code char} length. An unbounded {@code varchar} coerced to {@code char} produces
     * {@code char(MAX_LENGTH)}, mirroring Trino's {@code CharType.MAX_LENGTH}.
     */
    public static final long MAX_LENGTH = 65_536;

    public static final TypeConstructor CONSTRUCTOR = new CharTypeConstructor();

    static class CharTypeConstructor
            extends ParametricTypeConstructor
    {
        public CharTypeConstructor()
        {
            super("char",
                    List.of("@n"),
                    List.of(new RequireKind("@n", NUMBER)));
        }

        @Override
        public Type newInstance(List<Argument> arguments)
        {
            return new CharType(((NumericArgument) arguments.getFirst()).value());
        }
    }
}
