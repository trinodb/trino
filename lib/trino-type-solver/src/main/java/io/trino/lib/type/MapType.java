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

import org.weakref.solver.RequireComparable;
import org.weakref.solver.RequireKind;
import org.weakref.solver.type.ParametricTypeConstructor;
import org.weakref.solver.type.Type;
import org.weakref.solver.type.TypeConstructor;
import org.weakref.solver.type.TypeConstructor.Argument;
import org.weakref.solver.type.TypeConstructor.TypeArgument;

import java.util.List;

import static org.weakref.solver.Kind.TYPE;

public record MapType(Type keyType, Type valueType)
        implements Type
{
    public static final TypeConstructor CONSTRUCTOR = new MapTypeConstructor();

    static class MapTypeConstructor
            extends ParametricTypeConstructor
    {
        public MapTypeConstructor()
        {
            super("map",
                    List.of("@k", "@v"),
                    List.of(
                            new RequireComparable("@k"),
                            new RequireKind("@v", TYPE)));
        }

        // A map is comparable iff both key and value types are; it is never orderable.
        @Override
        public Trait comparable()
        {
            return Trait.STRUCTURAL;
        }

        @Override
        public Trait orderable()
        {
            return Trait.ABSENT;
        }

        @Override
        public Type newInstance(List<Argument> arguments)
        {
            return new MapType(
                    ((TypeArgument) arguments.get(0)).type(),
                    ((TypeArgument) arguments.get(1)).type());
        }
    }
}
