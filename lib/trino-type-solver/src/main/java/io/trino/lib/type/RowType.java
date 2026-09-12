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

import org.weakref.solver.Constraint;
import org.weakref.solver.RequireKind;
import org.weakref.solver.type.Type;
import org.weakref.solver.type.TypeConstructor;
import org.weakref.solver.type.TypeConstructor.Argument;
import org.weakref.solver.type.TypeConstructor.TypeArgument;

import java.util.List;
import java.util.Optional;

import static org.weakref.solver.Kind.TYPE;

public record RowType(List<Field> fields)
        implements Type
{
    record Field(Optional<String> name, Type type) {}

    public static final TypeConstructor CONSTRUCTOR = new RowTypeConstructor();

    static class RowTypeConstructor
            implements TypeConstructor
    {
        @Override
        public String name()
        {
            return "row";
        }

        @Override
        public List<String> parameters()
        {
            return List.of("@T");
        }

        @Override
        public List<Constraint> constraints()
        {
            return List.of(new RequireKind("@T", TYPE));
        }

        @Override
        public boolean variadic()
        {
            return true;
        }

        // A row is comparable/orderable iff all of its field types are.
        @Override
        public Trait comparable()
        {
            return Trait.STRUCTURAL;
        }

        @Override
        public Trait orderable()
        {
            return Trait.STRUCTURAL;
        }

        @Override
        public String toString()
        {
            return "row(l1:@T1, ..., ln:@Tn) ∀ @T1, ..., @Tn ∈ TYPE";
        }

        @Override
        public Type newInstance(List<Argument> arguments)
        {
            return new RowType(arguments.stream()
                    .map(TypeArgument.class::cast)
                    .map(argument -> new Field(argument.name(), argument.type()))
                    .toList());
        }
    }
}
