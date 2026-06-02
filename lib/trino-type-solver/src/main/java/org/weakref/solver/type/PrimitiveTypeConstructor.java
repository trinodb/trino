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
package org.weakref.solver.type;

import org.weakref.solver.Constraint;

import java.util.List;
import java.util.function.Supplier;

/**
 * {@link TypeConstructor} for a zero-parameter primitive type.
 * <p>
 * Used for atomic types like {@code integer}, {@code boolean}, {@code varchar}. The
 * supplier returns a fresh instance of the corresponding {@link Type} value class
 * whenever an embedder calls {@link #newInstance}.
 * <p>
 * Most primitives are both comparable and orderable; the two-argument constructor defaults
 * to that. Types that opt out (e.g. {@code json}, which supports neither) use the
 * four-argument form.
 */
public record PrimitiveTypeConstructor(String name, Supplier<Type> instantiator, Trait comparable, Trait orderable)
        implements TypeConstructor
{
    public PrimitiveTypeConstructor(String name, Supplier<Type> instantiator)
    {
        this(name, instantiator, Trait.PRESENT, Trait.PRESENT);
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public Trait comparable()
    {
        return comparable;
    }

    @Override
    public Trait orderable()
    {
        return orderable;
    }

    @Override
    public List<String> parameters()
    {
        return List.of();
    }

    @Override
    public List<Constraint> constraints()
    {
        return List.of();
    }

    @Override
    public Type newInstance(List<Argument> arguments)
    {
        if (!arguments.isEmpty()) {
            throw new IllegalArgumentException("Primitive type constructor takes no arguments");
        }

        return instantiator.get();
    }

    @Override
    public String toString()
    {
        return name;
    }
}
