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
import java.util.Optional;

/**
 * A named type shape registered with the solver.
 * <p>
 * A constructor has:
 * <ul>
 *   <li>a {@link #name} (e.g. {@code "integer"}, {@code "decimal"}, {@code "array"});</li>
 *   <li>a list of formal parameter names (possibly empty for primitive types) —
 *       e.g. {@code [@p, @s]} for {@code decimal(p, s)};</li>
 *   <li>a list of {@link #constraints} on those parameters. The solver injects these
 *       constraints whenever a type expression using this constructor appears in a
 *       problem. For example, {@code decimal} contributes {@code RequireKind(@p, NUMBER)}
 *       so the solver knows {@code @p} is an integer variable.</li>
 *   <li>optionally, {@link #variadic()} — true for constructors whose last parameter
 *       can repeat (e.g. {@code row}).</li>
 *   <li>a {@link #newInstance} factory, used by embedders to turn a solved
 *       {@link org.weakref.solver.Expression} back into a concrete {@link Type} value.</li>
 * </ul>
 * Most callers use {@link PrimitiveTypeConstructor} or {@link ParametricTypeConstructor}
 * instead of implementing this directly; custom constructors are only needed for types
 * whose validation constraints or parameter kinds don't fit those shapes.
 */
public interface TypeConstructor
{
    String name();

    List<String> parameters();

    List<Constraint> constraints();

    default boolean variadic()
    {
        return false;
    }

    /**
     * Whether instances of this type support equality comparison ({@code =}, {@code <>}).
     * Defaults to {@link Trait#PRESENT}, appropriate for the bulk of scalar types.
     *
     * @see org.weakref.solver.RequireComparable
     */
    default Trait comparable()
    {
        return Trait.PRESENT;
    }

    /**
     * Whether instances of this type support ordering ({@code <}, {@code <=}, ...).
     * Defaults to {@link Trait#PRESENT}, appropriate for the bulk of scalar types.
     *
     * @see org.weakref.solver.RequireOrderable
     */
    default Trait orderable()
    {
        return Trait.PRESENT;
    }

    Type newInstance(List<Argument> arguments);

    /**
     * How a type-class trait (comparability, orderability) attaches to a constructor.
     */
    enum Trait
    {
        /**
         * No instance supports the trait, regardless of arguments (e.g. {@code json}).
         */
        ABSENT,
        /**
         * Every instance supports the trait, regardless of arguments (e.g. {@code integer}, {@code varchar(n)}).
         */
        PRESENT,
        /**
         * An instance supports the trait iff all of its type arguments do (e.g. {@code array}, {@code map}, {@code row}).
         */
        STRUCTURAL,
    }

    /**
     * An argument passed to {@link #newInstance} — either a nested type or an integer.
     */
    sealed interface Argument
            permits TypeArgument, NumericArgument {}

    record NumericArgument(int value)
            implements Argument {}

    record TypeArgument(Optional<String> name, Type type)
            implements Argument {}
}
