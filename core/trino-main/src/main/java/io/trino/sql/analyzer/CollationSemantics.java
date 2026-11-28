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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import io.trino.spi.type.Collation;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.Collation.UCS_BASIC;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/// The SQL rules for the collation of a character string expression.
///
/// Each character string expression has a [Collated] value: a collation and a [Derivation]
/// recording how it was established. [#combine] implements the result of data type combinations
/// (ISO/IEC 9075-2, 9.5), used wherever operand types are combined into one, and [#determine]
/// implements collation determination (9.15), used by operations that compare strings.
///
/// A value of an array, map, or row type contains strings, and each has its own derivation: a
/// [CollationShape] records the derivation of every string a value contains, and the rules apply
/// position by position, as comparing rows compares their fields pairwise (ISO/IEC 9075-2, 8.2).
///
/// The rules are independent of how expressions are traversed, so any expression analyzer can
/// apply them at its own combination points.
public final class CollationSemantics
{
    private CollationSemantics() {}

    /// How an expression's collation was established, ordered from weakest to strongest.
    public enum Derivation
    {
        /// A literal, or an expression computed only from literals. Yields to any other operand.
        /// The SQL standard makes literals implicit; this level departs from it so that a
        /// collated column can be compared with a literal.
        COERCIBLE,
        /// Declared by a type: a column, a parameter, or a cast.
        IMPLICIT,
        /// Named by a `COLLATE` clause.
        EXPLICIT,
        /// The combination of implicit operands with different collations. It has no collation
        /// and cannot be compared.
        NONE,
    }

    /// The collation and derivation of a character string expression.
    public record Collated(Collation collation, Derivation derivation, Set<String> conflicting)
    {
        public Collated
        {
            requireNonNull(collation, "collation is null");
            requireNonNull(derivation, "derivation is null");
            conflicting = ImmutableSortedSet.copyOf(conflicting);
            if ((derivation == Derivation.NONE) == conflicting.isEmpty()) {
                throw new IllegalArgumentException("Only derivation NONE has conflicting collations");
            }
        }

        public static Collated coercible()
        {
            return new Collated(UCS_BASIC, Derivation.COERCIBLE, Set.of());
        }

        public static Collated implicit(Collation collation)
        {
            return new Collated(collation, Derivation.IMPLICIT, Set.of());
        }

        public static Collated explicit(Collation collation)
        {
            return new Collated(collation, Derivation.EXPLICIT, Set.of());
        }

        static Collated none(Set<String> conflicting)
        {
            return new Collated(UCS_BASIC, Derivation.NONE, conflicting);
        }
    }

    /// The derivations of the strings a value contains, mirroring the structure of its type.
    public sealed interface CollationShape
    {
        /// A character string.
        record Leaf(Collated collated)
                implements CollationShape
        {
            public Leaf
            {
                requireNonNull(collated, "collated is null");
            }
        }

        record ArrayShape(CollationShape element)
                implements CollationShape
        {
            public ArrayShape
            {
                requireNonNull(element, "element is null");
            }
        }

        record MapShape(CollationShape key, CollationShape value)
                implements CollationShape
        {
            public MapShape
            {
                requireNonNull(key, "key is null");
                requireNonNull(value, "value is null");
            }
        }

        record RowShape(List<CollationShape> fields)
                implements CollationShape
        {
            public RowShape
            {
                fields = ImmutableList.copyOf(fields);
            }
        }

        /// A value without strings, or of a type whose strings are not tracked.
        enum Opaque
                implements CollationShape
        {
            OPAQUE
        }

        /// The first string the value contains that has derivation none, if any.
        default Optional<Collated> firstNone()
        {
            return switch (this) {
                case Leaf(Collated collated) -> collated.derivation() == Derivation.NONE ? Optional.of(collated) : Optional.empty();
                case ArrayShape(CollationShape element) -> element.firstNone();
                case MapShape(CollationShape key, CollationShape value) -> key.firstNone().or(value::firstNone);
                case RowShape(List<CollationShape> fields) -> fields.stream()
                        .map(CollationShape::firstNone)
                        .flatMap(Optional::stream)
                        .findFirst();
                case Opaque _ -> Optional.empty();
            };
        }

        /// This shape with each contained string's derivation transformed.
        default CollationShape map(Function<Collated, Collated> function)
        {
            return switch (this) {
                case Leaf(Collated collated) -> new Leaf(function.apply(collated));
                case ArrayShape(CollationShape element) -> new ArrayShape(element.map(function));
                case MapShape(CollationShape key, CollationShape value) -> new MapShape(key.map(function), value.map(function));
                case RowShape(List<CollationShape> fields) -> new RowShape(fields.stream()
                        .map(field -> field.map(function))
                        .collect(toImmutableList()));
                case Opaque opaque -> opaque;
            };
        }

        /// The shape of a relation field: an explicit collation of a select item becomes the
        /// column's implicit collation.
        default CollationShape asField()
        {
            return map(collated -> collated.derivation() == Derivation.EXPLICIT ? Collated.implicit(collated.collation()) : collated);
        }
    }

    /// The result of combining operands into one value, such as the branches of a `CASE`.
    ///
    /// 1. All explicit operands must have the same collation, and the result is explicit with it.
    /// 2. Otherwise, implicit operands with the same collation give an implicit result; implicit
    ///    operands with different collations give derivation none. A none operand gives none.
    /// 3. Otherwise, the result is coercible.
    ///
    /// The rule is commutative and associative.
    ///
    /// @throws CollationConflictException if explicit operands have different collations
    public static Collated combine(List<Collated> operands)
    {
        Optional<Collation> explicit = Optional.empty();
        for (Collated operand : operands) {
            if (operand.derivation() == Derivation.EXPLICIT) {
                if (explicit.isPresent() && !explicit.get().equals(operand.collation())) {
                    throw new CollationConflictException(Derivation.EXPLICIT, ImmutableSortedSet.of(explicit.get().descriptor(), operand.collation().descriptor()));
                }
                explicit = Optional.of(operand.collation());
            }
        }
        if (explicit.isPresent()) {
            return Collated.explicit(explicit.get());
        }

        ImmutableSortedSet.Builder<String> implicitNames = ImmutableSortedSet.naturalOrder();
        Optional<Collation> implicit = Optional.empty();
        boolean none = false;
        for (Collated operand : operands) {
            switch (operand.derivation()) {
                case IMPLICIT -> {
                    implicitNames.add(operand.collation().descriptor());
                    implicit = Optional.of(operand.collation());
                }
                case NONE -> {
                    implicitNames.addAll(operand.conflicting());
                    none = true;
                }
                case COERCIBLE, EXPLICIT -> {}
            }
        }
        Set<String> names = implicitNames.build();
        if (none || names.size() > 1) {
            return Collated.none(names);
        }
        if (implicit.isPresent()) {
            return Collated.implicit(implicit.get());
        }
        return Collated.coercible();
    }

    /// The collation an operation compares its operands with.
    ///
    /// @throws CollationConflictException if the operands' collations conflict
    public static Collation determine(List<Collated> operands)
    {
        Collated combined = combine(operands);
        if (combined.derivation() == Derivation.NONE) {
            throw new CollationConflictException(Derivation.IMPLICIT, combined.conflicting());
        }
        return combined.collation();
    }

    /// [#combine(List)] applied to the strings at each position of values combined into one.
    /// Opaque operands, such as nulls, do not take part. Operands of different structure combine
    /// into an opaque shape, which leaves the type mismatch to type checking.
    ///
    /// @throws CollationConflictException if explicit operands at a position have different collations
    public static CollationShape combineShapes(List<CollationShape> operands)
    {
        return structurally(operands, CollationSemantics::combine);
    }

    /// The collation each string position is compared with, as [#determine(List)] at each
    /// position. Each leaf of the result is the determined collation with derivation implicit.
    ///
    /// @throws CollationConflictException if the collations at a position conflict
    public static CollationShape determineShapes(List<CollationShape> operands)
    {
        return structurally(operands, leaves -> Collated.implicit(determine(leaves)));
    }

    private static CollationShape structurally(List<CollationShape> operands, Function<List<Collated>, Collated> rule)
    {
        List<CollationShape> present = operands.stream()
                .filter(operand -> !(operand instanceof CollationShape.Opaque))
                .collect(toImmutableList());
        if (present.isEmpty()) {
            return CollationShape.Opaque.OPAQUE;
        }
        CollationShape first = present.getFirst();
        return switch (first) {
            case CollationShape.Leaf _ -> {
                ImmutableList.Builder<Collated> leaves = ImmutableList.builder();
                for (CollationShape operand : present) {
                    if (!(operand instanceof CollationShape.Leaf(Collated collated))) {
                        yield CollationShape.Opaque.OPAQUE;
                    }
                    leaves.add(collated);
                }
                yield new CollationShape.Leaf(rule.apply(leaves.build()));
            }
            case CollationShape.ArrayShape _ -> {
                ImmutableList.Builder<CollationShape> elements = ImmutableList.builder();
                for (CollationShape operand : present) {
                    if (!(operand instanceof CollationShape.ArrayShape(CollationShape element))) {
                        yield CollationShape.Opaque.OPAQUE;
                    }
                    elements.add(element);
                }
                yield new CollationShape.ArrayShape(structurally(elements.build(), rule));
            }
            case CollationShape.MapShape _ -> {
                ImmutableList.Builder<CollationShape> keys = ImmutableList.builder();
                ImmutableList.Builder<CollationShape> values = ImmutableList.builder();
                for (CollationShape operand : present) {
                    if (!(operand instanceof CollationShape.MapShape(CollationShape key, CollationShape value))) {
                        yield CollationShape.Opaque.OPAQUE;
                    }
                    keys.add(key);
                    values.add(value);
                }
                yield new CollationShape.MapShape(structurally(keys.build(), rule), structurally(values.build(), rule));
            }
            case CollationShape.RowShape(List<CollationShape> firstFields) -> {
                int arity = firstFields.size();
                List<ImmutableList.Builder<CollationShape>> fields = IntStream.range(0, arity)
                        .mapToObj(_ -> ImmutableList.<CollationShape>builder())
                        .collect(toImmutableList());
                for (CollationShape operand : present) {
                    if (!(operand instanceof CollationShape.RowShape(List<CollationShape> operandFields)) || operandFields.size() != arity) {
                        yield CollationShape.Opaque.OPAQUE;
                    }
                    for (int i = 0; i < arity; i++) {
                        fields.get(i).add(operandFields.get(i));
                    }
                }
                yield new CollationShape.RowShape(fields.stream()
                        .map(field -> structurally(field.build(), rule))
                        .collect(toImmutableList()));
            }
            case CollationShape.Opaque opaque -> opaque;
        };
    }

    /// Operands whose collations cannot be combined or compared.
    public static class CollationConflictException
            extends RuntimeException
    {
        private final Derivation derivation;
        private final Set<String> collations;

        public CollationConflictException(Derivation derivation, Set<String> collations)
        {
            super("%s collations %s conflict".formatted(derivation.name().toLowerCase(ENGLISH), collations));
            this.derivation = derivation;
            this.collations = ImmutableSortedSet.copyOf(collations);
        }

        public Derivation derivation()
        {
            return derivation;
        }

        public Set<String> collations()
        {
            return collations;
        }
    }
}
