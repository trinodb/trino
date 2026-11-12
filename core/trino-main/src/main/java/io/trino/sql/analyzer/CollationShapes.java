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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import io.trino.sql.analyzer.CollationSemantics.Collated;
import io.trino.sql.analyzer.CollationSemantics.CollationShape;
import io.trino.sql.analyzer.CollationSemantics.CollationShape.ArrayShape;
import io.trino.sql.analyzer.CollationSemantics.CollationShape.Leaf;
import io.trino.sql.analyzer.CollationSemantics.CollationShape.MapShape;
import io.trino.sql.analyzer.CollationSemantics.CollationShape.RowShape;
import io.trino.sql.analyzer.CollationSemantics.Derivation;

import java.util.List;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.type.collation.Collations.hasNonDefaultCollation;

/// How the collation shapes of values relate to their types.
final class CollationShapes
{
    private CollationShapes() {}

    /// The shape of a value whose strings have the implicit collations of its type.
    static CollationShape implicit(Type type)
    {
        return switch (type) {
            case VarcharType varchar -> new Leaf(Collated.implicit(varchar.getCollation()));
            case ArrayType array -> wrap(implicit(array.getElementType()), ArrayShape::new);
            case MapType map -> {
                CollationShape key = implicit(map.getKeyType());
                CollationShape value = implicit(map.getValueType());
                yield key == CollationShape.Opaque.OPAQUE && value == CollationShape.Opaque.OPAQUE ? CollationShape.Opaque.OPAQUE : new MapShape(key, value);
            }
            case RowType row -> {
                List<CollationShape> fields = row.getFieldTypes().stream()
                        .map(CollationShapes::implicit)
                        .collect(toImmutableList());
                yield fields.stream().allMatch(field -> field == CollationShape.Opaque.OPAQUE) ? CollationShape.Opaque.OPAQUE : new RowShape(fields);
            }
            default -> CollationShape.Opaque.OPAQUE;
        };
    }

    private static CollationShape wrap(CollationShape element, Function<CollationShape, CollationShape> wrapper)
    {
        return element == CollationShape.Opaque.OPAQUE ? CollationShape.Opaque.OPAQUE : wrapper.apply(element);
    }

    /// The type with each contained string's collation set to the collation at its position in
    /// the shape. A string with derivation none takes the default collation. Returns the type
    /// itself when no collation changes.
    static Type applyTo(Type type, CollationShape shape, TypeManager typeManager)
    {
        return switch (type) {
            case VarcharType varchar when shape instanceof Leaf(Collated collated) -> {
                VarcharType relabeled = varchar.withCollation(collated.collation());
                yield relabeled.equals(varchar) ? varchar : relabeled;
            }
            case ArrayType array when shape instanceof ArrayShape(CollationShape element) -> {
                Type elementType = applyTo(array.getElementType(), element, typeManager);
                yield elementType == array.getElementType() ? array : new ArrayType(elementType);
            }
            case MapType map when shape instanceof MapShape(CollationShape key, CollationShape value) -> {
                Type keyType = applyTo(map.getKeyType(), key, typeManager);
                Type valueType = applyTo(map.getValueType(), value, typeManager);
                yield keyType == map.getKeyType() && valueType == map.getValueType() ? map : new MapType(keyType, valueType, typeManager.getTypeOperators());
            }
            case RowType row when shape instanceof RowShape(List<CollationShape> fields) && fields.size() == row.getFields().size() -> {
                boolean changed = false;
                ImmutableList.Builder<RowType.Field> newFields = ImmutableList.builder();
                for (int i = 0; i < fields.size(); i++) {
                    RowType.Field field = row.getFields().get(i);
                    Type fieldType = applyTo(field.getType(), fields.get(i), typeManager);
                    changed |= fieldType != field.getType();
                    newFields.add(new RowType.Field(field.getName(), fieldType));
                }
                yield changed ? RowType.from(newFields.build()) : row;
            }
            default -> type;
        };
    }

    /// A recorded shape seen through a value's effective type: each string keeps its recorded
    /// derivation and has the collation of the type, which a relabeling coercion may have changed.
    static CollationShape reconcile(CollationShape recorded, Type type)
    {
        return switch (recorded) {
            case Leaf(Collated collated) when type instanceof VarcharType varchar -> collated.derivation() == Derivation.NONE
                    ? recorded
                    : new Leaf(new Collated(varchar.getCollation(), collated.derivation(), collated.conflicting()));
            case ArrayShape(CollationShape element) when type instanceof ArrayType array -> new ArrayShape(reconcile(element, array.getElementType()));
            case MapShape(CollationShape key, CollationShape value) when type instanceof MapType map -> new MapShape(reconcile(key, map.getKeyType()), reconcile(value, map.getValueType()));
            case RowShape(List<CollationShape> fields) when type instanceof RowType row && row.getFields().size() == fields.size() -> {
                ImmutableList.Builder<CollationShape> reconciled = ImmutableList.builder();
                for (int i = 0; i < fields.size(); i++) {
                    reconciled.add(reconcile(fields.get(i), row.getFieldTypes().get(i)));
                }
                yield new RowShape(reconciled.build());
            }
            default -> implicit(type);
        };
    }

    /// Whether comparing values of the shape needs no collation work: every contained string is
    /// implicit or coercible in the default collation.
    static boolean isTrivial(CollationShape shape, Type type)
    {
        return !hasNonDefaultCollation(type) && shape.firstNone().isEmpty() && !hasExplicit(shape);
    }

    private static boolean hasExplicit(CollationShape shape)
    {
        return switch (shape) {
            case Leaf(Collated collated) -> collated.derivation() == Derivation.EXPLICIT;
            case ArrayShape(CollationShape element) -> hasExplicit(element);
            case MapShape(CollationShape key, CollationShape value) -> hasExplicit(key) || hasExplicit(value);
            case RowShape(List<CollationShape> fields) -> fields.stream().anyMatch(CollationShapes::hasExplicit);
            case CollationShape.Opaque _ -> false;
        };
    }
}
