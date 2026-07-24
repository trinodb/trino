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
package io.trino.json;

import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.trino.json.JsonItemEncoding.TypeTag;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/// SQL scalar bridge for the path engine. Carries a Trino [Type] and its stack-form value
/// (boxed if the type is `long`/`double`/`boolean` for storage uniformity).
///
/// `TypedValue` is also the tree-form scalar leaf of [Json]: an INTEGER literal arriving
/// via `parseToTree` lands here directly with no encode/decode round-trip, and the path
/// engine reads it as the same instance it would have produced for `$.foo + 1`. When this
/// scalar needs to cross to a byte sink, [#encoding] materializes the corresponding
/// `TYPED_VALUE` item on demand. Scalars are tiny (≤ ~20 bytes) so each call re-encodes
/// instead of holding a per-instance cache — keeps `TypedValue` two-field and lets the
/// allocator keep these short-lived slices in the young generation.
public final class TypedValue
        implements Json
{
    private final Type type;
    private final Object value;

    public TypedValue(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        Class<?> wrappedJavaType = Primitives.wrap(type.getJavaType());
        checkArgument(
                wrappedJavaType.isAssignableFrom(value.getClass()),
                "%s value does not match the type %s",
                value.getClass(),
                type);
        this.type = type;
        this.value = value;
    }

    public Type type()
    {
        return type;
    }

    public Object value()
    {
        return value;
    }

    /// Boxes a `long` value into the canonical record form. Throws if `type`'s Java type
    /// isn't `long.class`.
    public TypedValue(Type type, long value)
    {
        this(checkLongType(type), Long.valueOf(value));
    }

    /// Boxes a `double` value into the canonical record form. Throws if `type`'s Java type
    /// isn't `double.class`.
    public TypedValue(Type type, double value)
    {
        this(checkDoubleType(type), Double.valueOf(value));
    }

    /// Boxes a `boolean` value into the canonical record form. Throws if `type`'s Java type
    /// isn't `boolean.class`.
    public TypedValue(Type type, boolean value)
    {
        this(checkBooleanType(type), Boolean.valueOf(value));
    }

    /// Adapter for callers that hold a value as `Object` regardless of the type's Java
    /// representation. Validates and routes through the appropriate constructor.
    public static TypedValue fromValueAsObject(Type type, Object valueAsObject)
    {
        requireNonNull(type, "type is null");
        requireNonNull(valueAsObject, "valueAsObject is null");
        if (long.class.equals(type.getJavaType())) {
            checkArgument(valueAsObject instanceof Long, "%s value does not match the type %s", valueAsObject.getClass(), type);
            return new TypedValue(type, (long) valueAsObject);
        }
        if (double.class.equals(type.getJavaType())) {
            checkArgument(valueAsObject instanceof Double, "%s value does not match the type %s", valueAsObject.getClass(), type);
            return new TypedValue(type, (double) valueAsObject);
        }
        if (boolean.class.equals(type.getJavaType())) {
            checkArgument(valueAsObject instanceof Boolean, "%s value does not match the type %s", valueAsObject.getClass(), type);
            return new TypedValue(type, (boolean) valueAsObject);
        }
        return new TypedValue(type, valueAsObject);
    }

    /// Returns the value when the type's Java representation is a reference type.
    /// Throws if the value is a boxed primitive — call [#getLongValue] / [#getDoubleValue]
    /// / [#getBooleanValue] instead.
    public Object getObjectValue()
    {
        Class<?> javaType = type.getJavaType();
        checkArgument(!javaType.isPrimitive(),
                "the type %s is represented as %s. call another method to retrieve the value",
                type,
                javaType);
        return value;
    }

    public long getLongValue()
    {
        checkArgument(long.class.equals(type.getJavaType()), "long value does not match the type %s", type);
        return (long) value;
    }

    public double getDoubleValue()
    {
        checkArgument(double.class.equals(type.getJavaType()), "double value does not match the type %s", type);
        return (double) value;
    }

    public boolean getBooleanValue()
    {
        checkArgument(boolean.class.equals(type.getJavaType()), "boolean value does not match the type %s", type);
        return (boolean) value;
    }

    private static Type checkLongType(Type type)
    {
        requireNonNull(type, "type is null");
        checkArgument(long.class.equals(type.getJavaType()), "long value does not match the type %s", type);
        return type;
    }

    private static Type checkDoubleType(Type type)
    {
        requireNonNull(type, "type is null");
        checkArgument(double.class.equals(type.getJavaType()), "double value does not match the type %s", type);
        return type;
    }

    private static Type checkBooleanType(Type type)
    {
        requireNonNull(type, "type is null");
        checkArgument(boolean.class.equals(type.getJavaType()), "boolean value does not match the type %s", type);
        return type;
    }

    // --- Json -------------------------------------------------------------------------

    @Override
    public Kind kind()
    {
        return Kind.SCALAR;
    }

    @Override
    public boolean isScalar()
    {
        return true;
    }

    @Override
    public TypeTag scalarType()
    {
        TypeTag tag = tagForOrNull(type);
        if (tag == null) {
            throw new IllegalArgumentException("Unsupported scalar type for SQL/JSON: " + type);
        }
        return tag;
    }

    @Override
    public TypedValue materializeScalar()
    {
        return this;
    }

    @Override
    public Slice encoding()
    {
        return JsonItems.encodeScalar(this).encoding();
    }

    @Override
    public Slice backingSlice()
    {
        return encoding();
    }

    @Override
    public int viewOffset()
    {
        return JsonItemEncoding.rootItemOffset(encoding());
    }

    @Override
    public int viewEnd()
    {
        return encoding().length();
    }

    @Override
    public boolean equals(Object other)
    {
        // One rule for the whole hierarchy: a scalar is equal to any Json holding the same
        // value, whether that operand arrived as a TypedValue or as encoded bytes. Comparing
        // (type, value) here instead would make CHAR 'a' and VARCHAR 'a' unequal while their
        // encodings compare equal — an equality that isn't transitive.
        if (this == other) {
            return true;
        }
        if (!(other instanceof Json that)) {
            return false;
        }
        return JsonItemSemantics.equal(this, that);
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(JsonItemSemantics.hash(this));
    }

    @Override
    public String toString()
    {
        return "TypedValue[" + type + ", " + value + "]";
    }

    /// Single source of truth for the type → tag mapping. Returns `null` for types
    /// that have no wire encoding, so [#hasWireEncoding] and [#scalarType] cannot
    /// drift if a new tag is added.
    private static TypeTag tagForOrNull(Type type)
    {
        if (type == BooleanType.BOOLEAN) {
            return TypeTag.BOOLEAN;
        }
        if (type == BigintType.BIGINT) {
            return TypeTag.BIGINT;
        }
        if (type == IntegerType.INTEGER) {
            return TypeTag.INTEGER;
        }
        if (type == SmallintType.SMALLINT) {
            return TypeTag.SMALLINT;
        }
        if (type == TinyintType.TINYINT) {
            return TypeTag.TINYINT;
        }
        if (type == DoubleType.DOUBLE) {
            return TypeTag.DOUBLE;
        }
        if (type == RealType.REAL) {
            return TypeTag.REAL;
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return TypeTag.VARCHAR;
        }
        if (type instanceof DecimalType) {
            return TypeTag.DECIMAL;
        }
        if (type == NumberType.NUMBER) {
            return TypeTag.NUMBER;
        }
        if (type == DateType.DATE) {
            return TypeTag.DATE;
        }
        if (type instanceof TimeType) {
            return TypeTag.TIME;
        }
        if (type instanceof TimeWithTimeZoneType) {
            return TypeTag.TIME_WITH_TIME_ZONE;
        }
        if (type instanceof TimestampType) {
            return TypeTag.TIMESTAMP;
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return TypeTag.TIMESTAMP_WITH_TIME_ZONE;
        }
        return null;
    }
}
