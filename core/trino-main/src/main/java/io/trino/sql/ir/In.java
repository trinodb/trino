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
package io.trino.sql.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Suppliers.memoize;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.sql.ir.IrUtils.validateType;

/// SQL equality against the elements of an array expression. The array's element
/// type must match the value's type. A null array produces null; an empty array
/// produces false, including for a null value. Otherwise a match produces true,
/// and no match produces null if any comparison is indeterminate, or false.
///
/// Array constructors and non-null array constants can use specialized IN
/// evaluation without materializing a new array. Other array expressions are
/// evaluated once at runtime. This is an internal IR representation; it does not
/// add SQL syntax or change the semantics of SQL IN lists.
///
/// Known candidates are extracted lazily and retained by this node for reuse by
/// planning and compilation. This derived view is not part of expression identity
/// or serialization and has the same lifetime as the node.
@JsonSerialize
public final class In
        implements Expression
{
    private final Expression value;
    private final Expression valueList;
    private final Supplier<Optional<List<Expression>>> valueListElements;

    @JsonCreator
    public In(@JsonProperty("value") Expression value, @JsonProperty("valueList") Expression valueList)
    {
        validateType(new ArrayType(value.type()), valueList);
        this.value = value;
        this.valueList = valueList;
        valueListElements = memoize(() -> extractArrayElements(valueList));
    }

    @JsonProperty
    public Expression value()
    {
        return value;
    }

    @JsonProperty
    public Expression valueList()
    {
        return valueList;
    }

    /// Returns a reusable view of known candidates. Empty means the array must
    /// be evaluated at runtime, including when it is a null constant.
    @JsonIgnore
    public Optional<List<Expression>> valueListElements()
    {
        return valueListElements.get();
    }

    private static Optional<List<Expression>> extractArrayElements(Expression expression)
    {
        if (expression instanceof Array array) {
            return Optional.of(array.elements());
        }
        if (expression instanceof Constant(ArrayType type, Block values)) {
            ImmutableList.Builder<Expression> elements = ImmutableList.builderWithExpectedSize(values.getPositionCount());
            for (int position = 0; position < values.getPositionCount(); position++) {
                elements.add(new Constant(type.getElementType(), readNativeValue(type.getElementType(), values, position)));
            }
            return Optional.of(elements.build());
        }
        return Optional.empty();
    }

    @Override
    public Type type()
    {
        return BOOLEAN;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitIn(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        return ImmutableList.of(value, valueList);
    }

    @Override
    public boolean equals(Object object)
    {
        return object instanceof In other && value.equals(other.value) && valueList.equals(other.valueList);
    }

    @Override
    public int hashCode()
    {
        return 31 * value.hashCode() + valueList.hashCode();
    }

    @Override
    public String toString()
    {
        return "$in(%s, %s)".formatted(value, valueList);
    }
}
