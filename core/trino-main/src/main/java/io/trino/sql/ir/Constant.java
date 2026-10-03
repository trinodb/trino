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
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import com.google.errorprone.annotations.DoNotCall;
import io.airlift.slice.Slice;
import io.trino.json.Json;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;

/// Array payloads have structural identity, preserving native scalar identities
/// and collection order recursively. This identity supports expression
/// deduplication; SQL comparisons continue to use the type's operators.
public record Constant(Type type, @JsonIgnore Object value)
        implements Expression
{
    @JsonCreator
    @DoNotCall // For JSON deserialization only
    public static Constant fromJson(
            @JsonProperty Type type,
            @JsonProperty Block valueAsBlock)
    {
        return new Constant(type, readNativeValue(type, valueAsBlock, 0));
    }

    public Constant
    {
        if (value != null && !Primitives.wrap(type.getJavaType()).isAssignableFrom(value.getClass())) {
            throw new IllegalArgumentException("Improper Java type (%s) for type '%s'".formatted(value.getClass().getName(), type));
        }
    }

    @JsonProperty
    public Block getValueAsBlock()
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        writeNativeValue(type, blockBuilder, value);
        return blockBuilder.build();
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitConstant(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        return ImmutableList.of();
    }

    // Folding an array must not give an otherwise identical expression a new identity.
    // Compare native scalar values, rather than SQL equality (which treats signed zero
    // as equal and can return unknown for nested nulls).
    @Override
    public boolean equals(Object object)
    {
        return object instanceof Constant other && type.equals(other.type) &&
                (type instanceof ArrayType ? valuesEqual(type, value, other.value) : Objects.equals(value, other.value));
    }

    @Override
    public int hashCode()
    {
        return 31 * type.hashCode() + (type instanceof ArrayType ? valueHash(type, value) : Objects.hashCode(value));
    }

    private static boolean valuesEqual(Type type, Object left, Object right)
    {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        if (type instanceof ArrayType arrayType) {
            Block leftArray = (Block) left;
            Block rightArray = (Block) right;
            if (leftArray.getPositionCount() != rightArray.getPositionCount()) {
                return false;
            }
            for (int position = 0; position < leftArray.getPositionCount(); position++) {
                if (!valuesEqual(arrayType.getElementType(), readNativeValue(arrayType.getElementType(), leftArray, position), readNativeValue(arrayType.getElementType(), rightArray, position))) {
                    return false;
                }
            }
            return true;
        }
        if (type instanceof RowType rowType) {
            SqlRow leftRow = (SqlRow) left;
            SqlRow rightRow = (SqlRow) right;
            for (int field = 0; field < rowType.getFields().size(); field++) {
                Type fieldType = rowType.getFields().get(field).getType();
                if (!valuesEqual(fieldType, readNativeValue(fieldType, leftRow.getRawFieldBlock(field), leftRow.getRawIndex()), readNativeValue(fieldType, rightRow.getRawFieldBlock(field), rightRow.getRawIndex()))) {
                    return false;
                }
            }
            return true;
        }
        if (type instanceof MapType mapType) {
            SqlMap leftMap = (SqlMap) left;
            SqlMap rightMap = (SqlMap) right;
            if (leftMap.getSize() != rightMap.getSize()) {
                return false;
            }
            // Preserve entry order, just as the native value does.
            for (int entry = 0; entry < leftMap.getSize(); entry++) {
                if (!valuesEqual(mapType.getKeyType(), readNativeValue(mapType.getKeyType(), leftMap.getRawKeyBlock(), leftMap.getRawOffset() + entry), readNativeValue(mapType.getKeyType(), rightMap.getRawKeyBlock(), rightMap.getRawOffset() + entry)) ||
                        !valuesEqual(mapType.getValueType(), readNativeValue(mapType.getValueType(), leftMap.getRawValueBlock(), leftMap.getRawOffset() + entry), readNativeValue(mapType.getValueType(), rightMap.getRawValueBlock(), rightMap.getRawOffset() + entry))) {
                    return false;
                }
            }
            return true;
        }
        return left.equals(right);
    }

    private static int valueHash(Type type, Object value)
    {
        if (value == null) {
            return 0;
        }
        int hash = 1;
        if (type instanceof ArrayType arrayType) {
            Block array = (Block) value;
            for (int position = 0; position < array.getPositionCount(); position++) {
                hash = 31 * hash + valueHash(arrayType.getElementType(), readNativeValue(arrayType.getElementType(), array, position));
            }
            return hash;
        }
        if (type instanceof RowType rowType) {
            SqlRow row = (SqlRow) value;
            for (int field = 0; field < rowType.getFields().size(); field++) {
                Type fieldType = rowType.getFields().get(field).getType();
                hash = 31 * hash + valueHash(fieldType, readNativeValue(fieldType, row.getRawFieldBlock(field), row.getRawIndex()));
            }
            return hash;
        }
        if (type instanceof MapType mapType) {
            SqlMap map = (SqlMap) value;
            for (int entry = 0; entry < map.getSize(); entry++) {
                hash = 31 * hash + valueHash(mapType.getKeyType(), readNativeValue(mapType.getKeyType(), map.getRawKeyBlock(), map.getRawOffset() + entry));
                hash = 31 * hash + valueHash(mapType.getValueType(), readNativeValue(mapType.getValueType(), map.getRawValueBlock(), map.getRawOffset() + entry));
            }
            return hash;
        }
        return value.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof Constant that) || !type.equals(that.type)) {
            return false;
        }
        // Expression substitution must preserve representation, unlike SQL grouping equality.
        if (value instanceof Json left && that.value instanceof Json right) {
            if (left.isRawText() != right.isRawText()) {
                return false;
            }
            Slice leftBytes = jsonBytes(left);
            Slice rightBytes = jsonBytes(right);
            return leftBytes.equals(jsonOffset(left), jsonLength(left), rightBytes, jsonOffset(right), jsonLength(right));
        }
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        int valueHash = value instanceof Json json
                ? jsonBytes(json).hashCode(jsonOffset(json), jsonLength(json))
                : Objects.hashCode(value);
        return 31 * type.hashCode() + valueHash;
    }

    private static Slice jsonBytes(Json value)
    {
        return value.isRawText() ? value.rawText() : value.backingSlice();
    }

    private static int jsonOffset(Json value)
    {
        return value.isRawText() ? 0 : value.viewOffset();
    }

    private static int jsonLength(Json value)
    {
        return value.isRawText() ? value.rawText().length() : value.viewEnd() - value.viewOffset();
    }

    @Override
    public String toString()
    {
        return "[%s]::%s".formatted(
                value == null ? "<null>" : type.getObjectValue(getValueAsBlock(), 0),
                type);
    }
}
