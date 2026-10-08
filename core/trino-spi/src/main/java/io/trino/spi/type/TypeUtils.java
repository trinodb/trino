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
package io.trino.spi.type;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.block.ValueBlock;
import jakarta.annotation.Nullable;

import java.util.List;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class TypeUtils
{
    public static final int NULL_HASH_CODE = 0;

    private TypeUtils() {}

    public static Object blockToNativeValue(Type type, Block block)
    {
        if (block.getPositionCount() != 1) {
            throw new IllegalArgumentException("Block should have exactly one position, but has: " + block.getPositionCount());
        }
        return readNativeValue(type, block, 0);
    }

    /**
     * Get the native value as an object in the value at {@code position} of {@code block}.
     */
    public static Object readNativeValue(Type type, Block block, int position)
    {
        Class<?> javaType = type.getJavaType();

        if (block.isNull(position)) {
            return null;
        }
        if (javaType == long.class) {
            return type.getLong(block, position);
        }
        if (javaType == double.class) {
            return type.getDouble(block, position);
        }
        if (javaType == boolean.class) {
            return type.getBoolean(block, position);
        }
        if (javaType == Slice.class) {
            return type.getSlice(block, position);
        }
        return type.getObject(block, position);
    }

    public static ValueBlock writeNativeValue(Type type, @Nullable Object value)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        writeNativeValue(type, blockBuilder, value);
        return blockBuilder.buildValueBlock();
    }

    /**
     * Write a native value object to the current entry of {@code blockBuilder}.
     */
    public static void writeNativeValue(Type type, BlockBuilder blockBuilder, @Nullable Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
        }
        else if (type.getJavaType() == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (type.getJavaType() == double.class) {
            type.writeDouble(blockBuilder, ((double) value));
        }
        else if (type.getJavaType() == long.class) {
            type.writeLong(blockBuilder, ((long) value));
        }
        else if (type.getJavaType() == Slice.class) {
            Slice slice = (Slice) value;
            type.writeSlice(blockBuilder, slice, 0, slice.length());
        }
        else {
            type.writeObject(blockBuilder, value);
        }
    }

    public static boolean typeHasNaN(Type type)
    {
        return type == REAL || type == DOUBLE || type == NUMBER || type.getTypeParameters().stream().anyMatch(TypeUtils::typeHasNaN);
    }

    public static boolean isFloatingPointNaN(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");

        if (type == REAL) {
            return Float.isNaN(intBitsToFloat(toIntExact((long) value)));
        }
        if (type == DOUBLE) {
            return Double.isNaN((double) value);
        }
        if (type == NUMBER) {
            return ((TrinoNumber) value).isNaN();
        }
        return false;
    }

    /**
     * Similar to isFloatingPointNaN but considers if a value has NaN nested anywhere inside it.
     */
    public static boolean valueHasNaN(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");

        if (isFloatingPointNaN(type, value)) {
            return true;
        }
        if (!typeHasNaN(type)) {
            return false;
        }
        return switch (type) {
            case ArrayType arrayType -> {
                Type elementType = arrayType.getElementType();
                if (!typeHasNaN(elementType)) {
                    yield false;
                }
                Block arrayBlock = (Block) value;
                for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                    if (!arrayBlock.isNull(i) && valueHasNaN(elementType, readNativeValue(elementType, arrayBlock, i))) {
                        yield true;
                    }
                }
                yield false;
            }
            case MapType mapType -> {
                Type keyType = mapType.getKeyType();
                Type valueType = mapType.getValueType();
                boolean keyTypeHasNaN = typeHasNaN(keyType);
                boolean valueTypeHasNaN = typeHasNaN(valueType);
                SqlMap sqlMap = (SqlMap) value;
                Block rawKeyBlock = sqlMap.getRawKeyBlock();
                Block rawValueBlock = sqlMap.getRawValueBlock();
                int rawOffset = sqlMap.getRawOffset();
                for (int i = 0; i < sqlMap.getSize(); i++) {
                    int position = rawOffset + i;
                    if (keyTypeHasNaN && !rawKeyBlock.isNull(position) && valueHasNaN(keyType, readNativeValue(keyType, rawKeyBlock, position))) {
                        yield true;
                    }
                    if (valueTypeHasNaN && !rawValueBlock.isNull(position) && valueHasNaN(valueType, readNativeValue(valueType, rawValueBlock, position))) {
                        yield true;
                    }
                }
                yield false;
            }
            case RowType rowType -> {
                SqlRow sqlRow = (SqlRow) value;
                int rawIndex = sqlRow.getRawIndex();
                List<RowType.Field> fields = rowType.getFields();
                for (int i = 0; i < fields.size(); i++) {
                    Type fieldType = fields.get(i).getType();
                    if (!typeHasNaN(fieldType)) {
                        continue;
                    }
                    Block fieldBlock = sqlRow.getRawFieldBlock(i);
                    if (!fieldBlock.isNull(rawIndex) && valueHasNaN(fieldType, readNativeValue(fieldType, fieldBlock, rawIndex))) {
                        yield true;
                    }
                }
                yield false;
            }
            default -> false;
        };
    }

    static void checkElementNotNull(boolean isNull, String errorMsg)
    {
        if (isNull) {
            throw new TrinoException(NOT_SUPPORTED, errorMsg);
        }
    }
}
