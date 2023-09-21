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
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import jakarta.annotation.Nullable;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class TypeUtils
{
    public static final int NULL_HASH_CODE = 0;

    private TypeUtils()
    {
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
            Slice slice;
            if (value instanceof byte[]) {
                slice = Slices.wrappedBuffer((byte[]) value);
            }
            else if (value instanceof String) {
                slice = Slices.utf8Slice((String) value);
            }
            else {
                slice = (Slice) value;
            }
            type.writeSlice(blockBuilder, slice, 0, slice.length());
        }
        else {
            type.writeObject(blockBuilder, value);
        }
    }

    public static boolean isFloatingPointNaN(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");

        if (type == DOUBLE) {
            return Double.isNaN((double) value);
        }
        if (type == REAL) {
            return Float.isNaN(intBitsToFloat(toIntExact((long) value)));
        }
        return false;
    }

    static void checkElementNotNull(boolean isNull, String errorMsg)
    {
        if (isNull) {
            throw new TrinoException(NOT_SUPPORTED, errorMsg);
        }
    }
}
