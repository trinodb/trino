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
package io.trino.util;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;

import java.math.BigDecimal;
import java.util.Map;

import static io.trino.spi.type.RealType.REAL;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Float.floatToRawIntBits;

public final class StructuralTestUtil
{
    private StructuralTestUtil() {}

    public static Block arrayBlockOf(Type elementType, Object... values)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, values.length);
        for (Object value : values) {
            appendToBlockBuilder(elementType, value, blockBuilder);
        }
        return blockBuilder.build();
    }

    public static Block mapBlockOf(Type keyType, Type valueType, Map<?, ?> value)
    {
        MapType mapType = mapType(keyType, valueType);
        BlockBuilder mapArrayBuilder = mapType.createBlockBuilder(null, 1);
        BlockBuilder singleMapWriter = mapArrayBuilder.beginBlockEntry();
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            appendToBlockBuilder(keyType, entry.getKey(), singleMapWriter);
            appendToBlockBuilder(valueType, entry.getValue(), singleMapWriter);
        }
        mapArrayBuilder.closeEntry();
        return mapType.getObject(mapArrayBuilder, 0);
    }

    public static MapType mapType(Type keyType, Type valueType)
    {
        return (MapType) TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.typeParameter(keyType.getTypeSignature()),
                TypeSignatureParameter.typeParameter(valueType.getTypeSignature())));
    }

    public static void appendToBlockBuilder(Type type, Object element, BlockBuilder blockBuilder)
    {
        Class<?> javaType = type.getJavaType();
        if (element == null) {
            blockBuilder.appendNull();
        }
        else if (type instanceof ArrayType && element instanceof Iterable<?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            for (Object subElement : (Iterable<?>) element) {
                appendToBlockBuilder(type.getTypeParameters().get(0), subElement, subBlockBuilder);
            }
            blockBuilder.closeEntry();
        }
        else if (type instanceof RowType && element instanceof Iterable<?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            int field = 0;
            for (Object subElement : (Iterable<?>) element) {
                appendToBlockBuilder(type.getTypeParameters().get(field), subElement, subBlockBuilder);
                field++;
            }
            blockBuilder.closeEntry();
        }
        else if (type instanceof MapType && element instanceof Map<?, ?>) {
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) element).entrySet()) {
                appendToBlockBuilder(type.getTypeParameters().get(0), entry.getKey(), subBlockBuilder);
                appendToBlockBuilder(type.getTypeParameters().get(1), entry.getValue(), subBlockBuilder);
            }
            blockBuilder.closeEntry();
        }
        else if (javaType == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) element);
        }
        else if (javaType == long.class) {
            if (element instanceof SqlDecimal) {
                type.writeLong(blockBuilder, ((SqlDecimal) element).getUnscaledValue().longValue());
            }
            else if (REAL.equals(type)) {
                type.writeLong(blockBuilder, floatToRawIntBits(((Number) element).floatValue()));
            }
            else {
                type.writeLong(blockBuilder, ((Number) element).longValue());
            }
        }
        else if (javaType == double.class) {
            type.writeDouble(blockBuilder, ((Number) element).doubleValue());
        }
        else if (javaType == Slice.class) {
            if (element instanceof String) {
                type.writeSlice(blockBuilder, Slices.utf8Slice(element.toString()));
            }
            else if (element instanceof byte[]) {
                type.writeSlice(blockBuilder, Slices.wrappedBuffer((byte[]) element));
            }
            else {
                type.writeSlice(blockBuilder, (Slice) element);
            }
        }
        else {
            if (element instanceof SqlDecimal) {
                type.writeObject(blockBuilder, Int128.valueOf(((SqlDecimal) element).getUnscaledValue()));
            }
            else if (element instanceof BigDecimal) {
                type.writeObject(blockBuilder, Decimals.valueOf((BigDecimal) element));
            }
            else {
                type.writeObject(blockBuilder, element);
            }
        }
    }
}
