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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeParameter;

import java.math.BigDecimal;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static io.trino.util.StructuralTestUtil.appendToBlockBuilder;

public final class StructuralTestUtil
{
    private StructuralTestUtil() {}

    public static Block arrayBlockOf(Type elementType, Object... values)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, 1024);
        for (Object value : values) {
            appendToBlockBuilder(elementType, value, blockBuilder);
        }
        return blockBuilder.build();
    }

    public static SqlMap sqlMapOf(Type keyType, Type valueType, Object key, Object value)
    {
        return buildMapValue(
                mapType(keyType, valueType),
                1,
                (keyBuilder, valueBuilder) -> {
                    appendToBlockBuilder(keyType, key, keyBuilder);
                    appendToBlockBuilder(valueType, value, valueBuilder);
                });
    }

    public static SqlMap sqlMapOf(Type keyType, Type valueType, Object[] keys, Object[] values)
    {
        checkArgument(keys.length == values.length, "keys/values must have the same length");
        return buildMapValue(
                mapType(keyType, valueType),
                keys.length,
                (keyBuilder, valueBuilder) -> {
                    for (int i = 0; i < keys.length; i++) {
                        Object key = keys[i];
                        Object value = values[i];
                        appendToBlockBuilder(keyType, key, keyBuilder);
                        appendToBlockBuilder(valueType, value, valueBuilder);
                    }
                });
    }

    public static SqlRow rowBlockOf(List<Type> parameterTypes, Object... values)
    {
        return buildRowValue(RowType.anonymous(parameterTypes), fields -> {
            for (int i = 0; i < values.length; i++) {
                appendToBlockBuilder(parameterTypes.get(i), values[i], fields.get(i));
            }
        });
    }

    public static Block decimalArrayBlockOf(DecimalType type, BigDecimal decimal)
    {
        if (type.isShort()) {
            long longDecimal = decimal.unscaledValue().longValue();
            return arrayBlockOf(type, longDecimal);
        }
        Int128 sliceDecimal = Int128.valueOf(decimal.unscaledValue());
        return arrayBlockOf(type, sliceDecimal);
    }

    public static SqlMap decimalSqlMapOf(DecimalType type, BigDecimal decimal)
    {
        if (type.isShort()) {
            long longDecimal = decimal.unscaledValue().longValue();
            return sqlMapOf(type, type, longDecimal, longDecimal);
        }
        Int128 sliceDecimal = Int128.valueOf(decimal.unscaledValue());
        return sqlMapOf(type, type, sliceDecimal, sliceDecimal);
    }

    public static MapType mapType(Type keyType, Type valueType)
    {
        return (MapType) TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeParameter.typeParameter(keyType.getTypeDescriptor()),
                TypeParameter.typeParameter(valueType.getTypeDescriptor())));
    }

    public static ArrayType arrayType(Type elementType)
    {
        return (ArrayType) TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(
                TypeParameter.typeParameter(elementType.getTypeDescriptor())));
    }
}
