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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;

import java.math.BigDecimal;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static io.trino.util.StructuralTestUtil.appendToBlockBuilder;

public final class StructuralTestUtil
{
    private static final BlockTypeOperators TYPE_OPERATORS_CACHE = new BlockTypeOperators();

    private StructuralTestUtil() {}

    public static boolean arrayBlocksEqual(Type elementType, Block block1, Block block2)
    {
        if (block1.getPositionCount() != block2.getPositionCount()) {
            return false;
        }
        BlockPositionEqual elementEqualOperator = TYPE_OPERATORS_CACHE.getEqualOperator(elementType);
        for (int i = 0; i < block1.getPositionCount(); i++) {
            if (block1.isNull(i) != block2.isNull(i)) {
                return false;
            }
            if (!block1.isNull(i) && !elementEqualOperator.equal(block1, i, block2, i)) {
                return false;
            }
        }
        return true;
    }

    public static boolean mapBlocksEqual(Type keyType, Type valueType, Block block1, Block block2)
    {
        if (block1.getPositionCount() != block2.getPositionCount()) {
            return false;
        }

        BlockPositionEqual keyEqualOperator = TYPE_OPERATORS_CACHE.getEqualOperator(keyType);
        BlockPositionEqual valueEqualOperator = TYPE_OPERATORS_CACHE.getEqualOperator(valueType);
        for (int i = 0; i < block1.getPositionCount(); i += 2) {
            if (block1.isNull(i) != block2.isNull(i) || block1.isNull(i + 1) != block2.isNull(i + 1)) {
                return false;
            }
            if (!block1.isNull(i) && !keyEqualOperator.equal(block1, i, block2, i)) {
                return false;
            }
            if (!block1.isNull(i + 1) && !valueEqualOperator.equal(block1, i + 1, block2, i + 1)) {
                return false;
            }
        }
        return true;
    }

    public static Block arrayBlockOf(Type elementType, Object... values)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, 1024);
        for (Object value : values) {
            appendToBlockBuilder(elementType, value, blockBuilder);
        }
        return blockBuilder.build();
    }

    public static Block mapBlockOf(Type keyType, Type valueType, Object key, Object value)
    {
        MapType mapType = mapType(keyType, valueType);
        BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 10);
        BlockBuilder singleMapBlockWriter = blockBuilder.beginBlockEntry();
        appendToBlockBuilder(keyType, key, singleMapBlockWriter);
        appendToBlockBuilder(valueType, value, singleMapBlockWriter);
        blockBuilder.closeEntry();
        return mapType.getObject(blockBuilder, 0);
    }

    public static Block mapBlockOf(Type keyType, Type valueType, Object[] keys, Object[] values)
    {
        checkArgument(keys.length == values.length, "keys/values must have the same length");
        MapType mapType = mapType(keyType, valueType);
        BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 10);
        BlockBuilder singleMapBlockWriter = blockBuilder.beginBlockEntry();
        for (int i = 0; i < keys.length; i++) {
            Object key = keys[i];
            Object value = values[i];
            appendToBlockBuilder(keyType, key, singleMapBlockWriter);
            appendToBlockBuilder(valueType, value, singleMapBlockWriter);
        }
        blockBuilder.closeEntry();
        return mapType.getObject(blockBuilder, 0);
    }

    public static Block rowBlockOf(List<Type> parameterTypes, Object... values)
    {
        RowType rowType = RowType.anonymous(parameterTypes);
        BlockBuilder blockBuilder = rowType.createBlockBuilder(null, 1);
        BlockBuilder singleRowBlockWriter = blockBuilder.beginBlockEntry();
        for (int i = 0; i < values.length; i++) {
            appendToBlockBuilder(parameterTypes.get(i), values[i], singleRowBlockWriter);
        }
        blockBuilder.closeEntry();
        return rowType.getObject(blockBuilder, 0);
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

    public static Block decimalMapBlockOf(DecimalType type, BigDecimal decimal)
    {
        if (type.isShort()) {
            long longDecimal = decimal.unscaledValue().longValue();
            return mapBlockOf(type, type, longDecimal, longDecimal);
        }
        Int128 sliceDecimal = Int128.valueOf(decimal.unscaledValue());
        return mapBlockOf(type, type, sliceDecimal, sliceDecimal);
    }

    public static MapType mapType(Type keyType, Type valueType)
    {
        return (MapType) TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.typeParameter(keyType.getTypeSignature()),
                TypeSignatureParameter.typeParameter(valueType.getTypeSignature())));
    }

    public static ArrayType arrayType(Type elementType)
    {
        return (ArrayType) TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(
                    TypeSignatureParameter.typeParameter(elementType.getTypeSignature())));
    }
}
