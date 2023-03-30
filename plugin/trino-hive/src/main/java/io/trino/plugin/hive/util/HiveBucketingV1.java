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
package io.trino.plugin.hive.util;

import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.plugin.hive.type.ListTypeInfo;
import io.trino.plugin.hive.type.MapTypeInfo;
import io.trino.plugin.hive.type.PrimitiveCategory;
import io.trino.plugin.hive.type.PrimitiveTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

final class HiveBucketingV1
{
    private HiveBucketingV1() {}

    static int getBucketHashCode(List<TypeInfo> types, Page page, int position)
    {
        checkArgument(types.size() <= page.getChannelCount());

        int result = 0;
        for (int i = 0; i < types.size(); i++) {
            int fieldHash = hash(types.get(i), page.getBlock(i), position);
            result = result * 31 + fieldHash;
        }
        return result;
    }

    static int getBucketHashCode(List<TypeInfo> types, Object[] values)
    {
        checkArgument(types.size() == values.length);
        int result = 0;
        for (int i = 0; i < values.length; i++) {
            int fieldHash = hash(types.get(i), values[i]);
            result = result * 31 + fieldHash;
        }
        return result;
    }

    static int hash(TypeInfo type, Block block, int position)
    {
        // This function mirrors the behavior of function hashCode in
        // HIVE-12025 ba83fd7bff serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java
        // https://github.com/apache/hive/blob/ba83fd7bff/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java

        if (block.isNull(position)) {
            return 0;
        }

        switch (type.getCategory()) {
            case PRIMITIVE:
                PrimitiveTypeInfo typeInfo = (PrimitiveTypeInfo) type;
                PrimitiveCategory primitiveCategory = typeInfo.getPrimitiveCategory();
                Type trinoType = requireNonNull(HiveTypeTranslator.fromPrimitiveType(typeInfo));
                switch (primitiveCategory) {
                    case BOOLEAN:
                        return trinoType.getBoolean(block, position) ? 1 : 0;
                    case BYTE:
                        return SignedBytes.checkedCast(trinoType.getLong(block, position));
                    case SHORT:
                        return Shorts.checkedCast(trinoType.getLong(block, position));
                    case INT:
                        return toIntExact(trinoType.getLong(block, position));
                    case LONG:
                        long bigintValue = trinoType.getLong(block, position);
                        return (int) ((bigintValue >>> 32) ^ bigintValue);
                    case FLOAT:
                        // convert to canonical NaN if necessary
                        return floatToIntBits(intBitsToFloat(toIntExact(trinoType.getLong(block, position))));
                    case DOUBLE:
                        long doubleValue = doubleToLongBits(trinoType.getDouble(block, position));
                        return (int) ((doubleValue >>> 32) ^ doubleValue);
                    case STRING:
                        return hashBytes(0, trinoType.getSlice(block, position));
                    case VARCHAR:
                        return hashBytes(1, trinoType.getSlice(block, position));
                    case DATE:
                        // day offset from 1970-01-01
                        return toIntExact(trinoType.getLong(block, position));
                    case TIMESTAMP:
                        // We do not support bucketing on timestamp
                        break;
                    case DECIMAL:
                    case CHAR:
                    case BINARY:
                    case TIMESTAMPLOCALTZ:
                    case INTERVAL_YEAR_MONTH:
                    case INTERVAL_DAY_TIME:
                        // TODO
                        break;
                    case VOID:
                    case UNKNOWN:
                        break;
                }
                throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive primitive category: " + primitiveCategory);
            case LIST:
                return hashOfList((ListTypeInfo) type, block.getObject(position, Block.class));
            case MAP:
                return hashOfMap((MapTypeInfo) type, block.getObject(position, Block.class));
            case STRUCT:
            case UNION:
                // TODO: support more types, e.g. ROW
        }
        throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive category: " + type.getCategory());
    }

    private static int hash(TypeInfo type, Object value)
    {
        if (value == null) {
            return 0;
        }

        switch (type.getCategory()) {
            case PRIMITIVE:
                PrimitiveTypeInfo typeInfo = (PrimitiveTypeInfo) type;
                PrimitiveCategory primitiveCategory = typeInfo.getPrimitiveCategory();
                switch (primitiveCategory) {
                    case BOOLEAN:
                        return (boolean) value ? 1 : 0;
                    case BYTE:
                        return SignedBytes.checkedCast((long) value);
                    case SHORT:
                        return Shorts.checkedCast((long) value);
                    case INT:
                        return toIntExact((long) value);
                    case LONG:
                        long bigintValue = (long) value;
                        return (int) ((bigintValue >>> 32) ^ bigintValue);
                    case FLOAT:
                        // convert to canonical NaN if necessary
                        return floatToIntBits(intBitsToFloat(toIntExact((long) value)));
                    case DOUBLE:
                        long doubleValue = doubleToLongBits((double) value);
                        return (int) ((doubleValue >>> 32) ^ doubleValue);
                    case STRING:
                        return hashBytes(0, (Slice) value);
                    case VARCHAR:
                        return hashBytes(1, (Slice) value);
                    case DATE:
                        // day offset from 1970-01-01
                        return toIntExact((long) value);
                    case TIMESTAMP:
                        // We do not support bucketing on timestamp
                        break;
                    case DECIMAL:
                    case CHAR:
                    case BINARY:
                    case TIMESTAMPLOCALTZ:
                    case INTERVAL_YEAR_MONTH:
                    case INTERVAL_DAY_TIME:
                        // TODO
                        break;
                    case VOID:
                    case UNKNOWN:
                        break;
                }
                throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive primitive category: " + primitiveCategory);
            case LIST:
                return hashOfList((ListTypeInfo) type, (Block) value);
            case MAP:
                return hashOfMap((MapTypeInfo) type, (Block) value);
            case STRUCT:
            case UNION:
                // TODO: support more types, e.g. ROW
        }
        throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive category: " + type.getCategory());
    }

    private static int hashOfMap(MapTypeInfo type, Block singleMapBlock)
    {
        TypeInfo keyTypeInfo = type.getMapKeyTypeInfo();
        TypeInfo valueTypeInfo = type.getMapValueTypeInfo();
        int result = 0;
        for (int i = 0; i < singleMapBlock.getPositionCount(); i += 2) {
            result += hash(keyTypeInfo, singleMapBlock, i) ^ hash(valueTypeInfo, singleMapBlock, i + 1);
        }
        return result;
    }

    private static int hashOfList(ListTypeInfo type, Block singleListBlock)
    {
        TypeInfo elementTypeInfo = type.getListElementTypeInfo();
        int result = 0;
        for (int i = 0; i < singleListBlock.getPositionCount(); i++) {
            result = result * 31 + hash(elementTypeInfo, singleListBlock, i);
        }
        return result;
    }

    private static int hashBytes(int initialValue, Slice bytes)
    {
        int result = initialValue;
        for (int i = 0; i < bytes.length(); i++) {
            result = result * 31 + bytes.getByte(i);
        }
        return result;
    }
}
