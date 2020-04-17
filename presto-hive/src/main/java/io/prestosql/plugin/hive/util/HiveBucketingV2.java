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
package io.prestosql.plugin.hive.util;

import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hive.common.util.Murmur3;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

final class HiveBucketingV2
{
    private HiveBucketingV2() {}

    static int getBucketHashCode(List<TypeInfo> types, Page page, int position)
    {
        checkArgument(types.size() == page.getChannelCount());
        int result = 0;
        for (int i = 0; i < page.getChannelCount(); i++) {
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

    private static int hash(TypeInfo type, Block block, int position)
    {
        // This function mirrors the behavior of function hashCodeMurmur in
        // HIVE-18910 (and following) 7dc47faddb serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java
        // https://github.com/apache/hive/blob/7dc47faddba9f079bbe2698aaa4d8712e7654f87/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java

        if (block.isNull(position)) {
            return 0;
        }

        switch (type.getCategory()) {
            case PRIMITIVE:
                PrimitiveTypeInfo typeInfo = (PrimitiveTypeInfo) type;
                PrimitiveCategory primitiveCategory = typeInfo.getPrimitiveCategory();
                Type prestoType = requireNonNull(HiveType.getPrimitiveType(typeInfo));
                switch (primitiveCategory) {
                    case BOOLEAN:
                        return prestoType.getBoolean(block, position) ? 1 : 0;
                    case BYTE:
                        return SignedBytes.checkedCast(prestoType.getLong(block, position));
                    case SHORT:
                        return Murmur3.hash32(bytes(Shorts.checkedCast(prestoType.getLong(block, position))));
                    case INT:
                        return Murmur3.hash32(bytes(toIntExact(prestoType.getLong(block, position))));
                    case LONG:
                        return Murmur3.hash32(bytes(prestoType.getLong(block, position)));
                    case FLOAT:
                        // convert to canonical NaN if necessary
                        // Sic! we're `floatToIntBits -> cast to float -> floatToRawIntBits` just as it is (implicitly) done in
                        // https://github.com/apache/hive/blob/7dc47faddba9f079bbe2698aaa4d8712e7654f87/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java#L830
                        return Murmur3.hash32(bytes(floatToRawIntBits(floatToIntBits(intBitsToFloat(toIntExact(prestoType.getLong(block, position)))))));
                    case DOUBLE:
                        // Sic! we're `doubleToLongBits -> cast to double -> doubleToRawLongBits` just as it is (implicitly) done in
                        // https://github.com/apache/hive/blob/7dc47faddba9f079bbe2698aaa4d8712e7654f87/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java#L836
                        return Murmur3.hash32(bytes(doubleToRawLongBits(doubleToLongBits(prestoType.getDouble(block, position)))));
                    case STRING:
                        return Murmur3.hash32(prestoType.getSlice(block, position).getBytes());
                    case VARCHAR:
                        return Murmur3.hash32(prestoType.getSlice(block, position).getBytes());
                    case DATE:
                        // day offset from 1970-01-01
                        return Murmur3.hash32(bytes(toIntExact(prestoType.getLong(block, position))));
                    // case TIMESTAMP: // TODO (https://github.com/prestosql/presto/issues/1706): support bucketing v2 for timestamp
                    default:
                        throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive primitive category: " + primitiveCategory);
                }
            case LIST:
                return hashOfList((ListTypeInfo) type, block.getObject(position, Block.class));
            case MAP:
                return hashOfMap((MapTypeInfo) type, block.getObject(position, Block.class));
            default:
                // TODO: support more types, e.g. ROW
                throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive category: " + type.getCategory());
        }
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
                        return Murmur3.hash32(bytes(Shorts.checkedCast((long) value)));
                    case INT:
                        return Murmur3.hash32(bytes(toIntExact((long) value)));
                    case LONG:
                        return Murmur3.hash32(bytes((long) value));
                    case FLOAT:
                        // convert to canonical NaN if necessary
                        // Sic! we're `floatToIntBits -> cast to float -> floatToRawIntBits` just as it is (implicitly) done in
                        // https://github.com/apache/hive/blob/7dc47faddba9f079bbe2698aaa4d8712e7654f87/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java#L830
                        return Murmur3.hash32(bytes(floatToRawIntBits(floatToIntBits(intBitsToFloat(toIntExact((long) value))))));
                    case DOUBLE:
                        // convert to canonical NaN if necessary
                        // Sic! we're `doubleToLongBits -> cast to double -> doubleToRawLongBits` just as it is (implicitly) done in
                        // https://github.com/apache/hive/blob/7dc47faddba9f079bbe2698aaa4d8712e7654f87/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java#L836
                        return Murmur3.hash32(bytes(doubleToRawLongBits(doubleToLongBits((double) value))));
                    case STRING:
                        return Murmur3.hash32(((Slice) value).getBytes());
                    case VARCHAR:
                        return Murmur3.hash32(((Slice) value).getBytes());
                    case DATE:
                        // day offset from 1970-01-01
                        return Murmur3.hash32(bytes(toIntExact((long) value)));
                    // case TIMESTAMP: // TODO (https://github.com/prestosql/presto/issues/1706): support bucketing v2 for timestamp
                    default:
                        throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive primitive category: " + primitiveCategory);
                }
            case LIST:
                return hashOfList((ListTypeInfo) type, (Block) value);
            case MAP:
                return hashOfMap((MapTypeInfo) type, (Block) value);
            default:
                // TODO: support more types, e.g. ROW
                throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive category: " + type.getCategory());
        }
    }

    private static int hashOfMap(MapTypeInfo type, Block singleMapBlock)
    {
        TypeInfo keyTypeInfo = type.getMapKeyTypeInfo();
        TypeInfo valueTypeInfo = type.getMapValueTypeInfo();
        int result = 0;
        for (int i = 0; i < singleMapBlock.getPositionCount(); i += 2) {
            // Sic! we're hashing map keys with v2 but map values with v1 just as in
            // https://github.com/apache/hive/blob/7dc47faddba9f079bbe2698aaa4d8712e7654f87/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java#L903-L904
            result += hash(keyTypeInfo, singleMapBlock, i) ^ HiveBucketingV1.hash(valueTypeInfo, singleMapBlock, i + 1);
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

    // big-endian
    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static byte[] bytes(short value)
    {
        return new byte[] {(byte) ((value >> 8) & 0xff), (byte) (value & 0xff)};
    }

    // big-endian
    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static byte[] bytes(int value)
    {
        return new byte[] {(byte) ((value >> 24) & 0xff), (byte) ((value >> 16) & 0xff), (byte) ((value >> 8) & 0xff), (byte) (value & 0xff)};
    }

    // big-endian
    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static byte[] bytes(long value)
    {
        return new byte[] {
                (byte) ((value >> 56) & 0xff), (byte) ((value >> 48) & 0xff), (byte) ((value >> 40) & 0xff), (byte) ((value >> 32) & 0xff),
                (byte) ((value >> 24) & 0xff), (byte) ((value >> 16) & 0xff), (byte) ((value >> 8) & 0xff), (byte) (value & 0xff)};
    }
}
