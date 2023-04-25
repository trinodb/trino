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

import com.google.common.primitives.Ints;
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
import io.trino.spi.type.VarcharType;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

final class HiveBucketingV2
{
    private HiveBucketingV2() {}

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
                Type trinoType = requireNonNull(HiveTypeTranslator.fromPrimitiveType(typeInfo));
                if (trinoType.equals(BOOLEAN)) {
                    return BOOLEAN.getBoolean(block, position) ? 1 : 0;
                }
                if (trinoType.equals(TINYINT)) {
                    return TINYINT.getByte(block, position);
                }
                if (trinoType.equals(SMALLINT)) {
                    return murmur3(bytes(SMALLINT.getShort(block, position)));
                }
                if (trinoType.equals(INTEGER)) {
                    return murmur3(bytes(INTEGER.getInt(block, position)));
                }
                if (trinoType.equals(BIGINT)) {
                    return murmur3(bytes(BIGINT.getLong(block, position)));
                }
                if (trinoType.equals(REAL)) {
                    // convert to canonical NaN if necessary
                    // Sic! we're `floatToIntBits -> cast to float -> floatToRawIntBits` just as it is (implicitly) done in
                    // https://github.com/apache/hive/blob/7dc47faddba9f079bbe2698aaa4d8712e7654f87/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java#L830
                    return murmur3(bytes(floatToRawIntBits(floatToIntBits(REAL.getFloat(block, position)))));
                }
                if (trinoType.equals(DOUBLE)) {
                    // Sic! we're `doubleToLongBits -> cast to double -> doubleToRawLongBits` just as it is (implicitly) done in
                    // https://github.com/apache/hive/blob/7dc47faddba9f079bbe2698aaa4d8712e7654f87/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java#L836
                    return murmur3(bytes(doubleToRawLongBits(doubleToLongBits(DOUBLE.getDouble(block, position)))));
                }
                if (trinoType instanceof VarcharType varcharType) {
                    return murmur3(varcharType.getSlice(block, position).getBytes());
                }
                if (trinoType.equals(DATE)) {
                    // day offset from 1970-01-01
                    return murmur3(bytes(DATE.getInt(block, position)));
                }

                // We do not support bucketing on the following:
                // TIMESTAMP DECIMAL CHAR BINARY TIMESTAMPLOCALTZ INTERVAL_YEAR_MONTH INTERVAL_DAY_TIME VOID UNKNOWN
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
                        return murmur3(bytes(Shorts.checkedCast((long) value)));
                    case INT:
                        return murmur3(bytes(toIntExact((long) value)));
                    case LONG:
                        return murmur3(bytes((long) value));
                    case FLOAT:
                        // convert to canonical NaN if necessary
                        // Sic! we're `floatToIntBits -> cast to float -> floatToRawIntBits` just as it is (implicitly) done in
                        // https://github.com/apache/hive/blob/7dc47faddba9f079bbe2698aaa4d8712e7654f87/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java#L830
                        return murmur3(bytes(floatToRawIntBits(floatToIntBits(intBitsToFloat(toIntExact((long) value))))));
                    case DOUBLE:
                        // convert to canonical NaN if necessary
                        // Sic! we're `doubleToLongBits -> cast to double -> doubleToRawLongBits` just as it is (implicitly) done in
                        // https://github.com/apache/hive/blob/7dc47faddba9f079bbe2698aaa4d8712e7654f87/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java#L836
                        return murmur3(bytes(doubleToRawLongBits(doubleToLongBits((double) value))));
                    case STRING:
                        return murmur3(((Slice) value).getBytes());
                    case VARCHAR:
                        return murmur3(((Slice) value).getBytes());
                    case DATE:
                        // day offset from 1970-01-01
                        return murmur3(bytes(toIntExact((long) value)));
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

    // copied from org.apache.hive.common.util.Murmur3
    // WARNING: this implementation incorrectly handles negative values in the tail
    @SuppressWarnings("fallthrough")
    private static int murmur3(byte[] data)
    {
        int length = data.length;
        int hash = 104729;
        int blocks = length / 4;

        // body
        for (int block = 0; block < blocks; block++) {
            int i = block * 4;
            int k = Ints.fromBytes(data[i + 3], data[i + 2], data[i + 1], data[i]);

            // mix functions
            k *= 0xcc9e2d51;
            k = Integer.rotateLeft(k, 15);
            k *= 0x1b873593;
            hash ^= k;
            hash = Integer.rotateLeft(hash, 13) * 5 + 0xe6546b64;
        }

        // tail
        int idx = blocks * 4;
        int k1 = 0;
        switch (length - idx) {
            // these should be unsigned, but the Hive version is broken
            case 3:
                k1 ^= data[idx + 2] << 16;
            case 2:
                k1 ^= data[idx + 1] << 8;
            case 1:
                k1 ^= data[idx];

                // mix functions
                k1 *= 0xcc9e2d51;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= 0x1b873593;
                hash ^= k1;
        }

        // finalization
        hash ^= length;
        hash ^= (hash >>> 16);
        hash *= 0x85ebca6b;
        hash ^= (hash >>> 13);
        hash *= 0xc2b2ae35;
        hash ^= (hash >>> 16);

        return hash;
    }
}
