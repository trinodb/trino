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
import io.trino.metastore.type.ListTypeInfo;
import io.trino.metastore.type.MapTypeInfo;
import io.trino.metastore.type.PrimitiveCategory;
import io.trino.metastore.type.PrimitiveTypeInfo;
import io.trino.metastore.type.TypeInfo;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.SqlMap;
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

        return switch (type.getCategory()) {
            case PRIMITIVE -> {
                PrimitiveTypeInfo typeInfo = (PrimitiveTypeInfo) type;
                PrimitiveCategory primitiveCategory = typeInfo.getPrimitiveCategory();
                Type trinoType = requireNonNull(HiveTypeTranslator.fromPrimitiveType(typeInfo));
                if (trinoType.equals(BOOLEAN)) {
                    yield BOOLEAN.getBoolean(block, position) ? 1 : 0;
                }
                if (trinoType.equals(TINYINT)) {
                    yield TINYINT.getByte(block, position);
                }
                if (trinoType.equals(SMALLINT)) {
                    yield murmur3(bytes(SMALLINT.getShort(block, position)));
                }
                if (trinoType.equals(INTEGER)) {
                    yield murmur3(bytes(INTEGER.getInt(block, position)));
                }
                if (trinoType.equals(BIGINT)) {
                    yield murmur3(bytes(BIGINT.getLong(block, position)));
                }
                if (trinoType.equals(REAL)) {
                    // convert to canonical NaN if necessary
                    // Sic! we're `floatToIntBits -> cast to float -> floatToRawIntBits` just as it is (implicitly) done in
                    // https://github.com/apache/hive/blob/7dc47faddba9f079bbe2698aaa4d8712e7654f87/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java#L830
                    yield murmur3(bytes(floatToRawIntBits(floatToIntBits(REAL.getFloat(block, position)))));
                }
                if (trinoType.equals(DOUBLE)) {
                    // Sic! we're `doubleToLongBits -> cast to double -> doubleToRawLongBits` just as it is (implicitly) done in
                    // https://github.com/apache/hive/blob/7dc47faddba9f079bbe2698aaa4d8712e7654f87/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java#L836
                    yield murmur3(bytes(doubleToRawLongBits(doubleToLongBits(DOUBLE.getDouble(block, position)))));
                }
                if (trinoType instanceof VarcharType varcharType) {
                    yield murmur3(varcharType.getSlice(block, position).getBytes());
                }
                if (trinoType.equals(DATE)) {
                    // day offset from 1970-01-01
                    yield murmur3(bytes(DATE.getInt(block, position)));
                }

                // We do not support bucketing on the following:
                // TIMESTAMP DECIMAL CHAR BINARY TIMESTAMPLOCALTZ INTERVAL_YEAR_MONTH INTERVAL_DAY_TIME VOID UNKNOWN
                throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive primitive category: " + primitiveCategory);
            }
            case LIST -> {
                Block array = ((ArrayBlock) block.getUnderlyingValueBlock()).getArray(block.getUnderlyingValuePosition(position));
                yield hashOfList((ListTypeInfo) type, array);
            }
            case MAP -> {
                SqlMap map = ((MapBlock) block.getUnderlyingValueBlock()).getMap(block.getUnderlyingValuePosition(position));
                yield hashOfMap((MapTypeInfo) type, map);
            }
            case STRUCT, UNION -> throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive category: " + type.getCategory());
        };
    }

    private static int hash(TypeInfo type, Object value)
    {
        if (value == null) {
            return 0;
        }

        return switch (type.getCategory()) {
            case PRIMITIVE -> {
                PrimitiveTypeInfo typeInfo = (PrimitiveTypeInfo) type;
                PrimitiveCategory primitiveCategory = typeInfo.getPrimitiveCategory();
                yield switch (primitiveCategory) {
                    case BOOLEAN -> (boolean) value ? 1 : 0;
                    case BYTE -> SignedBytes.checkedCast((long) value);
                    case SHORT -> murmur3(bytes(Shorts.checkedCast((long) value)));
                    case INT -> murmur3(bytes(toIntExact((long) value)));
                    case LONG -> murmur3(bytes((long) value));
                    case FLOAT -> {
                        // convert to canonical NaN if necessary
                        // Sic! we're `floatToIntBits -> cast to float -> floatToRawIntBits` just as it is (implicitly) done in
                        // https://github.com/apache/hive/blob/7dc47faddba9f079bbe2698aaa4d8712e7654f87/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java#L830
                        yield murmur3(bytes(floatToRawIntBits(floatToIntBits(intBitsToFloat(toIntExact((long) value))))));
                    }
                    case DOUBLE -> {
                        // convert to canonical NaN if necessary
                        // Sic! we're `doubleToLongBits -> cast to double -> doubleToRawLongBits` just as it is (implicitly) done in
                        // https://github.com/apache/hive/blob/7dc47faddba9f079bbe2698aaa4d8712e7654f87/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java#L836
                        yield murmur3(bytes(doubleToRawLongBits(doubleToLongBits((double) value))));
                    }
                    case STRING, VARCHAR -> murmur3(((Slice) value).getBytes());
                    case DATE -> {
                        // day offset from 1970-01-01
                        yield murmur3(bytes(toIntExact((long) value)));
                    }
                    case DECIMAL, CHAR, BINARY, VARIANT,
                         TIMESTAMP, TIMESTAMPLOCALTZ,
                         INTERVAL_YEAR_MONTH, INTERVAL_DAY_TIME,
                         VOID, UNKNOWN -> throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive primitive category: " + primitiveCategory);
                };
            }
            case LIST -> hashOfList((ListTypeInfo) type, (Block) value);
            case MAP -> hashOfMap((MapTypeInfo) type, (SqlMap) value);
            case STRUCT, UNION -> throw new UnsupportedOperationException("Computation of Hive bucket hashCode is not supported for Hive category: " + type.getCategory());
        };
    }

    private static int hashOfMap(MapTypeInfo type, SqlMap sqlMap)
    {
        TypeInfo keyTypeInfo = type.getMapKeyTypeInfo();
        TypeInfo valueTypeInfo = type.getMapValueTypeInfo();

        int rawOffset = sqlMap.getRawOffset();
        Block rawKeyBlock = sqlMap.getRawKeyBlock();
        Block rawValueBlock = sqlMap.getRawValueBlock();

        int result = 0;
        for (int i = 0; i < sqlMap.getSize(); i++) {
            // Sic! we're hashing map keys with v2 but map values with v1 just as in
            // https://github.com/apache/hive/blob/7dc47faddba9f079bbe2698aaa4d8712e7654f87/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java#L903-L904
            result += hash(keyTypeInfo, rawKeyBlock, rawOffset + i) ^ HiveBucketingV1.hash(valueTypeInfo, rawValueBlock, rawOffset + i);
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
        return new byte[] {(byte) ((value >> 8) & 0xFF), (byte) (value & 0xFF)};
    }

    // big-endian
    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static byte[] bytes(int value)
    {
        return new byte[] {(byte) ((value >> 24) & 0xFF), (byte) ((value >> 16) & 0xFF), (byte) ((value >> 8) & 0xFF), (byte) (value & 0xFF)};
    }

    // big-endian
    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static byte[] bytes(long value)
    {
        return new byte[] {
                (byte) ((value >> 56) & 0xFF), (byte) ((value >> 48) & 0xFF), (byte) ((value >> 40) & 0xFF), (byte) ((value >> 32) & 0xFF),
                (byte) ((value >> 24) & 0xFF), (byte) ((value >> 16) & 0xFF), (byte) ((value >> 8) & 0xFF), (byte) (value & 0xFF)};
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
            k *= 0xCC9E2D51;
            k = Integer.rotateLeft(k, 15);
            k *= 0x1B873593;
            hash ^= k;
            hash = Integer.rotateLeft(hash, 13) * 5 + 0xE6546B64;
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
                k1 *= 0xCC9E2D51;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= 0x1B873593;
                hash ^= k1;
        }

        // finalization
        hash ^= length;
        hash ^= (hash >>> 16);
        hash *= 0x85EBCA6B;
        hash ^= (hash >>> 13);
        hash *= 0xC2B2AE35;
        hash ^= (hash >>> 16);

        return hash;
    }
}
