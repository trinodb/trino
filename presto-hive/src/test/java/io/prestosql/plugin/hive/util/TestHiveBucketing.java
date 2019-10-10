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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.slice.Slices;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveTestUtils.TYPE_MANAGER;
import static io.prestosql.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static java.util.Arrays.asList;
import static java.util.Map.Entry;
import static org.testng.Assert.assertEquals;

public class TestHiveBucketing
{
    @Test
    public void testHashingCompare()
    {
        assertBucketEquals("boolean", null, 0);
        assertBucketEquals("boolean", true, 1);
        assertBucketEquals("boolean", false, 0);

        assertBucketEquals("tinyint", null, 0);
        assertBucketEquals("tinyint", (byte) 5, 5);
        assertBucketEquals("tinyint", Byte.MIN_VALUE, -128);
        assertBucketEquals("tinyint", Byte.MAX_VALUE, 127);

        assertBucketEquals("smallint", null, 0);
        assertBucketEquals("smallint", (short) 300, 300);
        assertBucketEquals("smallint", Short.MIN_VALUE, -32768);
        assertBucketEquals("smallint", Short.MAX_VALUE, 32767);

        assertBucketEquals("int", null, 0);
        assertBucketEquals("int", 300_000, 300000);
        assertBucketEquals("int", Integer.MIN_VALUE, -2147483648);
        assertBucketEquals("int", Integer.MAX_VALUE, 2147483647);

        assertBucketEquals("bigint", null, 0);
        assertBucketEquals("bigint", 300_000_000_000L, -647710651);
        assertBucketEquals("bigint", Long.MIN_VALUE, -2147483648);
        assertBucketEquals("bigint", Long.MAX_VALUE, -2147483648);

        assertBucketEquals("float", null, 0);
        assertBucketEquals("float", 12.34F, 1095069860);
        assertBucketEquals("float", -Float.MAX_VALUE, -8388609);
        assertBucketEquals("float", Float.MIN_VALUE, 1);
        assertBucketEquals("float", Float.POSITIVE_INFINITY, 2139095040);
        assertBucketEquals("float", Float.NEGATIVE_INFINITY, -8388608);
        assertBucketEquals("float", Float.NaN, 2143289344);
        assertBucketEquals("float", intBitsToFloat(0xffc00000), 2143289344); // also a NaN
        assertBucketEquals("float", intBitsToFloat(0x7fc00000), 2143289344); // also a NaN
        assertBucketEquals("float", intBitsToFloat(0x7fc01234), 2143289344); // also a NaN
        assertBucketEquals("float", intBitsToFloat(0xffc01234), 2143289344); // also a NaN

        assertBucketEquals("double", null, 0);
        assertBucketEquals("double", 12.34, 986311098);
        assertBucketEquals("double", -Double.MAX_VALUE, 1048576);
        assertBucketEquals("double", Double.MIN_VALUE, 1);
        assertBucketEquals("double", Double.POSITIVE_INFINITY, 2146435072);
        assertBucketEquals("double", Double.NEGATIVE_INFINITY, -1048576);
        assertBucketEquals("double", Double.NaN, 2146959360);
        assertBucketEquals("double", longBitsToDouble(0xfff8000000000000L), 2146959360); // also a NaN
        assertBucketEquals("double", longBitsToDouble(0x7ff8123412341234L), 2146959360); // also a NaN
        assertBucketEquals("double", longBitsToDouble(0xfff8123412341234L), 2146959360); // also a NaN

        assertBucketEquals("varchar(15)", null, 0);
        assertBucketEquals("varchar(15)", "", 1);
        assertBucketEquals("varchar(15)", "test string", -189841218);
        assertBucketEquals("varchar(15)", "\u5f3a\u5927\u7684Presto\u5f15\u64ce", 2136288313); // 3-byte UTF-8 sequences (in Basic Plane, i.e. Plane 0)
        assertBucketEquals("varchar(15)", "\uD843\uDFFC\uD843\uDFFD\uD843\uDFFE\uD843\uDFFF", -457487557); // 4 code points: 20FFC - 20FFF. 4-byte UTF-8 sequences in Supplementary Plane 2

        assertBucketEquals("string", null, 0);
        assertBucketEquals("string", "", 0);
        assertBucketEquals("string", "test string", -318923937);
        assertBucketEquals("string", "\u5f3a\u5927\u7684Presto\u5f15\u64ce", -120622694); // 3-byte UTF-8 sequences (in Basic Plane, i.e. Plane 0)
        assertBucketEquals("string", "\uD843\uDFFC\uD843\uDFFD\uD843\uDFFE\uD843\uDFFF", -1810797254); // 4 code points: 20FFC - 20FFF. 4-byte UTF-8 sequences in Supplementary Plane 2

        assertBucketEquals("date", null, 0);
        assertBucketEquals("date", Date.valueOf("1970-01-01"), 0);
        assertBucketEquals("date", Date.valueOf("2015-11-19"), 16758);
        assertBucketEquals("date", Date.valueOf("1950-11-19"), -6983);

        assertBucketEquals("timestamp", null, 0);
        assertBucketEquals("timestamp", Timestamp.valueOf("1970-01-01 00:00:00.000"), 7200);
        assertBucketEquals("timestamp", Timestamp.valueOf("1969-12-31 23:59:59.999"), -74736673);
        assertBucketEquals("timestamp", Timestamp.valueOf("1950-11-19 12:34:56.789"), -670699780);
        assertBucketEquals("timestamp", Timestamp.valueOf("2015-11-19 07:06:05.432"), 1278000719);

        assertBucketEquals("array<double>", null, 0);
        assertBucketEquals("array<boolean>", ImmutableList.of(), 0);
        assertBucketEquals("array<smallint>", ImmutableList.of((short) 5, (short) 8, (short) 13), 5066);
        assertBucketEquals("array<string>", ImmutableList.of("test1", "test2", "test3", "test4"), 957612994);
        assertBucketEquals("array<array<bigint>>", ImmutableList.of(ImmutableList.of(10L, 20L), ImmutableList.of(-10L, -20L), asList((Object) null)), 326368);

        assertBucketEquals("map<float,date>", null, 0);
        assertBucketEquals("map<double,timestamp>", ImmutableMap.of(), 0);
        assertBucketEquals("map<string,bigint>", ImmutableMap.of("key", 123L, "key2", 123456789L, "key3", -123456L), 127880789);

        assertBucketEquals("map<array<double>,map<int,string>>", ImmutableMap.of(ImmutableList.of(12.3, 45.7), ImmutableMap.of(123, "test99")), -34001111);

        // multiple bucketing columns
        assertBucketEquals(
                ImmutableList.of("float", "array<smallint>", "map<string,bigint>"),
                ImmutableList.of(12.34F, ImmutableList.of((short) 5, (short) 8, (short) 13), ImmutableMap.of("key", 123L)),
                95411006);
        assertBucketEquals(
                ImmutableList.of("double", "array<smallint>", "boolean", "map<string,bigint>", "tinyint"),
                asList(null, ImmutableList.of((short) 5, (short) 8, (short) 13), null, ImmutableMap.of("key", 123L), null),
                154207826);
    }

    private static void assertBucketEquals(String hiveTypeString, Object hiveValue, int expectedHashCode)
    {
        // Use asList to allow nulls
        assertBucketEquals(ImmutableList.of(hiveTypeString), asList(hiveValue), expectedHashCode);
    }

    private static void assertBucketEquals(List<String> hiveTypeStrings, List<Object> hiveValues, int expectedHashCode)
    {
        List<HiveType> hiveTypes = hiveTypeStrings.stream()
                .map(HiveType::valueOf)
                .collect(toImmutableList());
        List<TypeInfo> hiveTypeInfos = hiveTypes.stream()
                .map(HiveType::getTypeInfo)
                .collect(toImmutableList());

        assertEquals(computePresto(hiveTypeStrings, hiveValues, hiveTypes, hiveTypeInfos), expectedHashCode);
        assertEquals(computeHive(hiveTypeStrings, hiveValues, hiveTypeInfos), expectedHashCode);

        for (int bucketCount : new int[] {1, 2, 500, 997}) {
            int actual = HiveBucketing.getBucketNumber(expectedHashCode, bucketCount);
            int expected = ObjectInspectorUtils.getBucketNumber(expectedHashCode, bucketCount);
            assertEquals(actual, expected, "bucketCount " + bucketCount);
        }
    }

    private static int computeHive(List<String> hiveTypeStrings, List<Object> hiveValues, List<TypeInfo> hiveTypeInfos)
    {
        ImmutableList.Builder<Entry<ObjectInspector, Object>> columnBindingsBuilder = ImmutableList.builder();
        for (int i = 0; i < hiveTypeStrings.size(); i++) {
            Object javaValue = hiveValues.get(i);

            columnBindingsBuilder.add(Maps.immutableEntry(
                    TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(hiveTypeInfos.get(i)),
                    javaValue));
        }
        return getHiveBucketHashCode(columnBindingsBuilder.build());
    }

    private static int computePresto(List<String> hiveTypeStrings, List<Object> hiveValues, List<HiveType> hiveTypes, List<TypeInfo> hiveTypeInfos)
    {
        ImmutableList.Builder<Block> blockListBuilder = ImmutableList.builder();
        Object[] nativeContainerValues = new Object[hiveValues.size()];
        for (int i = 0; i < hiveTypeStrings.size(); i++) {
            Object hiveValue = hiveValues.get(i);
            Type type = hiveTypes.get(i).getType(TYPE_MANAGER);

            BlockBuilder blockBuilder = type.createBlockBuilder(null, 3);
            // prepend 2 nulls to make sure position is respected when HiveBucketing function
            blockBuilder.appendNull();
            blockBuilder.appendNull();
            appendToBlockBuilder(type, hiveValue, blockBuilder);
            Block block = blockBuilder.build();
            blockListBuilder.add(block);

            nativeContainerValues[i] = toNativeContainerValue(type, hiveValue);
        }
        ImmutableList<Block> blockList = blockListBuilder.build();
        int result1 = HiveBucketing.getBucketHashCode(hiveTypeInfos, new Page(blockList.toArray(new Block[blockList.size()])), 2);
        int result2 = HiveBucketing.getBucketHashCode(hiveTypeInfos, nativeContainerValues);
        assertEquals(result1, result2, "overloads of getBucketHashCode produced different result");
        return result1;
    }

    public static int getHiveBucketHashCode(List<Entry<ObjectInspector, Object>> columnBindings)
    {
        ObjectInspector[] objectInspectors = new ObjectInspector[columnBindings.size()];
        Object[] objects = new Object[columnBindings.size()];

        int i = 0;
        for (Entry<ObjectInspector, Object> entry : columnBindings) {
            objectInspectors[i] = entry.getKey();
            if (entry.getValue() != null && entry.getKey() instanceof JavaHiveVarcharObjectInspector) {
                JavaHiveVarcharObjectInspector varcharObjectInspector = (JavaHiveVarcharObjectInspector) entry.getKey();
                objects[i] = new HiveVarchar(((String) entry.getValue()), varcharObjectInspector.getMaxLength());
            }
            else {
                objects[i] = entry.getValue();
            }
            i++;
        }

        return getHiveBucketHashCodeV1(objects, objectInspectors);
    }

    @SuppressWarnings("deprecation")
    private static int getHiveBucketHashCodeV1(Object[] objects, ObjectInspector[] inspectors)
    {
        return ObjectInspectorUtils.getBucketHashCodeOld(objects, inspectors);
    }

    private static Object toNativeContainerValue(Type type, Object hiveValue)
    {
        if (hiveValue == null) {
            return null;
        }

        if (type instanceof ArrayType) {
            BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            for (Object subElement : (Iterable<?>) hiveValue) {
                appendToBlockBuilder(type.getTypeParameters().get(0), subElement, subBlockBuilder);
            }
            blockBuilder.closeEntry();
            return type.getObject(blockBuilder, 0);
        }
        if (type instanceof RowType) {
            BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            int field = 0;
            for (Object subElement : (Iterable<?>) hiveValue) {
                appendToBlockBuilder(type.getTypeParameters().get(field), subElement, subBlockBuilder);
                field++;
            }
            blockBuilder.closeEntry();
            return type.getObject(blockBuilder, 0);
        }
        if (type instanceof MapType) {
            BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            for (Entry<?, ?> entry : ((Map<?, ?>) hiveValue).entrySet()) {
                appendToBlockBuilder(type.getTypeParameters().get(0), entry.getKey(), subBlockBuilder);
                appendToBlockBuilder(type.getTypeParameters().get(1), entry.getValue(), subBlockBuilder);
            }
            blockBuilder.closeEntry();
            return type.getObject(blockBuilder, 0);
        }
        if (type instanceof BooleanType) {
            return hiveValue;
        }
        if (type instanceof TinyintType) {
            return (long) (byte) hiveValue;
        }
        if (type instanceof SmallintType) {
            return (long) (short) hiveValue;
        }
        if (type instanceof IntegerType) {
            return (long) (int) hiveValue;
        }
        if (type instanceof BigintType) {
            return hiveValue;
        }
        if (type instanceof RealType) {
            return (long) Float.floatToRawIntBits((float) hiveValue);
        }
        if (type instanceof DoubleType) {
            return hiveValue;
        }
        if (type instanceof VarcharType) {
            return Slices.utf8Slice(hiveValue.toString());
        }
        if (type instanceof DateType) {
            long daysSinceEpochInLocalZone = ((Date) hiveValue).toLocalDate().toEpochDay();
            assertEquals(daysSinceEpochInLocalZone, DateWritable.dateToDays((Date) hiveValue));
            return daysSinceEpochInLocalZone;
        }
        if (type instanceof TimestampType) {
            Instant instant = ((Timestamp) hiveValue).toInstant();
            long epochSecond = instant.getEpochSecond();
            int nano = instant.getNano();
            assertEquals(nano % 1_000_000, 0);
            return epochSecond * 1000 + nano / 1_000_000;
        }

        throw new UnsupportedOperationException("unknown type");
    }

    private static void appendToBlockBuilder(Type type, Object hiveValue, BlockBuilder blockBuilder)
    {
        writeNativeValue(type, blockBuilder, toNativeContainerValue(type, hiveValue));
    }
}
