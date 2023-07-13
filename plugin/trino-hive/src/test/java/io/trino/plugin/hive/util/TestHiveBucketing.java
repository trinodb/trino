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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveVarcharObjectInspector;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.newArrayList;
import static io.trino.plugin.hive.HiveTestUtils.toNativeContainerValue;
import static io.trino.plugin.hive.HiveType.HIVE_TIMESTAMP;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V2;
import static io.trino.plugin.hive.util.HiveBucketing.getHiveBuckets;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static java.util.Arrays.asList;
import static java.util.Map.Entry;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromTypeString;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestHiveBucketing
{
    @Test
    public void testHashingCompare()
    {
        assertBucketEquals("string", "Trino rocks", 1132136730, -399107423);
        assertEquals(HiveBucketing.getBucketNumber(1132136730, 4), 2);
        assertEquals(HiveBucketing.getBucketNumber(-399107423, 4), 1);

        assertBucketEquals("boolean", null, 0, 0);
        assertBucketEquals("boolean", true, 1, 1);
        assertBucketEquals("boolean", false, 0, 0);

        assertBucketEquals("tinyint", null, 0, 0);
        assertBucketEquals("tinyint", (byte) 5, 5, 5);
        assertBucketEquals("tinyint", Byte.MIN_VALUE, -128, -128);
        assertBucketEquals("tinyint", Byte.MAX_VALUE, 127, 127);

        assertBucketEquals("smallint", null, 0, 0);
        assertBucketEquals("smallint", (short) 300, 300, 2107031704);
        assertBucketEquals("smallint", Short.MIN_VALUE, -32768, 1342976838);
        assertBucketEquals("smallint", Short.MAX_VALUE, 32767, -684075052);

        assertBucketEquals("int", null, 0, 0);
        assertBucketEquals("int", 300_000, 300000, -678663480);
        assertBucketEquals("int", Integer.MIN_VALUE, -2147483648, 1194881028);
        assertBucketEquals("int", Integer.MAX_VALUE, 2147483647, 1133859967);

        assertBucketEquals("bigint", null, 0, 0);
        assertBucketEquals("bigint", 300_000_000_000L, -647710651, -888935297);
        assertBucketEquals("bigint", Long.MIN_VALUE, -2147483648, 1728983947);
        assertBucketEquals("bigint", Long.MAX_VALUE, -2147483648, -536577852);

        assertBucketEquals("float", null, 0, 0);
        assertBucketEquals("float", 12.34F, 1095069860, -381747602);
        assertBucketEquals("float", -Float.MAX_VALUE, -8388609, 470252243);
        assertBucketEquals("float", Float.MIN_VALUE, 1, 1206721797);
        assertBucketEquals("float", Float.POSITIVE_INFINITY, 2139095040, -292175804);
        assertBucketEquals("float", Float.NEGATIVE_INFINITY, -8388608, -1433270801);
        assertBucketEquals("float", Float.NaN, 2143289344, -480354314);
        assertBucketEquals("float", intBitsToFloat(0xffc00000), 2143289344, -480354314); // also a NaN
        assertBucketEquals("float", intBitsToFloat(0x7fc00000), 2143289344, -480354314); // also a NaN
        assertBucketEquals("float", intBitsToFloat(0x7fc01234), 2143289344, -480354314); // also a NaN
        assertBucketEquals("float", intBitsToFloat(0xffc01234), 2143289344, -480354314); // also a NaN

        assertBucketEquals("double", null, 0, 0);
        assertBucketEquals("double", 12.34, 986311098, -2070733568);
        assertBucketEquals("double", -Double.MAX_VALUE, 1048576, 14392725);
        assertBucketEquals("double", Double.MIN_VALUE, 1, -8838199);
        assertBucketEquals("double", Double.POSITIVE_INFINITY, 2146435072, 1614292060);
        assertBucketEquals("double", Double.NEGATIVE_INFINITY, -1048576, 141388605);
        assertBucketEquals("double", Double.NaN, 2146959360, 1138026565);
        assertBucketEquals("double", longBitsToDouble(0xfff8000000000000L), 2146959360, 1138026565); // also a NaN
        assertBucketEquals("double", longBitsToDouble(0x7ff8123412341234L), 2146959360, 1138026565); // also a NaN
        assertBucketEquals("double", longBitsToDouble(0xfff8123412341234L), 2146959360, 1138026565); // also a NaN

        assertBucketEquals("varchar(15)", null, 0, 0);
        assertBucketEquals("varchar(15)", "", 1, -965378730);
        assertBucketEquals("varchar(15)", "test string", -189841218, -138301454);
        assertBucketEquals("varchar(15)", "\u5f3a\u5927\u7684Trino\u5f15\u64ce", 1899852420, 1784416344); // 3-byte UTF-8 sequences (in Basic Plane, i.e. Plane 0)
        assertBucketEquals("varchar(15)", "\uD843\uDFFC\uD843\uDFFD\uD843\uDFFE\uD843\uDFFF", -457487557, -697348811); // 4 code points: 20FFC - 20FFF. 4-byte UTF-8 sequences in Supplementary Plane 2
        assertBucketEquals("string", null, 0, 0);
        assertBucketEquals("string", "", 0, -965378730);
        assertBucketEquals("string", "test string", -318923937, -138301454);
        assertBucketEquals("string", "\u5f3a\u5927\u7684Trino\u5f15\u64ce", 1688501507, 1784416344); // 3-byte UTF-8 sequences (in Basic Plane, i.e. Plane 0)
        assertBucketEquals("string", "\uD843\uDFFC\uD843\uDFFD\uD843\uDFFE\uD843\uDFFF", -1810797254, -697348811); // 4 code points: 20FFC - 20FFF. 4-byte UTF-8 sequences in Supplementary Plane 2

        assertBucketEquals("date", null, 0, 0);
        assertBucketEquals("date", Date.valueOf("1970-01-01"), 0, 1362653161);
        assertBucketEquals("date", Date.valueOf("2015-11-19"), 16758, 8542395);
        assertBucketEquals("date", Date.valueOf("1950-11-19"), -6983, -431619185);

        for (BucketingVersion version : BucketingVersion.values()) {
            List<TypeInfo> typeInfos = ImmutableList.of(HIVE_TIMESTAMP.getTypeInfo());

            assertThatThrownBy(() -> version.getBucketHashCode(typeInfos, new Object[] {0}))
                    .hasMessage("Computation of Hive bucket hashCode is not supported for Hive primitive category: TIMESTAMP");

            TimestampType timestampType = createTimestampType(3);
            BlockBuilder builder = timestampType.createBlockBuilder(null, 1);
            timestampType.writeLong(builder, 0);
            Page page = new Page(builder.build());

            assertThatThrownBy(() -> version.getBucketHashCode(typeInfos, page, 0))
                    .hasMessage("Computation of Hive bucket hashCode is not supported for Hive primitive category: TIMESTAMP");
        }

        assertBucketEquals("array<double>", null, 0, 0);
        assertBucketEquals("array<boolean>", ImmutableList.of(), 0, 0);
        assertBucketEquals("array<smallint>", ImmutableList.of((short) 5, (short) 8, (short) 13), 5066, -905011156);
        assertBucketEquals("array<string>", ImmutableList.of("test1", "test2", "test3", "test4"), 957612994, 1305539282);
        assertBucketEquals("array<array<bigint>>", ImmutableList.of(ImmutableList.of(10L, 20L), ImmutableList.of(-10L, -20L), asList((Object) null)), 326368, 611324477);

        assertBucketEquals("map<float,date>", null, 0, 0);
        assertBucketEquals("map<double,timestamp>", ImmutableMap.of(), 0, 0);
        assertBucketEquals("map<string,bigint>", ImmutableMap.of("key", 123L, "key2", 123456789L, "key3", -123456L), 127880789, -1910999650);

        assertBucketEquals("map<array<double>,map<int,string>>", ImmutableMap.of(ImmutableList.of(12.3, 45.7), ImmutableMap.of(123, "test99")), -34001111, -1565874874);

        // multiple bucketing columns
        assertBucketEquals(
                ImmutableList.of("float", "array<smallint>", "map<string,bigint>"),
                ImmutableList.of(12.34F, ImmutableList.of((short) 5, (short) 8, (short) 13), ImmutableMap.of("key", 123L)),
                95411006,
                932898434);
        assertBucketEquals(
                ImmutableList.of("double", "array<smallint>", "boolean", "map<string,bigint>", "tinyint"),
                asList(null, ImmutableList.of((short) 5, (short) 8, (short) 13), null, ImmutableMap.of("key", 123L), null),
                154207826,
                -1120812524);
    }

    @Test
    public void testBucketFilterFromDiscreteSets()
    {
        // multiple bucketing columns and multiple values for each bucketing column
        assertBucketsEqual(
                ImmutableList.of("int", "tinyint"),
                ImmutableList.of(
                        ImmutableList.of(1, 10),
                        ImmutableList.of((byte) 5, (byte) 6)),
                8,
                Optional.of(ImmutableSet.of(3, 4, 5)),
                Optional.of(ImmutableSet.of(0, 1, 5, 6)));

        assertBucketsEqual(
                ImmutableList.of("float", "array<smallint>", "map<string,bigint>"),
                ImmutableList.of(
                        ImmutableList.of(12.34F, 56.78F),
                        ImmutableList.of(ImmutableList.of((short) 5, (short) 8, (short) 13), ImmutableList.of((short) 1, (short) 2, (short) 3)),
                        ImmutableList.of(ImmutableMap.of("key1", 123L), ImmutableMap.of("key2", 456L))),
                32,
                Optional.of(ImmutableSet.of(3, 9, 11, 17, 21, 23, 29, 31)),
                Optional.of(ImmutableSet.of(0, 8, 10, 20, 30)));

        assertBucketsEqual(
                ImmutableList.of("double", "array<smallint>", "boolean", "map<string,bigint>", "tinyint"),
                ImmutableList.of(
                        newArrayList(null, 12.3),
                        newArrayList(null, ImmutableList.of((short) 1, (short) 2, (short) 3)),
                        newArrayList(null, false),
                        newArrayList(null, ImmutableMap.of("key", 123L)),
                        newArrayList(null, (byte) 120)),
                32,
                Optional.of(ImmutableSet.of(0, 1, 3, 18, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31)),
                Optional.of(ImmutableSet.of(0, 2, 10, 11, 19, 21, 24, 29)));

        // too many buckets to be enumerated compared to bucket count
        List<Integer> intValues = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            intValues.add(i);
        }
        List<Byte> tinyIntValues = new ArrayList<>(100);
        for (byte i = 0; i < 100; i++) {
            tinyIntValues.add(i);
        }
        assertBucketsEqual(
                ImmutableList.of("int", "tinyint"),
                ImmutableList.of(
                        ImmutableList.copyOf(intValues),
                        ImmutableList.copyOf(tinyIntValues)),
                32,
                Optional.empty(),
                Optional.empty());
    }

    private static void assertBucketsEqual(List<String> hiveTypeStrings, List<List<Object>> hiveValues, int bucketCount, Optional<Set<Integer>> expectedBucketsV1, Optional<Set<Integer>> expectedBucketsV2)
    {
        List<HiveType> hiveTypes = hiveTypeStrings.stream()
                .map(HiveType::valueOf)
                .collect(toImmutableList());
        List<TypeInfo> hiveTypeInfos = hiveTypes.stream()
                .map(HiveType::getTypeInfo)
                .collect(toImmutableList());
        List<Type> trinoTypes = hiveTypes.stream()
                .map(type -> type.getType(TESTING_TYPE_MANAGER))
                .collect(toImmutableList());

        ImmutableList.Builder<List<NullableValue>> values = ImmutableList.builder();
        for (int i = 0; i < hiveValues.size(); i++) {
            List<Object> valueList = hiveValues.get(i);
            Type trinoType = trinoTypes.get(i);
            values.add(valueList.stream()
                    .map(value -> new NullableValue(trinoType, toNativeContainerValue(trinoType, value)))
                    .collect(toImmutableList()));
        }

        assertEquals(getHiveBuckets(BUCKETING_V1, bucketCount, hiveTypeInfos, values.build()), expectedBucketsV1);

        assertEquals(getHiveBuckets(BUCKETING_V2, bucketCount, hiveTypeInfos, values.build()), expectedBucketsV2);
    }

    private static void assertBucketEquals(String hiveTypeString, Object hiveValue, int expectedHashCodeV1, int expectedHashCodeV2)
    {
        assertBucketEquals(hiveTypeString, hiveValue, BUCKETING_V1, expectedHashCodeV1);
        assertBucketEquals(hiveTypeString, hiveValue, BUCKETING_V2, expectedHashCodeV2);
    }

    private static void assertBucketEquals(String hiveTypeString, Object hiveValue, BucketingVersion bucketingVersion, int expectedHashCode)
    {
        // Use asList to allow nulls
        assertBucketEquals(ImmutableList.of(hiveTypeString), asList(hiveValue), bucketingVersion, expectedHashCode);
    }

    private static void assertBucketEquals(List<String> hiveTypeStrings, List<Object> hiveValues, int expectedHashCodeV1, int expectedHashCodeV2)
    {
        assertBucketEquals(hiveTypeStrings, hiveValues, BUCKETING_V1, expectedHashCodeV1);
        assertBucketEquals(hiveTypeStrings, hiveValues, BUCKETING_V2, expectedHashCodeV2);
    }

    private static void assertBucketEquals(List<String> hiveTypeStrings, List<Object> hiveValues, BucketingVersion bucketingVersion, int expectedHashCode)
    {
        List<HiveType> hiveTypes = hiveTypeStrings.stream()
                .map(HiveType::valueOf)
                .collect(toImmutableList());
        List<TypeInfo> hiveTypeInfos = hiveTypes.stream()
                .map(HiveType::getTypeInfo)
                .collect(toImmutableList());

        assertEquals(computeTrino(bucketingVersion, hiveTypeStrings, hiveValues, hiveTypes, hiveTypeInfos), expectedHashCode);
        assertEquals(computeHive(bucketingVersion, hiveTypeStrings, hiveValues, hiveTypeInfos), expectedHashCode);

        for (int bucketCount : new int[] {1, 2, 500, 997}) {
            int actual = HiveBucketing.getBucketNumber(expectedHashCode, bucketCount);
            int expected = ObjectInspectorUtils.getBucketNumber(expectedHashCode, bucketCount);
            assertEquals(actual, expected, "bucketCount " + bucketCount);
        }
    }

    private static int computeHive(BucketingVersion bucketingVersion, List<String> hiveTypeStrings, List<Object> hiveValues, List<TypeInfo> hiveTypeInfos)
    {
        ImmutableList.Builder<Entry<ObjectInspector, Object>> columnBindingsBuilder = ImmutableList.builder();
        for (int i = 0; i < hiveTypeStrings.size(); i++) {
            Object javaValue = hiveValues.get(i);
            var hiveTypeInfo = getTypeInfoFromTypeString(hiveTypeInfos.get(i).getTypeName());

            columnBindingsBuilder.add(Maps.immutableEntry(
                    getStandardJavaObjectInspectorFromTypeInfo(hiveTypeInfo),
                    javaValue));
        }
        return getHiveBucketHashCode(bucketingVersion, columnBindingsBuilder.build());
    }

    private static int computeTrino(BucketingVersion bucketingVersion, List<String> hiveTypeStrings, List<Object> hiveValues, List<HiveType> hiveTypes, List<TypeInfo> hiveTypeInfos)
    {
        ImmutableList.Builder<Block> blockListBuilder = ImmutableList.builder();
        Object[] nativeContainerValues = new Object[hiveValues.size()];
        for (int i = 0; i < hiveTypeStrings.size(); i++) {
            Object hiveValue = hiveValues.get(i);
            Type type = hiveTypes.get(i).getType(TESTING_TYPE_MANAGER);

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
        int result1 = bucketingVersion.getBucketHashCode(hiveTypeInfos, new Page(blockList.toArray(new Block[blockList.size()])), 2);
        int result2 = bucketingVersion.getBucketHashCode(hiveTypeInfos, nativeContainerValues);
        assertEquals(result1, result2, "overloads of getBucketHashCode produced different result");
        return result1;
    }

    public static int getHiveBucketHashCode(BucketingVersion bucketingVersion, List<Entry<ObjectInspector, Object>> columnBindings)
    {
        ObjectInspector[] objectInspectors = new ObjectInspector[columnBindings.size()];
        Object[] objects = new Object[columnBindings.size()];

        int i = 0;
        for (Entry<ObjectInspector, Object> entry : columnBindings) {
            objectInspectors[i] = entry.getKey();
            if (entry.getValue() != null && entry.getKey() instanceof JavaHiveVarcharObjectInspector varcharObjectInspector) {
                objects[i] = new HiveVarchar(((String) entry.getValue()), varcharObjectInspector.getMaxLength());
            }
            else {
                objects[i] = entry.getValue();
            }
            i++;
        }

        switch (bucketingVersion) {
            case BUCKETING_V1:
                @SuppressWarnings("deprecation")
                int hashCodeOld = ObjectInspectorUtils.getBucketHashCodeOld(objects, objectInspectors);
                return hashCodeOld;
            case BUCKETING_V2:
                return ObjectInspectorUtils.getBucketHashCode(objects, objectInspectors);
        }
        throw new IllegalArgumentException("Unsupported bucketing version: " + bucketingVersion);
    }

    private static void appendToBlockBuilder(Type type, Object hiveValue, BlockBuilder blockBuilder)
    {
        writeNativeValue(type, blockBuilder, toNativeContainerValue(type, hiveValue));
    }
}
