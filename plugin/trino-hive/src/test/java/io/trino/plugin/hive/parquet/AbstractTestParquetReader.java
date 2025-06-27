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
package io.trino.plugin.hive.parquet;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.primitives.Shorts;
import io.airlift.units.DataSize;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.Type;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Functions.compose;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Iterables.transform;
import static io.trino.plugin.hive.parquet.ParquetTester.ParquetSchemaOptions;
import static io.trino.plugin.hive.parquet.ParquetTester.TEST_COLUMN;
import static io.trino.plugin.hive.parquet.ParquetTester.insertNullEvery;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.trino.testing.StructuralTestUtil.mapType;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.math.BigInteger.ONE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class AbstractTestParquetReader
{
    private static final int MAX_PRECISION_INT32 = toIntExact(maxPrecision(4));
    private static final int MAX_PRECISION_INT64 = toIntExact(maxPrecision(8));

    private final ParquetTester tester;

    protected AbstractTestParquetReader(ParquetTester tester)
    {
        this.tester = tester;
        assertThat(DateTimeZone.getDefault()).isEqualTo(DateTimeZone.forID("America/Bahia_Banderas"));

        // Parquet has excessive logging at INFO level
        Logger.getLogger("org.apache.parquet.hadoop").setLevel(Level.WARNING);
    }

    @Test
    public void testArray()
            throws Exception
    {
        Iterable<List<Integer>> values = createTestArrays(limit(cycle(asList(1, null, 3, 5, null, null, null, 7, 11, null, 13, 17)), 30_000));
        tester.testRoundTrip(getStandardListObjectInspector(javaIntObjectInspector), values, values, new ArrayType(INTEGER));
    }

    @Test
    public void testEmptyArrays()
            throws Exception
    {
        Iterable<List<Integer>> values = limit(cycle(singletonList(Collections.emptyList())), 30_000);
        tester.testRoundTrip(getStandardListObjectInspector(javaIntObjectInspector), values, values, new ArrayType(INTEGER));
    }

    @Test
    public void testNestedArrays()
            throws Exception
    {
        int nestingLevel = ThreadLocalRandom.current().nextInt(1, 15);
        ObjectInspector objectInspector = getStandardListObjectInspector(javaIntObjectInspector);
        Type type = new ArrayType(INTEGER);
        Iterable<?> values = limit(cycle(asList(1, null, 3, null, 5, null, 7, null, null, null, 11, null, 13)), 3_210);
        for (int i = 0; i < nestingLevel; i++) {
            values = createNullableTestArrays(values);
            objectInspector = getStandardListObjectInspector(objectInspector);
            type = new ArrayType(type);
        }
        values = createTestArrays(values);
        tester.testRoundTrip(objectInspector, values, values, type);
    }

    @Test
    public void testSingleLevelSchemaNestedArrays()
            throws Exception
    {
        int nestingLevel = ThreadLocalRandom.current().nextInt(1, 15);
        ObjectInspector objectInspector = getStandardListObjectInspector(javaIntObjectInspector);
        Type type = new ArrayType(INTEGER);
        Iterable<?> values = intsBetween(0, 31_234);
        for (int i = 0; i < nestingLevel; i++) {
            values = createTestArrays(values);
            objectInspector = getStandardListObjectInspector(objectInspector);
            type = new ArrayType(type);
        }
        values = createTestArrays(values);
        tester.testSingleLevelArraySchemaRoundTrip(objectInspector, values, values, type);
    }

    @Test
    public void testArrayOfStructs()
            throws Exception
    {
        Iterable<List<?>> structs = createNullableTestStructs(transform(intsBetween(0, 31_234), Object::toString), longsBetween(0, 31_234));
        Iterable<List<List<?>>> values = createTestArrays(structs);
        List<String> structFieldNames = asList("stringField", "longField");
        Type structType = RowType.from(asList(field("stringField", VARCHAR), field("longField", BIGINT)));
        tester.testRoundTrip(
                getStandardListObjectInspector(getStandardStructObjectInspector(structFieldNames, asList(javaStringObjectInspector, javaLongObjectInspector))),
                values, values, new ArrayType(structType));
    }

    @Test
    public void testCustomSchemaArrayOfStructs()
            throws Exception
    {
        MessageType customSchemaArrayOfStructs = parseMessageType("message ParquetSchema { " +
                "  optional group self (LIST) { " +
                "    repeated group self_tuple { " +
                "      optional int64 a; " +
                "      optional boolean b; " +
                "      required binary c (UTF8); " +
                "    } " +
                "  } " +
                "}");
        Iterable<Long> aValues = limit(cycle(asList(1L, null, 3L, 5L, null, null, null, 7L, 11L, null, 13L, 17L)), 30_000);
        Iterable<Boolean> bValues = limit(cycle(asList(null, true, false, null, null, true, false)), 30_000);
        Iterable<String> cValues = transform(intsBetween(0, 31_234), Object::toString);

        Iterable<List<?>> structs = createTestStructs(aValues, bValues, cValues);
        Iterable<List<List<?>>> values = createTestArrays(structs);
        List<String> structFieldNames = asList("a", "b", "c");
        Type structType = RowType.from(asList(field("a", BIGINT), field("b", BOOLEAN), field("c", VARCHAR)));
        tester.testRoundTrip(
                getStandardListObjectInspector(getStandardStructObjectInspector(structFieldNames, asList(javaLongObjectInspector, javaBooleanObjectInspector, javaStringObjectInspector))),
                values,
                values,
                "self",
                new ArrayType(structType),
                Optional.of(customSchemaArrayOfStructs),
                ParquetSchemaOptions.withSingleLevelArray());
    }

    @Test
    public void testSingleLevelSchemaArrayOfStructs()
            throws Exception
    {
        Iterable<Long> aValues = limit(cycle(asList(1L, null, 3L, 5L, null, null, null, 7L, 11L, null, 13L, 17L)), 30_000);
        Iterable<Boolean> bValues = limit(cycle(asList(null, true, false, null, null, true, false)), 30_000);
        Iterable<String> cValues = transform(intsBetween(0, 31_234), Object::toString);

        Iterable<List<?>> structs = createTestStructs(aValues, bValues, cValues);
        Iterable<List<List<?>>> values = createTestArrays(structs);
        List<String> structFieldNames = asList("a", "b", "c");
        Type structType = RowType.from(asList(field("a", BIGINT), field("b", BOOLEAN), field("c", VARCHAR)));
        ObjectInspector objectInspector = getStandardListObjectInspector(getStandardStructObjectInspector(structFieldNames, asList(javaLongObjectInspector, javaBooleanObjectInspector, javaStringObjectInspector)));
        tester.testSingleLevelArraySchemaRoundTrip(objectInspector, values, values, new ArrayType(structType));
    }

    @Test
    public void testArrayOfArrayOfStructOfArray()
            throws Exception
    {
        Iterable<List<String>> stringArrayField = createNullableTestArrays(transform(intsBetween(0, 31_234), Object::toString));
        Iterable<List<?>> structs = createNullableTestStructs(stringArrayField, limit(cycle(asList(1, null, 3, 5, null, 7, 11, null, 17)), 31_234));
        List<String> structFieldNames = asList("stringArrayField", "intField");
        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(VARCHAR)), field("intField", INTEGER)));
        Iterable<List<List<?>>> arrays = createNullableTestArrays(structs);
        Iterable<List<List<List<?>>>> values = createTestArrays(arrays);
        tester.testRoundTrip(
                getStandardListObjectInspector(
                        getStandardListObjectInspector(
                                getStandardStructObjectInspector(
                                        structFieldNames,
                                        asList(getStandardListObjectInspector(javaStringObjectInspector), javaIntObjectInspector)))),
                values, values, new ArrayType(new ArrayType(structType)));
    }

    @Test
    public void testSingleLevelSchemaArrayOfArrayOfStructOfArray()
            throws Exception
    {
        Iterable<List<String>> stringArrayField = createNullableTestArrays(transform(intsBetween(0, 31_234), Object::toString));
        Iterable<List<?>> structs = createTestStructs(stringArrayField, limit(cycle(asList(1, null, 3, 5, null, 7, 11, null, 17)), 31_234));
        List<String> structFieldNames = asList("stringArrayField", "intField");
        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(VARCHAR)), field("intField", INTEGER)));
        Iterable<List<List<?>>> arrays = createTestArrays(structs);
        Iterable<List<List<List<?>>>> values = createTestArrays(arrays);
        tester.testSingleLevelArraySchemaRoundTrip(
                getStandardListObjectInspector(
                        getStandardListObjectInspector(
                                getStandardStructObjectInspector(
                                        structFieldNames,
                                        asList(getStandardListObjectInspector(javaStringObjectInspector), javaIntObjectInspector)))),
                values, values, new ArrayType(new ArrayType(structType)));
    }

    @Test
    public void testArrayOfStructOfArray()
            throws Exception
    {
        Iterable<List<String>> stringArrayField = createNullableTestArrays(transform(intsBetween(0, 31_234), Object::toString));
        Iterable<List<?>> structs = createNullableTestStructs(stringArrayField, limit(cycle(asList(1, 3, null, 5, 7, null, 11, 13, null, 17)), 31_234));
        List<String> structFieldNames = asList("stringArrayField", "intField");
        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(VARCHAR)), field("intField", INTEGER)));
        Iterable<List<List<?>>> values = createTestArrays(structs);
        tester.testRoundTrip(
                getStandardListObjectInspector(
                        getStandardStructObjectInspector(
                                structFieldNames,
                                asList(getStandardListObjectInspector(javaStringObjectInspector), javaIntObjectInspector))),
                values, values, new ArrayType(structType));
    }

    @Test
    public void testSingleLevelSchemaArrayOfStructOfArray()
            throws Exception
    {
        Iterable<List<String>> stringArrayField = createNullableTestArrays(transform(intsBetween(0, 31_234), Object::toString));
        Iterable<List<?>> structs = createTestStructs(stringArrayField, limit(cycle(asList(1, 3, null, 5, 7, null, 11, 13, null, 17)), 31_234));
        List<String> structFieldNames = asList("stringArrayField", "intField");
        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(VARCHAR)), field("intField", INTEGER)));
        Iterable<List<List<?>>> values = createTestArrays(structs);
        tester.testSingleLevelArraySchemaRoundTrip(
                getStandardListObjectInspector(
                        getStandardStructObjectInspector(
                                structFieldNames,
                                asList(getStandardListObjectInspector(javaStringObjectInspector), javaIntObjectInspector))),
                values, values, new ArrayType(structType));
    }

    @Test
    public void testMap()
            throws Exception
    {
        Iterable<Map<String, Long>> values = createTestMaps(transform(intsBetween(0, 100_000), Object::toString), longsBetween(0, 10_000));
        tester.testRoundTrip(getStandardMapObjectInspector(javaStringObjectInspector, javaLongObjectInspector), values, values, mapType(VARCHAR, BIGINT));
    }

    @Test
    public void testNestedMaps()
            throws Exception
    {
        int nestingLevel = ThreadLocalRandom.current().nextInt(1, 15);
        Iterable<Integer> keys = intsBetween(0, 3_210);
        Iterable<?> maps = limit(cycle(asList(null, "value2", "value3", null, null, "value6", "value7")), 3_210);
        ObjectInspector objectInspector = getStandardMapObjectInspector(javaIntObjectInspector, javaStringObjectInspector);
        Type type = mapType(INTEGER, VARCHAR);
        for (int i = 0; i < nestingLevel; i++) {
            maps = createNullableTestMaps(keys, maps);
            objectInspector = getStandardMapObjectInspector(javaIntObjectInspector, objectInspector);
            type = mapType(INTEGER, type);
        }
        maps = createTestMaps(keys, maps);
        tester.testRoundTrip(objectInspector, maps, maps, type);
    }

    @Test
    public void testArrayOfMaps()
            throws Exception
    {
        Iterable<Map<String, Long>> maps = createNullableTestMaps(transform(intsBetween(0, 10), Object::toString), longsBetween(0, 10));
        List<List<Map<String, Long>>> values = createTestArrays(maps);
        tester.testRoundTrip(getStandardListObjectInspector(getStandardMapObjectInspector(javaStringObjectInspector, javaLongObjectInspector)),
                values, values, new ArrayType(mapType(VARCHAR, BIGINT)));
    }

    @Test
    public void testSingleLevelSchemaArrayOfMaps()
            throws Exception
    {
        Iterable<Map<String, Long>> maps = createTestMaps(transform(intsBetween(0, 10), Object::toString), longsBetween(0, 10));
        List<List<Map<String, Long>>> values = createTestArrays(maps);
        ObjectInspector objectInspector = getStandardListObjectInspector(getStandardMapObjectInspector(javaStringObjectInspector, javaLongObjectInspector));
        tester.testSingleLevelArraySchemaRoundTrip(objectInspector, values, values, new ArrayType(mapType(VARCHAR, BIGINT)));
    }

    @Test
    public void testArrayOfMapOfStruct()
            throws Exception
    {
        Iterable<Integer> keys = intsBetween(0, 10_000);
        Iterable<List<?>> structs = createNullableTestStructs(transform(intsBetween(0, 10_000), Object::toString), longsBetween(0, 10_000));
        List<String> structFieldNames = asList("stringField", "longField");
        Type structType = RowType.from(asList(field("stringField", VARCHAR), field("longField", BIGINT)));
        Iterable<Map<Integer, List<?>>> maps = createNullableTestMaps(keys, structs);
        List<List<Map<Integer, List<?>>>> values = createTestArrays(maps);
        tester.testRoundTrip(getStandardListObjectInspector(
                        getStandardMapObjectInspector(
                                javaIntObjectInspector,
                                getStandardStructObjectInspector(structFieldNames, asList(javaStringObjectInspector, javaLongObjectInspector)))),
                values, values, new ArrayType(mapType(INTEGER, structType)));
    }

    @Test
    public void testSingleLevelArrayOfMapOfStruct()
            throws Exception
    {
        Iterable<Integer> keys = intsBetween(0, 10_000);
        Iterable<List<?>> structs = createNullableTestStructs(transform(intsBetween(0, 10_000), Object::toString), longsBetween(0, 10_000));
        List<String> structFieldNames = asList("stringField", "longField");
        Type structType = RowType.from(asList(field("stringField", VARCHAR), field("longField", BIGINT)));
        Iterable<Map<Integer, List<?>>> maps = createTestMaps(keys, structs);
        List<List<Map<Integer, List<?>>>> values = createTestArrays(maps);
        tester.testSingleLevelArraySchemaRoundTrip(getStandardListObjectInspector(
                        getStandardMapObjectInspector(
                                javaIntObjectInspector,
                                getStandardStructObjectInspector(structFieldNames, asList(javaStringObjectInspector, javaLongObjectInspector)))),
                values, values, new ArrayType(mapType(INTEGER, structType)));
    }

    @Test
    public void testSingleLevelArrayOfStructOfSingleElement()
            throws Exception
    {
        Iterable<List<?>> structs = createTestStructs(transform(intsBetween(0, 31_234), Object::toString));
        Iterable<List<List<?>>> values = createTestArrays(structs);
        List<String> structFieldNames = singletonList("test");
        Type structType = RowType.from(singletonList(field("test", VARCHAR)));
        tester.testRoundTrip(
                getStandardListObjectInspector(getStandardStructObjectInspector(structFieldNames, singletonList(javaStringObjectInspector))),
                values, values, new ArrayType(structType));
        tester.testSingleLevelArraySchemaRoundTrip(
                getStandardListObjectInspector(getStandardStructObjectInspector(structFieldNames, singletonList(javaStringObjectInspector))),
                values, values, new ArrayType(structType));
    }

    @Test
    public void testSingleLevelArrayOfStructOfStructOfSingleElement()
            throws Exception
    {
        Iterable<List<?>> structs = createTestStructs(transform(intsBetween(0, 31_234), Object::toString));
        Iterable<List<?>> structsOfStructs = createTestStructs(structs);
        Iterable<List<List<?>>> values = createTestArrays(structsOfStructs);
        List<String> structFieldNames = singletonList("test");
        List<String> structsOfStructsFieldNames = singletonList("test");
        Type structType = RowType.from(singletonList(field("test", VARCHAR)));
        Type structsOfStructsType = RowType.from(singletonList(field("test", structType)));
        ObjectInspector structObjectInspector = getStandardStructObjectInspector(structFieldNames, singletonList(javaStringObjectInspector));
        tester.testRoundTrip(
                getStandardListObjectInspector(
                        getStandardStructObjectInspector(structsOfStructsFieldNames, singletonList(structObjectInspector))),
                values, values, new ArrayType(structsOfStructsType));
        tester.testSingleLevelArraySchemaRoundTrip(
                getStandardListObjectInspector(
                        getStandardStructObjectInspector(structsOfStructsFieldNames, singletonList(structObjectInspector))),
                values, values, new ArrayType(structsOfStructsType));
    }

    @Test
    public void testArrayOfMapOfArray()
            throws Exception
    {
        Iterable<List<Integer>> arrays = createNullableTestArrays(limit(cycle(asList(1, null, 3, 5, null, null, null, 7, 11, null, 13, 17)), 10_000));
        Iterable<String> keys = transform(intsBetween(0, 10_000), Object::toString);
        Iterable<Map<String, List<Integer>>> maps = createNullableTestMaps(keys, arrays);
        List<List<Map<String, List<Integer>>>> values = createTestArrays(maps);
        tester.testRoundTrip(getStandardListObjectInspector(
                        getStandardMapObjectInspector(
                                javaStringObjectInspector,
                                getStandardListObjectInspector(javaIntObjectInspector))),
                values, values, new ArrayType(mapType(VARCHAR, new ArrayType(INTEGER))));
    }

    @Test
    public void testSingleLevelArrayOfMapOfArray()
            throws Exception
    {
        Iterable<List<Integer>> arrays = createNullableTestArrays(intsBetween(0, 10_000));
        Iterable<String> keys = transform(intsBetween(0, 10_000), Object::toString);
        Iterable<Map<String, List<Integer>>> maps = createTestMaps(keys, arrays);
        List<List<Map<String, List<Integer>>>> values = createTestArrays(maps);
        tester.testSingleLevelArraySchemaRoundTrip(getStandardListObjectInspector(
                        getStandardMapObjectInspector(
                                javaStringObjectInspector,
                                getStandardListObjectInspector(javaIntObjectInspector))),
                values, values, new ArrayType(mapType(VARCHAR, new ArrayType(INTEGER))));
    }

    @Test
    public void testMapOfArrayValues()
            throws Exception
    {
        Iterable<List<Integer>> arrays = createNullableTestArrays(limit(cycle(asList(1, null, 3, 5, null, null, null, 7, 11, null, 13, 17)), 30_000));
        Iterable<Integer> keys = intsBetween(0, 30_000);
        Iterable<Map<Integer, List<Integer>>> values = createTestMaps(keys, arrays);
        tester.testRoundTrip(getStandardMapObjectInspector(
                        javaIntObjectInspector,
                        getStandardListObjectInspector(javaIntObjectInspector)),
                values, values, mapType(INTEGER, new ArrayType(INTEGER)));
    }

    @Test
    public void testMapOfArrayKeys()
            throws Exception
    {
        Iterable<List<Integer>> mapKeys = createTestArrays(limit(cycle(asList(1, null, 3, 5, null, null, null, 7, 11, null, 13, 17)), 30_000));
        Iterable<Integer> mapValues = intsBetween(0, 30_000);
        Iterable<Map<List<Integer>, Integer>> testMaps = createTestMaps(mapKeys, mapValues);
        tester.testRoundTrip(
                getStandardMapObjectInspector(
                        getStandardListObjectInspector(javaIntObjectInspector),
                        javaIntObjectInspector),
                testMaps,
                testMaps,
                mapType(new ArrayType(INTEGER), INTEGER));
    }

    @Test
    public void testMapOfSingleLevelArray()
            throws Exception
    {
        Iterable<List<Integer>> arrays = createNullableTestArrays(intsBetween(0, 30_000));
        Iterable<Integer> keys = intsBetween(0, 30_000);
        Iterable<Map<Integer, List<Integer>>> values = createTestMaps(keys, arrays);
        tester.testSingleLevelArraySchemaRoundTrip(getStandardMapObjectInspector(
                        javaIntObjectInspector,
                        getStandardListObjectInspector(javaIntObjectInspector)),
                values, values, mapType(INTEGER, new ArrayType(INTEGER)));
    }

    @Test
    public void testMapOfStruct()
            throws Exception
    {
        Iterable<Long> keys = longsBetween(0, 30_000);
        Iterable<List<?>> structs = createNullableTestStructs(transform(intsBetween(0, 30_000), Object::toString), longsBetween(0, 30_000));
        List<String> structFieldNames = asList("stringField", "longField");
        Type structType = RowType.from(asList(field("stringField", VARCHAR), field("longField", BIGINT)));
        Iterable<Map<Long, List<?>>> values = createTestMaps(keys, structs);
        tester.testRoundTrip(getStandardMapObjectInspector(
                        javaLongObjectInspector,
                        getStandardStructObjectInspector(structFieldNames, asList(javaStringObjectInspector, javaLongObjectInspector))),
                values, values, mapType(BIGINT, structType));
    }

    @Test
    public void testMapWithNullValues()
            throws Exception
    {
        Iterable<Integer> mapKeys = intsBetween(0, 31_234);
        Iterable<String> mapValues = limit(cycle(asList(null, "value2", "value3", null, null, "value6", "value7")), 31_234);
        Iterable<Map<Integer, String>> values = createTestMaps(mapKeys, mapValues);
        tester.testRoundTrip(getStandardMapObjectInspector(javaIntObjectInspector, javaStringObjectInspector), values, values, mapType(INTEGER, VARCHAR));
    }

    @Test
    public void testStruct()
            throws Exception
    {
        List<List<?>> values = createTestStructs(transform(intsBetween(0, 31_234), Object::toString), longsBetween(0, 31_234));
        List<String> structFieldNames = asList("stringField", "longField");
        Type structType = RowType.from(asList(field("stringField", VARCHAR), field("longField", BIGINT)));
        tester.testRoundTrip(getStandardStructObjectInspector(structFieldNames, asList(javaStringObjectInspector, javaLongObjectInspector)), values, values, structType);
    }

    @Test
    public void testNestedStructs()
            throws Exception
    {
        int nestingLevel = ThreadLocalRandom.current().nextInt(1, 15);
        Optional<List<String>> structFieldNames = Optional.of(singletonList("structField"));
        Iterable<?> values = limit(cycle(asList(1, null, 3, null, 5, null, 7, null, null, null, 11, null, 13)), 3_210);
        ObjectInspector objectInspector = getStandardStructObjectInspector(structFieldNames.get(), singletonList(javaIntObjectInspector));
        Type type = RowType.from(singletonList(field("structField", INTEGER)));
        for (int i = 0; i < nestingLevel; i++) {
            values = createNullableTestStructs(values);
            objectInspector = getStandardStructObjectInspector(structFieldNames.get(), singletonList(objectInspector));
            type = RowType.from(singletonList(field("structField", type)));
        }
        values = createTestStructs(values);
        tester.testRoundTrip(objectInspector, values, values, type);
    }

    @Test
    public void testComplexNestedStructs()
            throws Exception
    {
        int n = 30;
        Iterable<Integer> mapKeys = intsBetween(0, n);
        Iterable<Integer> intPrimitives = limit(cycle(asList(1, null, 3, null, 5, null, 7, null, null, null, 11, null, 13)), n);
        Iterable<String> stringPrimitives = limit(cycle(asList(null, "value2", "value3", null, null, "value6", "value7")), n);
        Iterable<Double> doublePrimitives = limit(cycle(asList(1.1, null, 3.3, null, 5.5, null, 7.7, null, null, null, 11.11, null, 13.13)), n);
        Iterable<Boolean> booleanPrimitives = limit(cycle(asList(null, true, false, null, null, true, false)), n);
        Iterable<String> mapStringKeys = Stream.generate(() -> UUID.randomUUID().toString()).limit(n).collect(Collectors.toList());
        Iterable<Map<Integer, String>> mapsIntString = createNullableTestMaps(mapKeys, stringPrimitives);
        Iterable<List<String>> arraysString = createNullableTestArrays(stringPrimitives);
        Iterable<Map<Integer, Double>> mapsIntDouble = createNullableTestMaps(mapKeys, doublePrimitives);
        Iterable<List<Boolean>> arraysBoolean = createNullableTestArrays(booleanPrimitives);
        Iterable<Map<String, String>> mapsStringString = createNullableTestMaps(mapStringKeys, stringPrimitives);

        List<String> struct1FieldNames = asList("mapIntStringField", "stringArrayField", "intField");
        Iterable<?> structs1 = createNullableTestStructs(mapsIntString, arraysString, intPrimitives);
        ObjectInspector struct1ObjectInspector = getStandardStructObjectInspector(struct1FieldNames,
                asList(
                        getStandardMapObjectInspector(javaIntObjectInspector, javaStringObjectInspector),
                        getStandardListObjectInspector(javaStringObjectInspector),
                        javaIntObjectInspector));
        Type struct1Type = RowType.from(asList(
                field("mapIntStringField", mapType(INTEGER, VARCHAR)),
                field("stringArrayField", new ArrayType(VARCHAR)),
                field("intField", INTEGER)));

        List<String> struct2FieldNames = asList("mapIntStringField", "stringArrayField", "structField");
        Iterable<?> structs2 = createNullableTestStructs(mapsIntString, arraysString, structs1);
        ObjectInspector struct2ObjectInspector = getStandardStructObjectInspector(struct2FieldNames,
                asList(
                        getStandardMapObjectInspector(javaIntObjectInspector, javaStringObjectInspector),
                        getStandardListObjectInspector(javaStringObjectInspector),
                        struct1ObjectInspector));
        Type struct2Type = RowType.from(asList(
                field("mapIntStringField", mapType(INTEGER, VARCHAR)),
                field("stringArrayField", new ArrayType(VARCHAR)),
                field("structField", struct1Type)));

        List<String> struct3FieldNames = asList("mapIntDoubleField", "booleanArrayField", "booleanField");
        Iterable<?> structs3 = createNullableTestStructs(mapsIntDouble, arraysBoolean, booleanPrimitives);
        ObjectInspector struct3ObjectInspector = getStandardStructObjectInspector(struct3FieldNames,
                asList(
                        getStandardMapObjectInspector(javaIntObjectInspector, javaDoubleObjectInspector),
                        getStandardListObjectInspector(javaBooleanObjectInspector),
                        javaBooleanObjectInspector));
        Type struct3Type = RowType.from(asList(
                field("mapIntDoubleField", mapType(INTEGER, DOUBLE)),
                field("booleanArrayField", new ArrayType(BOOLEAN)),
                field("booleanField", BOOLEAN)));

        List<String> struct4FieldNames = asList("mapIntDoubleField", "booleanArrayField", "structField");
        Iterable<?> structs4 = createNullableTestStructs(mapsIntDouble, arraysBoolean, structs3);
        ObjectInspector struct4ObjectInspector = getStandardStructObjectInspector(struct4FieldNames,
                asList(
                        getStandardMapObjectInspector(javaIntObjectInspector, javaDoubleObjectInspector),
                        getStandardListObjectInspector(javaBooleanObjectInspector),
                        struct3ObjectInspector));
        Type struct4Type = RowType.from(asList(
                field("mapIntDoubleField", mapType(INTEGER, DOUBLE)),
                field("booleanArrayField", new ArrayType(BOOLEAN)),
                field("structField", struct3Type)));

        List<String> structFieldNames = asList("structField1", "structField2", "structField3", "structField4", "mapIntDoubleField", "booleanArrayField", "mapStringStringField");
        List<ObjectInspector> objectInspectors =
                asList(
                        struct1ObjectInspector,
                        struct2ObjectInspector,
                        struct3ObjectInspector,
                        struct4ObjectInspector,
                        getStandardMapObjectInspector(javaIntObjectInspector, javaDoubleObjectInspector),
                        getStandardListObjectInspector(javaBooleanObjectInspector),
                        getStandardMapObjectInspector(javaStringObjectInspector, javaStringObjectInspector));
        List<Type> types = ImmutableList.of(struct1Type, struct2Type, struct3Type, struct4Type, mapType(INTEGER, DOUBLE), new ArrayType(BOOLEAN), mapType(VARCHAR, VARCHAR));

        Iterable<?>[] values = new Iterable<?>[] {structs1, structs2, structs3, structs4, mapsIntDouble, arraysBoolean, mapsStringString};
        tester.assertRoundTrip(objectInspectors, values, values, structFieldNames, types, Optional.empty(), ParquetSchemaOptions.defaultOptions());
    }

    @Test
    public void testStructOfMaps()
            throws Exception
    {
        Iterable<Integer> mapKeys = Stream.generate(() -> ThreadLocalRandom.current().nextInt(10_000)).limit(10_000).collect(Collectors.toList());
        Iterable<Integer> intPrimitives = limit(cycle(asList(1, null, 3, null, 5, null, 7, null, null, null, 11, null, 13)), 10_000);
        Iterable<String> stringPrimitives = limit(cycle(asList(null, "value2", "value3", null, null, "value6", "value7")), 10_000);
        Iterable<Map<Integer, String>> maps = createNullableTestMaps(mapKeys, stringPrimitives);
        Iterable<List<String>> stringArrayField = createNullableTestArrays(stringPrimitives);
        List<List<?>> values = createTestStructs(maps, stringArrayField, intPrimitives);
        List<String> structFieldNames = asList("mapIntStringField", "stringArrayField", "intField");

        Type structType = RowType.from(asList(field("mapIntStringField", mapType(INTEGER, VARCHAR)), field("stringArrayField", new ArrayType(VARCHAR)), field("intField", INTEGER)));
        tester.testRoundTrip(getStandardStructObjectInspector(structFieldNames,
                        asList(
                                getStandardMapObjectInspector(javaIntObjectInspector, javaStringObjectInspector),
                                getStandardListObjectInspector(javaStringObjectInspector),
                                javaIntObjectInspector)),
                values, values, structType);
    }

    @Test
    public void testStructOfNullableMapBetweenNonNullFields()
            throws Exception
    {
        Iterable<Integer> intPrimitives = intsBetween(0, 10_000);
        Iterable<String> stringPrimitives = limit(cycle(asList(null, "value2", "value3", null, null, "value6", "value7")), 10_000);
        Iterable<Map<Integer, String>> maps = createNullableTestMaps(intPrimitives, stringPrimitives);
        List<List<?>> values = createTestStructs(intPrimitives, maps, intPrimitives);
        List<String> structFieldNames = asList("intField1", "mapIntStringField", "intField2");

        Type structType = RowType.from(asList(field("intField1", INTEGER), field("mapIntStringField", mapType(INTEGER, VARCHAR)), field("intField2", INTEGER)));
        tester.testRoundTrip(getStandardStructObjectInspector(structFieldNames,
                        asList(
                                javaIntObjectInspector,
                                getStandardMapObjectInspector(javaIntObjectInspector, javaStringObjectInspector),
                                javaIntObjectInspector)),
                values, values, structType);
    }

    @Test
    public void testStructOfNullableArrayBetweenNonNullFields()
            throws Exception
    {
        Iterable<Integer> intPrimitives = intsBetween(0, 10_000);
        Iterable<String> stringPrimitives = limit(cycle(asList(null, "value2", "value3", null, null, "value6", "value7")), 10_000);
        Iterable<List<String>> stringArrayField = createNullableTestArrays(stringPrimitives);
        List<List<?>> values = createTestStructs(intPrimitives, stringArrayField, intPrimitives);
        List<String> structFieldNames = asList("intField1", "arrayStringField", "intField2");

        Type structType = RowType.from(asList(field("intField1", INTEGER), field("arrayStringField", new ArrayType(VARCHAR)), field("intField2", INTEGER)));
        tester.testRoundTrip(getStandardStructObjectInspector(structFieldNames,
                        asList(
                                javaIntObjectInspector,
                                getStandardListObjectInspector(javaStringObjectInspector),
                                javaIntObjectInspector)),
                values, values, structType);
    }

    @Test
    public void testStructOfArrayAndPrimitive()
            throws Exception
    {
        Iterable<List<String>> stringArrayField = createNullableTestArrays(transform(intsBetween(0, 31_234), Object::toString));
        List<List<?>> values = createTestStructs(stringArrayField, limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 31_234));
        List<String> structFieldNames = asList("stringArrayField", "intField");

        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(VARCHAR)), field("intField", INTEGER)));
        tester.testRoundTrip(getStandardStructObjectInspector(structFieldNames,
                asList(getStandardListObjectInspector(javaStringObjectInspector), javaIntObjectInspector)), values, values, structType);
    }

    @Test
    public void testStructOfSingleLevelArrayAndPrimitive()
            throws Exception
    {
        Iterable<List<String>> stringArrayField = createNullableTestArrays(transform(intsBetween(0, 31_234), Object::toString));
        List<List<?>> values = createTestStructs(stringArrayField, limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 31_234));
        List<String> structFieldNames = asList("stringArrayField", "intField");

        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(VARCHAR)), field("intField", INTEGER)));
        tester.testSingleLevelArraySchemaRoundTrip(getStandardStructObjectInspector(structFieldNames,
                asList(getStandardListObjectInspector(javaStringObjectInspector), javaIntObjectInspector)), values, values, structType);
    }

    @Test
    public void testStructOfPrimitiveAndArray()
            throws Exception
    {
        Iterable<List<String>> stringArrayField = createNullableTestArrays(transform(intsBetween(0, 31_234), Object::toString));
        Iterable<Integer> intField = limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 31_234);
        List<List<?>> values = createTestStructs(intField, stringArrayField);
        List<String> structFieldNames = asList("intField", "stringArrayField");

        Type structType = RowType.from(asList(field("intField", INTEGER), field("stringArrayField", new ArrayType(VARCHAR))));
        tester.testRoundTrip(getStandardStructObjectInspector(structFieldNames,
                asList(javaIntObjectInspector, getStandardListObjectInspector(javaStringObjectInspector))), values, values, structType);
    }

    @Test
    public void testStructOfPrimitiveAndSingleLevelArray()
            throws Exception
    {
        Iterable<List<String>> stringArrayField = createNullableTestArrays(transform(intsBetween(0, 31_234), Object::toString));
        Iterable<Integer> intField = limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 31_234);
        List<List<?>> values = createTestStructs(intField, stringArrayField);
        List<String> structFieldNames = asList("intField", "stringArrayField");

        Type structType = RowType.from(asList(field("intField", INTEGER), field("stringArrayField", new ArrayType(VARCHAR))));
        tester.testSingleLevelArraySchemaRoundTrip(getStandardStructObjectInspector(structFieldNames,
                asList(javaIntObjectInspector, getStandardListObjectInspector(javaStringObjectInspector))), values, values, structType);
    }

    @Test
    public void testStructOfTwoArrays()
            throws Exception
    {
        Iterable<List<Integer>> intArrayField = createNullableTestArrays(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000));
        Iterable<List<String>> stringArrayField = createNullableTestArrays(transform(intsBetween(0, 30_000), Object::toString));
        List<List<?>> values = createTestStructs(stringArrayField, intArrayField);
        List<String> structFieldNames = asList("stringArrayField", "intArrayField");

        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(VARCHAR)), field("intArrayField", new ArrayType(INTEGER))));
        tester.testRoundTrip(getStandardStructObjectInspector(structFieldNames,
                asList(getStandardListObjectInspector(javaStringObjectInspector), getStandardListObjectInspector(javaIntObjectInspector))), values, values, structType);
    }

    @Test
    public void testStructOfTwoNestedArrays()
            throws Exception
    {
        Iterable<List<List<Integer>>> intArrayField = createNullableTestArrays(createNullableTestArrays(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000)));
        Iterable<List<List<String>>> stringArrayField = createNullableTestArrays(createNullableTestArrays(transform(intsBetween(0, 31_234), Object::toString)));
        List<List<?>> values = createTestStructs(stringArrayField, intArrayField);
        List<String> structFieldNames = asList("stringArrayField", "intArrayField");
        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(new ArrayType(VARCHAR))), field("intArrayField", new ArrayType(new ArrayType(INTEGER)))));
        tester.testRoundTrip(getStandardStructObjectInspector(structFieldNames,
                        asList(
                                getStandardListObjectInspector(getStandardListObjectInspector(javaStringObjectInspector)),
                                getStandardListObjectInspector(getStandardListObjectInspector(javaIntObjectInspector)))),
                values, values, structType);
    }

    @Test
    public void testStructOfTwoNestedSingleLevelSchemaArrays()
            throws Exception
    {
        Iterable<List<List<Integer>>> intArrayField = createNullableTestArrays(createTestArrays(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000)));
        Iterable<List<List<String>>> stringArrayField = createNullableTestArrays(createTestArrays(transform(intsBetween(0, 31_234), Object::toString)));
        List<List<?>> values = createTestStructs(stringArrayField, intArrayField);
        List<String> structFieldNames = asList("stringArrayField", "intArrayField");

        Type structType = RowType.from(asList(field("stringArrayField", new ArrayType(new ArrayType(VARCHAR))), field("intArrayField", new ArrayType(new ArrayType(INTEGER)))));
        ObjectInspector objectInspector = getStandardStructObjectInspector(structFieldNames,
                asList(
                        getStandardListObjectInspector(getStandardListObjectInspector(javaStringObjectInspector)),
                        getStandardListObjectInspector(getStandardListObjectInspector(javaIntObjectInspector))));
        tester.testSingleLevelArraySchemaRoundTrip(objectInspector, values, values, structType);
    }

    @Test
    public void testBooleanSequence()
            throws Exception
    {
        tester.testRoundTrip(javaBooleanObjectInspector, limit(cycle(ImmutableList.of(true, false, false)), 30_000), BOOLEAN);
    }

    @Test
    public void testLongSequence()
            throws Exception
    {
        testRoundTripNumeric(intsBetween(0, 31_234));
    }

    @Test
    public void testLongSequenceWithHoles()
            throws Exception
    {
        testRoundTripNumeric(skipEvery(5, intsBetween(0, 31_234)));
    }

    @Test
    public void testLongDirect()
            throws Exception
    {
        testRoundTripNumeric(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000));
    }

    @Test
    public void testLongDirect2()
            throws Exception
    {
        List<Integer> values = new ArrayList<>(31_234);
        for (int i = 0; i < 31_234; i++) {
            values.add(i);
        }
        Collections.shuffle(values, new Random(0));
        testRoundTripNumeric(values);
    }

    @Test
    public void testLongShortRepeat()
            throws Exception
    {
        testRoundTripNumeric(limit(repeatEach(4, cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17))), 30_000));
    }

    @Test
    public void testLongPatchedBase()
            throws Exception
    {
        testRoundTripNumeric(limit(cycle(concat(intsBetween(0, 18), ImmutableList.of(30_000, 20_000))), 30_000));
    }

    // copied from Parquet code to determine the max decimal precision supported by INT32/INT64
    private static long maxPrecision(int numBytes)
    {
        return Math.round(Math.floor(Math.log10(Math.pow(2, 8 * numBytes - 1) - 1)));
    }

    @Test
    public void testParquetShortDecimalWriteToTrinoDecimalWithNonMatchingScale()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType(format("message hive_decimal { optional INT64 test (DECIMAL(%d, %d)); }", 10, 1));
        tester.testRoundTrip(javaLongObjectInspector, ImmutableList.of(10L), ImmutableList.of(SqlDecimal.of(100L, 10, 2)), createDecimalType(10, 2), Optional.of(parquetSchema));
    }

    @Test
    public void testDecimals()
            throws Exception
    {
        for (DecimalInput decimalInput : DecimalInput.values()) {
            for (int precision = 1; precision <= decimalInput.getMaxSupportedPrecision(); precision++) {
                int scale = ThreadLocalRandom.current().nextInt(precision);
                MessageType parquetSchema = parseMessageType(format(
                        "message hive_decimal { optional %s test (DECIMAL(%d, %d)); }",
                        decimalInput.getPrimitiveTypeName(precision),
                        precision,
                        scale));
                ImmutableList.Builder<SqlDecimal> expectedValues = ImmutableList.builder();
                ImmutableList.Builder<SqlDecimal> expectedValuesMaxPrecision = ImmutableList.builder();
                ImmutableList.Builder<Object> writeValuesBuilder = ImmutableList.builder();

                BigInteger start = BigInteger.valueOf(10).pow(precision).subtract(ONE).negate();
                BigInteger end = BigInteger.valueOf(10).pow(precision);
                BigInteger step = BigInteger.valueOf(1).max(end.subtract(start).divide(BigInteger.valueOf(1_500)));
                for (BigInteger value = start; value.compareTo(end) < 0; value = value.add(step)) {
                    writeValuesBuilder.add(decimalInput.convertToWriteValue(value, scale));
                    expectedValues.add(new SqlDecimal(value, precision, scale));
                    expectedValuesMaxPrecision.add(new SqlDecimal(value, MAX_PRECISION, scale));
                }
                List<Object> writeValues = writeValuesBuilder.build();
                tester.testRoundTrip(
                        decimalInput.getParquetObjectInspector(precision, scale),
                        writeValues,
                        expectedValues.build(),
                        createDecimalType(precision, scale),
                        Optional.of(parquetSchema));
                tester.testRoundTrip(
                        decimalInput.getParquetObjectInspector(precision, scale),
                        writeValues,
                        expectedValuesMaxPrecision.build(),
                        createDecimalType(MAX_PRECISION, scale),
                        Optional.of(parquetSchema));
            }
        }
    }

    private enum DecimalInput
    {
        INT32 {
            @Override
            String getPrimitiveTypeName(int precision)
            {
                return "INT32";
            }

            @Override
            int getMaxSupportedPrecision()
            {
                return MAX_PRECISION_INT32;
            }

            @Override
            ObjectInspector getParquetObjectInspector(int precision, int scale)
            {
                return javaIntObjectInspector;
            }

            @Override
            Object convertToWriteValue(BigInteger value, int scale)
            {
                return value.intValueExact();
            }
        },
        INT64 {
            @Override
            String getPrimitiveTypeName(int precision)
            {
                return "INT64";
            }

            @Override
            int getMaxSupportedPrecision()
            {
                return MAX_PRECISION_INT64;
            }

            @Override
            ObjectInspector getParquetObjectInspector(int precision, int scale)
            {
                return javaLongObjectInspector;
            }

            @Override
            Object convertToWriteValue(BigInteger value, int scale)
            {
                return value.longValueExact();
            }
        },
        BINARY {
            @Override
            String getPrimitiveTypeName(int precision)
            {
                return "BINARY";
            }

            @Override
            int getMaxSupportedPrecision()
            {
                return MAX_PRECISION;
            }

            @Override
            ObjectInspector getParquetObjectInspector(int precision, int scale)
            {
                return new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(precision, scale));
            }

            @Override
            Object convertToWriteValue(BigInteger value, int scale)
            {
                return HiveDecimal.create(value, scale);
            }
        },
        FIXED_LEN_BYTE_ARRAY {
            @Override
            String getPrimitiveTypeName(int precision)
            {
                return format("FIXED_LEN_BYTE_ARRAY(%d)", ParquetHiveSerDe.PRECISION_TO_BYTE_COUNT[precision - 1]);
            }

            @Override
            int getMaxSupportedPrecision()
            {
                return MAX_PRECISION;
            }

            @Override
            ObjectInspector getParquetObjectInspector(int precision, int scale)
            {
                return new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(precision, scale));
            }

            @Override
            Object convertToWriteValue(BigInteger value, int scale)
            {
                return HiveDecimal.create(value, scale);
            }
        };

        abstract String getPrimitiveTypeName(int precision);

        abstract int getMaxSupportedPrecision();

        abstract ObjectInspector getParquetObjectInspector(int precision, int scale);

        abstract Object convertToWriteValue(BigInteger value, int scale);
    }

    @Test
    public void testParquetLongDecimalWriteToTrinoDecimalWithNonMatchingScale()
            throws Exception
    {
        tester.testRoundTrip(
                new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(38, 10)),
                ImmutableList.of(HiveDecimal.create(100 * longTenToNth(10), 10)),
                ImmutableList.of(new SqlDecimal(BigInteger.valueOf(100 * longTenToNth(9)), 38, 9)),
                createDecimalType(38, 9));
    }

    @Test
    public void testParquetShortDecimalWriteToTrinoTinyintBlock()
            throws Exception
    {
        for (int precision = 1; precision <= MAX_PRECISION_INT64; precision++) {
            MessageType parquetSchema = parseMessageType(format("message hive_decimal { optional INT64 test (DECIMAL(%d, %d)); }", precision, 0));
            ContiguousSet<Long> longValues = longsBetween(Byte.MIN_VALUE, Byte.MAX_VALUE);
            ImmutableList.Builder<Byte> expectedValues = ImmutableList.builder();
            for (Long value : longValues) {
                expectedValues.add(value.byteValue());
            }
            tester.testRoundTrip(javaLongObjectInspector, longValues, expectedValues.build(), TINYINT, Optional.of(parquetSchema));
        }
    }

    @Test
    public void testParquetShortDecimalWriteToTrinoSmallintBlock()
            throws Exception
    {
        int start = Short.MIN_VALUE;
        int end = Short.MAX_VALUE;
        int step = Math.max((end - start) / 2_000, 1);
        ImmutableList.Builder<Long> writeValues = ImmutableList.builder();
        for (long value = start; value <= end; value += step) {
            writeValues.add(value);
        }
        List<Long> longValues = writeValues.build();
        List<Short> expectedValues = longValues.stream().map(Long::shortValue).collect(toImmutableList());

        for (int precision = 1; precision <= MAX_PRECISION_INT64; precision++) {
            MessageType parquetSchema = parseMessageType(format("message hive_decimal { optional INT64 test (DECIMAL(%d, %d)); }", precision, 0));
            tester.testRoundTrip(
                    javaLongObjectInspector,
                    longValues,
                    expectedValues,
                    SMALLINT,
                    Optional.of(parquetSchema));
        }
    }

    @Test
    public void testParquetShortDecimalWriteToTrinoIntegerBlock()
            throws Exception
    {
        long start = Integer.MIN_VALUE;
        long end = Integer.MAX_VALUE;
        long step = Math.max((end - start) / 2_000, 1);
        ImmutableList.Builder<Long> writeValues = ImmutableList.builder();
        for (long value = start; value <= end; value += step) {
            writeValues.add(value);
        }
        List<Long> longValues = writeValues.build();
        List<Integer> expectedValues = longValues.stream().map(Math::toIntExact).collect(toImmutableList());

        for (int precision = 1; precision <= MAX_PRECISION_INT64; precision++) {
            MessageType parquetSchema = parseMessageType(format("message hive_decimal { optional INT64 test (DECIMAL(%d, %d)); }", precision, 0));
            tester.testRoundTrip(
                    javaLongObjectInspector,
                    longValues,
                    expectedValues,
                    INTEGER,
                    Optional.of(parquetSchema));
        }
    }

    @Test
    public void testParquetShortDecimalWriteToTrinoBigintBlock()
            throws Exception
    {
        BigInteger start = BigInteger.valueOf(Long.MIN_VALUE);
        BigInteger end = BigInteger.valueOf(Long.MAX_VALUE);
        int valuesCount = 8_000;
        long step = end.subtract(start).divide(BigInteger.valueOf(valuesCount)).max(BigInteger.valueOf(1)).longValueExact();
        ImmutableList.Builder<Long> writeValues = ImmutableList.builder();
        long value = Long.MIN_VALUE;
        for (int i = 0; i < valuesCount; i++) {
            value = Math.addExact(value, step);
            writeValues.add(value);
        }
        List<Long> longValues = writeValues.build();

        for (int precision = 4; precision <= MAX_PRECISION_INT64; precision++) {
            MessageType parquetSchema = parseMessageType(format("message hive_decimal { optional INT64 test (DECIMAL(%d, %d)); }", precision, 0));
            tester.testRoundTrip(
                    javaLongObjectInspector,
                    longValues,
                    longValues,
                    BIGINT,
                    Optional.of(parquetSchema));
        }
    }

    @Test
    public void testParquetShortDecimalWriteToTrinoBigintBlockWithNonZeroScale()
    {
        assertThatThrownBy(() -> {
            MessageType parquetSchema = parseMessageType(format("message hive_decimal { optional INT64 test (DECIMAL(%d, %d)); }", 10, 1));
            tester.testRoundTrip(javaLongObjectInspector, ImmutableList.of(1L), ImmutableList.of(1L), BIGINT, Optional.of(parquetSchema));
        }).hasMessage("Unsupported Trino column type (bigint) for Parquet column ([test] optional int64 test (DECIMAL(10,1)))")
                .isInstanceOf(TrinoException.class);

        assertThatThrownBy(() -> {
            MessageType parquetSchema = parseMessageType(format("message hive_decimal { optional INT32 test (DECIMAL(%d, %d)); }", 8, 1));
            tester.testRoundTrip(javaIntObjectInspector, ImmutableList.of(1), ImmutableList.of(1), BIGINT, Optional.of(parquetSchema));
        }).hasMessage("Unsupported Trino column type (bigint) for Parquet column ([test] optional int32 test (DECIMAL(8,1)))")
                .isInstanceOf(TrinoException.class);
    }

    @Test
    public void testParquetShortDecimalWriteToTrinoIntegerBlockWithNonZeroScale()
    {
        assertThatThrownBy(() -> {
            MessageType parquetSchema = parseMessageType(format("message hive_decimal { optional INT32 test (DECIMAL(%d, %d)); }", 8, 1));
            tester.testRoundTrip(javaIntObjectInspector, ImmutableList.of(1), ImmutableList.of(1), INTEGER, Optional.of(parquetSchema));
        }).hasMessage("Unsupported Trino column type (integer) for Parquet column ([test] optional int32 test (DECIMAL(8,1)))")
                .isInstanceOf(TrinoException.class);
    }

    @Test
    public void testParquetShortDecimalWriteToTrinoSmallBlockWithNonZeroScale()
    {
        assertThatThrownBy(() -> {
            MessageType parquetSchema = parseMessageType(format("message hive_decimal { optional INT32 test (DECIMAL(%d, %d)); }", 8, 1));
            tester.testRoundTrip(javaShortObjectInspector, ImmutableList.of((short) 1), ImmutableList.of((short) 1), SMALLINT, Optional.of(parquetSchema));
        }).hasMessage("Unsupported Trino column type (smallint) for Parquet column ([test] optional int32 test (DECIMAL(8,1)))")
                .isInstanceOf(TrinoException.class);
    }

    @Test
    public void testParquetShortDecimalWriteToTrinoTinyBlockWithNonZeroScale()
    {
        assertThatThrownBy(() -> {
            MessageType parquetSchema = parseMessageType(format("message hive_decimal { optional INT32 test (DECIMAL(%d, %d)); }", 8, 1));
            tester.testRoundTrip(javaByteObjectInspector, ImmutableList.of((byte) 1), ImmutableList.of((byte) 1), TINYINT, Optional.of(parquetSchema));
        }).hasMessage("Unsupported Trino column type (tinyint) for Parquet column ([test] optional int32 test (DECIMAL(8,1)))")
                .isInstanceOf(TrinoException.class);
    }

    @Test
    public void testReadParquetInt32AsTrinoShortDecimal()
            throws Exception
    {
        Iterable<Integer> writeValues = intsBetween(0, 31_234);
        Optional<MessageType> parquetSchema = Optional.of(parseMessageType("message hive_decimal { optional INT32 test; }"));
        // Read INT32 as a short decimal of precision >= 10 with zero scale
        tester.testRoundTrip(
                javaIntObjectInspector,
                writeValues,
                transform(writeValues, value -> new SqlDecimal(BigInteger.valueOf(value), 10, 0)),
                createDecimalType(10),
                parquetSchema);

        // Read INT32 as a short decimal of precision >= 10 with non-zero scale
        tester.testRoundTrip(
                javaIntObjectInspector,
                ImmutableList.of(Integer.MAX_VALUE),
                ImmutableList.of(new SqlDecimal(BigInteger.valueOf(Integer.MAX_VALUE * 10L), 10, 1)),
                createDecimalType(10, 1),
                parquetSchema);

        // Read INT32 as a short decimal if value is within supported precision
        tester.testRoundTrip(
                javaIntObjectInspector,
                ImmutableList.of(9999),
                ImmutableList.of(new SqlDecimal(BigInteger.valueOf(9999), 4, 0)),
                createDecimalType(4, 0),
                parquetSchema);

        // Cannot read INT32 as a short decimal if value exceeds supported precision
        assertThatThrownBy(() -> tester.assertRoundTripWithHiveWriter(
                List.of(javaIntObjectInspector),
                new Iterable[] {ImmutableList.of(Integer.MAX_VALUE)},
                new Iterable[] {ImmutableList.of(new SqlDecimal(BigInteger.valueOf(Integer.MAX_VALUE), 9, 0))},
                List.of("test"),
                List.of(createDecimalType(9, 0)),
                parquetSchema,
                ParquetSchemaOptions.defaultOptions()))
                .hasMessage("Cannot read parquet INT32 value '2147483647' as DECIMAL(9, 0)")
                .isInstanceOf(TrinoException.class);
    }

    @Test
    public void testTimestamp()
            throws Exception
    {
        for (HiveTimestampPrecision precision : HiveTimestampPrecision.values()) {
            List<Long> epochMillisValues = ContiguousSet.create(Range.closedOpen((long) -1_000, (long) 1_000), DiscreteDomain.longs()).stream()
                    .map(millis -> System.currentTimeMillis() + millis)
                    .collect(toImmutableList());
            List<Timestamp> writeValues = epochMillisValues.stream()
                    .map(AbstractTestParquetReader::longToTimestamp)
                    .collect(toImmutableList());
            List<SqlTimestamp> readValues = epochMillisValues.stream()
                    .map(epochMillis -> SqlTimestamp.newInstance(precision.getPrecision(), epochMillis * 1_000, 0))
                    .collect(toImmutableList());
            // INT96 backed timestamps are written by the default ParquetSchemaOptions
            tester.testRoundTrip(
                    javaTimestampObjectInspector,
                    writeValues,
                    readValues,
                    createTimestampType(precision.getPrecision()),
                    Optional.empty());
            tester.testRoundTrip(
                    javaTimestampObjectInspector,
                    writeValues,
                    readValues,
                    getOnlyElement(TEST_COLUMN),
                    createTimestampType(precision.getPrecision()),
                    Optional.empty(),
                    ParquetSchemaOptions.withInt64BackedTimestamps());
        }
    }

    @Test
    public void testReadInt32AsDate()
            throws Exception
    {
        List<Integer> writeValues = IntStream.range(0, 1000).boxed().collect(toImmutableList());
        List<SqlDate> readValues = writeValues.stream()
                .map(AbstractTestParquetReader::intToSqlDate)
                .collect(toImmutableList());
        tester.testRoundTrip(javaIntObjectInspector, writeValues, readValues, DATE);
        tester.testRoundTrip(
                javaIntObjectInspector,
                writeValues,
                readValues,
                DATE,
                Optional.of(parseMessageType("message hive_date { optional INT32 test (INTEGER(16,false)); }")));
    }

    @Test
    public void testSchemaWithRepeatedOptionalRequiredFields()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_schema {" +
                "  optional group address_book {" +
                "    required binary owner (UTF8);" +
                "    optional group owner_phone_numbers (LIST) {" +
                "      repeated group bag {" +
                "        optional binary array_element (UTF8);" +
                "      }" +
                "    }" +
                "    optional group contacts (LIST) {" +
                "      repeated group bag {" +
                "        optional group array_element {" +
                "          required binary name (UTF8);" +
                "          optional binary phone_number (UTF8);" +
                "        }" +
                "      }" +
                "    }" +
                "  }" +
                "} ");

        Iterable<String> owner = limit(cycle(asList("owner1", "owner2", "owner3")), 50_000);
        Iterable<List<String>> ownerPhoneNumbers = limit(cycle(asList(null, asList("phoneNumber2", "phoneNumber3", null), asList(null, "phoneNumber6", "phoneNumber7"))), 50_000);
        Iterable<String> name = asList("name1", "name2", "name3", "name4", "name5", "name6", "name7");
        Iterable<String> phoneNumber = asList(null, "phoneNumber2", "phoneNumber3", null, null, "phoneNumber6", "phoneNumber7");
        Iterable<List<?>> contact = createNullableTestStructs(name, phoneNumber);
        Iterable<List<List<?>>> contacts = createNullableTestArrays(limit(cycle(contact), 50_000));
        List<List<?>> values = createTestStructs(owner, ownerPhoneNumbers, contacts);
        List<String> addressBookFieldNames = asList("owner", "owner_phone_numbers", "contacts");
        List<String> contactsFieldNames = asList("name", "phone_number");
        Type contactsType = new ArrayType(RowType.from(asList(field("name", VARCHAR), field("phone_number", VARCHAR))));
        Type addressBookType = RowType.from(asList(field("owner", VARCHAR), field("owner_phone_numbers", new ArrayType(VARCHAR)), field("contacts", contactsType)));
        tester.testRoundTrip(getStandardStructObjectInspector(addressBookFieldNames,
                        asList(
                                javaStringObjectInspector,
                                getStandardListObjectInspector(javaStringObjectInspector),
                                getStandardListObjectInspector(
                                        getStandardStructObjectInspector(contactsFieldNames, asList(javaStringObjectInspector, javaStringObjectInspector))))),
                values, values, "address_book", addressBookType, Optional.of(parquetSchema));
    }

    @Test
    public void testSchemaWithOptionalOptionalRequiredFields()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_schema {" +
                "  optional group a {" +
                "    optional group b {" +
                "      optional group c {" +
                "        required binary d (UTF8);" +
                "      }" +
                "    }" +
                "  }" +
                "} ");
        Type cType = RowType.from(singletonList(field("d", VARCHAR)));
        Type bType = RowType.from(singletonList(field("c", cType)));
        Type aType = RowType.from(singletonList(field("b", bType)));
        Iterable<String> dValues = asList("d1", "d2", "d3", "d4", "d5", "d6", "d7");
        Iterable<List<?>> cValues = createNullableTestStructs(dValues);
        Iterable<List<?>> bValues = createNullableTestStructs(cValues);
        List<List<?>> aValues = createTestStructs(bValues);
        ObjectInspector cInspector = getStandardStructObjectInspector(singletonList("d"), singletonList(javaStringObjectInspector));
        ObjectInspector bInspector = getStandardStructObjectInspector(singletonList("c"), singletonList(cInspector));
        ObjectInspector aInspector = getStandardStructObjectInspector(singletonList("b"), singletonList(bInspector));
        tester.testRoundTrip(aInspector, aValues, aValues, "a", aType, Optional.of(parquetSchema));
    }

    @Test
    public void testSchemaWithOptionalRequiredOptionalFields()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_schema {" +
                "  optional group a {" +
                "    optional group b {" +
                "      required group c {" +
                "        optional int32 d;" +
                "      }" +
                "    }" +
                "  }" +
                "} ");
        Type cType = RowType.from(singletonList(field("d", INTEGER)));
        Type bType = RowType.from(singletonList(field("c", cType)));
        Type aType = RowType.from(singletonList(field("b", bType)));
        Iterable<Integer> dValues = asList(111, null, 333, 444, null, 666, 777);
        List<List<?>> cValues = createTestStructs(dValues);
        Iterable<List<?>> bValues = createNullableTestStructs(cValues);
        List<List<?>> aValues = createTestStructs(bValues);
        ObjectInspector cInspector = getStandardStructObjectInspector(singletonList("d"), singletonList(javaIntObjectInspector));
        ObjectInspector bInspector = getStandardStructObjectInspector(singletonList("c"), singletonList(cInspector));
        ObjectInspector aInspector = getStandardStructObjectInspector(singletonList("b"), singletonList(bInspector));
        tester.testRoundTrip(aInspector, aValues, aValues, "a", aType, Optional.of(parquetSchema));
    }

    @Test
    public void testSchemaWithRequiredRequiredOptionalFields()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_schema {" +
                "  optional group a {" +
                "    required group b {" +
                "      required group c {" +
                "        optional int32 d;" +
                "      }" +
                "    }" +
                "  }" +
                "} ");
        Type cType = RowType.from(singletonList(field("d", INTEGER)));
        Type bType = RowType.from(singletonList(field("c", cType)));
        Type aType = RowType.from(singletonList(field("b", bType)));
        Iterable<Integer> dValues = asList(111, null, 333, 444, null, 666, 777);
        List<List<?>> cValues = createTestStructs(dValues);
        List<List<?>> bValues = createTestStructs(cValues);
        List<List<?>> aValues = createTestStructs(bValues);
        ObjectInspector cInspector = getStandardStructObjectInspector(singletonList("d"), singletonList(javaIntObjectInspector));
        ObjectInspector bInspector = getStandardStructObjectInspector(singletonList("c"), singletonList(cInspector));
        ObjectInspector aInspector = getStandardStructObjectInspector(singletonList("b"), singletonList(bInspector));
        tester.testRoundTrip(aInspector, aValues, aValues, "a", aType, Optional.of(parquetSchema));
    }

    @Test
    public void testSchemaWithRequiredOptionalOptionalFields()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_schema {" +
                "  optional group a {" +
                "    required group b {" +
                "      optional group c {" +
                "        optional int32 d;" +
                "      }" +
                "    }" +
                "  }" +
                "} ");
        Type cType = RowType.from(singletonList(field("d", INTEGER)));
        Type bType = RowType.from(singletonList(field("c", cType)));
        Type aType = RowType.from(singletonList(field("b", bType)));
        Iterable<Integer> dValues = asList(111, null, 333, 444, null, 666, 777);
        Iterable<List<?>> cValues = createNullableTestStructs(dValues);
        List<List<?>> bValues = createTestStructs(cValues);
        List<List<?>> aValues = createTestStructs(bValues);
        ObjectInspector cInspector = getStandardStructObjectInspector(singletonList("d"), singletonList(javaIntObjectInspector));
        ObjectInspector bInspector = getStandardStructObjectInspector(singletonList("c"), singletonList(cInspector));
        ObjectInspector aInspector = getStandardStructObjectInspector(singletonList("b"), singletonList(bInspector));
        tester.testRoundTrip(aInspector, aValues, aValues, "a", aType, Optional.of(parquetSchema));
    }

    @Test
    public void testSchemaWithRequiredOptionalRequiredFields()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_schema {" +
                "  optional group a {" +
                "    required group b {" +
                "      optional group c {" +
                "        required binary d (UTF8);" +
                "      }" +
                "    }" +
                "  }" +
                "} ");
        Type cType = RowType.from(singletonList(field("d", VARCHAR)));
        Type bType = RowType.from(singletonList(field("c", cType)));
        Type aType = RowType.from(singletonList(field("b", bType)));
        Iterable<String> dValues = asList("d1", "d2", "d3", "d4", "d5", "d6", "d7");
        Iterable<List<?>> cValues = createNullableTestStructs(dValues);
        List<List<?>> bValues = createTestStructs(cValues);
        List<List<?>> aValues = createTestStructs(bValues);
        ObjectInspector cInspector = getStandardStructObjectInspector(singletonList("d"), singletonList(javaStringObjectInspector));
        ObjectInspector bInspector = getStandardStructObjectInspector(singletonList("c"), singletonList(cInspector));
        ObjectInspector aInspector = getStandardStructObjectInspector(singletonList("b"), singletonList(bInspector));
        tester.testRoundTrip(aInspector, aValues, aValues, "a", aType, Optional.of(parquetSchema));
    }

    @Test
    public void testSchemaWithRequiredStruct()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_schema {" +
                "  required group a {" +
                "    required group b {" +
                "        required binary c (UTF8);" +
                "        required int32 d;" +
                "    }" +
                "    required binary e (UTF8);" +
                "  }" +
                "} ");
        Type bType = RowType.from(asList(field("c", VARCHAR), field("d", INTEGER)));
        Type aType = RowType.from(asList(field("b", bType), field("e", VARCHAR)));
        Iterable<String> cValues = limit(cycle(asList("c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7")), 30000);
        Iterable<Integer> dValues = intsBetween(0, 30000);
        Iterable<String> eValues = limit(cycle(asList("e0", "e1", "e2", "e3", "e4", "e5", "e6", "e7")), 30000);
        List<List<?>> bValues = createTestStructs(cValues, dValues);
        List<List<?>> aValues = createTestStructs(bValues, eValues);
        ObjectInspector bInspector = getStandardStructObjectInspector(asList("c", "d"), asList(javaStringObjectInspector, javaIntObjectInspector));
        ObjectInspector aInspector = getStandardStructObjectInspector(asList("b", "e"), asList(bInspector, javaStringObjectInspector));
        tester.assertRoundTrip(aInspector, aValues, aValues, "a", aType, Optional.of(parquetSchema));
    }

    @Test
    public void testSchemaWithRequiredOptionalRequired2Fields()
            throws Exception
    {
        MessageType parquetSchema = parseMessageType("message hive_schema {" +
                "  optional group a {" +
                "    required group b {" +
                "      optional group c {" +
                "        required binary d (UTF8);" +
                "      }" +
                "    }" +
                "  }" +
                "  optional group e {" +
                "    required group f {" +
                "      optional group g {" +
                "        required binary h (UTF8);" +
                "      }" +
                "    }" +
                "  }" +
                "} ");

        Type cType = RowType.from(singletonList(field("d", VARCHAR)));
        Type bType = RowType.from(singletonList(field("c", cType)));
        Type aType = RowType.from(singletonList(field("b", bType)));
        Iterable<String> dValues = asList("d1", "d2", "d3", "d4", "d5", "d6", "d7");
        Iterable<List<?>> cValues = createNullableTestStructs(dValues);
        List<List<?>> bValues = createTestStructs(cValues);
        List<List<?>> aValues = createTestStructs(bValues);

        Type gType = RowType.from(singletonList(field("h", VARCHAR)));
        Type fType = RowType.from(singletonList(field("g", gType)));
        Type eType = RowType.from(singletonList(field("f", fType)));
        Iterable<String> hValues = asList("h1", "h2", "h3", "h4", "h5", "h6", "h7");
        Iterable<List<?>> gValues = createNullableTestStructs(hValues);
        List<List<?>> fValues = createTestStructs(gValues);
        List<List<?>> eValues = createTestStructs(fValues);

        ObjectInspector cInspector = getStandardStructObjectInspector(singletonList("d"), singletonList(javaStringObjectInspector));
        ObjectInspector bInspector = getStandardStructObjectInspector(singletonList("c"), singletonList(cInspector));
        ObjectInspector aInspector = getStandardStructObjectInspector(singletonList("b"), singletonList(bInspector));
        ObjectInspector gInspector = getStandardStructObjectInspector(singletonList("h"), singletonList(javaStringObjectInspector));
        ObjectInspector fInspector = getStandardStructObjectInspector(singletonList("g"), singletonList(gInspector));
        ObjectInspector eInspector = getStandardStructObjectInspector(singletonList("f"), singletonList(fInspector));
        tester.testRoundTrip(asList(aInspector, eInspector),
                new Iterable<?>[] {aValues, eValues}, new Iterable<?>[] {aValues, eValues},
                asList("a", "e"), asList(aType, eType), Optional.of(parquetSchema), ParquetSchemaOptions.defaultOptions());
    }

    @Test
    public void testOldAvroArray()
            throws Exception
    {
        MessageType parquetMrAvroSchema = parseMessageType("message avro_schema_old {" +
                "  optional group my_list (LIST){" +
                "        repeated int32 array;" +
                "  }" +
                "} ");
        Iterable<List<Integer>> nonNullArrayElements = createTestArrays(intsBetween(0, 31_234));
        tester.testRoundTrip(
                getStandardListObjectInspector(javaIntObjectInspector),
                nonNullArrayElements,
                nonNullArrayElements,
                "my_list",
                new ArrayType(INTEGER),
                Optional.of(parquetMrAvroSchema),
                ParquetSchemaOptions.withSingleLevelArray());
    }

    @Test
    public void testNewAvroArray()
            throws Exception
    {
        MessageType parquetMrAvroSchema = parseMessageType("message avro_schema_new { " +
                "  optional group my_list (LIST) { " +
                "    repeated group list { " +
                "      optional int32 element; " +
                "    } " +
                "  } " +
                "}");
        Iterable<List<Integer>> values = createTestArrays(limit(cycle(asList(1, null, 3, 5, null, null, null, 7, 11, null, 13, 17)), 30_000));
        tester.testRoundTrip(getStandardListObjectInspector(javaIntObjectInspector), values, values, "my_list", new ArrayType(INTEGER), Optional.of(parquetMrAvroSchema));
    }

    /**
     * Test reading various arrays schemas compatible with spec
     * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
     */
    @Test
    public void testArraySchemas()
            throws Exception
    {
        MessageType parquetMrNullableSpecSchema = parseMessageType("message hive_schema {" +
                "  optional group my_list (LIST){" +
                "    repeated group list {" +
                "        required int32 element;" +
                "    }" +
                "  }" +
                "} ");
        Iterable<List<Integer>> nonNullArrayElements = createTestArrays(intsBetween(0, 31_234));
        tester.testRoundTrip(getStandardListObjectInspector(javaIntObjectInspector), nonNullArrayElements, nonNullArrayElements, "my_list", new ArrayType(INTEGER), Optional.of(parquetMrNullableSpecSchema));

        MessageType parquetMrNonNullSpecSchema = parseMessageType("message hive_schema {" +
                "  required group my_list (LIST){" +
                "    repeated group list {" +
                "        optional int32 element;" +
                "    }" +
                "  }" +
                "} ");
        Iterable<List<Integer>> values = createTestArrays(limit(cycle(asList(1, null, 3, 5, null, null, null, 7, 11, null, 13, 17)), 30_000));
        tester.assertRoundTrip(
                getStandardListObjectInspector(javaIntObjectInspector),
                values,
                values,
                "my_list",
                new ArrayType(INTEGER),
                Optional.of(parquetMrNonNullSpecSchema));

        // this style of schema is also written by the trino optimized parquet writer
        MessageType sparkSchema = parseMessageType("message hive_schema {" +
                "  optional group my_list (LIST){" +
                "    repeated group list {" +
                "        optional int32 element;" +
                "    }" +
                "  }" +
                "} ");
        tester.testRoundTrip(getStandardListObjectInspector(javaIntObjectInspector), values, values, "my_list", new ArrayType(INTEGER), Optional.of(sparkSchema));

        MessageType hiveSchema = parseMessageType("message hive_schema {" +
                "  optional group my_list (LIST){" +
                "    repeated group bag {" +
                "        optional int32 array_element;" +
                "    }" +
                "  }" +
                "} ");
        tester.testRoundTrip(getStandardListObjectInspector(javaIntObjectInspector), values, values, "my_list", new ArrayType(INTEGER), Optional.of(hiveSchema));

        MessageType customNamingSchema = parseMessageType("message hive_schema {" +
                "  optional group my_list (LIST){" +
                "    repeated group bag {" +
                "        optional int32 array;" +
                "    }" +
                "  }" +
                "} ");
        tester.testRoundTrip(getStandardListObjectInspector(javaIntObjectInspector), values, values, "my_list", new ArrayType(INTEGER), Optional.of(customNamingSchema));

        MessageType optimizedParquetWriterOldListSchema = parseMessageType("message trino_schema {" +
                "  optional group my_list (LIST){" +
                "    repeated group list {" +
                "        optional int32 array;" +
                "    }" +
                "  }" +
                "} ");
        tester.testRoundTrip(getStandardListObjectInspector(javaIntObjectInspector), values, values, "my_list", new ArrayType(INTEGER), Optional.of(optimizedParquetWriterOldListSchema));
    }

    /**
     * Test reading various maps schemas compatible with spec
     * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps
     */
    @Test
    public void testMapSchemas()
            throws Exception
    {
        Iterable<Map<String, Integer>> values = createTestMaps(transform(intsBetween(0, 100_000), Object::toString), intsBetween(0, 10_000));
        Iterable<Map<String, Integer>> nullableValues = createTestMaps(transform(intsBetween(0, 30_000), Object::toString), limit(cycle(asList(1, null, 3, 5, null, null, null, 7, 11, null, 13, 17)), 30_000));
        tester.testRoundTrip(getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector), values, values, mapType(VARCHAR, INTEGER));

        // Map<String, Integer> (nullable map, non-null values)
        MessageType map = parseMessageType("message hive_schema {" +
                " optional group my_map (MAP) {" +
                "     repeated group map { " +
                "        required binary str (UTF8);   " +
                "        required int32 num;  " +
                "    }  " +
                "  }" +
                "}   ");
        tester.testRoundTrip(getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector), values, values, "my_map", mapType(VARCHAR, INTEGER), Optional.of(map));

        // Map<String, Integer> (nullable map, non-null values)
        map = parseMessageType("message hive_schema {" +
                " optional group my_map (MAP_KEY_VALUE) {" +
                "     repeated group map { " +
                "        required binary str (UTF8);   " +
                "        required int32 num;  " +
                "    }  " +
                "  }" +
                "}   ");
        tester.testRoundTrip(getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector), values, values, "my_map", mapType(VARCHAR, INTEGER), Optional.of(map));

        // Map<String, Integer> (non-null map, nullable values)
        map = parseMessageType("message hive_schema {" +
                " required group my_map (MAP) { " +
                "    repeated group map {  " +
                "        required binary key (UTF8);      " +
                "       optional int32 value;   " +
                "    }   " +
                "  }" +
                " }  ");
        tester.assertRoundTrip(
                getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector),
                nullableValues,
                nullableValues,
                "my_map",
                mapType(VARCHAR, INTEGER),
                Optional.of(map));

        // Map<String, Integer> (non-null map, nullable values)
        map = parseMessageType("message hive_schema {" +
                " required group my_map (MAP_KEY_VALUE) { " +
                "    repeated group map {  " +
                "        required binary key (UTF8);      " +
                "       optional int32 value;   " +
                "    }   " +
                "  }" +
                " }  ");
        tester.assertRoundTrip(
                getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector),
                nullableValues,
                nullableValues,
                "my_map",
                mapType(VARCHAR, INTEGER),
                Optional.of(map));

        // Map<String, Integer> (non-null map, nullable values)
        map = parseMessageType("message hive_schema {" +
                " required group my_map (MAP) { " +
                "    repeated group map {  " +
                "        required binary key (UTF8);      " +
                "       required int32 value;   " +
                "    }   " +
                "  }" +
                " }  ");
        tester.assertRoundTrip(
                getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector),
                values,
                values,
                "my_map",
                mapType(VARCHAR, INTEGER),
                Optional.of(map));

        // Map<String, Integer> (non-null map, nullable values)
        map = parseMessageType("message hive_schema {" +
                " required group my_map (MAP_KEY_VALUE) { " +
                "    repeated group map {  " +
                "        required binary key (UTF8);      " +
                "       required int32 value;   " +
                "    }   " +
                "  }" +
                " }  ");
        tester.assertRoundTrip(
                getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector),
                values,
                values,
                "my_map",
                mapType(VARCHAR, INTEGER),
                Optional.of(map));

        // Map<String, Integer> (nullable map, nullable values)
        map = parseMessageType("message hive_schema {" +
                " optional group my_map (MAP) { " +
                "    repeated group map {  " +
                "       required binary key (UTF8);      " +
                "       optional int32 value;   " +
                "    }   " +
                "  }" +
                " }  ");
        tester.testRoundTrip(getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector), nullableValues, nullableValues, "my_map", mapType(VARCHAR, INTEGER), Optional.of(map));

        // Map<String, Integer> (nullable map, nullable values)
        map = parseMessageType("message hive_schema {" +
                " optional group my_map (MAP_KEY_VALUE) { " +
                "    repeated group map {  " +
                "       required binary key (UTF8);      " +
                "       optional int32 value;   " +
                "    }   " +
                "  }" +
                " }  ");
        tester.testRoundTrip(getStandardMapObjectInspector(javaStringObjectInspector, javaIntObjectInspector), nullableValues, nullableValues, "my_map", mapType(VARCHAR, INTEGER), Optional.of(map));
    }

    @Test
    public void testLongStrideDictionary()
            throws Exception
    {
        testRoundTripNumeric(concat(ImmutableList.of(1), Collections.nCopies(9999, 123), ImmutableList.of(2), Collections.nCopies(9999, 123)));
    }

    private void testRoundTripNumeric(Iterable<Integer> writeValues)
            throws Exception
    {
        tester.testRoundTrip(javaByteObjectInspector,
                transform(writeValues, AbstractTestParquetReader::intToByte),
                AbstractTestParquetReader::byteToInt,
                INTEGER);

        tester.testRoundTrip(javaShortObjectInspector,
                transform(writeValues, AbstractTestParquetReader::intToShort),
                AbstractTestParquetReader::shortToInt,
                INTEGER);

        tester.testRoundTrip(javaIntObjectInspector, writeValues, INTEGER);
        tester.testRoundTrip(javaLongObjectInspector, transform(writeValues, AbstractTestParquetReader::intToLong), BIGINT);
        // Add millis of a day to the writeValues to avoid creating illegal instant for small values due to time zone offset transition
        Iterable<Integer> timestampValues = transform(writeValues, value -> value + MILLISECONDS_PER_DAY);
        tester.testRoundTrip(javaTimestampObjectInspector,
                transform(timestampValues, AbstractTestParquetReader::intToTimestamp),
                transform(timestampValues, AbstractTestParquetReader::intToSqlTimestamp),
                TIMESTAMP_MILLIS);

        tester.testRoundTrip(javaDateObjectInspector,
                transform(writeValues, AbstractTestParquetReader::intToDate),
                transform(writeValues, AbstractTestParquetReader::intToSqlDate),
                DATE);
    }

    @Test
    public void testFloatSequence()
            throws Exception
    {
        Iterable<Float> writeValues = floatSequence(0.0f, 0.1f, 30_000);
        tester.testRoundTrip(javaFloatObjectInspector, writeValues, REAL);
        tester.testRoundTrip(javaFloatObjectInspector, writeValues, transform(writeValues, AbstractTestParquetReader::floatToDouble), DOUBLE);
    }

    @Test
    public void testFloatNaNInfinity()
            throws Exception
    {
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(1000.0f, -1.23f, Float.POSITIVE_INFINITY), REAL);
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(-1000.0f, Float.NEGATIVE_INFINITY, 1.23f), REAL);
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(0.0f, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY), REAL);

        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(Float.NaN, -0.0f, 1.0f), REAL);
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(Float.NaN, -1.0f, Float.POSITIVE_INFINITY), REAL);
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(Float.NaN, Float.NEGATIVE_INFINITY, 1.0f), REAL);
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY), REAL);

        Iterable<Float> writeValues = ImmutableList.of(Float.NaN, -1000.0f, -0.0f, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY);
        tester.testRoundTrip(javaFloatObjectInspector, writeValues, transform(writeValues, AbstractTestParquetReader::floatToDouble), DOUBLE);
    }

    @Test
    public void testDoubleSequence()
            throws Exception
    {
        tester.testRoundTrip(javaDoubleObjectInspector, doubleSequence(0, 0.1, 30_000), DOUBLE);
    }

    @Test
    public void testDoubleNaNInfinity()
            throws Exception
    {
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(1000.0, -1.0, Double.POSITIVE_INFINITY), DOUBLE);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(-1000.0, Double.NEGATIVE_INFINITY, 1.0), DOUBLE);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(0.0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY), DOUBLE);

        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(Double.NaN, -1.0, 1.0), DOUBLE);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(Double.NaN, -1.0, Double.POSITIVE_INFINITY), DOUBLE);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, 1.0), DOUBLE);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY), DOUBLE);
    }

    @Test
    public void testStringUnicode()
            throws Exception
    {
        Iterable<String> writeValues = limit(cycle(ImmutableList.of("apple", "apple pie", "apple\uD835\uDC03", "apple\uFFFD")), 30_000);
        tester.testRoundTrip(javaStringObjectInspector, writeValues, createUnboundedVarcharType());
        tester.testRoundTrip(javaStringObjectInspector, writeValues, createVarcharType(25));
        CharType charType = createCharType(25);
        tester.testRoundTrip(javaStringObjectInspector, writeValues, value -> Chars.padSpaces(value, charType), charType);
    }

    @Test
    public void testStringDirectSequence()
            throws Exception
    {
        Iterable<String> writeValues = transform(intsBetween(0, 30_000), Object::toString);
        tester.testRoundTrip(javaStringObjectInspector, writeValues, createUnboundedVarcharType());
        tester.testRoundTrip(javaStringObjectInspector, writeValues, createVarcharType(5));
        CharType charType = createCharType(5);
        tester.testRoundTrip(javaStringObjectInspector, writeValues, value -> Chars.padSpaces(value, charType), charType);
    }

    @Test
    public void testStringDictionarySequence()
            throws Exception
    {
        Iterable<String> writeValues = limit(cycle(transform(ImmutableList.of(1, 3, 5, 7, 11, 13, 17), Object::toString)), 30_000);
        tester.testRoundTrip(javaStringObjectInspector, writeValues, createUnboundedVarcharType());
        tester.testRoundTrip(javaStringObjectInspector, writeValues, createVarcharType(3));
        CharType charType = createCharType(3);
        tester.testRoundTrip(javaStringObjectInspector, writeValues, value -> Chars.padSpaces(value, charType), charType);
    }

    @Test
    public void testStringStrideDictionary()
            throws Exception
    {
        Iterable<String> writeValues = concat(ImmutableList.of("a"), Collections.nCopies(9999, "123"), ImmutableList.of("b"), Collections.nCopies(9999, "123"));
        tester.testRoundTrip(javaStringObjectInspector, writeValues, createUnboundedVarcharType());
        tester.testRoundTrip(javaStringObjectInspector, writeValues, createVarcharType(3));
        CharType charType = createCharType(3);
        tester.testRoundTrip(javaStringObjectInspector, writeValues, value -> Chars.padSpaces(value, charType), charType);
    }

    @Test
    public void testEmptyStringSequence()
            throws Exception
    {
        Iterable<String> writeValues = limit(cycle(""), 30_000);
        tester.testRoundTrip(javaStringObjectInspector, writeValues, createUnboundedVarcharType());
        tester.testRoundTrip(javaStringObjectInspector, writeValues, createVarcharType(3));
        CharType charType = createCharType(3);
        tester.testRoundTrip(javaStringObjectInspector, writeValues, value -> Chars.padSpaces(value, charType), charType);
    }

    @Test
    public void testBinaryDirectSequence()
            throws Exception
    {
        Iterable<byte[]> writeValues = transform(intsBetween(0, 30_000), compose(AbstractTestParquetReader::stringToByteArray, Object::toString));
        tester.testRoundTrip(javaByteArrayObjectInspector,
                writeValues,
                transform(writeValues, AbstractTestParquetReader::byteArrayToVarbinary),
                VARBINARY);
    }

    @Test
    public void testReadFixedWidthByteArrayAsVarBinary()
            throws Exception
    {
        Random random = new Random(2342890824L);
        int typeLength = 5;
        List<byte[]> writeValues = IntStream.range(0, 30_000)
                .mapToObj(i -> {
                    byte[] value = new byte[typeLength];
                    random.nextBytes(value);
                    return value;
                })
                .collect(toImmutableList());
        tester.testRoundTrip(
                javaByteArrayObjectInspector,
                writeValues,
                transform(writeValues, AbstractTestParquetReader::byteArrayToVarbinary),
                VARBINARY,
                Optional.of(parseMessageType(format("message varbinary { optional FIXED_LEN_BYTE_ARRAY(%s) test; }", typeLength))));
    }

    @Test
    public void testBinaryDictionarySequence()
            throws Exception
    {
        Iterable<byte[]> writeValues = limit(cycle(transform(ImmutableList.of(1, 3, 5, 7, 11, 13, 17), compose(AbstractTestParquetReader::stringToByteArray, Object::toString))), 30_000);
        Iterable<SqlVarbinary> readValues = transform(writeValues, AbstractTestParquetReader::byteArrayToVarbinary);

        tester.testRoundTrip(
                javaByteArrayObjectInspector,
                writeValues,
                readValues,
                VARBINARY);

        tester.testMaxReadBytes(
                javaByteArrayObjectInspector,
                writeValues,
                readValues,
                VARBINARY,
                DataSize.ofBytes(1_000));
    }

    @Test
    public void testEmptyBinarySequence()
            throws Exception
    {
        tester.testRoundTrip(javaByteArrayObjectInspector, limit(cycle(List.of(new byte[0])), 30_000), AbstractTestParquetReader::byteArrayToVarbinary, VARBINARY);
    }

    private static <T> Iterable<T> skipEvery(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;

            @Override
            protected T computeNext()
            {
                while (true) {
                    if (!delegate.hasNext()) {
                        return endOfData();
                    }

                    T next = delegate.next();
                    position++;
                    if (position <= n) {
                        return next;
                    }
                    position = 0;
                }
            }
        };
    }

    @Test
    public void testStructMaxReadBytes()
            throws Exception
    {
        DataSize maxReadBlockSize = DataSize.ofBytes(1_000);
        List<List<?>> structValues = createTestStructs(
                Collections.nCopies(500, join("", Collections.nCopies(33, "test"))),
                Collections.nCopies(500, join("", Collections.nCopies(1, "test"))));
        List<String> structFieldNames = asList("a", "b");
        Type structType = RowType.from(asList(field("a", VARCHAR), field("b", VARCHAR)));

        tester.testMaxReadBytes(
                getStandardStructObjectInspector(structFieldNames, asList(javaStringObjectInspector, javaStringObjectInspector)),
                structValues,
                structValues,
                structType,
                maxReadBlockSize);
    }

    @Test
    public void testArrayMaxReadBytes()
            throws Exception
    {
        DataSize maxReadBlockSize = DataSize.ofBytes(1_000);
        Iterable<List<Integer>> values = createFixedTestArrays(limit(cycle(asList(1, null, 3, 5, null, null, null, 7, 11, null, 13, 17)), 30_000));
        tester.testMaxReadBytes(getStandardListObjectInspector(javaIntObjectInspector), values, values, new ArrayType(INTEGER), maxReadBlockSize);
    }

    @Test
    public void testMapMaxReadBytes()
            throws Exception
    {
        DataSize maxReadBlockSize = DataSize.ofBytes(1_000);
        Iterable<Map<String, Long>> values = createFixedTestMaps(Collections.nCopies(5_000, join("", Collections.nCopies(33, "test"))), longsBetween(0, 5_000));
        tester.testMaxReadBytes(getStandardMapObjectInspector(javaStringObjectInspector, javaLongObjectInspector), values, values, mapType(VARCHAR, BIGINT), maxReadBlockSize);
    }

    private static <T> Iterable<T> repeatEach(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;
            private T value;

            @Override
            protected T computeNext()
            {
                if (position == 0) {
                    if (!delegate.hasNext()) {
                        return endOfData();
                    }
                    value = delegate.next();
                }

                position++;
                if (position >= n) {
                    position = 0;
                }
                return value;
            }
        };
    }

    private static Iterable<Float> floatSequence(double start, double step, int items)
    {
        return transform(doubleSequence(start, step, items), input -> {
            if (input == null) {
                return null;
            }
            return input.floatValue();
        });
    }

    private static Iterable<Double> doubleSequence(double start, double step, int items)
    {
        return () -> new AbstractSequentialIterator<>(start)
        {
            private int item;

            @Override
            protected Double computeNext(Double previous)
            {
                if (item >= items) {
                    return null;
                }
                item++;
                return previous + step;
            }
        };
    }

    private static ContiguousSet<Integer> intsBetween(int lowerInclusive, int upperExclusive)
    {
        return ContiguousSet.create(Range.closedOpen(lowerInclusive, upperExclusive), DiscreteDomain.integers());
    }

    private static ContiguousSet<Long> longsBetween(long lowerInclusive, long upperExclusive)
    {
        return ContiguousSet.create(Range.closedOpen(lowerInclusive, upperExclusive), DiscreteDomain.longs());
    }

    private <F> List<List<?>> createTestStructs(Iterable<F> fieldValues)
    {
        checkArgument(fieldValues.iterator().hasNext(), "struct field values cannot be empty");
        List<List<?>> structs = new ArrayList<>();
        for (F field : fieldValues) {
            structs.add(singletonList(field));
        }
        return structs;
    }

    private List<List<?>> createTestStructs(Iterable<?>... values)
    {
        List<List<?>> structs = new ArrayList<>();
        List<Iterator<?>> iterators = Arrays.stream(values).map(Iterable::iterator).collect(Collectors.toList());
        iterators.forEach(iter -> checkArgument(iter.hasNext(), "struct field values cannot be empty"));
        while (iterators.stream().allMatch(Iterator::hasNext)) {
            structs.add(iterators.stream().map(Iterator::next).collect(Collectors.toList()));
        }
        return structs;
    }

    private Iterable<List<?>> createNullableTestStructs(Iterable<?>... values)
    {
        return insertNullEvery(ThreadLocalRandom.current().nextInt(2, 5), createTestStructs(values));
    }

    private <T> List<List<T>> createTestArrays(Iterable<T> values)
    {
        List<List<T>> arrays = new ArrayList<>();
        Iterator<T> valuesIter = values.iterator();
        List<T> array = new ArrayList<>();
        while (valuesIter.hasNext()) {
            if (ThreadLocalRandom.current().nextBoolean()) {
                arrays.add(array);
                array = new ArrayList<>();
            }
            if (ThreadLocalRandom.current().nextInt(10) == 0) {
                arrays.add(Collections.emptyList());
            }
            array.add(valuesIter.next());
        }
        return arrays;
    }

    private <T> Iterable<List<T>> createNullableTestArrays(Iterable<T> values)
    {
        return insertNullEvery(ThreadLocalRandom.current().nextInt(2, 5), createTestArrays(values));
    }

    private <T> List<List<T>> createFixedTestArrays(Iterable<T> values)
    {
        List<List<T>> arrays = new ArrayList<>();
        Iterator<T> valuesIter = values.iterator();
        List<T> array = new ArrayList<>();
        int count = 1;
        while (valuesIter.hasNext()) {
            if (count % 10 == 0) {
                arrays.add(array);
                array = new ArrayList<>();
            }
            if (count % 20 == 0) {
                arrays.add(Collections.emptyList());
            }
            array.add(valuesIter.next());
            ++count;
        }
        return arrays;
    }

    private <K, V> Iterable<Map<K, V>> createFixedTestMaps(Iterable<K> keys, Iterable<V> values)
    {
        List<Map<K, V>> maps = new ArrayList<>();
        Iterator<K> keysIterator = keys.iterator();
        Iterator<V> valuesIterator = values.iterator();
        Map<K, V> map = new HashMap<>();
        int count = 1;
        while (keysIterator.hasNext() && valuesIterator.hasNext()) {
            if (count % 5 == 0) {
                maps.add(map);
                map = new HashMap<>();
            }
            if (count % 10 == 0) {
                maps.add(Collections.emptyMap());
            }
            map.put(keysIterator.next(), valuesIterator.next());
            ++count;
        }
        return maps;
    }

    private <K, V> Iterable<Map<K, V>> createTestMaps(Iterable<K> keys, Iterable<V> values)
    {
        List<Map<K, V>> maps = new ArrayList<>();
        Iterator<K> keysIterator = keys.iterator();
        Iterator<V> valuesIterator = values.iterator();
        Map<K, V> map = new HashMap<>();
        while (keysIterator.hasNext() && valuesIterator.hasNext()) {
            if (ThreadLocalRandom.current().nextInt(5) == 0) {
                maps.add(map);
                map = new HashMap<>();
            }
            if (ThreadLocalRandom.current().nextInt(10) == 0) {
                maps.add(Collections.emptyMap());
            }
            map.put(keysIterator.next(), valuesIterator.next());
        }
        return maps;
    }

    private <K, V> Iterable<Map<K, V>> createNullableTestMaps(Iterable<K> keys, Iterable<V> values)
    {
        return insertNullEvery(ThreadLocalRandom.current().nextInt(2, 5), createTestMaps(keys, values));
    }

    private static Byte intToByte(Integer input)
    {
        if (input == null) {
            return null;
        }
        return (byte) (input & 0xFF);
    }

    private static Short intToShort(Integer input)
    {
        if (input == null) {
            return null;
        }
        return Shorts.checkedCast(input);
    }

    private static Integer byteToInt(Byte input)
    {
        return toInteger(input);
    }

    private static Integer shortToInt(Short input)
    {
        return toInteger(input);
    }

    private static Long intToLong(Integer input)
    {
        return toLong(input);
    }

    private static <N extends Number> Integer toInteger(N input)
    {
        if (input == null) {
            return null;
        }
        return input.intValue();
    }

    private static <N extends Number> Long toLong(N input)
    {
        if (input == null) {
            return null;
        }
        return input.longValue();
    }

    private static byte[] stringToByteArray(String input)
    {
        return input.getBytes(UTF_8);
    }

    private static SqlVarbinary byteArrayToVarbinary(byte[] input)
    {
        if (input == null) {
            return null;
        }
        return new SqlVarbinary(input);
    }

    private static Timestamp intToTimestamp(Integer epochMillis)
    {
        if (epochMillis == null) {
            return null;
        }
        return longToTimestamp(Long.valueOf(epochMillis));
    }

    private static Timestamp longToTimestamp(Long epochMillis)
    {
        if (epochMillis == null) {
            return null;
        }
        long seconds = floorDiv(epochMillis, MILLISECONDS_PER_SECOND);
        int nanos = floorMod(epochMillis, MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND;
        return Timestamp.ofEpochSecond(seconds, nanos);
    }

    private static SqlTimestamp intToSqlTimestamp(Integer input)
    {
        if (input == null) {
            return null;
        }
        return sqlTimestampOf((long) input);
    }

    private static Date intToDate(Integer input)
    {
        if (input == null) {
            return null;
        }
        return Date.ofEpochDay(input);
    }

    private static SqlDate intToSqlDate(Integer input)
    {
        if (input == null) {
            return null;
        }
        return new SqlDate(input);
    }

    private static Double floatToDouble(Float input)
    {
        if (input == null) {
            return null;
        }
        return Double.valueOf(input);
    }
}
