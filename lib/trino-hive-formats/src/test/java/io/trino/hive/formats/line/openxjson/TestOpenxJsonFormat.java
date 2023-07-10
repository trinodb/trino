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
package io.trino.hive.formats.line.openxjson;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.trino.hive.formats.FormatTestUtils;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.hive.formats.line.LineSerializer;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.openx.data.jsonserde.JsonSerDe;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.LongFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.hive.formats.FormatTestUtils.assertColumnValueEquals;
import static io.trino.hive.formats.FormatTestUtils.assertColumnValuesEquals;
import static io.trino.hive.formats.FormatTestUtils.createLineBuffer;
import static io.trino.hive.formats.FormatTestUtils.decodeRecordReaderValue;
import static io.trino.hive.formats.FormatTestUtils.isScalarType;
import static io.trino.hive.formats.FormatTestUtils.readTrinoValues;
import static io.trino.hive.formats.FormatTestUtils.toHiveWriteValue;
import static io.trino.hive.formats.FormatTestUtils.toSingleRowPage;
import static io.trino.hive.formats.FormatTestUtils.toSqlTimestamp;
import static io.trino.hive.formats.line.openxjson.OpenXJsonOptions.DEFAULT_OPEN_X_JSON_OPTIONS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.toIntExact;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

public class TestOpenxJsonFormat
{
    static {
        // Increase the level of the JsonSerDe logger as it is excessively logs
        Logger.getLogger(JsonSerDe.class.getName()).setLevel(Level.SEVERE);
    }

    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private static final DecimalType SHORT_DECIMAL = createDecimalType(MAX_SHORT_PRECISION, 2);
    private static final DecimalType LONG_DECIMAL = createDecimalType(MAX_PRECISION, 2);

    @Test
    public void testLine()
    {
        List<Column> columns = ImmutableList.of(
                new Column("a", DOUBLE, 0),
                new Column("b", VARCHAR, 1));

        // if line does not start with '{' or '[', after trim, all values are null
        assertLine(columns, "", Arrays.asList(null, null), DEFAULT_OPEN_X_JSON_OPTIONS);
        assertLine(columns, "null", Arrays.asList(null, null), DEFAULT_OPEN_X_JSON_OPTIONS);
        assertLine(columns, "123", Arrays.asList(null, null), DEFAULT_OPEN_X_JSON_OPTIONS);
        assertLine(columns, "#", Arrays.asList(null, null), DEFAULT_OPEN_X_JSON_OPTIONS);

        // leading and trailing whitespace is ignored
        assertLine(columns, "   {\"a\":1.23}  ", Arrays.asList(1.23, null), DEFAULT_OPEN_X_JSON_OPTIONS);
        assertLine(columns, "   [1.23]  ", Arrays.asList(1.23, null), DEFAULT_OPEN_X_JSON_OPTIONS);

        // trailing junk is ignored
        assertLine(columns, "   {\"a\":1.23}anything here", Arrays.asList(1.23, null), DEFAULT_OPEN_X_JSON_OPTIONS);
        assertLine(columns, "   [1.23]anything here", Arrays.asList(1.23, null), DEFAULT_OPEN_X_JSON_OPTIONS);

        // malformed lines resul in a failure, unless ignore malformed is enabled
        OpenXJsonOptions ignoreMalformed = OpenXJsonOptions.builder().ignoreMalformedJson().build();
        assertLineFails(columns, "[", DEFAULT_OPEN_X_JSON_OPTIONS);
        assertLine(columns, "[", Arrays.asList(null, null, null), ignoreMalformed);
        assertLineFails(columns, "{", DEFAULT_OPEN_X_JSON_OPTIONS);
        assertLine(columns, "{", Arrays.asList(null, null, null), ignoreMalformed);
    }

    @Test
    public void testExplicitNulls()
    {
        OpenXJsonOptions explicitNulls = OpenXJsonOptions.builder().explicitNull().build();
        List<Column> columns = ImmutableList.of(
                new Column("a", DOUBLE, 0),
                new Column("b", rowType(field("x", BOOLEAN), field("y", VARCHAR)), 1),
                new Column("c", new MapType(BIGINT, DOUBLE, TYPE_OPERATORS), 1));
        // all columns nulls
        assertExactLine(columns, Arrays.asList(null, null, null), "{}", DEFAULT_OPEN_X_JSON_OPTIONS);
        assertExactLine(columns, Arrays.asList(null, null, null), "{\"a\":null,\"b\":null,\"c\":null}", explicitNulls);
        // single primitive non-null column
        assertExactLine(columns, Arrays.asList(4.56, null, null), "{\"a\":4.56}", DEFAULT_OPEN_X_JSON_OPTIONS);
        assertExactLine(columns, Arrays.asList(4.56, null, null), "{\"a\":4.56,\"b\":null,\"c\":null}", explicitNulls);
        // empty row
        assertExactLine(columns, Arrays.asList(null, Arrays.asList(null, null), null), "{\"b\":{}}", DEFAULT_OPEN_X_JSON_OPTIONS);
        assertExactLine(columns, Arrays.asList(null, Arrays.asList(null, null), null), "{\"a\":null,\"b\":{\"x\":null,\"y\":null},\"c\":null}", explicitNulls);
        // single value in row
        assertExactLine(columns, Arrays.asList(null, Arrays.asList(true, null), null), "{\"b\":{\"x\":true}}", DEFAULT_OPEN_X_JSON_OPTIONS);
        assertExactLine(columns, Arrays.asList(null, Arrays.asList(true, null), null), "{\"a\":null,\"b\":{\"x\":true,\"y\":null},\"c\":null}", explicitNulls);
        // empty map
        assertExactLine(columns, Arrays.asList(null, null, emptyMap()), "{\"c\":{}}", DEFAULT_OPEN_X_JSON_OPTIONS);
        assertExactLine(columns, Arrays.asList(null, null, emptyMap()), "{\"a\":null,\"b\":null,\"c\":{}}", explicitNulls);
        // map with null value (Starburst does not write null values)
        assertThat(writeTrinoLine(columns, Arrays.asList(null, null, singletonMap(42L, null)), DEFAULT_OPEN_X_JSON_OPTIONS))
                .isEqualTo("{\"c\":{\"42\":null}}");
        assertThat(writeTrinoLine(columns, Arrays.asList(null, null, singletonMap(42L, null)), explicitNulls))
                .isEqualTo("{\"a\":null,\"b\":null,\"c\":{\"42\":null}}");
    }

    @Test
    public void testCaseInsensitive()
    {
        OpenXJsonOptions caseSensitive = OpenXJsonOptions.builder().caseSensitive().build();
        // this is only valid if we do not lower keys like the starburst version does
        assertValue(VARCHAR, "{\"FOO\":42}", "{\"FOO\":42}", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValue(VARCHAR, "{\"FOO\":42}", "{\"FOO\":42}", caseSensitive, false);
        // case-insensitive eliminates duplicates in maps keys that only differ in case
        assertValue(VARCHAR, "{\"FOO\":42,\"FoO\":42}", "{\"FoO\":42}", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValue(VARCHAR, "{\"FOO\":42,\"FoO\":42}", "{\"FOO\":42,\"FoO\":42}", caseSensitive, false);

        // top level column
        assertLine(ImmutableList.of(new Column("foo", BOOLEAN, 0)), "{\"FoO\":true}", singletonList(true), DEFAULT_OPEN_X_JSON_OPTIONS);
        assertLine(ImmutableList.of(new Column("foo", BOOLEAN, 0)), "{\"FoO\":true}", singletonList(null), caseSensitive);

        // row value
        assertValue(rowType(field("a", rowType(field("foo", BOOLEAN)))), "{\"a\":{\"FoO\":true}}", singletonList(singletonList(true)), DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValue(rowType(field("a", rowType(field("foo", BOOLEAN)))), "{\"a\":{\"FoO\":true}}", singletonList(singletonList(null)), caseSensitive, false);

        // multiple levels
        assertValue(rowType(field("a", rowType(field("foo", BOOLEAN)))), "{\"A\":{\"FoO\":true}}", singletonList(singletonList(true)), DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValue(rowType(field("a", rowType(field("foo", BOOLEAN)))), "{\"A\":{\"FoO\":true}}", singletonList(null), caseSensitive, false);
    }

    @Test
    public void testDotsInFieldNames()
    {
        OpenXJsonOptions dotsInFieldNames = OpenXJsonOptions.builder().dotsInFieldNames().build();

        // top level column
        assertLine(ImmutableList.of(new Column("a_b", BIGINT, 0)), "{\"a.b\":42}", singletonList(null), DEFAULT_OPEN_X_JSON_OPTIONS);
        assertLine(ImmutableList.of(new Column("a_b", BIGINT, 0)), "{\"a.b\":42}", singletonList(42L), dotsInFieldNames);
        // todo Starburst is always case-sensitive for dotted names
        internalAssertLineHive(ImmutableList.of(new Column("a_b", BIGINT, 0)), "{\"a.B\":42}", singletonList(null), dotsInFieldNames);
        internalAssertLineTrino(ImmutableList.of(new Column("a_b", BIGINT, 0)), "{\"a.B\":42}", singletonList(42L), dotsInFieldNames);

        // row value
        assertValue(rowType(field("a", rowType(field("x_y", BIGINT)))), "{\"a\":{\"x.y\":42}}", singletonList(singletonList(null)), DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValue(rowType(field("a", rowType(field("x_y", BIGINT)))), "{\"a\":{\"x.y\":42}}", singletonList(singletonList(42L)), dotsInFieldNames, false);
        // todo Starburst is always case-sensitive for dotted names
        internalAssertValueHive(rowType(field("a", rowType(field("x_y", BIGINT)))), "{\"a\":{\"X.y\":42}}", singletonList(singletonList(null)), dotsInFieldNames);
        internalAssertValueTrino(rowType(field("a", rowType(field("x_y", BIGINT)))), "{\"a\":{\"X.y\":42}}", singletonList(singletonList(42L)), dotsInFieldNames);

        // multiple levels
        assertValue(rowType(field("a_b", rowType(field("x_y", BIGINT)))), "{\"a.b\":{\"x.y\":42}}", singletonList(null), DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValue(rowType(field("a_b", rowType(field("x_y", BIGINT)))), "{\"a.b\":{\"x.y\":42}}", singletonList(singletonList(42L)), dotsInFieldNames, false);

        // field mappings are not considered for dotted names
        OpenXJsonOptions mappedFieldNames = OpenXJsonOptions.builder(dotsInFieldNames)
                .addFieldMapping("apple", "a_b")
                .build();
        assertLine(ImmutableList.of(new Column("apple", BIGINT, 0)), "{\"a.b\":42}", singletonList(null), mappedFieldNames);
        assertLine(ImmutableList.of(new Column("a_b", BIGINT, 0)), "{\"a.b\":42}", singletonList(42L), mappedFieldNames);
    }

    @Test
    public void testMappedFieldNames()
    {
        OpenXJsonOptions mappedFieldNames = OpenXJsonOptions.builder()
                .addFieldMapping("apple", "a")
                .addFieldMapping("banana", "b")
                .addFieldMapping("cherry", "c")
                .build();
        OpenXJsonOptions caseSensitiveMapping = OpenXJsonOptions.builder(mappedFieldNames)
                .caseSensitive()
                .build();

        // top level column
        assertLine(ImmutableList.of(new Column("apple", BIGINT, 0)), "{\"a\":42}", singletonList(null), DEFAULT_OPEN_X_JSON_OPTIONS);
        assertLine(ImmutableList.of(new Column("apple", BIGINT, 0)), "{\"a\":42}", singletonList(42L), mappedFieldNames);
        // mappings follow case sensitivity
        internalAssertLineTrino(ImmutableList.of(new Column("apple", BIGINT, 0)), "{\"A\":42}", singletonList(42L), mappedFieldNames);
        internalAssertLineTrino(ImmutableList.of(new Column("apple", BIGINT, 0)), "{\"A\":42}", singletonList(null), caseSensitiveMapping);
        // declared mappings are case-sensitive
        assertLine(ImmutableList.of(new Column("Apple", BIGINT, 0)), "{\"a\":42}", singletonList(42L), mappedFieldNames);

        // row value
        assertValue(rowType(field("x", rowType(field("banana", BIGINT)))), "{\"x\":{\"b\":42}}", singletonList(singletonList(null)), DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValue(rowType(field("x", rowType(field("banana", BIGINT)))), "{\"x\":{\"b\":42}}", singletonList(singletonList(42L)), mappedFieldNames, false);
        internalAssertValueTrino(rowType(field("x", rowType(field("banana", BIGINT)))), "{\"x\":{\"B\":42}}", singletonList(singletonList(42L)), mappedFieldNames);
        internalAssertValueTrino(rowType(field("x", rowType(field("Banana", BIGINT)))), "{\"x\":{\"B\":42}}", singletonList(singletonList(null)), caseSensitiveMapping);

        // multiple levels
        assertValue(rowType(field("apple", rowType(field("banana", BIGINT)))), "{\"a\":{\"b\":42}}", singletonList(null), DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValue(rowType(field("apple", rowType(field("banana", BIGINT)))), "{\"a\":{\"b\":42}}", singletonList(singletonList(42L)), mappedFieldNames, false);
        assertValue(rowType(field("apple", rowType(field("banana", BIGINT)))), "{\"a\":{\"B\":42}}", singletonList(singletonList(42L)), mappedFieldNames, false);
        assertValue(rowType(field("apple", rowType(field("banana", BIGINT)))), "{\"a\":{\"B\":42}}", singletonList(singletonList(null)), caseSensitiveMapping, false);
    }

    @Test
    public void testRow()
    {
        RowType rowType = rowType(field("a", BIGINT), field("b", BIGINT), field("c", BIGINT));

        assertValue(rowType, "null", null);

        // string containing only whitespace is null
        assertValue(rowType, "\"\"", Arrays.asList(null, null, null));
        assertValue(rowType, "\"    \"", Arrays.asList(null, null, null));
        assertValue(rowType, "\"  \t\t  \"", Arrays.asList(null, null, null));

        assertValue(rowType, "{ \"a\" : 1, \"b\" : 2, \"c\" : 3 }", ImmutableList.of(1L, 2L, 3L));
        assertValue(rowType, "{ \"c\" : 3, \"a\" : 1, \"b\" : 2 }", ImmutableList.of(1L, 2L, 3L));
        assertValue(rowType, "{ \"x\" : 3, \"c\" : 3, \"a\" : 1, \"b\" : 2 , \"y\" : 2 }", ImmutableList.of(1L, 2L, 3L));
        assertValue(rowType, "{}", Arrays.asList(null, null, null));
        assertValue(rowType, "{ \"b\" : 2 }", Arrays.asList(null, 2L, null));

        // Duplicate fields are supported, and the last value is used
        assertValue(rowType, "{ \"a\" : 1, \"a\" : 2 }", Arrays.asList(2L, null, null));
        // and we only parse the last field
        assertValue(rowType, "{ \"a\" : true, \"a\" : 42 }", Arrays.asList(42L, null, null));

        // OpenX JsonSerDe supports arrays using column ordinal position
        assertValue(rowType, "[ 1, 2, 3 ]", Arrays.asList(1L, 2L, 3L));
        assertValue(rowType, "[ 1, , 3 ]", Arrays.asList(1L, null, 3L));
        assertValue(rowType, "[ 1, , 3, ]", Arrays.asList(1L, null, 3L));

        assertValueFails(rowType, "true");
        assertValueFails(rowType, "12");
        assertValueFails(rowType, "12.34");
        assertValueFails(rowType, "\"string\"");
    }

    @Test
    public void testMap()
    {
        MapType mapType = new MapType(VARCHAR, BIGINT, TYPE_OPERATORS);
        assertValue(mapType, "null", null);

        // string containing only whitespace is null
        assertValue(mapType, "\"\"", null);
        assertValue(mapType, "\"    \"", null);
        assertValue(mapType, "\"  \t\t  \"", null);

        assertValue(mapType, "{}", ImmutableMap.of());
        assertValue(
                mapType,
                "{ \"a\" : 1, \"b\" : 2, \"c\" : 3 }",
                ImmutableMap.builder()
                        .put("a", 1L)
                        .put("b", 2L)
                        .put("c", 3L)
                        .buildOrThrow());
        assertValue(
                mapType,
                "{ \"c\" : 3, \"a\" : 1, \"b\" : 2 }",
                ImmutableMap.builder()
                        .put("a", 1L)
                        .put("b", 2L)
                        .put("c", 3L)
                        .buildOrThrow());

        // Duplicate fields are supported, and the last value is used
        assertValue(
                mapType,
                "{ \"a\" : 1, \"b\" : 2 , \"a\" : 3 , \"b\" : 4 }",
                ImmutableMap.builder()
                        .put("a", 3L)
                        .put("b", 4L)
                        .buildOrThrow());

        assertValueFails(mapType, "true");
        assertValueFails(mapType, "12");
        assertValueFails(mapType, "12.34");
        assertValueFails(mapType, "\"string\"");
        assertValueFails(mapType, "[ 42 ]");
    }

    @Test
    public void testMapWithContainerKey()
    {
        // Generally containers can not be used for key, because there are limited
        // coercions from primitive values to container types.

        MapType arrayKey = new MapType(new ArrayType(VARCHAR), BIGINT, TYPE_OPERATORS);
        assertValue(arrayKey, "null", null);
        assertValue(arrayKey, "{}", ImmutableMap.of());
        // empty string is coerced to null
        assertValue(arrayKey, "{\"\":42}", ImmutableMap.of(), false);
        // single values are automatically wrapped in a single element array
        assertValue(arrayKey, "{\"a\":42}", ImmutableMap.of(List.of("a"), 42L), false);
        assertValue(new MapType(new ArrayType(BOOLEAN), BIGINT, TYPE_OPERATORS), "{\"true\":42}", ImmutableMap.of(List.of(true), 42L), false);

        MapType mapKey = new MapType(new MapType(VARCHAR, VARCHAR, TYPE_OPERATORS), BIGINT, TYPE_OPERATORS);
        assertValue(mapKey, "null", null);
        assertValue(mapKey, "{}", ImmutableMap.of());
        // strings containing only white space are coerced to null
        assertValue(mapKey, "{\"\":42}", ImmutableMap.of(), false);
        assertValue(mapKey, "{\" \t\t  \":42}", ImmutableMap.of(), false);

        MapType rowKey = new MapType(rowType(field("a", BIGINT), field("b", BIGINT)), BIGINT, TYPE_OPERATORS);
        assertValue(rowKey, "null", null);
        assertValue(rowKey, "{}", ImmutableMap.of());
        // strings containing only white space are coerced to null
        assertValue(rowKey, "{\"\":42}", ImmutableMap.of(Arrays.asList(null, null), 42L), false);
        assertValue(rowKey, "{\" \t\t  \":42}", ImmutableMap.of(Arrays.asList(null, null), 42L), false);
    }

    @Test
    public void testVarchar()
    {
        testString(VARCHAR, createVarcharType(3));
        testString(createVarcharType(100), createVarcharType(3));
    }

    @Test
    public void testChar()
    {
        testString(createCharType(100), createCharType(3));
    }

    private static void testString(Type unboundedType, Type boundedType)
    {
        assertValue(unboundedType, "null", null);

        assertString(unboundedType, "");
        assertString(unboundedType, "  ");

        assertString(unboundedType, "value");
        assertString(unboundedType, "     value");
        assertString(unboundedType, "value     ");
        assertString(unboundedType, "     value     ");

        // Truncation
        assertString(boundedType, "v");
        assertString(boundedType, "val");
        assertString(boundedType, "value", "val");

        // Escape
        assertString(unboundedType, "tab \\t tab", "tab \t tab");
        assertString(unboundedType, "new \\n line", "new \n line");
        assertString(unboundedType, "carriage \\r return", "carriage \r return");

        assertVarcharCanonicalization(unboundedType, "true", "true", "true");
        assertVarcharCanonicalization(unboundedType, "false", "false", "false");
        assertVarcharCanonicalization(unboundedType, "tRUe", "true", "true");
        assertVarcharCanonicalization(unboundedType, "fAlSe", "false", "false");

        assertVarcharCanonicalization(unboundedType, "-1", "-1", "-1");
        assertVarcharCanonicalization(unboundedType, "1.23", "1.23", "1.23");
        assertVarcharCanonicalization(unboundedType, "1.23e45", "1.23E+45", "1.23E45");
        assertVarcharCanonicalization(unboundedType, "33.23e45", "3.323E+46", "3.323E46");
        assertVarcharCanonicalization(unboundedType, "1.23E45", "1.23E+45", "1.23E45");
        assertVarcharCanonicalization(unboundedType, "33.23E45", "3.323E+46", "3.323E46");
        assertString(unboundedType, "NaN", "NaN");
        assertString(unboundedType, "Infinity", "Infinity");
        assertString(unboundedType, "+Infinity", "+Infinity");
        assertString(unboundedType, "-Infinity", "-Infinity");

        assertStringObjectOrArrayCoercion(unboundedType, "[ \"value\" ]", "[\"value\"]");
        assertStringObjectOrArrayCoercion(unboundedType, "[ \"foo\"; TrUe]", "[\"foo\",true]");

        assertStringObjectOrArrayCoercion(unboundedType, "{ \"x\" = \"value\" }", "{\"x\":\"value\"}");
        assertStringObjectOrArrayCoercion(unboundedType, "{ \"foo\" => TrUe }", "{\"foo\":true}");
    }

    private static void assertString(Type type, String jsonValue)
    {
        assertString(type, jsonValue, jsonValue);
    }

    private static void assertString(Type type, String jsonValue, String expectedValue)
    {
        if (type instanceof CharType charType) {
            expectedValue = padSpaces(expectedValue, charType);
        }

        if (!jsonValue.startsWith("[") && !jsonValue.startsWith("{")) {
            assertValue(type, "\"" + jsonValue + "\"", expectedValue);
            if (!jsonValue.isEmpty() && CharMatcher.whitespace().matchesNoneOf(jsonValue)) {
                assertValue(type, jsonValue, expectedValue);
            }
        }
        else {
            assertValue(type, jsonValue, expectedValue, false);
        }
    }

    private static void assertStringObjectOrArrayCoercion(Type type, String jsonValue, String expectedValue)
    {
        if (type instanceof CharType charType) {
            expectedValue = padSpaces(expectedValue, charType);
        }

        if (type == VARCHAR) {
            assertValue(type, jsonValue, expectedValue, false);
        }
        else {
            // Starburst code can not handle JSON array and map coercions for bounded VARCHAR or CHAR types
            assertValueTrino(type, jsonValue, expectedValue, DEFAULT_OPEN_X_JSON_OPTIONS, false);
            assertValueFailsHive(type, jsonValue, DEFAULT_OPEN_X_JSON_OPTIONS, false);
        }
    }

    private static void assertVarcharCanonicalization(Type type, String jsonValue, String trinoCanonicalValue, String hiveCanonicalValue)
    {
        String nonCanonicalValue = jsonValue;
        if (type instanceof CharType charType) {
            nonCanonicalValue = padSpaces(nonCanonicalValue, charType);
            trinoCanonicalValue = padSpaces(trinoCanonicalValue, charType);
            hiveCanonicalValue = padSpaces(hiveCanonicalValue, charType);
        }

        assertTrue(CharMatcher.whitespace().matchesNoneOf(jsonValue));

        // quoted values are not canonicalized
        assertValue(type, "\"" + jsonValue + "\"", nonCanonicalValue);

        // field names are not canonicalized
        internalAssertValueTrino(new MapType(type, BIGINT, TYPE_OPERATORS), "{" + jsonValue + ":43}", singletonMap(nonCanonicalValue, 43L), DEFAULT_OPEN_X_JSON_OPTIONS);
        // starburst version does not allow for non-canonical field names, but original version does
        assertValueFailsHive(new MapType(type, BIGINT, TYPE_OPERATORS), "{" + jsonValue + ":43}", DEFAULT_OPEN_X_JSON_OPTIONS, false);

        // unquoted values are canonicalized (using slightly different rules)
        internalAssertValueTrino(type, jsonValue, trinoCanonicalValue, DEFAULT_OPEN_X_JSON_OPTIONS);
        internalAssertValueTrino(new ArrayType(type), "[" + jsonValue + "]", singletonList(trinoCanonicalValue), DEFAULT_OPEN_X_JSON_OPTIONS);
        internalAssertValueTrino(new MapType(BIGINT, type, TYPE_OPERATORS), "{43:" + jsonValue + "}", singletonMap(43L, trinoCanonicalValue), DEFAULT_OPEN_X_JSON_OPTIONS);

        if (type instanceof VarcharType varcharType && varcharType.isUnbounded()) {
            internalAssertValueHive(type, jsonValue, hiveCanonicalValue, DEFAULT_OPEN_X_JSON_OPTIONS);
            internalAssertValueHive(new ArrayType(type), "[" + jsonValue + "]", singletonList(hiveCanonicalValue), DEFAULT_OPEN_X_JSON_OPTIONS);
            internalAssertValueHive(new MapType(BIGINT, type, TYPE_OPERATORS), "{\"43\":" + jsonValue + "}", singletonMap(43L, hiveCanonicalValue), DEFAULT_OPEN_X_JSON_OPTIONS);
        }
        else {
            // varchar and char type in Starburst doesn't support canonicalization (the code above uses Hive string type)
            assertValueFailsHive(type, jsonValue, DEFAULT_OPEN_X_JSON_OPTIONS, false);
            assertValueFailsHive(new ArrayType(type), "[" + jsonValue + "]", DEFAULT_OPEN_X_JSON_OPTIONS, false);
            assertValueFailsHive(new MapType(BIGINT, type, TYPE_OPERATORS), "{\"43\":" + jsonValue + "}", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        }
    }

    @Test
    public void testVarbinary()
    {
        assertValue(VARBINARY, "null", null);

        byte[] allBytes = new byte[255];
        for (int i = 0; i < allBytes.length; i++) {
            allBytes[i] = (byte) (Byte.MIN_VALUE + i);
        }
        String base64 = Base64.getEncoder().encodeToString(allBytes);
        assertValue(
                VARBINARY,
                "\"" + base64 + "\"",
                new SqlVarbinary(allBytes));

        // all other json fails
        assertValueFails(VARBINARY, "true", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValueFails(VARBINARY, "false", DEFAULT_OPEN_X_JSON_OPTIONS, false);

        assertValueFails(VARBINARY, "-1", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValueFails(VARBINARY, "1.23", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValueFails(VARBINARY, "1.23e45", DEFAULT_OPEN_X_JSON_OPTIONS, false);

        assertValueFails(VARBINARY, "\"value\"", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValueFails(VARBINARY, "[ \"value\" ]", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValueFails(VARBINARY, "{ \"x\" : \"value\" }", DEFAULT_OPEN_X_JSON_OPTIONS, false);
    }

    @Test
    public void testBoolean()
    {
        assertValue(BOOLEAN, "null", null);

        assertValue(BOOLEAN, "true", true);
        assertValue(BOOLEAN, "false", false);

        assertValue(BOOLEAN, "\"true\"", true);
        assertValue(BOOLEAN, "\"tRuE\"", true);
        assertValue(BOOLEAN, "\"unknown\"", false);
        assertValue(BOOLEAN, "\"null\"", false);

        assertValue(BOOLEAN, "-1", false);
        assertValue(BOOLEAN, "0", false);
        assertValue(BOOLEAN, "1", false);
        assertValue(BOOLEAN, "1.23", false);
        assertValue(BOOLEAN, "1.23e45", false);

        assertValue(BOOLEAN, "invalid", false);
        assertValue(BOOLEAN, "[]", false);
        assertValue(BOOLEAN, "[ 123 ]", false);
        assertValue(BOOLEAN, "[ true ]", false);
        assertValue(BOOLEAN, "[ \"true\" ]", false, false);
        assertValue(BOOLEAN, "{ \"x\" : false }", false, false);
    }

    @Test
    public void testBigint()
    {
        testIntegralNumber(BIGINT, Long.MAX_VALUE, Long.MIN_VALUE, value -> value);
    }

    @Test
    public void testInteger()
    {
        testIntegralNumber(INTEGER, Integer.MAX_VALUE, Integer.MIN_VALUE, Ints::checkedCast);
    }

    @Test
    public void testSmallInt()
    {
        testIntegralNumber(SMALLINT, Short.MAX_VALUE, Short.MIN_VALUE, Shorts::checkedCast);
    }

    @Test
    public void testTinyint()
    {
        testIntegralNumber(TINYINT, Byte.MAX_VALUE, Byte.MIN_VALUE, SignedBytes::checkedCast);
    }

    private static void testIntegralNumber(Type type, long maxValue, long minValue, LongFunction<? extends Number> coercion)
    {
        assertValue(type, "null", null);

        assertNumber(type, "0", coercion.apply(0L));
        assertNumber(type, "-0", coercion.apply(0L));
        assertNumber(type, "+0", coercion.apply(0L));
        assertNumber(type, "1", coercion.apply(1L));
        assertNumber(type, "+1", coercion.apply(1L));
        assertNumber(type, "-1", coercion.apply(-1L));
        assertNumber(type, String.valueOf(maxValue), coercion.apply(maxValue));
        assertNumber(type, "+" + maxValue, coercion.apply(maxValue));
        assertNumber(type, String.valueOf(minValue), coercion.apply(minValue));

        assertNumberOutOfBounds(type, BigInteger.valueOf(maxValue).add(BigInteger.ONE).toString(), coercion.apply(minValue));
        assertNumberOutOfBounds(type, BigInteger.valueOf(minValue).subtract(BigInteger.ONE).toString(), coercion.apply(maxValue));

        // Decimal values are truncated
        assertNumber(type, "1.23", coercion.apply(1L), true);
        assertNumber(type, "1.56", coercion.apply(1L), true);
        assertNumber(type, maxValue + ".9999", coercion.apply(maxValue), true);
        assertNumber(type, minValue + ".9999", coercion.apply(minValue), true);

        // Exponents are expanded, and decimals are truncated
        assertNumber(type, "1.2345e2", coercion.apply(123L), true);
        assertNumber(type, "%1.18e".formatted(new BigDecimal(maxValue)).trim(), coercion.apply(maxValue), true);
        assertNumber(type, "%+1.18e".formatted(new BigDecimal(maxValue)).trim(), coercion.apply(maxValue), true);
        assertNumber(type, "%1.18e".formatted(new BigDecimal(minValue)).trim(), coercion.apply(minValue), true);
        assertNumberOutOfBounds(type, "%1.18e".formatted(new BigDecimal(maxValue).add(BigDecimal.ONE)).trim(), coercion.apply(minValue));
        assertNumberOutOfBounds(type, "%1.18e".formatted(new BigDecimal(minValue).subtract(BigDecimal.ONE)).trim(), coercion.apply(maxValue));

        // Hex is supported
        assertNumber(type, "0x0", coercion.apply(0));
        assertNumber(type, "0x1", coercion.apply(1));
        assertNumber(type, "0x" + Long.toUnsignedString(maxValue, 16), coercion.apply(maxValue));
        // But negative values are not allowed
        assertInvalidNumber(type, "0x8000000000000000");
        assertInvalidNumber(type, "0xFFFFFFFFFFFFFFFF");
        assertInvalidNumber(type, "0x" + Long.toUnsignedString(minValue, 16));

        // Octal is supported
        assertNumber(type, "00", coercion.apply(0));
        assertNumber(type, "01", coercion.apply(1));
        assertNumber(type, "0" + Long.toUnsignedString(maxValue, 8), coercion.apply(maxValue), true);
        // But negative values are not allowed
        // Only test with Trino here, as Hive handling of octal is very broken
        assertValueTrino(type, "01777777777777777777777", null, DEFAULT_OPEN_X_JSON_OPTIONS, true);
        assertValueTrino(type, "07777777777777777777777", null, DEFAULT_OPEN_X_JSON_OPTIONS, true);
        assertValueTrino(type, "0" + Long.toUnsignedString(minValue, 8), null, DEFAULT_OPEN_X_JSON_OPTIONS, true);

        // all other string values are invalid
        assertInvalidNumber(type, "\"\"");
        assertInvalidNumber(type, "invalid");
        assertInvalidNumber(type, "true");
        assertInvalidNumber(type, "false");
        assertInvalidNumber(type, "\"null\"");

        // array and object cause failures
        assertValueFails(type, "[ 42 ]", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValueFails(type, "{ \"x\" : 42 }", DEFAULT_OPEN_X_JSON_OPTIONS, false);
    }

    private static void assertNumber(Type type, String jsonValue, Number expectedValue)
    {
        assertNumber(type, jsonValue, expectedValue, false);
    }

    private static void assertNumber(Type type, String jsonValue, Number expectedValue, boolean hiveQuotedValueFails)
    {
        assertValueTrino(type, jsonValue, expectedValue, DEFAULT_OPEN_X_JSON_OPTIONS, true);
        assertValueTrino(type, "\"" + jsonValue + "\"", expectedValue, DEFAULT_OPEN_X_JSON_OPTIONS, true);

        assertValueHive(type, jsonValue, expectedValue, DEFAULT_OPEN_X_JSON_OPTIONS, !hiveQuotedValueFails);
        if (hiveQuotedValueFails) {
            assertValueFailsHive(type, "\"" + jsonValue + "\"", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        }
        else {
            assertValueHive(type, "\"" + jsonValue + "\"", expectedValue, DEFAULT_OPEN_X_JSON_OPTIONS, true);
        }
    }

    private static void assertNumberOutOfBounds(Type type, String jsonValue, Number hiveExpectedValue)
    {
        assertValueTrino(type, jsonValue, null, DEFAULT_OPEN_X_JSON_OPTIONS, true);
        assertValueTrino(type, "\"" + jsonValue + "\"", null, DEFAULT_OPEN_X_JSON_OPTIONS, true);

        Object hiveActualValue = readHiveValue(type, jsonValue, DEFAULT_OPEN_X_JSON_OPTIONS);
        if (!hiveActualValue.equals(hiveExpectedValue)) {
            // Hive reads values using double which will lose precision, so Integer.MAX_VALUE + 1 round trips
            assertThat(hiveActualValue).isInstanceOf(Integer.class);
            if (hiveExpectedValue.intValue() < 0) {
                assertThat(hiveActualValue).isEqualTo(Integer.MAX_VALUE);
            }
            else {
                assertThat(hiveActualValue).isEqualTo(Integer.MIN_VALUE);
            }
        }
    }

    private static void assertInvalidNumber(Type type, String jsonValue)
    {
        assertValueTrino(type, jsonValue, null, DEFAULT_OPEN_X_JSON_OPTIONS, true);
        if (!jsonValue.startsWith("\"")) {
            assertValueTrino(type, "\"" + jsonValue + "\"", null, DEFAULT_OPEN_X_JSON_OPTIONS, true);
        }

        assertValueFailsHive(type, jsonValue, DEFAULT_OPEN_X_JSON_OPTIONS, false);
        if (!jsonValue.startsWith("\"")) {
            assertValueFailsHive(type, "\"" + jsonValue + "\"", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        }
    }

    @Test
    public void testDecimalShort()
    {
        assertValue(SHORT_DECIMAL, "10000000000000000.00", null);
        assertValue(SHORT_DECIMAL, "null", null);

        // allowed range for JsonSerDe
        assertDecimal(SHORT_DECIMAL, "0");
        assertDecimal(SHORT_DECIMAL, "1");
        assertDecimal(SHORT_DECIMAL, "-1");
        assertDecimal(SHORT_DECIMAL, "9999999999999999.99");
        assertDecimal(SHORT_DECIMAL, "-9999999999999999.99");

        assertDecimal(SHORT_DECIMAL, "1.2345e2");
        assertDecimal(SHORT_DECIMAL, "1.5645e15");

        // Hive does not enforce size bounds
        assertValue(SHORT_DECIMAL, "10000000000000000.00", null);
        assertValue(SHORT_DECIMAL, "-10000000000000000.00", null);
        assertValue(SHORT_DECIMAL, "1e19", null);
        assertValue(SHORT_DECIMAL, "-1e19", null);

        // test rounding
        DecimalType roundingType = createDecimalType(4, 2);
        assertValue(roundingType, "10.001", SqlDecimal.decimal("10.00", roundingType));
        assertValue(roundingType, "10.005", SqlDecimal.decimal("10.01", roundingType));
        assertValue(roundingType, "99.999", null);

        assertValue(SHORT_DECIMAL, "invalid", null);
        assertValue(SHORT_DECIMAL, "true", null);
        assertValue(SHORT_DECIMAL, "false", null);
        assertValue(SHORT_DECIMAL, "\"string\"", null);
        assertValue(SHORT_DECIMAL, "\"null\"", null);

        assertValueFailsTrino(SHORT_DECIMAL, "[ 42 ]", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValueFailsTrino(SHORT_DECIMAL, "{ \"x\" : 42 }", DEFAULT_OPEN_X_JSON_OPTIONS, false);
    }

    @Test
    public void testDecimalLong()
    {
        assertValue(LONG_DECIMAL, "null", null);

        // allowed range for JsonSerDe
        assertDecimal(LONG_DECIMAL, "0");
        assertDecimal(LONG_DECIMAL, "1");
        assertDecimal(LONG_DECIMAL, "-1");
        assertDecimal(LONG_DECIMAL, "9999999999999999.99");
        assertDecimal(LONG_DECIMAL, "-9999999999999999.99");
        assertDecimal(LONG_DECIMAL, "10000000000000000.00");
        assertDecimal(LONG_DECIMAL, "-10000000000000000.00");
        assertDecimal(LONG_DECIMAL, "999999999999999999999999999999999999.99");
        assertDecimal(LONG_DECIMAL, "-999999999999999999999999999999999999.99");

        assertDecimal(LONG_DECIMAL, "1.2345e2");
        assertDecimal(LONG_DECIMAL, "1.5645e15");
        assertDecimal(LONG_DECIMAL, "1.5645e35");

        // Hive does not enforce size bounds
        assertValue(LONG_DECIMAL, "1000000000000000000000000000000000000.00", null);
        assertValue(LONG_DECIMAL, "-1000000000000000000000000000000000000.00", null);
        assertValue(LONG_DECIMAL, "1e39", null);
        assertValue(LONG_DECIMAL, "-1e39", null);

        // test rounding (Hive doesn't seem to enforce scale)
        DecimalType roundingType = createDecimalType(38, 2);
        assertValue(roundingType, "10.001", SqlDecimal.decimal("10.00", roundingType));
        assertValue(roundingType, "10.005", SqlDecimal.decimal("10.01", roundingType));

        assertValue(LONG_DECIMAL, "invalid", null);
        assertValue(LONG_DECIMAL, "true", null);
        assertValue(LONG_DECIMAL, "false", null);
        assertValue(LONG_DECIMAL, "\"string\"", null);
        assertValue(LONG_DECIMAL, "\"null\"", null);

        assertValueFailsTrino(LONG_DECIMAL, "[ 42 ]", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValueFailsTrino(LONG_DECIMAL, "{ \"x\" : 42 }", DEFAULT_OPEN_X_JSON_OPTIONS, false);
    }

    private static void assertDecimal(DecimalType decimalType, String jsonValue)
    {
        SqlDecimal expectedValue = toSqlDecimal(decimalType, jsonValue);
        assertValue(decimalType, jsonValue, expectedValue);
        // value can be passed as a JSON string
        assertValue(decimalType, "\"" + jsonValue + "\"", expectedValue);
    }

    private static SqlDecimal toSqlDecimal(DecimalType decimalType, String expectedValueString)
    {
        BigDecimal bigDecimal = new BigDecimal(expectedValueString);
        BigDecimal newBigDecimal = Decimals.rescale(bigDecimal, decimalType);
        SqlDecimal expectedValue = new SqlDecimal(newBigDecimal.unscaledValue(), decimalType.getPrecision(), decimalType.getScale());
        return expectedValue;
    }

    @Test
    public void testReal()
    {
        assertValue(REAL, "null", null);

        // allowed range for JsonSerDe
        assertValue(REAL, "0", 0.0f);
        assertValue(REAL, "123", 123.0f);
        assertValue(REAL, "-123", -123.0f);
        assertValue(REAL, "1.23", 1.23f);
        assertValue(REAL, "-1.23", -1.23f);
        assertValue(REAL, "1.5645e33", 1.5645e33f);

        assertValue(REAL, "NaN", Float.NaN);
        assertValue(REAL, "Infinity", Float.POSITIVE_INFINITY);
        assertValue(REAL, "+Infinity", Float.POSITIVE_INFINITY);
        assertValue(REAL, "-Infinity", Float.NEGATIVE_INFINITY);
        assertValueTrinoOnly(REAL, "+Inf", null);
        assertValueTrinoOnly(REAL, "-Inf", null);

        assertValueTrinoOnly(REAL, "invalid", null);
        assertValueTrinoOnly(REAL, "true", null);
        assertValueTrinoOnly(REAL, "false", null);
        assertValue(REAL, "\"123.45\"", 123.45f);
        assertValueTrinoOnly(REAL, "\"string\"", null);
        assertValueTrinoOnly(REAL, "\"null\"", null);

        assertValueFails(REAL, "[ 42 ]", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValueFails(REAL, "{ \"x\" : 42 }", DEFAULT_OPEN_X_JSON_OPTIONS, false);
    }

    @Test
    public void testDouble()
    {
        assertValue(DOUBLE, "null", null);

        // allowed range for JsonSerDe
        assertValue(DOUBLE, "0", 0.0);
        assertValueTrino(DOUBLE, "-0", -0.0, DEFAULT_OPEN_X_JSON_OPTIONS, true);
        assertValue(DOUBLE, "123", 123.0);
        assertValue(DOUBLE, "-123", -123.0);
        assertValue(DOUBLE, "1.23", 1.23);
        assertValue(DOUBLE, "-1.23", -1.23);
        assertValue(DOUBLE, "1.5645e33", 1.5645e33);

        assertValue(DOUBLE, "NaN", Double.NaN);
        assertValue(DOUBLE, "Infinity", Double.POSITIVE_INFINITY);
        assertValue(DOUBLE, "+Infinity", Double.POSITIVE_INFINITY);
        assertValue(DOUBLE, "-Infinity", Double.NEGATIVE_INFINITY);
        assertValueTrinoOnly(DOUBLE, "+Inf", null);
        assertValueTrinoOnly(DOUBLE, "-Inf", null);

        assertValueTrinoOnly(DOUBLE, "invalid", null);
        assertValueTrinoOnly(DOUBLE, "true", null);
        assertValueTrinoOnly(DOUBLE, "false", null);
        assertValue(DOUBLE, "\"123.45\"", 123.45);
        assertValueTrinoOnly(DOUBLE, "\"string\"", null);
        assertValueTrinoOnly(DOUBLE, "\"null\"", null);

        assertValueFails(DOUBLE, "[ 42 ]", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValueFails(DOUBLE, "{ \"x\" : 42 }", DEFAULT_OPEN_X_JSON_OPTIONS, false);
    }

    @Test
    public void testDate()
    {
        assertValue(DATE, "null", null);

        // allowed range for JsonSerDe
        assertDate("\"1970-01-01\"", 0);
        assertDate("\"1970-01-02\"", 1);
        assertDate("\"1969-12-31\"", -1);

        // Hive ignores everything after the first space
        assertDate("\"1986-01-01 anything is allowed here\"", LocalDate.of(1986, 1, 1).toEpochDay());

        assertDate("\"1986-01-01\"", LocalDate.of(1986, 1, 1).toEpochDay());
        assertDate("\"1986-01-33\"", LocalDate.of(1986, 2, 2).toEpochDay());

        assertDate("\"5881580-07-11\"", Integer.MAX_VALUE);
        assertDate("\"-5877641-06-23\"", Integer.MIN_VALUE);

        // Hive does not enforce size bounds and truncates the results in Date.toEpochDay
        assertValueTrinoOnly(DATE, "\"5881580-07-12\"", null);
        assertValueTrinoOnly(DATE, "\"-5877641-06-22\"", null);

        // numbers are translated into epoch days
        assertDate("0", 0, false);
        assertDate("1", 1, false);
        assertDate("-1", -1, false);
        assertDate("123", 123, false);
        assertDate(String.valueOf(Integer.MAX_VALUE), Integer.MAX_VALUE, false);
        assertDate(String.valueOf(Integer.MIN_VALUE), Integer.MIN_VALUE, false);

        // hex
        assertDate("0x0", 0, false);
        assertDate("0x1", 1, false);
        assertDate("0x123", 0x123, false);
        assertDate("0x" + Long.toUnsignedString(Integer.MAX_VALUE, 16), Integer.MAX_VALUE, false);

        // octal
        assertDate("00", 0, false);
        assertDate("01", 1, false);
        assertDate("0123", 83, false);
        assertDate("0" + Long.toUnsignedString(Integer.MAX_VALUE, 8), Integer.MAX_VALUE, false);

        // out of bounds
        assertValueTrinoOnly(DATE, String.valueOf(Integer.MAX_VALUE + 1L), null);
        assertValueTrinoOnly(DATE, String.valueOf(Integer.MIN_VALUE - 1L), null);

        // unsupported values are null
        assertValueTrinoOnly(DATE, "1.23", null);
        assertValueTrinoOnly(DATE, "1.2345e2", null);
        assertValueTrinoOnly(DATE, "1.56", null);
        assertValueTrinoOnly(DATE, "1.5645e2", null);
        assertValueTrinoOnly(DATE, "1.5645e300", null);

        assertValueTrinoOnly(DATE, "true", null);
        assertValueTrinoOnly(DATE, "false", null);
        assertValueTrinoOnly(DATE, "\"string\"", null);
        assertValueTrinoOnly(DATE, "\"null\"", null);

        assertValueFails(DATE, "[ 42 ]", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValueFails(DATE, "{ \"x\" : 42 }", DEFAULT_OPEN_X_JSON_OPTIONS, false);
    }

    private static void assertDate(String jsonValue, long days)
    {
        assertDate(jsonValue, days, true);
    }

    private static void assertDate(String jsonValue, long days, boolean testHiveMapKey)
    {
        Object expectedValue = new SqlDate(toIntExact(days));
        assertValueHive(DATE, jsonValue, expectedValue, DEFAULT_OPEN_X_JSON_OPTIONS, testHiveMapKey);
        assertValueTrino(DATE, jsonValue, expectedValue, DEFAULT_OPEN_X_JSON_OPTIONS, true);
    }

    @Test
    public void testTimestamp()
    {
        testTimestamp(TIMESTAMP_NANOS);
        testTimestamp(TIMESTAMP_MICROS);
        testTimestamp(TIMESTAMP_MILLIS);
        testTimestamp(TIMESTAMP_SECONDS);
    }

    private static void testTimestamp(TimestampType timestampType)
    {
        assertValue(timestampType, "null", null);

        // Standard SQL format
        assertTimestamp(timestampType, "2020-05-10 12:34:56.123456789", LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_789));
        assertTrinoTimestamp(timestampType, "2020-5-6 7:8:9.123456789", LocalDateTime.of(2020, 5, 6, 7, 8, 9, 123_456_789));
        assertTimestamp(timestampType, "2020-05-10 12:34:56.123456", LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_000));
        assertTimestamp(timestampType, "2020-05-10 12:34:56.123", LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_000_000));
        assertTimestamp(timestampType, "2020-05-10 12:34:56", LocalDateTime.of(2020, 5, 10, 12, 34, 56));
        assertInvalidTimestamp(timestampType, "2020-05-10 12:34");
        assertInvalidTimestamp(timestampType, "2020-05-10 12");
        assertInvalidTimestamp(timestampType, "2020-05-10");

        // Lenient timestamp parsing is used and values wrap around
        assertTrinoTimestamp(timestampType, "2020-13-10 12:34:56.123456789", LocalDateTime.of(2021, 1, 10, 12, 34, 56, 123_456_789));
        assertTrinoTimestamp(timestampType, "2020-05-35 12:34:56.123456789", LocalDateTime.of(2020, 6, 4, 12, 34, 56, 123_456_789));
        assertTrinoTimestamp(timestampType, "2020-05-10 12:65:56.123456789", LocalDateTime.of(2020, 5, 10, 13, 5, 56, 123_456_789));

        assertTimestamp(timestampType, "1970-01-01 00:00:00.000000000", LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0));
        assertTimestamp(timestampType, "1960-05-10 12:34:56.123456789", LocalDateTime.of(1960, 5, 10, 12, 34, 56, 123_456_789));

        // test bounds
        assertTimestamp(timestampType,
                "294247-01-10 04:00:54" + truncateNanosFor(timestampType, ".775807999"),
                LocalDateTime.of(294247, 1, 10, 4, 0, 54, truncateNanosFor(timestampType, 775_807_999)));
        assertTrinoTimestamp(timestampType, "-290308-12-21 19:59:06.224192000", LocalDateTime.of(-290308, 12, 21, 19, 59, 6, 224_192_000));

        assertInvalidTimestamp(timestampType, "294247-01-10 04:00:54.775808000");
        assertInvalidTimestamp(timestampType, "-290308-12-21 19:59:05.224192000");

        // custom formats
        assertTimestamp(
                timestampType,
                "05/10/2020 12.34.56.123",
                LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_000_000),
                "MM/dd/yyyy HH.mm.ss.SSS");
        assertTimestamp(
                timestampType,
                "10.05.2020 12:34",
                LocalDateTime.of(2020, 5, 10, 12, 34, 0, 0),
                "dd.MM.yyyy HH:mm");
        assertTimestamp(
                timestampType,
                "05/10/2020 12.34.56.123",
                LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_000_000),
                "yyyy",
                "MM/dd/yyyy HH.mm.ss.SSS",
                "dd.MM.yyyy HH:mm");
        assertTimestamp(
                timestampType,
                "10.05.2020 12:34",
                LocalDateTime.of(2020, 5, 10, 12, 34, 0, 0),
                "yyyy",
                "MM/dd/yyyy HH.mm.ss.SSS",
                "dd.MM.yyyy HH:mm");

        // Default timestamp formats are always supported when formats are supplied
        assertTrinoTimestamp(
                timestampType,
                "2020-05-10 12:34:56.123456789",
                LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_789),
                "dd.MM.yyyy HH:mm");
        assertTrinoTimestamp(
                timestampType,
                "2020-05-10T12:34:56.123456789",
                LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_789),
                "dd.MM.yyyy HH:mm");

        // fixed offset time zone is allowed
        assertValueTrino(timestampType,
                "\"2020-05-10T12:34:56.123456789-0800\"",
                toSqlTimestamp(timestampType, LocalDateTime.of(2020, 5, 10, 12 + 8, 34, 56, 123_456_789)),
                DEFAULT_OPEN_X_JSON_OPTIONS,
                true);

        // seconds.millis
        assertTimestampNumeric(timestampType, "456.123", LocalDateTime.ofEpochSecond(456, 123_000_000, UTC), false);
        assertTimestampNumeric(timestampType, "456.1239", LocalDateTime.ofEpochSecond(456, 123_000_000, UTC), false);
        assertTimestampNumeric(timestampType, "0.123", LocalDateTime.ofEpochSecond(0, 123_000_000, UTC), false);
        assertTimestampNumeric(timestampType, "00.123", LocalDateTime.ofEpochSecond(0, 123_000_000, UTC), false);
        assertTimestampNumeric(timestampType, ".123", LocalDateTime.ofEpochSecond(0, 123_000_000, UTC), false);

        // due to bugs, Starburst supports exponents, but Rcongiu does not
        assertTimestampNumeric(timestampType, "1.2345E2", null, true);

        // 13 digits or more is millis
        assertTimestampNumeric(timestampType, "1234567890123", LocalDateTime.ofEpochSecond(1234567890L, 123_000_000, UTC), false);
        assertTimestampNumeric(timestampType, "12345678901234", LocalDateTime.ofEpochSecond(12345678901L, 234_000_000, UTC), false);
        // leading zeros are counted (and not interpreted as octal)
        assertTimestampNumeric(timestampType, "0034567890123", LocalDateTime.ofEpochSecond(34567890L, 123_000_000, UTC), true);
        // sign is counted
        assertTimestampNumeric(timestampType, "+234567890123", LocalDateTime.ofEpochSecond(234567890L, 123_000_000, UTC), true);
        assertTimestampNumeric(timestampType, "-234567890123", LocalDateTime.ofEpochSecond(-234567891L, 877_000_000, UTC), false);

        // 12 digits or fewer is seconds
        assertTimestampNumeric(timestampType, "123456789012", LocalDateTime.ofEpochSecond(123456789012L, 0, UTC), false);
        assertTimestampNumeric(timestampType, "12345678901", LocalDateTime.ofEpochSecond(12345678901L, 0, UTC), false);

        // hex is not supported
        assertInvalidTimestamp(timestampType, "0x123");

        // values that don't parse are null
        assertInvalidTimestamp(timestampType, "true");
        assertInvalidTimestamp(timestampType, "false");
        assertInvalidTimestamp(timestampType, "string");

        assertValueFails(timestampType, "[ 42 ]", DEFAULT_OPEN_X_JSON_OPTIONS, false);
        assertValueFails(timestampType, "{ x : 42 }", DEFAULT_OPEN_X_JSON_OPTIONS, false);
    }

    private static void assertTimestamp(TimestampType timestampType, String jsonValue, LocalDateTime expectedDateTime, String... timestampFormats)
    {
        assertTimestamp(timestampType, jsonValue, expectedDateTime, ImmutableList.copyOf(timestampFormats), true);
    }

    private static void assertTrinoTimestamp(TimestampType timestampType, String jsonValue, LocalDateTime expectedDateTime, String... timestampFormats)
    {
        assertTimestamp(timestampType, jsonValue, expectedDateTime, ImmutableList.copyOf(timestampFormats), false);
    }

    private static void assertTimestamp(TimestampType timestampType, String timestampString, LocalDateTime expectedDateTime, List<String> timestampFormats, boolean testHive)
    {
        SqlTimestamp expectedTimestamp = toSqlTimestamp(timestampType, expectedDateTime);
        OpenXJsonOptions options = OpenXJsonOptions.builder().timestampFormats(timestampFormats).build();

        List<String> testValues = new ArrayList<>();
        testValues.add(timestampString);
        if (timestampFormats.isEmpty()) {
            for (Character separator : ImmutableList.of('T', 't')) {
                for (String zone : ImmutableList.of("", "z", "Z", "-0000", "+0000", "-00:00", "+00:00")) {
                    testValues.add(timestampString.replace(' ', separator) + zone);
                }
            }
        }
        for (String testValue : testValues) {
            try {
                if (!testHive) {
                    assertValueTrino(timestampType, "\"" + testValue + "\"", expectedTimestamp, options, true);
                }
                else if (!isSupportedByHiveTimestamp(expectedDateTime, testValue)) {
                    // Hive code can not parse very negative dates
                    assertValueTrinoOnly(timestampType, "\"" + testValue + "\"", expectedTimestamp, options);
                }
                else {
                    assertValue(timestampType, "\"" + testValue + "\"", expectedTimestamp, options, true);
                }
            }
            catch (Throwable e) {
                throw new RuntimeException(testValue, e);
            }
        }
    }

    private static boolean isSupportedByHiveTimestamp(LocalDateTime expectedDateTime, String testValue)
    {
        if (testValue.endsWith("z")) {
            return false;
        }
        if (expectedDateTime.getYear() < -10_000) {
            // ends with Z or fixed zone
            return !testValue.matches("(?i).*([-+]\\d{4}|z)");
        }
        if (expectedDateTime.getYear() > 10_000) {
            return !testValue.toLowerCase(Locale.ROOT).contains("t");
        }
        return true;
    }

    private static void assertTimestampNumeric(TimestampType timestampType, String jsonValue, LocalDateTime expectedDateTime, boolean trinoOnly)
    {
        SqlTimestamp expectedTimestamp = toSqlTimestamp(timestampType, expectedDateTime);

        for (String testJson : ImmutableList.of(jsonValue, jsonValue.toLowerCase(Locale.ROOT))) {
            if (trinoOnly) {
                assertValueTrinoOnly(timestampType, testJson, expectedTimestamp);

                // In Starburst, quoted octal is parsed as a decimal, but unquoted octal is parsed as octal
                // Trino always parses as decimal
                if ((testJson.startsWith("0") || testJson.startsWith("+")) && CharMatcher.inRange('0', '9').matchesAllOf(jsonValue.substring(1))) {
                    assertValue(timestampType, "\"" + testJson + "\"", expectedTimestamp);
                }
                else {
                    assertValueTrinoOnly(timestampType, "\"" + testJson + "\"", expectedTimestamp);
                }
            }
            else {
                assertValue(timestampType, testJson, expectedTimestamp);
                assertValue(timestampType, "\"" + testJson + "\"", expectedTimestamp);
            }
        }
    }

    private static void assertInvalidTimestamp(TimestampType timestampType, String jsonValue, String... timestampFormats)
    {
        OpenXJsonOptions options = OpenXJsonOptions.builder().timestampFormats(timestampFormats).build();
        assertValueTrinoOnly(timestampType, "\"" + jsonValue + "\"", null, options);

        if (CharMatcher.whitespace().or(CharMatcher.is(':')).matchesNoneOf(jsonValue)) {
            assertValueTrinoOnly(timestampType, jsonValue, null, options);
        }
    }

    private static String truncateNanosFor(TimestampType timestampType, String nanoString)
    {
        if (timestampType.getPrecision() == 0) {
            return "";
        }
        return nanoString.substring(0, timestampType.getPrecision() + 1);
    }

    private static int truncateNanosFor(TimestampType timestampType, int nanos)
    {
        long nanoRescale = (long) Math.pow(10, 9 - timestampType.getPrecision());
        return toIntExact((nanos / nanoRescale) * nanoRescale);
    }

    private static void assertValue(Type type, String jsonValue, Object expectedValue)
    {
        assertValue(type, jsonValue, expectedValue, true);
    }

    private static void assertValue(Type type, String jsonValue, Object expectedValue, boolean testMapKey)
    {
        assertValue(type, jsonValue, expectedValue, DEFAULT_OPEN_X_JSON_OPTIONS, testMapKey);
    }

    private static void assertValue(Type type, String jsonValue, Object expectedValue, OpenXJsonOptions options, boolean testMapKey)
    {
        assertValueHive(type, jsonValue, expectedValue, options, testMapKey);
        assertValueTrino(type, jsonValue, expectedValue, options, testMapKey);
    }

    private static void assertLine(List<Column> columns, String line, List<Object> expectedValues, OpenXJsonOptions options)
    {
        internalAssertLineHive(columns, line, expectedValues, options);
        internalAssertLineTrino(columns, line, expectedValues, options);
    }

    private static void assertValueTrinoOnly(Type type, String jsonValue, Object expectedValue)
    {
        assertValueTrinoOnly(type, jsonValue, expectedValue, DEFAULT_OPEN_X_JSON_OPTIONS);
    }

    private static void assertValueTrinoOnly(Type type, String jsonValue, Object expectedValue, OpenXJsonOptions defaultOpenXJsonOptions)
    {
        assertValueTrino(type, jsonValue, expectedValue, defaultOpenXJsonOptions, true);
        assertThatThrownBy(() -> assertValueHive(type, jsonValue, expectedValue, defaultOpenXJsonOptions, true));
    }

    private static void assertValueTrino(Type type, String jsonValue, Object expectedValue, OpenXJsonOptions options, boolean testMapKey)
    {
        internalAssertValueTrino(type, jsonValue, expectedValue, options);
        internalAssertValueTrino(new ArrayType(type), "[" + jsonValue + "]", singletonList(expectedValue), options);
        internalAssertValueTrino(
                rowType(field("a", type), field("nested", type), field("b", type)),
                "{ \"nested\" : " + jsonValue + " }",
                Arrays.asList(null, expectedValue, null),
                options);
        if (expectedValue != null) {
            internalAssertValueTrino(
                    new MapType(BIGINT, type, TYPE_OPERATORS),
                    "{ \"1234\" : " + jsonValue + " }",
                    singletonMap(1234L, expectedValue),
                    options);
        }
        if (expectedValue != null && isScalarType(type)) {
            if (testMapKey) {
                internalAssertValueTrino(toMapKeyType(type), toMapKeyJson(jsonValue), toMapKeyExpectedValue(expectedValue), options);
            }
            else {
                internalAssertValueFailsTrino(toMapKeyType(type), toMapKeyJson(jsonValue), options);
            }
        }
    }

    private static void internalAssertValueTrino(Type type, String jsonValue, Object expectedValue, OpenXJsonOptions options)
    {
        List<Column> columns = ImmutableList.of(new Column("test", type, 33));
        internalAssertLineTrino(columns, "{\"test\" : " + jsonValue + "}", singletonList(expectedValue), options);
        internalAssertLineTrino(columns, "[" + jsonValue + "]", singletonList(expectedValue), options);
    }

    private static void internalAssertLineTrino(List<Column> columns, String line, List<Object> expectedValues, OpenXJsonOptions options)
    {
        // read normal json
        List<Object> actualValues = readTrinoLine(columns, line, options);
        assertColumnValuesEquals(columns, actualValues, expectedValues);

        // if type is not supported (e.g., Map with complex key), skip round trip test
        if (!columns.stream().map(Column::type).allMatch(OpenXJsonSerializer::isSupportedType)) {
            return;
        }

        // write with Trino and verify that trino reads the value back correctly
        String trinoLine = writeTrinoLine(columns, expectedValues, options);
        List<Object> trinoValues = readTrinoLine(columns, trinoLine, DEFAULT_OPEN_X_JSON_OPTIONS);
        assertColumnValuesEquals(columns, trinoValues, expectedValues);

        // verify that Hive can read the value back
        List<Object> hiveValues;
        try {
            hiveValues = readHiveLine(columns, trinoLine, DEFAULT_OPEN_X_JSON_OPTIONS);
        }
        catch (Exception e) {
            // Hive can not read back timestamps that start with `+`
            assertThat(e.getMessage()).isEqualTo("Cannot create timestamp, parsing error");
            return;
        }
        assertColumnValuesEquals(columns, hiveValues, expectedValues);

        // Check if Hive is capable of writing the values
        String hiveLine;
        try {
            hiveLine = writeHiveLine(columns, expectedValues, options);
        }
        catch (Exception e) {
            return;
        }
        // Check if Hive can read back the line it wrote
        try {
            hiveValues = readHiveLine(columns, hiveLine, DEFAULT_OPEN_X_JSON_OPTIONS);
            assertColumnValuesEquals(columns, hiveValues, expectedValues);
        }
        catch (RuntimeException | AssertionError ignored) {
            // If Hive does not round trip values correctly, then stop testing
            // this happens for types like Char, Date, and Decimal which are not rendered
            return;
        }

        // verify that Trino and Hive wrote the exact same line
        assertThat(trinoLine).isEqualTo(hiveLine);
    }

    private static void assertExactLine(List<Column> columns, List<Object> values, String expectedLine, OpenXJsonOptions options)
    {
        // verify Hive produces the exact line
        String hiveLine = writeHiveLine(columns, values, options);
        assertThat(hiveLine).isEqualTo(expectedLine);

        // verify Trino produces the exact line
        String trinoLine = writeTrinoLine(columns, values, options);
        assertThat(trinoLine).isEqualTo(expectedLine);
    }

    private static List<Object> readTrinoLine(List<Column> columns, String jsonLine, OpenXJsonOptions options)
    {
        try {
            LineDeserializer deserializer = new OpenXJsonDeserializerFactory().create(columns, options.toSchema());
            PageBuilder pageBuilder = new PageBuilder(1, deserializer.getTypes());
            deserializer.deserialize(createLineBuffer(jsonLine), pageBuilder);
            Page page = pageBuilder.build();
            return readTrinoValues(columns, page, 0);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String writeTrinoLine(List<Column> columns, List<Object> values, OpenXJsonOptions options)
    {
        try {
            Page page = toSingleRowPage(columns, values);

            // write the data to json
            LineSerializer serializer = new OpenXJsonSerializerFactory().create(columns, options.toSchema());
            SliceOutput sliceOutput = new DynamicSliceOutput(1024);
            serializer.write(page, 0, sliceOutput);
            return sliceOutput.slice().toStringUtf8();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void assertValueHive(Type type, String jsonValue, Object expectedValue, OpenXJsonOptions options, boolean testMapKey)
    {
        // The non-hcatalog version of JsonSerDe has a broken implementation of ordinal fields that always fails
        internalAssertValueHive(type, jsonValue, expectedValue, options);
        if (expectedValue != null && isScalarType(type)) {
            if (testMapKey) {
                internalAssertValueHive(toMapKeyType(type), toMapKeyJson(jsonValue), toMapKeyExpectedValue(expectedValue), options);
            }
            else {
                internalAssertValueFailsHive(toMapKeyType(type), toMapKeyJson(jsonValue), options);
            }
        }
    }

    private static void internalAssertValueHive(Type type, String jsonValue, Object expectedValue, OpenXJsonOptions options)
    {
        Object actualValue = readHiveValue(type, jsonValue, options);
        assertColumnValueEquals(type, actualValue, expectedValue);
    }

    private static void internalAssertLineHive(List<Column> columns, String jsonLine, List<Object> expectedValues, OpenXJsonOptions options)
    {
        List<Object> actualValues = readHiveLine(columns, jsonLine, options);
        assertColumnValuesEquals(columns, actualValues, expectedValues);
    }

    private static Object readHiveValue(Type type, String jsonValue, OpenXJsonOptions options)
    {
        List<Column> columns = ImmutableList.of(
                new Column("a", BIGINT, 0),
                new Column("test", type, 1),
                new Column("b", BIGINT, 2));
        String jsonLine = "{\"test\" : " + jsonValue + "}";
        return readHiveLine(columns, jsonLine, options).get(1);
    }

    private static List<Object> readHiveLine(List<Column> columns, String jsonLine, OpenXJsonOptions options)
    {
        try {
            Deserializer deserializer = createHiveSerDe(columns, options);

            Object rowData = deserializer.deserialize(new Text(jsonLine));

            List<Object> fieldValues = new ArrayList<>();
            StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
            for (Column column : columns) {
                StructField field = rowInspector.getStructFieldRef(column.name());
                Object fieldValue = rowInspector.getStructFieldData(rowData, field);
                fieldValue = unwrapJsonObject(fieldValue, field.getFieldObjectInspector());
                fieldValue = decodeRecordReaderValue(column.type(), fieldValue);
                fieldValues.add(fieldValue);
            }
            return fieldValues;
        }
        catch (SerDeException e) {
            throw new RuntimeException(e);
        }
    }

    private static JsonSerDe createHiveSerDe(List<Column> columns, OpenXJsonOptions options)
    {
        try {
            JobConf configuration = new JobConf(newEmptyConfiguration());

            Properties schema = new Properties();
            schema.putAll(createOpenXJsonSerDeProperties(columns, options));

            JsonSerDe jsonSerDe = new JsonSerDe();
            jsonSerDe.initialize(configuration, schema);
            configuration.set(SERIALIZATION_LIB, jsonSerDe.getClass().getName());
            return jsonSerDe;
        }
        catch (SerDeException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object unwrapJsonObject(Object object, ObjectInspector inspector)
    {
        if (object == null) {
            return null;
        }
        return switch (inspector.getCategory()) {
            case PRIMITIVE -> ((PrimitiveObjectInspector) inspector).getPrimitiveJavaObject(object);
            case LIST -> serializeList(object, (ListObjectInspector) inspector);
            case MAP -> serializeMap(object, (MapObjectInspector) inspector, false);
            case STRUCT -> serializeStruct(object, (StructObjectInspector) inspector);
            case UNION -> throw new UnsupportedOperationException("Union not implemented");
        };
    }

    private static List<Object> serializeList(Object object, ListObjectInspector inspector)
    {
        List<?> list = inspector.getList(object);
        if (list == null) {
            return null;
        }

        ObjectInspector elementInspector = inspector.getListElementObjectInspector();
        return list.stream()
                .map(element -> unwrapJsonObject(element, elementInspector))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private static Map<Object, Object> serializeMap(Object object, MapObjectInspector inspector, boolean filterNullMapKeys)
    {
        Map<?, ?> map = inspector.getMap(object);
        if (map == null) {
            return null;
        }

        ObjectInspector keyInspector = inspector.getMapKeyObjectInspector();
        ObjectInspector valueInspector = inspector.getMapValueObjectInspector();
        Map<Object, Object> values = new HashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            // Hive skips map entries with null keys
            if (!filterNullMapKeys || entry.getKey() != null) {
                values.put(
                        unwrapJsonObject(entry.getKey(), keyInspector),
                        unwrapJsonObject(entry.getValue(), valueInspector));
            }
        }
        return values;
    }

    private static List<Object> serializeStruct(Object object, StructObjectInspector inspector)
    {
        if (object == null) {
            return null;
        }

        List<? extends StructField> fields = inspector.getAllStructFieldRefs();
        List<Object> values = new ArrayList<>();
        for (StructField field : fields) {
            values.add(unwrapJsonObject(inspector.getStructFieldData(object, field), field.getFieldObjectInspector()));
        }
        return values;
    }

    private static String writeHiveLine(List<Column> columns, List<Object> values, OpenXJsonOptions options)
    {
        SettableStructObjectInspector objectInspector = getStandardStructObjectInspector(
                columns.stream().map(Column::name).collect(toImmutableList()),
                columns.stream().map(Column::type).map(FormatTestUtils::getJavaObjectInspector).collect(toImmutableList()));

        Object row = objectInspector.create();
        for (int i = 0; i < columns.size(); i++) {
            Object value = toHiveWriteValue(columns.get(i).type(), values.get(i), Optional.empty());
            objectInspector.setStructFieldData(row, objectInspector.getAllStructFieldRefs().get(i), value);
        }

        JsonSerDe serializer = createHiveSerDe(columns, options);
        try {
            return serializer.serialize(row, objectInspector).toString();
        }
        catch (SerDeException e) {
            throw new RuntimeException(e);
        }
    }

    private static void assertLineFails(List<Column> columns, String jsonValue, OpenXJsonOptions options)
    {
        assertLineFailsHive(columns, jsonValue, options);
        assertLineFailsTrino(columns, jsonValue, options);
    }

    private static void assertValueFails(Type type, String jsonValue)
    {
        assertValueFails(type, jsonValue, DEFAULT_OPEN_X_JSON_OPTIONS, true);
    }

    private static void assertValueFails(Type type, String jsonValue, OpenXJsonOptions options, boolean testMapKey)
    {
        assertValueFailsHive(type, jsonValue, options, testMapKey);
        assertValueFailsTrino(type, jsonValue, options, testMapKey);
    }

    private static void assertValueFailsTrino(Type type, String jsonValue, OpenXJsonOptions options, boolean testMapKey)
    {
        internalAssertValueFailsTrino(type, jsonValue, options);

        // ignore array and object json
        if (testMapKey && isScalarType(type)) {
            internalAssertValueFailsTrino(toMapKeyType(type), toMapKeyJson(jsonValue), options);
        }
    }

    private static void internalAssertValueFailsTrino(Type type, String jsonValue, OpenXJsonOptions options)
    {
        String jsonLine = "{\"test\" : " + jsonValue + "}";
        List<Column> columns = ImmutableList.of(new Column("test", type, 33));
        assertLineFailsTrino(columns, jsonLine, options);
    }

    private static void assertLineFailsTrino(List<Column> columns, String jsonLine, OpenXJsonOptions options)
    {
        LineDeserializer deserializer = new OpenXJsonDeserializerFactory().create(columns, options.toSchema());
        assertThatThrownBy(() -> deserializer.deserialize(createLineBuffer(jsonLine), new PageBuilder(1, deserializer.getTypes())))
                .isInstanceOf(Exception.class);
    }

    private static void assertValueFailsHive(Type type, String jsonValue, OpenXJsonOptions options, boolean testMapKey)
    {
        internalAssertValueFailsHive(type, jsonValue, options);
        if (testMapKey && isScalarType(type)) {
            internalAssertValueFailsHive(toMapKeyType(type), toMapKeyJson(jsonValue), options);
        }
    }

    private static void internalAssertValueFailsHive(Type type, String jsonValue, OpenXJsonOptions options)
    {
        List<Column> columns = ImmutableList.of(new Column("test", type, 0));
        String jsonLine = "{\"test\" : " + jsonValue + "}";
        assertLineFailsHive(columns, jsonLine, options);
    }

    private static void assertLineFailsHive(List<Column> columns, String jsonLine, OpenXJsonOptions options)
    {
        assertThatThrownBy(() -> {
            JobConf configuration = new JobConf(newEmptyConfiguration());

            Properties schema = new Properties();
            schema.putAll(createOpenXJsonSerDeProperties(columns, options));

            Deserializer deserializer = new JsonSerDe();
            deserializer.initialize(configuration, schema);
            configuration.set(SERIALIZATION_LIB, deserializer.getClass().getName());

            Object rowData = deserializer.deserialize(new Text(jsonLine));

            StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
            for (Column column : columns) {
                StructField field = rowInspector.getStructFieldRef(column.name());
                Object actualValue = rowInspector.getStructFieldData(rowData, field);
                actualValue = unwrapJsonObject(actualValue, field.getFieldObjectInspector());
                decodeRecordReaderValue(column.type(), actualValue);
            }
        });
    }

    private static MapType toMapKeyType(Type type)
    {
        assertTrue(isScalarType(type));
        return new MapType(type, BIGINT, TYPE_OPERATORS);
    }

    private static String toMapKeyJson(String jsonValue)
    {
        if (jsonValue.startsWith("\"")) {
            jsonValue = jsonValue.substring(1, jsonValue.length() - 1);
        }
        return "{ \"" + jsonValue + "\" : 8675309 }";
    }

    private static Map<Object, Long> toMapKeyExpectedValue(Object value)
    {
        return ImmutableMap.of(value, 8675309L);
    }

    private static Map<String, String> createOpenXJsonSerDeProperties(List<Column> columns, OpenXJsonOptions options)
    {
        ImmutableMap.Builder<String, String> schema = ImmutableMap.builder();
        schema.put(LIST_COLUMNS, columns.stream()
                .sorted(Comparator.comparing(Column::ordinal))
                .map(Column::name)
                .collect(joining(",")));
        schema.put(LIST_COLUMN_TYPES, columns.stream()
                        .sorted(Comparator.comparing(Column::ordinal))
                        .map(Column::type)
                        .map(FormatTestUtils::getJavaObjectInspector)
                        .map(ObjectInspector::getTypeName)
                        .collect(joining(",")));
        schema.putAll(options.toSchema());
        return schema.buildOrThrow();
    }
}
