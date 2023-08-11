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
package io.trino.hive.formats.line.simple;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.hive.formats.FormatTestUtils;
import io.trino.hive.formats.encodings.text.TextEncodingOptions;
import io.trino.hive.formats.encodings.text.TextEncodingOptions.NestingLevels;
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
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.hive.formats.FormatTestUtils.assertColumnValueEquals;
import static io.trino.hive.formats.FormatTestUtils.createLineBuffer;
import static io.trino.hive.formats.FormatTestUtils.decodeRecordReaderValue;
import static io.trino.hive.formats.FormatTestUtils.isScalarType;
import static io.trino.hive.formats.FormatTestUtils.readTrinoValues;
import static io.trino.hive.formats.FormatTestUtils.toHiveWriteValue;
import static io.trino.hive.formats.FormatTestUtils.toSingleRowPage;
import static io.trino.hive.formats.FormatTestUtils.toSqlTimestamp;
import static io.trino.hive.formats.encodings.text.TextEncodingOptions.DEFAULT_SIMPLE_OPTIONS;
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
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSimpleFormat
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private static final DecimalType SHORT_DECIMAL = createDecimalType(MAX_SHORT_PRECISION, 2);
    private static final DecimalType LONG_DECIMAL = createDecimalType(MAX_PRECISION, 2);
    private static final VarcharType VARCHAR_3 = createVarcharType(3);
    private static final CharType CHAR_200 = createCharType(200);
    private static final CharType CHAR_3 = createCharType(3);
    private static final TextEncodingOptions NOPE_NULL_OPTION = TextEncodingOptions.builder().nullSequence(utf8Slice("NOPE")).build();

    @Test
    public void testTable()
            throws Exception
    {
        assertLine(
                ImmutableList.of(
                        new Column("a", VARCHAR, 0),
                        new Column("b", BOOLEAN, 1),
                        new Column("c", BIGINT, 2)),
                "",
                Arrays.asList("", null, null),
                DEFAULT_SIMPLE_OPTIONS,
                true);

        assertLine(
                ImmutableList.of(
                        new Column("a", VARCHAR, 0),
                        new Column("b", BOOLEAN, 1),
                        new Column("c", VARCHAR, 2)),
                "\1\1foo\1\bar\1baz",
                Arrays.asList("", null, "foo\1\bar\1baz"),
                TextEncodingOptions.builder()
                        .lastColumnTakesRest()
                        .build(),
                true);

        // last column takes rest does not work well with null
        assertLine(
                ImmutableList.of(
                        new Column("a", VARCHAR, 0),
                        new Column("b", BOOLEAN, 1),
                        new Column("c", VARCHAR, 2)),
                "\1\1\\N\1\bar\1baz",
                Arrays.asList("", null, "\\N\1\bar\1baz"),
                TextEncodingOptions.builder()
                        .lastColumnTakesRest()
                        .build(),
                true);
    }

    @Test
    public void testNestingLevels()
            throws Exception
    {
        for (NestingLevels nestingLevels : NestingLevels.values()) {
            testNestingLevels(
                    nestingLevels.getLevels(),
                    Optional.ofNullable(nestingLevels.getTableProperty()),
                    TextEncodingOptions.builder().nestingLevels(nestingLevels).build());
        }
    }

    @SuppressWarnings("resource")
    private static void testNestingLevels(int nestingLevels, Optional<String> tableProperty, TextEncodingOptions options)
            throws Exception
    {
        Type type = VARCHAR;
        Object expectedValue = "value";
        Slice value = utf8Slice("value");
        for (int i = 0; i < nestingLevels - 1; i++) {
            type = new ArrayType(type);
            expectedValue = Arrays.asList(null, expectedValue, null);

            char separator = (char) options.getSeparators().getByte(nestingLevels - 1 - i);
            value = new DynamicSliceOutput(value.length() + 6)
                    .appendBytes(options.getNullSequence())
                    .appendByte(separator)
                    .appendBytes(value)
                    .appendByte(separator)
                    .appendBytes(options.getNullSequence())
                    .copySlice();
        }

        // line must be built as a slice because it is invalid UTF-8
        Slice line = new DynamicSliceOutput(value.length() + 6)
                .appendByte(options.getSeparators().getByte(0))
                .appendBytes(value)
                .appendByte(options.getSeparators().getByte(0))
                .copySlice();

        // verify that nesting works to the specified level
        assertLine(
                ImmutableList.of(
                        new Column("a", BIGINT, 0),
                        new Column("b", type, 1),
                        new Column("c", BIGINT, 2)),
                line,
                Arrays.asList(null, expectedValue, null),
                options,
                true);

        // verify adding one more nesting level fails
        type = new ArrayType(type);
        ImmutableList<Column> columns = ImmutableList.of(
                new Column("a", BIGINT, 0),
                new Column("b", type, 1),
                new Column("c", BIGINT, 2));

        assertThatThrownBy(
                () -> {
                    Properties schema = new Properties();
                    schema.putAll(createLazySimpleSerDeProperties(columns, options));
                    new LazySimpleSerDe().initialize(new JobConf(newEmptyConfiguration()), schema);
                })
                .isInstanceOf(SerDeException.class)
                .hasMessageContaining("nesting");

        assertThatThrownBy(() -> new SimpleDeserializerFactory().create(columns, createLazySimpleSerDeProperties(columns, options)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(nestingLevels + " nesting levels")
                .hasMessageContaining(tableProperty.orElse(""));
    }

    @Test
    public void testStruct()
            throws Exception
    {
        RowType rowType = RowType.rowType(field("a", BIGINT), field("b", VARCHAR), field("c", DOUBLE));

        assertValue(rowType, "\\N", null);
        assertValue(rowType, "NOPE", null, NOPE_NULL_OPTION);

        // empty value
        assertValue(rowType, "", Arrays.asList(null, null, null));
        // too few values
        assertValue(rowType, "1", Arrays.asList(1L, null, null));
        assertValue(rowType, "1\2a", Arrays.asList(1L, "a", null));
        //exact number of values
        assertValue(rowType, "1\2a\2NaN", Arrays.asList(1L, "a", Double.NaN));
        // too many values
        assertValue(rowType, "1\2a\2NaN\2ign\2ored", Arrays.asList(1L, "a", Double.NaN));
        assertValue(rowType, "\2a\2", Arrays.asList(null, "a", null));

        // nested null
        assertValue(
                RowType.rowType(field("a", VARCHAR), field("b", VARCHAR), field("c", VARCHAR)),
                "\2NOPE\2",
                Arrays.asList("", null, ""),
                NOPE_NULL_OPTION);

        // last column takes rest
        // this has to be tested as a full line due to automatic testing of deeper nesting in assertValue
        assertLine(
                ImmutableList.of(
                        new Column("a", VARCHAR, 0),
                        new Column("b", RowType.rowType(field("a", VARCHAR), field("b", BOOLEAN), field("c", VARCHAR)), 1),
                        new Column("c", VARCHAR, 2)),
                "apple\1\2\2foo\2\bar\2baz\1cherry",
                Arrays.asList(
                        "apple",
                        Arrays.asList("", null, "foo\2\bar\2baz"),
                        "cherry"),
                TextEncodingOptions.builder()
                        .lastColumnTakesRest()
                        .build(),
                true);

        // last column takes rest does not work well with null
        assertLine(
                ImmutableList.of(
                        new Column("a", VARCHAR, 0),
                        new Column("b", RowType.rowType(field("a", VARCHAR), field("b", BOOLEAN), field("c", VARCHAR)), 1),
                        new Column("c", VARCHAR, 2)),
                "apple\1\2\2\\N\2\bar\2baz\1cherry",
                Arrays.asList(
                        "apple",
                        Arrays.asList("", null, "\\N\2\bar\2baz"),
                        "cherry"),
                TextEncodingOptions.builder()
                        .lastColumnTakesRest()
                        .build(),
                true);
    }

    @Test
    public void testMap()
            throws Exception
    {
        MapType mapType = new MapType(VARCHAR, BIGINT, TYPE_OPERATORS);

        assertValue(mapType, "\\N", null);
        assertValue(mapType, "NOPE", null, NOPE_NULL_OPTION);

        assertValue(mapType, "", ImmutableMap.of());

        assertValue(mapType,
                toMapLine(ImmutableList.<Entry<String, String>>builder()
                        .add(Map.entry("a", "1"))
                        .add(Map.entry("b", "2"))
                        .add(Map.entry("c", "3"))
                        .build()),
                ImmutableMap.builder()
                        .put("a", 1L)
                        .put("b", 2L)
                        .put("c", 3L)
                        .buildOrThrow());

        // We do not test byte for byte here because the map comes out in the wrong order
        assertValue(mapType,
                toMapLine(ImmutableList.<Entry<String, String>>builder()
                        .add(Map.entry("c", "3"))
                        .add(Map.entry("b", "2"))
                        .add(Map.entry("a", "1"))
                        .build()),
                ImmutableMap.builder()
                        .put("a", 1L)
                        .put("b", 2L)
                        .put("c", 3L)
                        .buildOrThrow(),
                DEFAULT_SIMPLE_OPTIONS,
                false);

        // Duplicate fields are supported, and the last value is used
        assertValue(mapType,
                toMapLine(ImmutableList.<Entry<String, String>>builder()
                        .add(Map.entry("a", "1"))
                        .add(Map.entry("b", "7"))
                        .add(Map.entry("a", "2"))
                        .add(Map.entry("b", "8"))
                        .build()),
                ImmutableMap.builder()
                        .put("a", 1L)
                        .put("b", 7L)
                        .buildOrThrow());

        assertValue(mapType, "keyOnly", singletonMap("keyOnly", null));

        Map<Object, Object> expectedValue = new HashMap<>();
        expectedValue.put(1L, "a");
        expectedValue.put(2L, null);
        expectedValue.put(3L, "c");
        assertValue(
                new MapType(BIGINT, VARCHAR, TYPE_OPERATORS),
                toMapLine(ImmutableList.<Entry<String, String>>builder()
                        .add(Map.entry("1", "a"))
                        .add(Map.entry("2", "NOPE"))
                        .add(Map.entry("3", "c"))
                        .build()),
                expectedValue,
                NOPE_NULL_OPTION);
    }

    private static String toMapLine(Collection<Entry<String, String>> entries)
    {
        return entries.stream()
                .map(entry -> entry.getKey() + "\3" + entry.getValue())
                .collect(joining("\2"));
    }

    @Test
    public void testVarchar()
            throws Exception
    {
        assertValue(VARCHAR, "\\N", null);
        assertValue(VARCHAR, "NOPE", null, NOPE_NULL_OPTION);

        assertValue(VARCHAR, "", "");
        assertValue(VARCHAR, "value", "value");

        // Trailing spaces are NOT truncated
        assertValue(VARCHAR, "value     ", "value     ");

        // Truncation
        // Hive doesn't seem to truncate
        assertValue(VARCHAR_3, "v", "v");
        assertValue(VARCHAR_3, "val", "val");
        assertValue(VARCHAR_3, "value", "val");

        // Escape
        testStringEscaping(VARCHAR, '\\');
        testStringEscaping(VARCHAR, '~');

        assertValue(VARCHAR, "true", "true");
        assertValue(VARCHAR, "false", "false");

        assertValue(VARCHAR, "-1", "-1");
        assertValue(VARCHAR, "1.23", "1.23");
        assertValue(VARCHAR, "1.23e45", "1.23e45");
    }

    @Test
    public void testChar()
            throws Exception
    {
        assertValue(CHAR_200, "\\N", null);
        assertValue(CHAR_200, "NOPE", null, NOPE_NULL_OPTION);

        assertString(CHAR_200, "", "");

        assertString(CHAR_200, "value", "value");

        // Trailing spaces are truncated
        assertString(CHAR_200, "value     ", "value");

        // Truncation
        // Hive doesn't seem to truncate
        assertString(CHAR_3, "v", "v");
        assertString(CHAR_3, "val", "val");
        assertString(CHAR_3, "value", "val");

        // Escape
        testStringEscaping(CHAR_200, '\\');
        testStringEscaping(CHAR_200, '~');

        assertString(CHAR_200, "true", "true");
        assertString(CHAR_200, "false", "false");

        assertString(CHAR_200, "-1", "-1");
        assertString(CHAR_200, "1.23", "1.23");
        assertString(CHAR_200, "1.23e45", "1.23e45");
    }

    private static void testStringEscaping(Type type, char escape)
            throws Exception
    {
        assertString(type, "tab \t tab", "tab \t tab");
        assertString(type, "new \n line", "new \n line");
        assertString(type, "carriage \r return", "carriage \r return");

        TextEncodingOptions options = TextEncodingOptions.builder().escapeByte((byte) escape).build();
        assertString(type, "tab " + escape + "\t tab", "tab \t tab", options);
        assertString(type, "new " + escape + "\n line", "new \n line", options);
        assertString(type, "carriage " + escape + "\r return", "carriage \r return", options);
        assertString(type, "escape " + escape + escape + " char", "escape " + escape + " char", options);
        assertString(type, "double " + escape + escape + escape + escape + " escape", "double " + escape + escape + " escape", options);
        assertString(type, "simple " + escape + "X char", "simple X char", options);

        String allControlCharacters = IntStream.range(0, 32)
                .mapToObj(i -> i + " " + ((char) i))
                .collect(joining(" "));
        String allControlCharactersEscaped = IntStream.range(0, 32)
                .mapToObj(i -> i + " " + escape + ((char) i))
                .collect(joining(" "));
        assertString(type, allControlCharactersEscaped, allControlCharacters, options);
    }

    private static void assertString(Type type, String value, String expectedValue)
            throws Exception
    {
        assertString(type, value, expectedValue, DEFAULT_SIMPLE_OPTIONS);
    }

    private static void assertString(Type type, String value, String expectedValue, TextEncodingOptions options)
            throws Exception
    {
        if (type instanceof CharType charType) {
            expectedValue = padSpaces(expectedValue, charType);
        }
        assertValue(type, value, expectedValue, options);
    }

    @Test
    public void testVarbinary()
            throws Exception
    {
        assertValue(VARBINARY, "\\N", null);
        assertValue(VARBINARY, "NOPE", null, NOPE_NULL_OPTION);

        assertVarbinary("");

        assertVarbinary("value");
        assertVarbinary("true");
        assertVarbinary("false");

        assertVarbinary("-1");
        assertVarbinary("1.23");
        assertVarbinary("1.23e45");

        byte[] allBytes = new byte[255];
        for (int i = 0; i < allBytes.length; i++) {
            allBytes[i] = (byte) (Byte.MIN_VALUE + i);
        }
        assertVarbinary(Slices.wrappedBuffer(allBytes));

        // Hive allows raw non-base64 binary, but only if the data contains a non-base64 character
        assertValue(VARBINARY, "$value", new SqlVarbinary("$value".getBytes(UTF_8)));
    }

    private static void assertVarbinary(String value)
            throws Exception
    {
        assertVarbinary(utf8Slice(value));
    }

    private static void assertVarbinary(Slice value)
            throws Exception
    {
        // Hive decoder allows both STANDARD (+/) and URL_SAFE (-_) base64
        byte[] byteArray = value.getBytes();
        assertValue(VARBINARY, Base64.getEncoder().encodeToString(byteArray), new SqlVarbinary(byteArray));
        assertValue(VARBINARY, Base64.getUrlEncoder().encodeToString(byteArray), new SqlVarbinary(byteArray));
    }

    @Test
    public void testBoolean()
            throws Exception
    {
        assertValue(BOOLEAN, "\\N", null);
        assertValue(BOOLEAN, "NOPE", null, NOPE_NULL_OPTION);

        // empty value is not allowed
        assertValue(BOOLEAN, "", null);

        assertValue(BOOLEAN, "true", true);
        assertValue(BOOLEAN, "TRUE", true);
        assertValue(BOOLEAN, "tRuE", true);

        assertValue(BOOLEAN, "false", false);
        assertValue(BOOLEAN, "FALSE", false);
        assertValue(BOOLEAN, "fAlSe", false);

        assertValue(BOOLEAN, "t", null);
        assertValue(BOOLEAN, "T", null);
        assertValue(BOOLEAN, "1", null);
        assertValue(BOOLEAN, "f", null);
        assertValue(BOOLEAN, "F", null);
        assertValue(BOOLEAN, "0", null);

        assertValue(BOOLEAN, "unknown", null);
        assertValue(BOOLEAN, "null", null);
        assertValue(BOOLEAN, "-1", null);
        assertValue(BOOLEAN, "1.23", null);
        assertValue(BOOLEAN, "1.23e45", null);
    }

    @Test
    public void testBigint()
            throws Exception
    {
        assertValue(BIGINT, "\\N", null);
        assertValue(BIGINT, "NOPE", null, NOPE_NULL_OPTION);

        // empty value is not allowed
        assertValue(BIGINT, "", null);

        // allowed range for LazySimpleSerDe
        assertValue(BIGINT, "0", 0L);
        assertValue(BIGINT, "1", 1L);
        assertValue(BIGINT, "-1", -1L);

        // empty value or just a sign is not allowed
        assertValue(BIGINT, "-", null);
        assertValue(BIGINT, "+", null);

        // max value
        assertValue(BIGINT, String.valueOf(Long.MAX_VALUE), Long.MAX_VALUE);
        assertValue(BIGINT, "+" + Long.MAX_VALUE, Long.MAX_VALUE);
        assertValue(BIGINT, Long.MAX_VALUE + ".999", Long.MAX_VALUE);
        assertValue(BIGINT, "+" + Long.MAX_VALUE + ".999", Long.MAX_VALUE);
        assertValue(BIGINT, "0000" + Long.MAX_VALUE, Long.MAX_VALUE);
        assertValue(BIGINT, "+0000" + Long.MAX_VALUE, Long.MAX_VALUE);
        assertValue(BIGINT, "0000" + Long.MAX_VALUE + ".999", Long.MAX_VALUE);
        assertValue(BIGINT, "+0000" + Long.MAX_VALUE + ".999", Long.MAX_VALUE);

        // min value need special handling due to extra negative value;
        assertValue(BIGINT, String.valueOf(Long.MIN_VALUE), Long.MIN_VALUE);
        assertValue(BIGINT, "-0000" + String.valueOf(Long.MIN_VALUE).substring(1), Long.MIN_VALUE);
        assertValue(BIGINT, "-0000" + String.valueOf(Long.MIN_VALUE).substring(1) + ".999", Long.MIN_VALUE);

        // verify out of range values
        for (int increment = 1; increment < 20; increment++) {
            assertValue(BIGINT, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(increment)).toString(), null);
            assertValue(BIGINT, BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.valueOf(increment)).toString(), null);
        }

        // Normal value supports decimal notation and is truncated, but decimal map keys throw
        assertValue(BIGINT, "1.23", 1L);
        assertValue(BIGINT, "1.56", 1L);
        assertValue(BIGINT, "0.123", 0L);
        assertValue(BIGINT, "0.999", 0L);

        // decimals without leading digits are allowed
        assertValue(BIGINT, ".123", 0L);
        assertValue(BIGINT, "+.123", 0L);
        assertValue(BIGINT, "-.123", 0L);

        // Exponents are not allowed
        assertValue(BIGINT, "1.2345e2", null);
        assertValue(BIGINT, "1.5645e2", null);

        // other values are not allowed
        assertValue(BIGINT, "true", null);
        assertValue(BIGINT, "false", null);
        assertValue(BIGINT, "\"123\"", null);
        assertValue(BIGINT, "\"string\"", null);
        assertValue(BIGINT, "\"null\"", null);
    }

    @Test
    public void testInteger()
            throws Exception
    {
        assertValue(INTEGER, "\\N", null);
        assertValue(INTEGER, "NOPE", null, NOPE_NULL_OPTION);

        // empty value is not allowed
        assertValue(INTEGER, "", null);

        // allowed range for LazySimpleSerDe
        assertValue(INTEGER, "0", 0);
        assertValue(INTEGER, "1", 1);
        assertValue(INTEGER, "-1", -1);

        // empty value or just a sign is not allowed
        assertValue(INTEGER, "-", null);
        assertValue(INTEGER, "+", null);

        // max value
        assertValue(INTEGER, String.valueOf(Integer.MAX_VALUE), Integer.MAX_VALUE);
        assertValue(INTEGER, "+" + Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertValue(INTEGER, "0000" + Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertValue(INTEGER, "+0000" + Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertValue(INTEGER, "0000" + Integer.MAX_VALUE + ".999", Integer.MAX_VALUE);
        assertValue(INTEGER, "+0000" + Integer.MAX_VALUE + ".999", Integer.MAX_VALUE);

        // min value need special handling due to extra negative value;
        assertValue(INTEGER, String.valueOf(Integer.MIN_VALUE), Integer.MIN_VALUE);
        assertValue(INTEGER, "-0000" + String.valueOf(Integer.MIN_VALUE).substring(1), Integer.MIN_VALUE);
        assertValue(INTEGER, "-0000" + String.valueOf(Integer.MIN_VALUE).substring(1) + ".999", Integer.MIN_VALUE);

        // verify out of range values
        for (long increment = 1; increment < 20; increment++) {
            assertValue(INTEGER, String.valueOf(Integer.MAX_VALUE + increment), null);
            assertValue(INTEGER, String.valueOf(Integer.MIN_VALUE - increment), null);
        }

        // Normal value supports decimal notation and is truncated, but decimal map keys throw
        assertValue(INTEGER, "1.23", 1);
        assertValue(INTEGER, "1.56", 1);
        assertValue(INTEGER, "0.123", 0);
        assertValue(INTEGER, "0.999", 0);

        // decimals without leading digits are allowed
        assertValue(INTEGER, ".123", 0);
        assertValue(INTEGER, "+.123", 0);
        assertValue(INTEGER, "-.123", 0);

        // Exponents are not allowed
        assertValue(INTEGER, "1.2345e2", null);
        assertValue(INTEGER, "1.5645e2", null);

        // other values are not allowed
        assertValue(INTEGER, "true", null);
        assertValue(INTEGER, "false", null);
        assertValue(INTEGER, "\"123\"", null);
        assertValue(INTEGER, "\"string\"", null);
        assertValue(INTEGER, "\"null\"", null);
    }

    @Test
    public void testSmallInt()
            throws Exception
    {
        assertValue(SMALLINT, "\\N", null);
        assertValue(SMALLINT, "NOPE", null, NOPE_NULL_OPTION);

        // empty value is not allowed
        assertValue(SMALLINT, "", null);

        // allowed range for LazySimpleSerDe
        assertValue(SMALLINT, "0", (short) 0);
        assertValue(SMALLINT, "1", (short) 1);
        assertValue(SMALLINT, "-1", (short) -1);

        // empty value or just a sign is not allowed
        assertValue(SMALLINT, "-", null);
        assertValue(SMALLINT, "+", null);

        // max value
        assertValue(SMALLINT, String.valueOf(Short.MAX_VALUE), Short.MAX_VALUE);
        assertValue(SMALLINT, "+" + Short.MAX_VALUE, Short.MAX_VALUE);
        assertValue(SMALLINT, "0000" + Short.MAX_VALUE, Short.MAX_VALUE);
        assertValue(SMALLINT, "+0000" + Short.MAX_VALUE, Short.MAX_VALUE);
        assertValue(SMALLINT, "0000" + Short.MAX_VALUE + ".999", Short.MAX_VALUE);
        assertValue(SMALLINT, "+0000" + Short.MAX_VALUE + ".999", Short.MAX_VALUE);

        // min value need special handling due to extra negative value;
        assertValue(SMALLINT, String.valueOf(Short.MIN_VALUE), Short.MIN_VALUE);
        assertValue(SMALLINT, "-0000" + String.valueOf(Short.MIN_VALUE).substring(1), Short.MIN_VALUE);
        assertValue(SMALLINT, "-0000" + String.valueOf(Short.MIN_VALUE).substring(1) + ".999", Short.MIN_VALUE);

        // verify out of range values
        for (long increment = 1; increment < 20; increment++) {
            assertValue(SMALLINT, String.valueOf(Short.MAX_VALUE + increment), null);
            assertValue(SMALLINT, String.valueOf(Short.MIN_VALUE - increment), null);
        }

        // Normal value supports decimal notation and is truncated, but decimal map keys throw
        assertValue(SMALLINT, "1.23", (short) 1);
        assertValue(SMALLINT, "1.56", (short) 1);
        assertValue(SMALLINT, "0.123", (short) 0);
        assertValue(SMALLINT, "0.999", (short) 0);

        // decimals without leading digits are allowed
        assertValue(SMALLINT, ".123", (short) 0);
        assertValue(SMALLINT, "+.123", (short) 0);
        assertValue(SMALLINT, "-.123", (short) 0);

        // Exponents are not allowed
        assertValue(SMALLINT, "1.2345e2", null);
        assertValue(SMALLINT, "1.5645e2", null);

        // other values are not allowed
        assertValue(SMALLINT, "true", null);
        assertValue(SMALLINT, "false", null);
        assertValue(SMALLINT, "\"123\"", null);
        assertValue(SMALLINT, "\"string\"", null);
        assertValue(SMALLINT, "\"null\"", null);
    }

    @Test
    public void testTinyint()
            throws Exception
    {
        assertValue(TINYINT, "\\N", null);
        assertValue(TINYINT, "NOPE", null, NOPE_NULL_OPTION);

        // empty value is not allowed
        assertValue(TINYINT, "", null);

        // allowed range for LazySimpleSerDe
        assertValue(TINYINT, "0", (byte) 0);
        assertValue(TINYINT, "1", (byte) 1);
        assertValue(TINYINT, "-1", (byte) -1);

        // empty value or just a sign is not allowed
        assertValue(TINYINT, "-", null);
        assertValue(TINYINT, "+", null);

        // max value
        assertValue(TINYINT, String.valueOf(Byte.MAX_VALUE), Byte.MAX_VALUE);
        assertValue(TINYINT, "+" + Byte.MAX_VALUE, Byte.MAX_VALUE);
        assertValue(TINYINT, "0000" + Byte.MAX_VALUE, Byte.MAX_VALUE);
        assertValue(TINYINT, "+0000" + Byte.MAX_VALUE, Byte.MAX_VALUE);
        assertValue(TINYINT, "0000" + Byte.MAX_VALUE + ".999", Byte.MAX_VALUE);
        assertValue(TINYINT, "+0000" + Byte.MAX_VALUE + ".999", Byte.MAX_VALUE);

        // min value need special handling due to extra negative value;
        assertValue(TINYINT, String.valueOf(Byte.MIN_VALUE), Byte.MIN_VALUE);
        assertValue(TINYINT, "-0000" + String.valueOf(Byte.MIN_VALUE).substring(1), Byte.MIN_VALUE);
        assertValue(TINYINT, "-0000" + String.valueOf(Byte.MIN_VALUE).substring(1) + ".999", Byte.MIN_VALUE);

        // verify out of range values
        for (long increment = 1; increment < 20; increment++) {
            assertValue(TINYINT, String.valueOf(Byte.MAX_VALUE + increment), null);
            assertValue(TINYINT, String.valueOf(Byte.MIN_VALUE - increment), null);
        }

        // Normal value supports decimal notation and is truncated, but decimal map keys throw
        assertValue(TINYINT, "1.23", (byte) 1);
        assertValue(TINYINT, "1.56", (byte) 1);
        assertValue(TINYINT, "0.123", (byte) 0);
        assertValue(TINYINT, "0.999", (byte) 0);

        // decimals without leading digits are allowed
        assertValue(TINYINT, ".123", (byte) 0);
        assertValue(TINYINT, "+.123", (byte) 0);
        assertValue(TINYINT, "-.123", (byte) 0);

        // Exponents are not allowed
        assertValue(TINYINT, "1.2345e2", null);
        assertValue(TINYINT, "1.5645e2", null);

        // other values are not allowed
        assertValue(TINYINT, "true", null);
        assertValue(TINYINT, "false", null);
        assertValue(TINYINT, "\"123\"", null);
        assertValue(TINYINT, "\"string\"", null);
        assertValue(TINYINT, "\"null\"", null);
    }

    @Test
    public void testDecimalShort()
            throws Exception
    {
        assertValue(SHORT_DECIMAL, "\\N", null);
        assertValue(SHORT_DECIMAL, "NOPE", null, NOPE_NULL_OPTION);

        // empty value is not allowed
        assertValue(SHORT_DECIMAL, "", null);

        // allowed range for LazySimpleSerDe
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

        DecimalType roundingType = createDecimalType(4, 2);
        assertValue(roundingType, "10.001", SqlDecimal.decimal("10.00", roundingType));
        assertValue(roundingType, "10.005", SqlDecimal.decimal("10.01", roundingType));
        assertValue(roundingType, "99.999", null);

        assertValue(SHORT_DECIMAL, "true", null);
        assertValue(SHORT_DECIMAL, "false", null);
        assertValue(SHORT_DECIMAL, "\"string\"", null);
        assertValue(SHORT_DECIMAL, "\"null\"", null);
    }

    @Test
    public void testDecimalLong()
            throws Exception
    {
        assertValue(LONG_DECIMAL, "\\N", null);
        assertValue(LONG_DECIMAL, "NOPE", null, NOPE_NULL_OPTION);

        // empty value is not allowed
        assertValue(LONG_DECIMAL, "", null);

        // allowed range for LazySimpleSerDe
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

        assertValue(LONG_DECIMAL, "1000000000000000000000000000000000000.00", null);
        assertValue(LONG_DECIMAL, "-1000000000000000000000000000000000000.00", null);
        assertValue(LONG_DECIMAL, "1e39", null);
        assertValue(LONG_DECIMAL, "-1e39", null);

        DecimalType roundingType = createDecimalType(38, 2);
        assertValue(roundingType, "10.001", toSqlDecimal(roundingType, "10.00"));
        assertValue(roundingType, "10.005", toSqlDecimal(roundingType, "10.01"));
        assertDecimal(LONG_DECIMAL, "999999999999999999999999999999999999.99000");
        assertDecimal(LONG_DECIMAL, "-999999999999999999999999999999999999.99000");
        assertValue(LONG_DECIMAL, "999999999999999999999999999999999999.99123", toSqlDecimal(LONG_DECIMAL, "999999999999999999999999999999999999.99"));
        assertValue(LONG_DECIMAL, "-999999999999999999999999999999999999.99123", toSqlDecimal(LONG_DECIMAL, "-999999999999999999999999999999999999.99"));
        assertValue(LONG_DECIMAL, "999999999999999999999999999999999999.999", null);
        assertValue(LONG_DECIMAL, "-999999999999999999999999999999999999.999", null);

        assertValue(LONG_DECIMAL, "true", null);
        assertValue(LONG_DECIMAL, "false", null);
        assertValue(LONG_DECIMAL, "\"string\"", null);
        assertValue(LONG_DECIMAL, "\"null\"", null);
    }

    private static void assertDecimal(DecimalType decimalType, String value)
            throws Exception
    {
        assertValue(decimalType, value, toSqlDecimal(decimalType, value));
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
            throws Exception
    {
        assertValue(REAL, "\\N", null);
        assertValue(REAL, "NOPE", null, NOPE_NULL_OPTION);

        // allowed range for LazySimpleSerDe
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
        assertValue(REAL, "+Inf", null);
        assertValue(REAL, "-Inf", null);

        assertValue(REAL, "true", null);
        assertValue(REAL, "false", null);
        assertValue(REAL, "\"123\"", null);
        assertValue(REAL, "\"string\"", null);
        assertValue(REAL, "\"null\"", null);
    }

    @Test
    public void testDouble()
            throws Exception
    {
        assertValue(DOUBLE, "\\N", null);
        assertValue(DOUBLE, "NOPE", null, NOPE_NULL_OPTION);

        // allowed range for LazySimpleSerDe
        assertValue(DOUBLE, "0", 0.0);
        assertValue(DOUBLE, "-0", -0.0);
        assertValue(DOUBLE, "123", 123.0);
        assertValue(DOUBLE, "-123", -123.0);
        assertValue(DOUBLE, "1.23", 1.23);
        assertValue(DOUBLE, "-1.23", -1.23);
        assertValue(DOUBLE, "1.5645e33", 1.5645e33);

        assertValue(DOUBLE, "NaN", Double.NaN);
        assertValue(DOUBLE, "Infinity", Double.POSITIVE_INFINITY);
        assertValue(DOUBLE, "+Infinity", Double.POSITIVE_INFINITY);
        assertValue(DOUBLE, "-Infinity", Double.NEGATIVE_INFINITY);
        assertValue(DOUBLE, "+Inf", null);
        assertValue(DOUBLE, "-Inf", null);

        assertValue(DOUBLE, "true", null);
        assertValue(DOUBLE, "false", null);
        assertValue(DOUBLE, "\"123\"", null);
        assertValue(DOUBLE, "\"string\"", null);
        assertValue(DOUBLE, "\"null\"", null);
    }

    @Test
    public void testDate()
            throws Exception
    {
        assertValue(DATE, "\\N", null);
        assertValue(DATE, "NOPE", null, NOPE_NULL_OPTION);

        // allowed range for LazySimpleSerDe
        String value = "1970-01-01";
        int days = 0;
        assertDate(value, days);
        assertDate("1970-01-02", 1);
        assertDate("1969-12-31", -1);

        // Hive ignores everything after the first space
        assertDate("1986-01-01 anything is allowed here", LocalDate.of(1986, 1, 1).toEpochDay());

        assertDate("1986-01-01", LocalDate.of(1986, 1, 1).toEpochDay());
        assertDate("1986-01-33", LocalDate.of(1986, 2, 2).toEpochDay());

        assertDate("5881580-07-11", Integer.MAX_VALUE);
        assertDate("-5877641-06-23", Integer.MIN_VALUE);

        // Hive does not enforce size bounds and truncates the results in Date.toEpochDay
        // For Trino we fail as this behavior is error-prone
        assertValueHive(DATE, "5881580-07-12", new SqlDate(Integer.MIN_VALUE), DEFAULT_SIMPLE_OPTIONS);
        assertValueFailsTrino(DATE, "5881580-07-12");
        assertValueHive(DATE, "-5877641-06-22", new SqlDate(Integer.MAX_VALUE), DEFAULT_SIMPLE_OPTIONS);
        assertValueFailsTrino(DATE, "-5877641-06-22");

        assertValue(DATE, "1", null);
        assertValue(DATE, "1.23", null);
        assertValue(DATE, "1.2345e2", null);
        assertValue(DATE, "1.56", null);
        assertValue(DATE, "1.5645e2", null);
        assertValue(DATE, "1.5645e300", null);

        assertValue(DATE, "true", null);
        assertValue(DATE, "false", null);
        assertValue(DATE, "\"123\"", null);
        assertValue(DATE, "\"string\"", null);
        assertValue(DATE, "\"null\"", null);
    }

    private static void assertDate(String value, long days)
            throws Exception
    {
        assertValue(DATE, value, new SqlDate(toIntExact(days)));
    }

    @Test
    public void testTimestampMicros()
            throws Exception
    {
        assertValue(TIMESTAMP_MICROS, "\\N", null);
        assertValue(TIMESTAMP_MICROS, "NOPE", null, NOPE_NULL_OPTION);

        // allowed range for LazySimpleSerDe
        assertTimestamp(TIMESTAMP_MICROS, "1970-01-01 00:00:00.000000", LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0));
        assertTimestamp(TIMESTAMP_MICROS, "2020-05-10 12:34:56.123456", LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_000));
        assertTimestamp(TIMESTAMP_MICROS, "1960-05-10 12:34:56.123456", LocalDateTime.of(1960, 5, 10, 12, 34, 56, 123_456_000));

        assertTimestamp(TIMESTAMP_MICROS, "294247-01-10 04:00:54.775807", LocalDateTime.of(294247, 1, 10, 4, 0, 54, 775_807_000));
        assertTimestamp(TIMESTAMP_MICROS, "-290308-12-21 19:59:06.224192", LocalDateTime.of(-290308, 12, 21, 19, 59, 6, 224_192_000));

        // test rounding
        assertTimestamp(TIMESTAMP_MICROS, "2020-05-10 12:34:56.1234561", LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_000));
        assertTimestamp(TIMESTAMP_MICROS, "2020-05-10 12:34:56.1234565", LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_457_000));

        // Hive does not enforce size bounds
        assertValueFailsTrino(TIMESTAMP_MICROS, "294247-01-10 04:00:54.775808");
        // it isn't really possible to test for exact min value, because the sequence gets doesn't transfer through testing framework
        assertValueFailsTrino(TIMESTAMP_MICROS, "-290308-12-21 19:59:05.224192");

        assertValue(TIMESTAMP_MICROS, "1", null);
        assertValue(TIMESTAMP_MICROS, "1.23", null);
        assertValue(TIMESTAMP_MICROS, "1.2345e2", null);
        assertValue(TIMESTAMP_MICROS, "1.56", null);
        assertValue(TIMESTAMP_MICROS, "1.5645e2", null);
        assertValue(TIMESTAMP_MICROS, "1.5645e300", null);

        assertValue(TIMESTAMP_MICROS, "true", null);
        assertValue(TIMESTAMP_MICROS, "false", null);
        assertValue(TIMESTAMP_MICROS, "\"123\"", null);
        assertValue(TIMESTAMP_MICROS, "\"string\"", null);
        assertValue(TIMESTAMP_MICROS, "\"null\"", null);
    }

    @Test
    public void testTimestampNanos()
            throws Exception
    {
        assertValue(TIMESTAMP_NANOS, "\\N", null);
        assertValue(TIMESTAMP_NANOS, "NOPE", null, NOPE_NULL_OPTION);

        assertTimestamp(TIMESTAMP_NANOS, "1970-01-01 00:00:00.000000000", LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0));
        assertTimestamp(TIMESTAMP_NANOS, "2020-05-10 12:34:56.123456789", LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_789));
        assertTimestamp(TIMESTAMP_NANOS, "1960-05-10 12:34:56.123456789", LocalDateTime.of(1960, 5, 10, 12, 34, 56, 123_456_789));

        assertTimestamp(TIMESTAMP_NANOS, "294247-01-10 04:00:54.775807999", LocalDateTime.of(294247, 1, 10, 4, 0, 54, 775_807_999));
        assertTimestamp(TIMESTAMP_NANOS, "-290308-12-21 19:59:06.224192000", LocalDateTime.of(-290308, 12, 21, 19, 59, 6, 224_192_000));

        // ISO 8601 also allowed
        assertTimestamp(TIMESTAMP_NANOS, "2020-05-10T12:34:56.123456789", LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_789));

        // Hive does not enforce size bounds
        assertValueFailsTrino(TIMESTAMP_NANOS, "294247-01-10 04:00:54.775808000");
        // it isn't really possible to test for exact min value, because the sequence gets doesn't transfer through testing framework
        assertValueFailsTrino(TIMESTAMP_NANOS, "-290308-12-21 19:59:05.224192000");

        // custom formats
        assertTimestamp(
                TIMESTAMP_NANOS,
                "05/10/2020 12.34.56.123",
                LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_000_000),
                "MM/dd/yyyy HH.mm.ss.SSS");
        assertTimestamp(
                TIMESTAMP_NANOS,
                "05/10/2020 7",
                LocalDateTime.of(2020, 5, 10, 7, 0, 0, 0),
                "MM/dd/yyyy HH");
        assertTimestamp(
                TIMESTAMP_NANOS,
                "1589114096123.777",
                LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_000_000),
                "millis");
        assertTimestamp(
                TIMESTAMP_NANOS,
                "05/10/2020 12.34.56.123",
                LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_000_000),
                "yyyy",
                "MM/dd/yyyy HH.mm.ss.SSS",
                "millis");
        assertTimestamp(
                TIMESTAMP_NANOS,
                "1589114096123.777",
                LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_000_000),
                "yyyy",
                "MM/dd/yyyy HH.mm.ss.SSS",
                "millis");

        // Hive requires that values be at least 8 characters
        assertTimestamp(
                TIMESTAMP_NANOS,
                "1/1/-10",
                null,
                "MM/dd/yyyy");
        assertTimestamp(
                TIMESTAMP_NANOS,
                "01/1/-10",
                LocalDateTime.of(-10, 1, 1, 0, 0),
                "MM/dd/yyyy");

        // Default Hive timestamp and ISO 8601 always supported in both normal values and map keys
        assertValue(
                TIMESTAMP_NANOS,
                "2020-05-10 12:34:56.123456789",
                toSqlTimestamp(TIMESTAMP_NANOS, LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_789)),
                TextEncodingOptions.builder()
                        .timestampFormats("yyyy", "MM/dd/yyyy HH.mm.ss.SSS", "millis")
                        .build());
        assertValue(
                TIMESTAMP_NANOS,
                "2020-05-10T12:34:56.123456789",
                toSqlTimestamp(TIMESTAMP_NANOS, LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_789)),
                TextEncodingOptions.builder()
                        .timestampFormats("yyyy", "MM/dd/yyyy HH.mm.ss.SSS", "millis")
                        .build());

        assertValue(TIMESTAMP_NANOS, "1", null);
        assertValue(TIMESTAMP_NANOS, "1.23", null);
        assertValue(TIMESTAMP_NANOS, "1.2345e2", null);
        assertValue(TIMESTAMP_NANOS, "1.56", null);
        assertValue(TIMESTAMP_NANOS, "1.5645e2", null);
        assertValue(TIMESTAMP_NANOS, "1.5645e300", null);

        assertValue(TIMESTAMP_NANOS, "true", null);
        assertValue(TIMESTAMP_NANOS, "false", null);
        assertValue(TIMESTAMP_NANOS, "\"123\"", null);
        assertValue(TIMESTAMP_NANOS, "\"string\"", null);
        assertValue(TIMESTAMP_NANOS, "\"null\"", null);
        assertValue(TIMESTAMP_NANOS, "NULL", null);
    }

    private static void assertTimestamp(TimestampType timestampType, String value, LocalDateTime localDateTime, String... timestampFormats)
            throws Exception
    {
        assertValue(
                timestampType,
                value,
                toSqlTimestamp(timestampType, localDateTime),
                TextEncodingOptions.builder()
                        .timestampFormats(timestampFormats)
                        .build());
    }

    private static void assertValue(Type type, String value, Object expectedValue)
            throws Exception
    {
        assertValue(type, value, expectedValue, DEFAULT_SIMPLE_OPTIONS);
    }

    private static void assertValue(Type type, String value, Object expectedValue, TextEncodingOptions textEncodingOptions)
            throws Exception
    {
        assertValue(type, value, expectedValue, textEncodingOptions, true);
    }

    private static void assertValue(Type type, String value, Object expectedValue, TextEncodingOptions textEncodingOptions, boolean verifyByteForByte)
            throws Exception
    {
        assertValueHive(type, value, expectedValue, textEncodingOptions);
        assertValueTrino(type, value, expectedValue, textEncodingOptions, verifyByteForByte);
    }

    private static void assertValueTrino(Type type, String value, Object expectedValue, TextEncodingOptions textEncodingOptions, boolean verifyByteForByte)
            throws Exception
    {
        internalAssertValueTrino(type, value, expectedValue, textEncodingOptions, verifyByteForByte);
        internalAssertValueTrino(
                new ArrayType(type),
                increaseDepth(value, 1, textEncodingOptions.getEscapeByte()) + "\2" + increaseDepth(value, 1, textEncodingOptions.getEscapeByte()),
                Arrays.asList(expectedValue, expectedValue),
                textEncodingOptions,
                verifyByteForByte);
        internalAssertValueTrino(
                RowType.rowType(field("a", BIGINT), field("nested", type), field("b", BIGINT)),
                "\2" + increaseDepth(value, 1, textEncodingOptions.getEscapeByte()) + "\2",
                Arrays.asList(null, expectedValue, null),
                textEncodingOptions,
                verifyByteForByte);
        internalAssertValueTrino(
                new MapType(BIGINT, type, TYPE_OPERATORS),
                "1234\3" + increaseDepth(value, 2, textEncodingOptions.getEscapeByte()),
                singletonMap(1234L, expectedValue),
                textEncodingOptions,
                verifyByteForByte);
        if (expectedValue != null) {
            internalAssertValueTrino(
                    new MapType(type, BIGINT, TYPE_OPERATORS),
                    increaseDepth(value, 2, textEncodingOptions.getEscapeByte()) + "\3" + "1234",
                    singletonMap(expectedValue, 1234L),
                    textEncodingOptions,
                    verifyByteForByte);
        }
    }

    private static String increaseDepth(String value, int depth, Byte escapeByte)
    {
        char[] chars = value.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (escapeByte != null && chars[i] == escapeByte) {
                // skip next character
                //noinspection AssignmentToForLoopParameter
                i++;
                continue;
            }
            if (chars[i] < 9) {
                chars[i] = (char) ((int) chars[i] + depth);
            }
        }
        return new String(chars);
    }

    private static void internalAssertValueTrino(Type type, String value, Object expectedValue, TextEncodingOptions textEncodingOptions, boolean verifyByteForByte)
            throws Exception
    {
        String line = "ignore\1" + value + "\1ignore";
        List<Column> columns = ImmutableList.of(
                new Column("a", BIGINT, 0),
                new Column("test", type, 1),
                new Column("b", BIGINT, 2));
        assertLineTrino(columns, utf8Slice(line), Arrays.asList(null, expectedValue, null), textEncodingOptions, verifyByteForByte);
    }

    private static void assertLine(List<Column> columns, String line, List<Object> expectedValues, TextEncodingOptions textEncodingOptions, boolean verifyByteForByte)
            throws Exception
    {
        assertLine(columns, utf8Slice(line), expectedValues, textEncodingOptions, verifyByteForByte);
    }

    private static void assertLine(List<Column> columns, Slice line, List<Object> expectedValues, TextEncodingOptions textEncodingOptions, boolean verifyByteForByte)
            throws Exception
    {
        assertLineHive(columns, line, expectedValues, textEncodingOptions);
        assertLineTrino(columns, line, expectedValues, textEncodingOptions, verifyByteForByte);
    }

    private static void assertLineTrino(List<Column> columns, Slice line, List<Object> expectedValues, TextEncodingOptions textEncodingOptions, boolean verifyByteForByte)
            throws Exception
    {
        List<Object> actualValues = readTrinoLine(columns, line, textEncodingOptions);
        for (int i = 0; i < columns.size(); i++) {
            Type type = columns.get(i).type();
            Object actualValue = actualValues.get(i);
            Object expectedValue = expectedValues.get(i);
            assertColumnValueEquals(type, actualValue, expectedValue);
        }

        // write the value using Trino
        Slice trinoLine = writeTrinoLine(columns, expectedValues, textEncodingOptions);

        // verify Trino can read back the Trino line
        actualValues = readTrinoLine(columns, trinoLine, textEncodingOptions);
        for (int i = 0; i < columns.size(); i++) {
            Type type = columns.get(i).type();
            Object actualValue = actualValues.get(i);
            Object expectedValue = expectedValues.get(i);
            assertColumnValueEquals(type, actualValue, expectedValue);
        }

        // Hive has a bug where it can not write a complex map key, but it can read it
        for (Column column : columns) {
            if (!hasComplexMapKey(column.type())) {
                return;
            }
        }

        // write the value using Hive
        Slice hiveLine = writeHiveLine(columns, line, textEncodingOptions);

        // Note, Hive does not use timestamp format when writing, and in some cases fails on read if the format is set
        TextEncodingOptions withoutTimestampFormats = TextEncodingOptions.builder(textEncodingOptions).timestampFormats(ImmutableList.of()).build();

        // If hive can not read back it own line
        try {
            assertLineHive(columns, hiveLine, expectedValues, withoutTimestampFormats);
        }
        catch (AssertionError ignored) {
            // This serde has many bugs in the render that result in data that can not be read
            // In this case we do not attempt to verify, byte-for-byte
            return;
        }

        // Hive has bugs with very negative Date and Timestamp that cause it to render them as
        // positive values, and render them as negative values.  This means the value round trips,
        // but the rendered value is invalid.
        if (!verifyByteForByte) {
            return;
        }

        // verify Hive can read back the Trino line
        assertLineHive(columns, trinoLine, expectedValues, withoutTimestampFormats);

        // verify Trino can read back the Hive line
        actualValues = readTrinoLine(columns, hiveLine, textEncodingOptions);
        for (int i = 0; i < columns.size(); i++) {
            Type type = columns.get(i).type();
            Object actualValue = actualValues.get(i);
            Object expectedValue = expectedValues.get(i);
            assertColumnValueEquals(type, actualValue, expectedValue);
        }

        assertThat(trinoLine).isEqualTo(hiveLine);
    }

    // TODO should we just block these types
    private static boolean hasComplexMapKey(Type type)
    {
        if (type instanceof MapType mapType) {
            if (!isScalarType(mapType.getKeyType())) {
                // hive can not read complex map keys (but can write them)
                return false;
            }
            return hasComplexMapKey(mapType.getKeyType()) && hasComplexMapKey(mapType.getValueType());
        }
        else if (type instanceof ArrayType arrayType) {
            return hasComplexMapKey(arrayType.getElementType());
        }
        else if (type instanceof RowType rowType) {
            return rowType.getTypeParameters().stream().allMatch(TestSimpleFormat::hasComplexMapKey);
        }
        return true;
    }

    private static List<Object> readTrinoLine(List<Column> columns, Slice line, TextEncodingOptions textEncodingOptions)
            throws IOException
    {
        Page page = decodeTrinoLine(columns, line, textEncodingOptions);
        return readTrinoValues(columns, page, 0);
    }

    private static Page decodeTrinoLine(List<Column> columns, Slice line, TextEncodingOptions textEncodingOptions)
            throws IOException
    {
        Map<String, String> schema = createLazySimpleSerDeProperties(columns, textEncodingOptions);
        LineDeserializer deserializer = new SimpleDeserializerFactory().create(columns, schema);
        PageBuilder pageBuilder = new PageBuilder(1, columns.stream()
                .map(Column::type)
                .collect(toImmutableList()));
        deserializer.deserialize(createLineBuffer(line), pageBuilder);
        return pageBuilder.build();
    }

    private static Slice writeTrinoLine(List<Column> columns, List<Object> expectedValues, TextEncodingOptions options)
            throws IOException
    {
        Page page = toSingleRowPage(columns, expectedValues);

        LineSerializer serializer = new SimpleSerializerFactory().create(columns, options.toSchema());
        SliceOutput sliceOutput = new DynamicSliceOutput(1024);
        serializer.write(page, 0, sliceOutput);
        return sliceOutput.slice().copy();
    }

    private static void assertValueHive(Type type, String value, Object expectedValue, TextEncodingOptions textEncodingOptions)
            throws SerDeException
    {
        String line = "\1" + value + "\1";
        List<Column> columns = ImmutableList.of(
                new Column("a", BIGINT, 0),
                new Column("test", type, 1),
                new Column("b", BIGINT, 2));

        List<Object> actualValues = readHive(columns, line, textEncodingOptions);
        assertColumnValueEquals(type, actualValues.get(1), expectedValue);
    }

    private static void assertLineHive(List<Column> columns, Slice line, List<Object> expectedValues, TextEncodingOptions textEncodingOptions)
            throws SerDeException
    {
        List<Object> actualValues = readHive(columns, line, textEncodingOptions);
        for (int i = 0; i < columns.size(); i++) {
            Type type = columns.get(i).type();
            Object actualValue = actualValues.get(i);
            Object expectedValue = expectedValues.get(i);
            assertColumnValueEquals(type, actualValue, expectedValue);
        }
    }

    private static List<Object> readHive(List<Column> columns, String line, TextEncodingOptions textEncodingOptions)
            throws SerDeException
    {
        return readHive(columns, utf8Slice(line), textEncodingOptions);
    }

    private static List<Object> readHive(List<Column> columns, Slice line, TextEncodingOptions textEncodingOptions)
            throws SerDeException
    {
        Deserializer deserializer = createHiveSerDe(columns, textEncodingOptions);

        Object row = deserializer.deserialize(new Text(line.getBytes()));
        StructObjectInspector inspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<Object> actualValues = columns.stream()
                .map(column -> decodeRecordReaderValue(
                        column.type(),
                        inspector.getStructFieldData(row, inspector.getStructFieldRef(column.name()))))
                .collect(toCollection(ArrayList::new));
        return actualValues;
    }

    private static Slice writeHiveLine(List<Column> columns, Slice line, TextEncodingOptions options)
            throws SerDeException
    {
        List<Object> values = readHive(columns, line, options);

        SettableStructObjectInspector objectInspector = getStandardStructObjectInspector(
                columns.stream().map(Column::name).collect(toImmutableList()),
                columns.stream().map(Column::type).map(FormatTestUtils::getJavaObjectInspector).collect(toImmutableList()));

        Object row = objectInspector.create();
        for (int i = 0; i < columns.size(); i++) {
            Object value = toHiveWriteValue(columns.get(i).type(), values.get(i), Optional.empty());
            objectInspector.setStructFieldData(row, objectInspector.getAllStructFieldRefs().get(i), value);
        }

        LazySimpleSerDe serializer = createHiveSerDe(columns, options);
        Text serialize = (Text) serializer.serialize(row, objectInspector);
        return Slices.wrappedBuffer(serialize.copyBytes());
    }

    private static LazySimpleSerDe createHiveSerDe(List<Column> columns, TextEncodingOptions textEncodingOptions)
            throws SerDeException
    {
        Properties schema = new Properties();
        schema.putAll(createLazySimpleSerDeProperties(columns, textEncodingOptions));

        JobConf configuration = new JobConf(newEmptyConfiguration());
        LazySimpleSerDe deserializer = new LazySimpleSerDe();
        deserializer.initialize(configuration, schema);
        return deserializer;
    }

    private static void assertValueFailsTrino(Type type, String value)
    {
        String line = "\1" + value + "\1";
        List<Column> columns = ImmutableList.of(
                new Column("a", BIGINT, 0),
                new Column("test", type, 1),
                new Column("b", BIGINT, 2));
        assertLineFailsTrino(columns, line);
    }

    private static void assertLineFailsTrino(List<Column> columns, String line)
    {
        LineDeserializer deserializer = new SimpleDeserializerFactory().create(
                columns,
                createLazySimpleSerDeProperties(columns, DEFAULT_SIMPLE_OPTIONS));
        PageBuilder pageBuilder = new PageBuilder(1, columns.stream()
                .map(Column::type)
                .collect(toImmutableList()));
        assertThatThrownBy(() -> deserializer.deserialize(createLineBuffer(utf8Slice(line)), pageBuilder))
                .isInstanceOf(Exception.class);
    }

    private static Map<String, String> createLazySimpleSerDeProperties(List<Column> columns, TextEncodingOptions textEncodingOptions)
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
        schema.putAll(textEncodingOptions.toSchema());
        return schema.buildOrThrow();
    }
}
