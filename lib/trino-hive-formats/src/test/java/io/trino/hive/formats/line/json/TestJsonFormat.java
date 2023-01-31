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
package io.trino.hive.formats.line.json;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import io.trino.spi.type.DateType;
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
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.hive.formats.FormatTestUtils.assertColumnValueEquals;
import static io.trino.hive.formats.FormatTestUtils.createLineBuffer;
import static io.trino.hive.formats.FormatTestUtils.decodeRecordReaderValue;
import static io.trino.hive.formats.FormatTestUtils.isScalarType;
import static io.trino.hive.formats.FormatTestUtils.readTrinoValues;
import static io.trino.hive.formats.FormatTestUtils.toSingleRowPage;
import static io.trino.hive.formats.FormatTestUtils.toSqlTimestamp;
import static io.trino.hive.formats.HiveFormatUtils.TIMESTAMP_FORMATS_KEY;
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
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.SerDeUtils.escapeString;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

public class TestJsonFormat
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private static final DecimalType SHORT_DECIMAL = createDecimalType(MAX_SHORT_PRECISION, 2);
    private static final DecimalType LONG_DECIMAL = createDecimalType(MAX_PRECISION, 2);
    private static final VarcharType VARCHAR_3 = createVarcharType(3);
    private static final CharType CHAR_100 = createCharType(100);
    private static final CharType CHAR_3 = createCharType(3);

    @Test
    public void testStruct()
            throws Exception
    {
        RowType rowType = RowType.rowType(field("a", BIGINT), field("b", BIGINT), field("c", BIGINT));
        assertValue(rowType, "{ \"_col0\" : 1, \"_col1\" : 2, \"_col2\" : 3 }", ImmutableList.of(1L, 2L, 3L));

        assertValue(rowType, "null", null);

        assertValue(rowType, "{ \"a\" : 1, \"b\" : 2, \"c\" : 3 }", ImmutableList.of(1L, 2L, 3L));
        assertValue(rowType, "{ \"c\" : 3, \"a\" : 1, \"b\" : 2 }", ImmutableList.of(1L, 2L, 3L));
        assertValue(rowType, "{ \"x\" : 3, \"c\" : 3, \"a\" : 1, \"b\" : 2 , \"y\" : 2 }", ImmutableList.of(1L, 2L, 3L));
        assertValue(rowType, "{}", Arrays.asList(null, null, null));
        assertValue(rowType, "{ \"b\" : 2 }", Arrays.asList(null, 2L, null));

        // Duplicate fields are supported, and the last value is used
        assertValue(rowType, "{ \"a\" : 1, \"a\" : 2 }", Arrays.asList(2L, null, null));
        // but Hive parses all fields
        assertValueFailsHive(rowType, "{ \"a\" : true, \"a\" : 42 }", false);
        // and we only parse the last field
        assertValueTrino(rowType, "{ \"a\" : true, \"a\" : 42 }", Arrays.asList(42L, null, null));

        // Hive allows columns to have names based on ordinals
        assertValue(rowType, "{ \"_col0\" : 1, \"_col1\" : 2, \"_col2\" : 3 }", ImmutableList.of(1L, 2L, 3L));
        assertValue(rowType, "{ \"_col2\" : 3, \"_col0\" : 1, \"_col1\" : 2 }", ImmutableList.of(1L, 2L, 3L));
        assertValue(rowType, "{ \"_col2\" : 3, \"a\" : 1, \"b\" : 2 }", ImmutableList.of(1L, 2L, 3L));
        assertValueTrino(rowType, "{ \"_col0\" : true, \"a\" : 42 }", Arrays.asList(42L, null, null));
        assertValueTrino(rowType, "{ \"a\" : true, \"_col0\" : 42 }", Arrays.asList(42L, null, null));

        assertValueFails(rowType, "true");
        assertValueFails(rowType, "12");
        assertValueFails(rowType, "12.34");
        assertValueFails(rowType, "\"string\"");
        assertValueFails(rowType, "[ 42 ]");
    }

    @Test
    public void testMap()
            throws Exception
    {
        MapType mapType = new MapType(VARCHAR, BIGINT, TYPE_OPERATORS);
        assertValue(mapType, "null", null);

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

        // but Trino parses only the last value whereas Hive parses all values
        assertValueTrino(
                mapType,
                "{ \"a\" : false, \"a\" : 2 }",
                ImmutableMap.builder()
                        .put("a", 2L)
                        .buildOrThrow());
        assertValueFailsHive(mapType, "{ \"a\" : false, \"a\" : 2 }", false);

        assertValueFails(mapType, "true");
        assertValueFails(mapType, "12");
        assertValueFails(mapType, "12.34");
        assertValueFails(mapType, "\"string\"");
        assertValueFails(mapType, "[ 42 ]");
    }

    @Test
    public void testVarchar()
            throws Exception
    {
        assertValue(VARCHAR, "null", null);

        assertValue(VARCHAR, "\"value\"", "value");

        // Trailing spaces are NOT truncated
        assertValue(VARCHAR, "\"value     \"", "value     ");

        // Truncation
        assertValue(VARCHAR_3, "\"v\"", "v");
        assertValue(VARCHAR_3, "\"val\"", "val");
        assertValue(VARCHAR_3, "\"value\"", "val");

        // Escape
        assertValue(VARCHAR, "\"tab \\t tab\"", "tab \t tab");
        assertValue(VARCHAR, "\"new \\n line\"", "new \n line");
        assertValue(VARCHAR, "\"carriage \\r return\"", "carriage \r return");

        assertValue(VARCHAR, "true", "true");
        assertValue(VARCHAR, "false", "false");

        assertValue(VARCHAR, "-1", "-1");
        assertValue(VARCHAR, "1.23", "1.23");
        assertValue(VARCHAR, "1.23e45", "1.23e45");

        assertValueFails(VARCHAR, "[ \"value\" ]");
        assertValueFailsTrino(VARCHAR, "{ \"x\" : \"value\" }");
    }

    @Test
    public void testVarbinary()
            throws Exception
    {
        assertValue(VARBINARY, "null", null);

        assertVarbinary("\"value\"", "value");

        assertVarbinary("true", "true");
        assertVarbinary("false", "false");

        assertVarbinary("-1", "-1");
        assertVarbinary("1.23", "1.23");
        assertVarbinary("1.23e45", "1.23e45");

        assertValueFails(VARBINARY, "[ \"value\" ]");
        assertValueFailsTrino(VARBINARY, "{ \"x\" : \"value\" }");

        // Hive does not properly decode binary data, and these tests simply verify our behavior is the same as Hive

        // all possible JSON value bytes
        StringBuilder allBytesJsonValueBuilder = new StringBuilder();
        allBytesJsonValueBuilder.append('\"');
        for (int i = 0; i < 255; i++) {
            allBytesJsonValueBuilder.append("\\u");
            String hex = Integer.toHexString(i);
            allBytesJsonValueBuilder.append("0".repeat(4 - hex.length()));
            allBytesJsonValueBuilder.append(hex);
        }
        allBytesJsonValueBuilder.append('\"');
        String allBytesJsonValue = allBytesJsonValueBuilder.toString();

        //noinspection SpellCheckingInspection
        assertVarbinary(
                allBytesJsonValue,
                "" +
                "\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\t\n\u000B\f\r\u000E\u000F" +
                "\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001A\u001B\u001C\u001D\u001E\u001F" +
                " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~" +
                "\u007F\u0080\u0081\u0082\u0083\u0084\u0085\u0086\u0087\u0088\u0089\u008A\u008B\u008C\u008D\u008E" +
                "\u008F\u0090\u0091\u0092\u0093\u0094\u0095\u0096\u0097\u0098\u0099\u009A\u009B\u009C\u009D\u009E" +
                "\u009F\u00A0\u00A1\u00A2\u00A3\u00A4\u00A5\u00A6\u00A7\u00A8\u00A9\u00AA\u00AB\u00AC\u00AD" +
                "®¯°±²³´µ¶·¸¹º»¼½¾¿ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖ×ØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþ");

        // all possible input bytes processed by Hive writer
        byte[] allBytes = new byte[255];
        for (int i = 0; i < allBytes.length; i++) {
            allBytes[i] = (byte) i;
        }
        StringBuilder hiveJsonValueBuilder = new StringBuilder();
        hiveJsonValueBuilder
                .append(SerDeUtils.QUOTE)
                .append(escapeString(new Text(allBytes).toString()))
                .append(SerDeUtils.QUOTE);
        String hiveJsonValue = hiveJsonValueBuilder.toString();

        //noinspection SpellCheckingInspection
        assertVarbinary(
                hiveJsonValue,
                "" +
                "\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\t\n\u000B\f\r\u000E\u000F" +
                "\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001A\u001B\u001C\u001D\u001E\u001F" +
                " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~" +
                "\u007F" +
                "�������������������������������������������������������������������������������������������������������������������������������");
    }

    private static void assertVarbinary(String jsonValue, String expectedValue)
            throws Exception
    {
        assertValue(VARBINARY, jsonValue, new SqlVarbinary(expectedValue.getBytes(UTF_8)));
    }

    @Test
    public void testChar()
            throws Exception
    {
        assertValue(CHAR_100, "null", null);

        assertChar(CHAR_100, "\"value\"", "value");

        // Trailing spaces are truncated
        assertChar(CHAR_100, "\"value     \"", "value");

        // Truncation
        assertChar(CHAR_3, "\"v\"", "v");
        assertChar(CHAR_3, "\"val\"", "val");
        assertChar(CHAR_3, "\"value\"", "val");

        // Escape
        assertChar(CHAR_100, "\"tab \\t tab\"", "tab \t tab");
        assertChar(CHAR_100, "\"new \\n line\"", "new \n line");
        assertChar(CHAR_100, "\"carriage \\r return\"", "carriage \r return");

        assertChar(CHAR_100, "true", "true");
        assertChar(CHAR_100, "false", "false");

        assertChar(CHAR_100, "-1", "-1");
        assertChar(CHAR_100, "1.23", "1.23");
        assertChar(CHAR_100, "1.23e45", "1.23e45");

        assertValueFails(CHAR_100, "[ \"value\" ]");
        assertValueFailsTrino(CHAR_100, "{ \"x\" : \"value\" }");
    }

    private static void assertChar(CharType charType, String jsonValue, String expectedValue)
            throws Exception
    {
        assertValue(charType, jsonValue, padSpaces(expectedValue, charType));
    }

    @Test
    public void testBoolean()
            throws Exception
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

        assertValueFails(BOOLEAN, "[ true ]", false);
        assertValueFailsTrino(BOOLEAN, "{ \"x\" : false }", false);
    }

    @Test
    public void testBigint()
            throws Exception
    {
        assertValue(BIGINT, "null", null);

        // allowed range for JsonSerDe
        assertValue(BIGINT, "0", 0L);
        assertValue(BIGINT, "1", 1L);
        assertValue(BIGINT, "-1", -1L);
        assertValue(BIGINT, String.valueOf(Long.MAX_VALUE), Long.MAX_VALUE);
        assertValue(BIGINT, String.valueOf(Long.MIN_VALUE), Long.MIN_VALUE);

        assertValueFails(BIGINT, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE).toString());
        assertValueFails(BIGINT, BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE).toString());

        // Normal value supports decimal notation and is truncated, but decimal map keys throw
        assertValue(BIGINT, "1.23", 1L, false);
        assertValue(BIGINT, "1.2345e2", 123L, false);
        assertValue(BIGINT, "1.56", 1L, false);
        assertValue(BIGINT, "1.5645e2", 156L, false);
        assertValueFails(BIGINT, "1.5645e300");

        assertValueFails(BIGINT, "true");
        assertValueFails(BIGINT, "false");
        assertValueFails(BIGINT, "\"123\"", false);
        assertValueFails(BIGINT, "\"string\"");
        assertValueFails(BIGINT, "\"null\"");

        assertValueFails(BIGINT, "[ 42 ]", false);
        assertValueFails(BIGINT, "{ \"x\" : 42 }", false);
    }

    @Test
    public void testInteger()
            throws Exception
    {
        assertValue(INTEGER, "null", null);

        // allowed range for JsonSerDe
        assertValue(INTEGER, "0", 0);
        assertValue(INTEGER, "1", 1);
        assertValue(INTEGER, "-1", -1);

        assertValue(INTEGER, String.valueOf(Integer.MAX_VALUE), Integer.MAX_VALUE);
        assertValue(INTEGER, String.valueOf(Integer.MIN_VALUE), Integer.MIN_VALUE);
        assertValueFails(INTEGER, String.valueOf(Integer.MAX_VALUE + 1L));
        assertValueFails(INTEGER, String.valueOf(Integer.MIN_VALUE - 1L));

        // Normal value supports decimal notation and is truncated, but decimal map keys throw
        assertValue(INTEGER, "1.23", 1, false);
        assertValue(INTEGER, "1.2345e2", 123, false);
        assertValue(INTEGER, "1.56", 1, false);
        assertValue(INTEGER, "1.5645e2", 156, false);
        assertValueFails(INTEGER, "1.5645e300");

        assertValueFails(INTEGER, "true");
        assertValueFails(INTEGER, "false");
        assertValueFails(INTEGER, "\"123\"", false);
        assertValueFails(INTEGER, "\"string\"");
        assertValueFails(INTEGER, "\"null\"");

        assertValueFails(INTEGER, "[ 42 ]", false);
        assertValueFails(INTEGER, "{ \"x\" : 42 }", false);
    }

    @Test
    public void testSmallInt()
            throws Exception
    {
        assertValue(SMALLINT, "null", null);

        // allowed range for JsonSerDe
        assertValue(SMALLINT, "0", (short) 0);
        assertValue(SMALLINT, "1", (short) 1);
        assertValue(SMALLINT, "-1", (short) -1);
        assertValue(SMALLINT, "32767", (short) 32767);
        assertValue(SMALLINT, "-32768", (short) -32768);
        assertValueFails(SMALLINT, "32768");
        assertValueFails(SMALLINT, "-32769");

        // Normal value supports decimal notation and is truncated, but decimal map keys throw
        assertValue(SMALLINT, "1.23", (short) 1, false);
        assertValue(SMALLINT, "1.2345e2", (short) 123, false);
        assertValue(SMALLINT, "1.56", (short) 1, false);
        assertValue(SMALLINT, "1.5645e2", (short) 156, false);
        assertValueFails(SMALLINT, "1.5645e7");

        assertValueFails(SMALLINT, "true");
        assertValueFails(SMALLINT, "false");
        assertValueFails(SMALLINT, "\"123\"", false);
        assertValueFails(SMALLINT, "\"string\"");
        assertValueFails(SMALLINT, "\"null\"");

        assertValueFails(SMALLINT, "[ 42 ]", false);
        assertValueFails(SMALLINT, "{ \"x\" : 42 }", false);
    }

    @Test
    public void testTinyint()
            throws Exception
    {
        assertValue(TINYINT, "null", null);

        // allowed range for JsonSerDe
        assertValue(TINYINT, "0", (byte) 0);
        assertValue(TINYINT, "1", (byte) 1);
        assertValue(TINYINT, "-1", (byte) -1);
        assertValue(TINYINT, "127", (byte) 127);
        assertValue(TINYINT, "-128", (byte) -128);
        // allowed range for value is [-128, 255], but map key is [-128, 127]
        assertValue(TINYINT, "128", (byte) -128, false);
        assertValue(TINYINT, "129", (byte) -127, false);
        assertValue(TINYINT, "255", (byte) -1, false);
        assertValueFails(TINYINT, "256");
        assertValueFails(TINYINT, "-129");

        // Normal value supports decimal notation and is truncated, but decimal map keys throw
        assertValue(TINYINT, "1.23", (byte) 1, false);
        assertValue(TINYINT, "1.2345e2", (byte) 123, false);
        assertValue(TINYINT, "1.56", (byte) 1, false);
        assertValue(TINYINT, "1.5645e2", (byte) 156, false);
        assertValueFails(TINYINT, "1.5645e3");

        assertValueFails(TINYINT, "true");
        assertValueFails(TINYINT, "false");
        assertValueFails(TINYINT, "\"123\"", false);
        assertValueFails(TINYINT, "\"string\"");
        assertValueFails(TINYINT, "\"null\"");

        assertValueFails(TINYINT, "[ 42 ]", false);
        assertValueFails(TINYINT, "{ \"x\" : 42 }", false);
    }

    @Test
    public void testDecimalShort()
            throws Exception
    {
        assertValue(SHORT_DECIMAL, "null", null);

        // allowed range for JsonSerDe
        assertDecimal(SHORT_DECIMAL, "0");
        assertDecimal(SHORT_DECIMAL, "1");
        assertDecimal(SHORT_DECIMAL, "-1");
        assertDecimal(SHORT_DECIMAL, "9999999999999999.99");
        assertDecimal(SHORT_DECIMAL, "-9999999999999999.99");

        assertDecimal(SHORT_DECIMAL, "1.2345e2");
        assertDecimal(SHORT_DECIMAL, "1.5645e15");

        // size bounds
        assertValueFails(SHORT_DECIMAL, "10000000000000000.00");
        assertValueFails(SHORT_DECIMAL, "-10000000000000000.00");
        assertValueFails(SHORT_DECIMAL, "1e19");
        assertValueFails(SHORT_DECIMAL, "-1e19");

        // rounding
        DecimalType roundingType = createDecimalType(4, 2);
        assertValue(roundingType, "10.001", SqlDecimal.decimal("10.00", roundingType));
        assertValue(roundingType, "10.005", SqlDecimal.decimal("10.01", roundingType));
        assertValueFails(roundingType, "99.999");

        assertValue(SHORT_DECIMAL, "true", null);
        assertValue(SHORT_DECIMAL, "false", null);
        assertValue(SHORT_DECIMAL, "\"string\"", null);
        assertValue(SHORT_DECIMAL, "\"null\"", null);

        assertValueFails(SHORT_DECIMAL, "[ 42 ]", false);
        assertValueFailsTrino(SHORT_DECIMAL, "{ \"x\" : 42 }", false);
    }

    @Test
    public void testDecimalLong()
            throws Exception
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

        // size bounds
        assertValueFails(LONG_DECIMAL, "1000000000000000000000000000000000000.00");
        assertValueFails(LONG_DECIMAL, "-1000000000000000000000000000000000000.00");

        // rounding
        DecimalType roundingType = createDecimalType(38, 2);
        assertValue(roundingType, "10.001", SqlDecimal.decimal("10.00", roundingType));
        assertValue(roundingType, "10.005", SqlDecimal.decimal("10.01", roundingType));

        assertValue(LONG_DECIMAL, "true", null);
        assertValue(LONG_DECIMAL, "false", null);
        assertValue(LONG_DECIMAL, "\"string\"", null);
        assertValue(LONG_DECIMAL, "\"null\"", null);

        assertValueFails(LONG_DECIMAL, "[ 42 ]", false);
        assertValueFailsTrino(LONG_DECIMAL, "{ \"x\" : 42 }", false);
    }

    private static void assertDecimal(DecimalType decimalType, String jsonValue)
            throws Exception
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
            throws Exception
    {
        assertValue(REAL, "null", null);

        // allowed range for JsonSerDe
        assertValue(REAL, "0", 0.0f);
        assertValue(REAL, "123", 123.0f);
        assertValue(REAL, "-123", -123.0f);
        assertValue(REAL, "1.23", 1.23f);
        assertValue(REAL, "-1.23", -1.23f);
        assertValue(REAL, "1.5645e33", 1.5645e33f);

        assertValueFails(REAL, "NaN", false);
        assertValueFails(REAL, "Infinity", false);
        assertValueFails(REAL, "+Infinity", false);
        assertValueFails(REAL, "-Infinity", false);
        assertValueFails(REAL, "+Inf");
        assertValueFails(REAL, "-Inf");
        // Map keys support NaN, infinity and negative infinity
        assertValue(toMapKeyType(REAL), toMapKeyJson("NaN"), toMapKeyExpectedValue(Float.NaN), false);
        assertValue(toMapKeyType(REAL), toMapKeyJson("Infinity"), toMapKeyExpectedValue(Float.POSITIVE_INFINITY), false);
        assertValue(toMapKeyType(REAL), toMapKeyJson("+Infinity"), toMapKeyExpectedValue(Float.POSITIVE_INFINITY), false);
        assertValue(toMapKeyType(REAL), toMapKeyJson("-Infinity"), toMapKeyExpectedValue(Float.NEGATIVE_INFINITY), false);

        assertValueFails(REAL, "true");
        assertValueFails(REAL, "false");
        assertValueFails(REAL, "\"123\"", false);
        assertValueFails(REAL, "\"string\"");
        assertValueFails(REAL, "\"null\"");

        assertValueFails(REAL, "[ 42 ]", false);
        assertValueFails(REAL, "{ \"x\" : 42 }", false);
    }

    @Test
    public void testDouble()
            throws Exception
    {
        assertValue(DOUBLE, "null", null);

        // allowed range for JsonSerDe
        assertValue(DOUBLE, "0", 0.0);
        assertValue(DOUBLE, "-0", -0.0);
        assertValue(DOUBLE, "123", 123.0);
        assertValue(DOUBLE, "-123", -123.0);
        assertValue(DOUBLE, "1.23", 1.23);
        assertValue(DOUBLE, "-1.23", -1.23);
        assertValue(DOUBLE, "1.5645e33", 1.5645e33);

        assertValueFails(DOUBLE, "NaN", false);
        assertValueFails(DOUBLE, "Infinity", false);
        assertValueFails(DOUBLE, "+Infinity", false);
        assertValueFails(DOUBLE, "-Infinity", false);
        assertValueFails(DOUBLE, "+Inf");
        assertValueFails(DOUBLE, "-Inf");
        // Map keys support NaN, infinity and negative infinity
        assertValue(toMapKeyType(DOUBLE), toMapKeyJson("NaN"), toMapKeyExpectedValue(Double.NaN), false);
        assertValue(toMapKeyType(DOUBLE), toMapKeyJson("Infinity"), toMapKeyExpectedValue(Double.POSITIVE_INFINITY), false);
        assertValue(toMapKeyType(DOUBLE), toMapKeyJson("+Infinity"), toMapKeyExpectedValue(Double.POSITIVE_INFINITY), false);
        assertValue(toMapKeyType(DOUBLE), toMapKeyJson("-Infinity"), toMapKeyExpectedValue(Double.NEGATIVE_INFINITY), false);

        assertValueFails(DOUBLE, "true");
        assertValueFails(DOUBLE, "false");
        assertValueFails(DOUBLE, "\"123\"", false);
        assertValueFails(DOUBLE, "\"string\"");
        assertValueFails(DOUBLE, "\"null\"");

        assertValueFails(DOUBLE, "[ 42 ]", false);
        assertValueFails(DOUBLE, "{ \"x\" : 42 }", false);
    }

    @Test
    public void testDate()
            throws Exception
    {
        assertValue(DATE, "null", null);

        // allowed range for JsonSerDe
        assertDate(DATE, "\"1970-01-01\"", 0);
        assertDate(DATE, "\"1970-01-02\"", 1);
        assertDate(DATE, "\"1969-12-31\"", -1);

        // Hive ignores everything after the first space
        assertDate(DATE, "\"1986-01-01 anything is allowed here\"", LocalDate.of(1986, 1, 1).toEpochDay());

        assertDate(DATE, "\"1986-01-01\"", LocalDate.of(1986, 1, 1).toEpochDay());
        assertDate(DATE, "\"1986-01-33\"", LocalDate.of(1986, 2, 2).toEpochDay());

        assertDate(DATE, "\"5881580-07-11\"", Integer.MAX_VALUE);
        assertDate(DATE, "\"-5877641-06-23\"", Integer.MIN_VALUE);

        // Hive does not enforce size bounds and truncates the results in Date.toEpochDay
        // For Trino we fail as this behavior is error-prone
        assertValueFailsTrino(DATE, "\"5881580-07-12\"");
        assertValueFailsTrino(DATE, "\"-5877641-06-22\"");

        assertValueFails(DATE, "1");
        assertValueFails(DATE, "1.23");
        assertValueFails(DATE, "1.2345e2");
        assertValueFails(DATE, "1.56");
        assertValueFails(DATE, "1.5645e2");
        assertValueFails(DATE, "1.5645e300");

        assertValueFails(DATE, "true");
        assertValueFails(DATE, "false");
        assertValueFails(DATE, "\"123\"");
        assertValueFails(DATE, "\"string\"");
        assertValueFails(DATE, "\"null\"");

        assertValueFails(DATE, "[ 42 ]");
        assertValueFails(DATE, "{ \"x\" : 42 }");
    }

    private static void assertDate(DateType date, String jsonValue, long days)
            throws Exception
    {
        assertValue(date, jsonValue, new SqlDate(toIntExact(days)));
    }

    @Test
    public void testTimestampMicros()
            throws Exception
    {
        assertValue(TIMESTAMP_MICROS, "null", null);

        // allowed range for JsonSerDe
        assertTimestamp(TIMESTAMP_MICROS, "\"1970-01-01 00:00:00.000000\"", LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0));
        assertTimestamp(TIMESTAMP_MICROS, "\"2020-05-10 12:34:56.123456\"", LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_000));
        assertTimestamp(TIMESTAMP_MICROS, "\"1960-05-10 12:34:56.123456\"", LocalDateTime.of(1960, 5, 10, 12, 34, 56, 123_456_000));

        assertTimestamp(TIMESTAMP_MICROS, "\"294247-01-10 04:00:54.775807\"", LocalDateTime.of(294247, 1, 10, 4, 0, 54, 775_807_000));
        assertTimestamp(TIMESTAMP_MICROS, "\"-290308-12-21 19:59:06.224192\"", LocalDateTime.of(-290308, 12, 21, 19, 59, 6, 224_192_000));

        // test rounding
        assertValue(TIMESTAMP_MICROS, "\"2020-05-10 12:34:56.1234561\"", toSqlTimestamp(TIMESTAMP_MICROS, LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_000)));
        assertValue(TIMESTAMP_MICROS, "\"2020-05-10 12:34:56.1234565\"", toSqlTimestamp(TIMESTAMP_MICROS, LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_457_000)));

        // size bounds
        assertValueFails(TIMESTAMP_MICROS, "\"294247-01-10 04:00:54.775808\"");
        // it isn't really possible to test for exact min value, because the sequence gets doesn't transfer through testing framework
        assertValueFails(TIMESTAMP_MICROS, "\"-290308-12-21 19:59:05.224192\"");

        assertValueFails(TIMESTAMP_MICROS, "1");
        assertValueFails(TIMESTAMP_MICROS, "1.23");
        assertValueFails(TIMESTAMP_MICROS, "1.2345e2");
        assertValueFails(TIMESTAMP_MICROS, "1.56");
        assertValueFails(TIMESTAMP_MICROS, "1.5645e2");
        assertValueFails(TIMESTAMP_MICROS, "1.5645e300");

        assertValueFails(TIMESTAMP_MICROS, "true");
        assertValueFails(TIMESTAMP_MICROS, "false");
        assertValueFails(TIMESTAMP_MICROS, "\"123\"");
        assertValueFails(TIMESTAMP_MICROS, "\"string\"");
        assertValueFails(TIMESTAMP_MICROS, "\"null\"");

        assertValueFails(TIMESTAMP_MICROS, "[ 42 ]");
        assertValueFails(TIMESTAMP_MICROS, "{ \"x\" : 42 }");
    }

    @Test
    public void testTimestampNanos()
            throws Exception
    {
        assertValue(TIMESTAMP_NANOS, "null", null);

        assertTimestamp(TIMESTAMP_NANOS, "\"1970-01-01 00:00:00.000000000\"", LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0));
        assertTimestamp(TIMESTAMP_NANOS, "\"2020-05-10 12:34:56.123456789\"", LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_789));
        assertTimestamp(TIMESTAMP_NANOS, "\"1960-05-10 12:34:56.123456789\"", LocalDateTime.of(1960, 5, 10, 12, 34, 56, 123_456_789));

        assertTimestamp(TIMESTAMP_NANOS, "\"294247-01-10 04:00:54.775807999\"", LocalDateTime.of(294247, 1, 10, 4, 0, 54, 775_807_999));
        assertTimestamp(TIMESTAMP_NANOS, "\"-290308-12-21 19:59:06.224192000\"", LocalDateTime.of(-290308, 12, 21, 19, 59, 6, 224_192_000));

        // ISO 8601 also allowed
        assertTimestamp(TIMESTAMP_NANOS, "\"2020-05-10T12:34:56.123456789\"", LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_789));

        // size bounds
        assertValueFails(TIMESTAMP_NANOS, "\"294247-01-10 04:00:54.775808000\"");
        // it isn't really possible to test for exact min value, because the sequence gets doesn't transfer through testing framework
        assertValueFails(TIMESTAMP_NANOS, "\"-290308-12-21 19:59:05.224192000\"");

        // custom formats
        assertTimestamp(
                TIMESTAMP_NANOS,
                "\"05/10/2020 12.34.56.123\"",
                LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_000_000),
                "MM/dd/yyyy HH.mm.ss.SSS");
        assertTimestamp(
                TIMESTAMP_NANOS,
                "\"7\"",
                LocalDateTime.of(1970, 1, 1, 7, 0, 0, 0),
                "HH");
        assertTimestamp(
                TIMESTAMP_NANOS,
                "\"7\"",
                LocalDateTime.of(1970, 7, 1, 0, 0, 0, 0),
                "MM");
        assertTimestamp(
                TIMESTAMP_NANOS,
                "\"2020\"",
                LocalDateTime.of(2020, 1, 1, 0, 0, 0, 0),
                "yyyy");
        assertTimestamp(
                TIMESTAMP_NANOS,
                "\"1589114096123.777\"",
                LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_000_000),
                "millis");
        assertTimestamp(
                TIMESTAMP_NANOS,
                "\"05/10/2020 12.34.56.123\"",
                LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_000_000),
                "yyyy",
                "MM/dd/yyyy HH.mm.ss.SSS",
                "millis");
        assertTimestamp(
                TIMESTAMP_NANOS,
                "\"1589114096123.777\"",
                LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_000_000),
                "yyyy",
                "MM/dd/yyyy HH.mm.ss.SSS",
                "millis");
        // Default Hive timestamp and ISO 8601 always supported in both normal values and map keys
        assertValue(
                TIMESTAMP_NANOS,
                "\"2020-05-10 12:34:56.123456789\"",
                toSqlTimestamp(TIMESTAMP_NANOS, LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_789)),
                ImmutableList.of(
                        "yyyy",
                        "MM/dd/yyyy HH.mm.ss.SSS",
                        "millis"),
                true);
        assertValue(
                TIMESTAMP_NANOS,
                "\"2020-05-10T12:34:56.123456789\"",
                toSqlTimestamp(TIMESTAMP_NANOS, LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_789)),
                ImmutableList.of(
                        "yyyy",
                        "MM/dd/yyyy HH.mm.ss.SSS",
                        "millis"),
                true);

        assertValueFails(TIMESTAMP_NANOS, "1");
        assertValueFails(TIMESTAMP_NANOS, "1.23");
        assertValueFails(TIMESTAMP_NANOS, "1.2345e2");
        assertValueFails(TIMESTAMP_NANOS, "1.56");
        assertValueFails(TIMESTAMP_NANOS, "1.5645e2");
        assertValueFails(TIMESTAMP_NANOS, "1.5645e300");

        assertValueFails(TIMESTAMP_NANOS, "true");
        assertValueFails(TIMESTAMP_NANOS, "false");
        assertValueFails(TIMESTAMP_NANOS, "\"123\"");
        assertValueFails(TIMESTAMP_NANOS, "\"string\"");
        assertValueFails(TIMESTAMP_NANOS, "\"null\"");

        assertValueFails(TIMESTAMP_NANOS, "[ 42 ]");
        assertValueFails(TIMESTAMP_NANOS, "{ \"x\" : 42 }");
    }

    private static void assertTimestamp(TimestampType timestampType, String jsonValue, LocalDateTime localDateTime, String... timestampFormats)
            throws Exception
    {
        assertValue(
                timestampType,
                jsonValue,
                toSqlTimestamp(timestampType, localDateTime),
                ImmutableList.copyOf(timestampFormats),
                timestampFormats.length == 0);
    }

    private static void assertValue(Type type, String jsonValue, Object expectedValue)
            throws Exception
    {
        assertValue(type, jsonValue, expectedValue, true);
    }

    private static void assertValue(Type type, String jsonValue, Object expectedValue, boolean testMapKey)
            throws Exception
    {
        assertValue(type, jsonValue, expectedValue, ImmutableList.of(), testMapKey);
    }

    private static void assertValue(Type type, String jsonValue, Object expectedValue, List<String> timestampFormats, boolean testMapKey)
            throws Exception
    {
        assertValueHive(type, jsonValue, expectedValue, timestampFormats, testMapKey);
        assertValueTrino(type, jsonValue, expectedValue, timestampFormats, testMapKey);
    }

    private static void assertValueTrino(Type type, String jsonValue, Object expectedValue)
            throws IOException, SerDeException
    {
        assertValueTrino(type, jsonValue, expectedValue, ImmutableList.of(), true);
    }

    private static void assertValueTrino(Type type, String jsonValue, Object expectedValue, List<String> timestampFormats, boolean testMapKey)
            throws IOException, SerDeException
    {
        internalAssertValueTrino(type, jsonValue, expectedValue, timestampFormats);
        internalAssertValueTrino(new ArrayType(type), "[" + jsonValue + "]", singletonList(expectedValue), timestampFormats);
        internalAssertValueTrino(
                RowType.rowType(field("a", type), field("nested", type), field("b", type)),
                "{ \"nested\" : " + jsonValue + " }",
                Arrays.asList(null, expectedValue, null),
                timestampFormats);
        internalAssertValueTrino(
                new MapType(BIGINT, type, TYPE_OPERATORS),
                "{ \"1234\" : " + jsonValue + " }",
                singletonMap(1234L, expectedValue),
                timestampFormats);
        if (expectedValue != null && isScalarType(type)) {
            if (testMapKey) {
                internalAssertValueTrino(toMapKeyType(type), toMapKeyJson(jsonValue), toMapKeyExpectedValue(expectedValue), timestampFormats);
            }
            else {
                internalAssertValueFailsTrino(toMapKeyType(type), toMapKeyJson(jsonValue));
            }
        }
    }

    private static void internalAssertValueTrino(Type type, String jsonValue, Object expectedValue, List<String> timestampFormats)
            throws IOException, SerDeException
    {
        List<Column> columns = ImmutableList.of(new Column("test", type, 33));

        // read normal json
        Object actualValue = readTrinoLine("{\"test\" : " + jsonValue + "}", columns, timestampFormats).get(0);
        assertColumnValueEquals(type, actualValue, expectedValue);

        // read column ordinal json
        actualValue = readTrinoLine("{\"_col33\" : " + jsonValue + "}", columns, timestampFormats).get(0);
        assertColumnValueEquals(type, actualValue, expectedValue);

        // write the value using Trino
        String trinoLine = writeTrinoValue(type, expectedValue);

        // verify Trino can read back the Trino json
        actualValue = readTrinoLine(trinoLine, columns, timestampFormats).get(0);
        assertColumnValueEquals(type, actualValue, expectedValue);

        // verify Hive can read back the Trino json
        for (boolean hcatalog : ImmutableList.of(true, false)) {
            actualValue = readHiveLine(columns, trinoLine, ImmutableList.of(), hcatalog).get(0);
            assertColumnValueEquals(type, actualValue, expectedValue);
        }
    }

    private static List<Object> readTrinoLine(String jsonLine, List<Column> columns, List<String> timestampFormats)
            throws IOException
    {
        LineDeserializer deserializer = new JsonDeserializerFactory().create(columns, createJsonProperties(timestampFormats));
        PageBuilder pageBuilder = new PageBuilder(1, deserializer.getTypes());
        deserializer.deserialize(createLineBuffer(jsonLine), pageBuilder);
        Page page = pageBuilder.build();
        return readTrinoValues(columns, page, 0);
    }

    private static String writeTrinoValue(Type type, Object expectedValue)
            throws IOException
    {
        List<Column> columns = ImmutableList.of(new Column("test", type, 33));
        Page page = toSingleRowPage(columns, singletonList(expectedValue));

        // write the data to json
        LineSerializer serializer = new JsonSerializerFactory().create(columns, ImmutableMap.of());
        SliceOutput sliceOutput = new DynamicSliceOutput(1024);
        serializer.write(page, 0, sliceOutput);
        return sliceOutput.slice().toStringUtf8();
    }

    private static void assertValueHive(Type type, String jsonValue, Object expectedValue, List<String> timestampFormats, boolean testMapKey)
            throws SerDeException
    {
        for (boolean hcatalog : ImmutableList.of(true, false)) {
            // The non-hcatalog version of JsonSerDe has a broken implementation of ordinal fields that always fails
            if (hcatalog || !jsonValue.contains("\"_col")) {
                internalAssertValueHive(type, jsonValue, expectedValue, timestampFormats, hcatalog);
            }
            else {
                internalAssertValueFailsHive(type, jsonValue, false);
            }
            if (expectedValue != null && isScalarType(type)) {
                if (testMapKey) {
                    internalAssertValueHive(toMapKeyType(type), toMapKeyJson(jsonValue), toMapKeyExpectedValue(expectedValue), timestampFormats, hcatalog);
                }
                else {
                    internalAssertValueFailsHive(toMapKeyType(type), toMapKeyJson(jsonValue), hcatalog);
                }
            }
        }
    }

    private static void internalAssertValueHive(Type type, String jsonValue, Object expectedValue, List<String> timestampFormats, boolean hcatalog)
            throws SerDeException
    {
        List<Column> columns = ImmutableList.of(new Column("test", type, 33));
        String jsonLine = "{\"test\" : " + jsonValue + "}";
        Object actualValue = readHiveLine(columns, jsonLine, timestampFormats, hcatalog).get(0);
        assertColumnValueEquals(type, actualValue, expectedValue);
    }

    private static List<Object> readHiveLine(List<Column> columns, String jsonLine, List<String> timestampFormats, boolean hcatalog)
            throws SerDeException
    {
        Deserializer deserializer = createHiveDeserializer(columns, timestampFormats, hcatalog);

        Object rowData = deserializer.deserialize(new Text(jsonLine));

        List<Object> fieldValues = new ArrayList<>();
        StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
        for (Column column : columns) {
            StructField field = rowInspector.getStructFieldRef(column.name());
            Object fieldValue = rowInspector.getStructFieldData(rowData, field);
            fieldValue = decodeRecordReaderValue(column.type(), fieldValue);
            fieldValues.add(fieldValue);
        }
        return fieldValues;
    }

    private static Deserializer createHiveDeserializer(List<Column> columns, List<String> timestampFormats, boolean hcatalog)
            throws SerDeException
    {
        JobConf configuration = new JobConf(newEmptyConfiguration());

        Properties schema = new Properties();
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
        schema.putAll(createJsonProperties(timestampFormats));

        Deserializer deserializer;
        if (hcatalog) {
            deserializer = new org.apache.hive.hcatalog.data.JsonSerDe();
        }
        else {
            deserializer = new org.apache.hadoop.hive.serde2.JsonSerDe();
        }
        deserializer.initialize(configuration, schema);
        configuration.set(SERIALIZATION_LIB, deserializer.getClass().getName());
        return deserializer;
    }

    private static void assertValueFails(Type type, String jsonValue)
    {
        assertValueFails(type, jsonValue, true);
    }

    private static void assertValueFails(Type type, String jsonValue, boolean testMapKey)
    {
        assertValueFailsHive(type, jsonValue, testMapKey);
        assertValueFailsTrino(type, jsonValue, testMapKey);
    }

    private static void assertValueFailsTrino(Type type, String jsonValue)
    {
        assertValueFailsTrino(type, jsonValue, true);
    }

    private static void assertValueFailsTrino(Type type, String jsonValue, boolean testMapKey)
    {
        internalAssertValueFailsTrino(type, jsonValue);

        // ignore array and object json
        if (testMapKey && isScalarType(type)) {
            internalAssertValueFailsTrino(toMapKeyType(type), toMapKeyJson(jsonValue));
        }
    }

    private static void internalAssertValueFailsTrino(Type type, String jsonValue)
    {
        String jsonLine = "{\"test\" : " + jsonValue + "}";
        LineDeserializer deserializer = new JsonDeserializerFactory().create(ImmutableList.of(new Column("test", type, 33)), ImmutableMap.of());
        assertThatThrownBy(() -> deserializer.deserialize(createLineBuffer(jsonLine), new PageBuilder(1, ImmutableList.of(type))))
                .isInstanceOf(Exception.class);
    }

    private static void assertValueFailsHive(Type type, String jsonValue, boolean testMapKey)
    {
        for (boolean hcatalog : ImmutableList.of(true, false)) {
            internalAssertValueFailsHive(type, jsonValue, hcatalog);
            if (testMapKey && isScalarType(type)) {
                internalAssertValueFailsHive(toMapKeyType(type), toMapKeyJson(jsonValue), hcatalog);
            }
        }
    }

    private static void internalAssertValueFailsHive(Type type, String jsonValue, boolean hcatalog)
    {
        List<Column> columns = ImmutableList.of(new Column("test", type, 33));
        String jsonLine = "{\"test\" : " + jsonValue + "}";
        assertThatThrownBy(() -> {
            Deserializer deserializer = createHiveDeserializer(columns, ImmutableList.of(), hcatalog);
            Object rowData = deserializer.deserialize(new Text(jsonLine));

            StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
            for (Column column : columns) {
                StructField field = rowInspector.getStructFieldRef(column.name());
                Object fieldValue = rowInspector.getStructFieldData(rowData, field);
                decodeRecordReaderValue(column.type(), fieldValue);
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

    private static Map<String, String> createJsonProperties(List<String> timestampFormats)
    {
        ImmutableMap.Builder<String, String> schema = ImmutableMap.builder();
        if (!timestampFormats.isEmpty()) {
            schema.put(TIMESTAMP_FORMATS_KEY, timestampFormats.stream()
                    .map(format -> format.replace("\\", "\\\\"))
                    .map(format -> format.replace(",", "\\,"))
                    .collect(joining(",")));
        }
        return schema.buildOrThrow();
    }
}
