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
package io.trino.hive.formats.line.regex;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.hive.formats.FormatTestUtils;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.hive.formats.line.regex.RegexDeserializer.UnsupportedTypeException;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.RegexSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
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
import static io.trino.hive.formats.FormatTestUtils.readTrinoValues;
import static io.trino.hive.formats.FormatTestUtils.toSqlTimestamp;
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
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.toIntExact;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRegexFormat
{
    private static final DecimalType SHORT_DECIMAL = createDecimalType(MAX_SHORT_PRECISION, 2);
    private static final DecimalType LONG_DECIMAL = createDecimalType(MAX_PRECISION, 2);
    private static final VarcharType VARCHAR_3 = createVarcharType(3);
    private static final CharType CHAR_100 = createCharType(100);
    private static final CharType CHAR_3 = createCharType(3);

    @Test
    public void testNoMatch()
            throws Exception
    {
        assert3PartLine("a~b~c", "a", "b", "c");
        assert3PartLine("a~~", "a", null, null);
        assert3PartLine("~b~", null, "b", null);
        assert3PartLine("~~c", null, null, "c");
        assert3PartLine("no match", null, null, null);
    }

    @Test
    public void testMissingGroups()
            throws Exception
    {
        assert3PartLine("a~b~c", "a", "b", "c");
        assert3PartLine("a~~", "a", null, null);
        assert3PartLine("~b~", null, "b", null);
        assert3PartLine("~~c", null, null, "c");
    }

    private static void assert3PartLine(String line, String first, String second, String third)
            throws IOException
    {
        // read columns out of order to test ordering logic
        assertLine(
                ImmutableList.of(
                        new Column("c", VARCHAR, 2),
                        new Column("a", VARCHAR, 0),
                        new Column("b", VARCHAR, 1)),
                line,
                "([^~]+)?~([^~]+)?~([^~]+)?",
                false,
                Arrays.asList(third, first, second));
    }

    @Test
    public void testNotEnoughGroups()
            throws Exception
    {
        assertLineTrino(
                ImmutableList.of(
                        new Column("c", VARCHAR, 2),
                        new Column("a", VARCHAR, 0),
                        new Column("b", VARCHAR, 1)),
                "a",
                "(.*)",
                false,
                Arrays.asList(null, "a", null));
    }

    @Test
    public void testCaseInsensitive()
            throws Exception
    {
        verifyCaseInsensitive(true);
    }

    private static void verifyCaseInsensitive(boolean caseSensitive)
            throws IOException
    {
        List<Object> expectedValues;
        if (caseSensitive) {
            expectedValues = Arrays.asList("FoO", "BaR", "BaZ");
        }
        else {
            expectedValues = Arrays.asList(null, null, null);
        }
        assertLineTrino(
                ImmutableList.of(
                        new Column("a", VARCHAR, 0),
                        new Column("b", VARCHAR, 1),
                        new Column("c", VARCHAR, 2)),
                "FoOxBaRxBaZ",
                "(foo)x(bar)x(baz)",
                caseSensitive,
                expectedValues);
    }

    @Test
    public void testInvalidType()
    {
        assertThatThrownBy(
                () -> readTrinoLine(
                        ImmutableList.of(new Column("a", VARBINARY, 0)),
                        "line",
                        "(line)",
                        false))
                .isInstanceOf(UnsupportedTypeException.class);
        assertThatThrownBy(
                () -> readLineHive(
                        ImmutableList.of(new Column("a", VARBINARY, 0)),
                        "line",
                        "(line)",
                        false))
                .hasRootCauseInstanceOf(SerDeException.class);
    }

    @Test
    public void testVarchar()
            throws Exception
    {
        assertValue(VARCHAR, "new \n line", "new \n line");
        assertValue(VARCHAR, "value", "value");

        // Trailing spaces are NOT truncated
        assertValue(VARCHAR, "value     ", "value     ");

        // Truncation
        assertValue(VARCHAR_3, "v", "v");
        assertValue(VARCHAR_3, "val", "val");
        assertValue(VARCHAR_3, "value", "val");

        // Special chars
        assertValue(VARCHAR, "tab \t tab", "tab \t tab");
        assertValue(VARCHAR, "new \n line", "new \n line");
        assertValue(VARCHAR, "carriage \r return", "carriage \r return");
    }

    @Test
    public void testChar()
            throws Exception
    {
        assertChar(CHAR_100, "value", "value");

        // Trailing spaces are truncated
        assertChar(CHAR_100, "value     ", "value");

        // Truncation
        assertChar(CHAR_3, "v", "v");
        assertChar(CHAR_3, "val", "val");
        assertChar(CHAR_3, "value", "val");

        // Special chars
        assertChar(CHAR_100, "tab \t tab", "tab \t tab");
        assertChar(CHAR_100, "new \n line", "new \n line");
        assertChar(CHAR_100, "carriage \r return", "carriage \r return");
    }

    private static void assertChar(CharType charType, String regexValue, String expectedValue)
            throws Exception
    {
        assertValue(charType, regexValue, padSpaces(expectedValue, charType));
    }

    @Test
    public void testBoolean()
            throws Exception
    {
        assertValue(BOOLEAN, "true", true);
        assertValue(BOOLEAN, "tRuE", true);

        assertValue(BOOLEAN, "false", false);
        assertValue(BOOLEAN, "unknown", false);
        assertValue(BOOLEAN, "-1", false);
        assertValue(BOOLEAN, "0", false);
        assertValue(BOOLEAN, "1", false);
        assertValue(BOOLEAN, "1.23", false);
        assertValue(BOOLEAN, "1.23e45", false);
    }

    @Test
    public void testBigint()
            throws Exception
    {
        assertValue(BIGINT, "0", 0L);
        assertValue(BIGINT, "1", 1L);
        assertValue(BIGINT, "-1", -1L);
        assertValue(BIGINT, String.valueOf(Long.MAX_VALUE), Long.MAX_VALUE);
        assertValue(BIGINT, String.valueOf(Long.MIN_VALUE), Long.MIN_VALUE);

        assertValue(BIGINT, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE).toString(), null);
        assertValue(BIGINT, BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE).toString(), null);

        assertValue(BIGINT, "1.23", null);
        assertValue(BIGINT, "1.2345e2", null);
        assertValue(BIGINT, "1.56", null);
        assertValue(BIGINT, "1.5645e2", null);
        assertValue(BIGINT, "1.5645e300", null);

        assertValue(BIGINT, "true", null);
        assertValue(BIGINT, "false", null);
    }

    @Test
    public void testInteger()
            throws Exception
    {
        assertValue(INTEGER, "0", 0);
        assertValue(INTEGER, "1", 1);
        assertValue(INTEGER, "-1", -1);

        assertValue(INTEGER, String.valueOf(Integer.MAX_VALUE), Integer.MAX_VALUE);
        assertValue(INTEGER, String.valueOf(Integer.MIN_VALUE), Integer.MIN_VALUE);
        assertValue(INTEGER, String.valueOf(Integer.MAX_VALUE + 1L), null);
        assertValue(INTEGER, String.valueOf(Integer.MIN_VALUE - 1L), null);

        assertValue(INTEGER, "1.23", null);
        assertValue(INTEGER, "1.2345e2", null);
        assertValue(INTEGER, "1.56", null);
        assertValue(INTEGER, "1.5645e2", null);
        assertValue(INTEGER, "1.5645e300", null);

        assertValue(INTEGER, "true", null);
        assertValue(INTEGER, "false", null);
    }

    @Test
    public void testSmallInt()
            throws Exception
    {
        assertValue(SMALLINT, "0", (short) 0);
        assertValue(SMALLINT, "1", (short) 1);
        assertValue(SMALLINT, "-1", (short) -1);
        assertValue(SMALLINT, "32767", (short) 32767);
        assertValue(SMALLINT, "-32768", (short) -32768);
        assertValue(SMALLINT, "32768", null);
        assertValue(SMALLINT, "-32769", null);

        assertValue(SMALLINT, "1.23", null);
        assertValue(SMALLINT, "1.2345e2", null);
        assertValue(SMALLINT, "1.56", null);
        assertValue(SMALLINT, "1.5645e2", null);
        assertValue(SMALLINT, "1.5645e7", null);

        assertValue(SMALLINT, "true", null);
        assertValue(SMALLINT, "false", null);
    }

    @Test
    public void testTinyint()
            throws Exception
    {
        assertValue(TINYINT, "0", (byte) 0);
        assertValue(TINYINT, "1", (byte) 1);
        assertValue(TINYINT, "-1", (byte) -1);
        assertValue(TINYINT, "127", (byte) 127);
        assertValue(TINYINT, "-128", (byte) -128);
        assertValue(TINYINT, "128", null);
        assertValue(TINYINT, "-129", null);

        assertValue(TINYINT, "1.23", null);
        assertValue(TINYINT, "1.2345e2", null);
        assertValue(TINYINT, "1.56", null);
        assertValue(TINYINT, "1.5645e2", null);
        assertValue(TINYINT, "1.5645e3", null);

        assertValue(TINYINT, "true", null);
        assertValue(TINYINT, "false", null);
    }

    @Test
    public void testDecimalShort()
            throws Exception
    {
        assertDecimal(SHORT_DECIMAL, "0");
        assertDecimal(SHORT_DECIMAL, "1");
        assertDecimal(SHORT_DECIMAL, "-1");
        assertDecimal(SHORT_DECIMAL, "9999999999999999.99");
        assertDecimal(SHORT_DECIMAL, "-9999999999999999.99");

        assertDecimal(SHORT_DECIMAL, "1.2345e2");
        assertDecimal(SHORT_DECIMAL, "1.5645e15");

        // Hive does not enforce size bounds
        assertValueTrino(SHORT_DECIMAL, "10000000000000000.00", null);
        assertValueTrino(SHORT_DECIMAL, "-10000000000000000.00", null);
        assertValueTrino(SHORT_DECIMAL, "1e19", null);
        assertValueTrino(SHORT_DECIMAL, "-1e19", null);

        // test rounding (Hive doesn't seem to enforce scale)
        DecimalType roundingType = createDecimalType(4, 2);
        assertValueTrino(roundingType, "10.001", SqlDecimal.decimal("10.00", roundingType));
        assertValueTrino(roundingType, "10.005", SqlDecimal.decimal("10.01", roundingType));
        assertValueTrino(roundingType, "99.999", null);

        assertValue(SHORT_DECIMAL, "true", null);
        assertValue(SHORT_DECIMAL, "false", null);
    }

    @Test
    public void testDecimalLong()
            throws Exception
    {
        // allowed range for RegexSerDe
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
        assertValueTrino(LONG_DECIMAL, "1000000000000000000000000000000000000.00", null);
        assertValueTrino(LONG_DECIMAL, "-1000000000000000000000000000000000000.00", null);
        assertValue(LONG_DECIMAL, "1e39", null);
        assertValue(LONG_DECIMAL, "-1e39", null);

        // test rounding (Hive doesn't seem to enforce scale)
        DecimalType roundingType = createDecimalType(38, 2);
        assertValueTrino(roundingType, "10.001", SqlDecimal.decimal("10.00", roundingType));
        assertValueTrino(roundingType, "10.005", SqlDecimal.decimal("10.01", roundingType));

        assertValue(LONG_DECIMAL, "true", null);
        assertValue(LONG_DECIMAL, "false", null);
        assertValue(LONG_DECIMAL, "value", null);
        assertValue(LONG_DECIMAL, "null", null);
    }

    private static void assertDecimal(DecimalType decimalType, String regexValue)
            throws Exception
    {
        SqlDecimal expectedValue = toSqlDecimal(decimalType, regexValue);
        assertValue(decimalType, regexValue, expectedValue);
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
        // allowed range for RegexSerDe
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
    }

    @Test
    public void testDouble()
            throws Exception
    {
        // allowed range for RegexSerDe
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
    }

    @Test
    public void testDate()
            throws Exception
    {
        // allowed range for RegexSerDe
        assertDate("1970-01-01", 0);
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
        assertValueTrino(DATE, "5881580-07-12", null);
        assertValueTrino(DATE, "-5877641-06-22", null);

        assertValue(DATE, "1", null);
        assertValue(DATE, "1.23", null);
        assertValue(DATE, "1.2345e2", null);
        assertValue(DATE, "1.56", null);
        assertValue(DATE, "1.5645e2", null);
        assertValue(DATE, "1.5645e300", null);

        assertValue(DATE, "true", null);
        assertValue(DATE, "false", null);
    }

    private static void assertDate(String regexValue, long days)
            throws Exception
    {
        assertValue(DATE, regexValue, new SqlDate(toIntExact(days)));
    }

    @Test
    public void testTimestampMicros()
            throws Exception
    {
        // allowed range for RegexSerDe
        assertTimestamp(TIMESTAMP_MICROS, "1970-01-01 00:00:00.000000", LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0));
        assertTimestamp(TIMESTAMP_MICROS, "2020-05-10 12:34:56.123456", LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_000));
        assertTimestamp(TIMESTAMP_MICROS, "1960-05-10 12:34:56.123456", LocalDateTime.of(1960, 5, 10, 12, 34, 56, 123_456_000));

        assertTimestamp(TIMESTAMP_MICROS, "294247-01-10 04:00:54.775807", LocalDateTime.of(294247, 1, 10, 4, 0, 54, 775_807_000));
        assertTimestamp(TIMESTAMP_MICROS, "-290308-12-21 19:59:06.224192", LocalDateTime.of(-290308, 12, 21, 19, 59, 6, 224_192_000));

        // test rounding
        assertValueTrino(TIMESTAMP_MICROS, "2020-05-10 12:34:56.1234561", toSqlTimestamp(TIMESTAMP_MICROS, LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_000)));
        assertValueTrino(TIMESTAMP_MICROS, "2020-05-10 12:34:56.1234565", toSqlTimestamp(TIMESTAMP_MICROS, LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_457_000)));

        // Hive does not enforce size bounds
        assertValueTrino(TIMESTAMP_MICROS, "294247-01-10 04:00:54.775808", null);
        // it isn't really possible to test for exact min value, because the sequence gets doesn't transfer through testing framework
        assertValueTrino(TIMESTAMP_MICROS, "-290308-12-21 19:59:05.224192", null);

        assertValue(TIMESTAMP_MICROS, "1", null);
        assertValue(TIMESTAMP_MICROS, "1.23", null);
        assertValue(TIMESTAMP_MICROS, "1.2345e2", null);
        assertValue(TIMESTAMP_MICROS, "1.56", null);
        assertValue(TIMESTAMP_MICROS, "1.5645e2", null);
        assertValue(TIMESTAMP_MICROS, "1.5645e300", null);

        assertValue(TIMESTAMP_MICROS, "true", null);
        assertValue(TIMESTAMP_MICROS, "false", null);
    }

    @Test
    public void testTimestampNanos()
            throws Exception
    {
        assertTimestamp(TIMESTAMP_NANOS, "1970-01-01 00:00:00.000000000", LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0));
        assertTimestamp(TIMESTAMP_NANOS, "2020-05-10 12:34:56.123456789", LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_789));
        assertTimestamp(TIMESTAMP_NANOS, "1960-05-10 12:34:56.123456789", LocalDateTime.of(1960, 5, 10, 12, 34, 56, 123_456_789));

        assertTimestamp(TIMESTAMP_NANOS, "294247-01-10 04:00:54.775807999", LocalDateTime.of(294247, 1, 10, 4, 0, 54, 775_807_999));
        assertTimestamp(TIMESTAMP_NANOS, "-290308-12-21 19:59:06.224192000", LocalDateTime.of(-290308, 12, 21, 19, 59, 6, 224_192_000));

        // ISO 8601 also allowed
        assertTimestamp(TIMESTAMP_NANOS, "2020-05-10T12:34:56.123456789", LocalDateTime.of(2020, 5, 10, 12, 34, 56, 123_456_789));

        // Hive does not enforce size bounds
        assertValueTrino(TIMESTAMP_NANOS, "294247-01-10 04:00:54.775808000", null);
        // it isn't really possible to test for exact min value, because the sequence gets doesn't transfer through testing framework
        assertValueTrino(TIMESTAMP_NANOS, "-290308-12-21 19:59:05.224192000", null);

        assertValue(TIMESTAMP_NANOS, "1", null);
        assertValue(TIMESTAMP_NANOS, "1.23", null);
        assertValue(TIMESTAMP_NANOS, "1.2345e2", null);
        assertValue(TIMESTAMP_NANOS, "1.56", null);
        assertValue(TIMESTAMP_NANOS, "1.5645e2", null);
        assertValue(TIMESTAMP_NANOS, "1.5645e300", null);

        assertValue(TIMESTAMP_NANOS, "true", null);
        assertValue(TIMESTAMP_NANOS, "false", null);
    }

    private static void assertTimestamp(TimestampType timestampType, String regexValue, LocalDateTime localDateTime)
            throws Exception
    {
        assertValue(
                timestampType,
                regexValue,
                toSqlTimestamp(timestampType, localDateTime));
    }

    private static void assertValue(Type type, String regexValue, Object expectedValue)
            throws Exception
    {
        assertValueHive(type, regexValue, expectedValue);
        assertValueTrino(type, regexValue, expectedValue);
    }

    private static void assertValueTrino(Type type, String regexValue, Object expectedValue)
            throws IOException
    {
        Object actualValue = readValueTrino(type, regexValue);
        assertColumnValueEquals(type, actualValue, expectedValue);
    }

    private static Object readValueTrino(Type type, String value)
            throws IOException
    {
        List<Object> values = readTrinoLine(
                ImmutableList.of(new Column("b", type, 1)),
                "ignore~" + value + "~ignore",
                "([^~]*)~([^~]*)~([^~]*)",
                false);
        return values.get(0);
    }

    private static void assertLine(List<Column> columns, String line, String regex, boolean caseSensitive, List<Object> expectedValues)
            throws IOException
    {
        assertLineTrino(columns, line, regex, caseSensitive, expectedValues);
        assertLineHive(columns, line, regex, caseSensitive, expectedValues);
    }

    private static void assertLineTrino(List<Column> columns, String line, String regex, boolean caseSensitive, List<Object> expectedValues)
            throws IOException
    {
        List<Object> actualValues = readTrinoLine(columns, line, regex, caseSensitive);
        for (int i = 0; i < columns.size(); i++) {
            Type type = columns.get(i).type();
            Object actualValue = actualValues.get(i);
            Object expectedValue = expectedValues.get(i);
            assertColumnValueEquals(type, actualValue, expectedValue);
        }
    }

    private static List<Object> readTrinoLine(List<Column> columns, String line, String regex, boolean caseSensitive)
            throws IOException
    {
        LineDeserializer deserializer = new RegexDeserializerFactory().create(columns, createRegexProperties(regex, caseSensitive));
        PageBuilder pageBuilder = new PageBuilder(1, deserializer.getTypes());
        deserializer.deserialize(createLineBuffer(line), pageBuilder);
        return readTrinoValues(columns, pageBuilder.build(), 0);
    }

    private static void assertValueHive(Type type, String value, Object expectedValue)
            throws IOException
    {
        Object actualValue = readValueHive(type, value);
        assertColumnValueEquals(type, actualValue, expectedValue);
    }

    private static Object readValueHive(Type type, String value)
            throws IOException
    {
        List<Object> values = readLineHive(
                ImmutableList.of(
                        new Column("a", BOOLEAN, 0),
                        new Column("b", type, 1),
                        new Column("c", BOOLEAN, 2)),
                "ignore~" + value + "~ignore",
                "([^~]*)~([^~]*)~([^~]*)",
                false);
        return values.get(1);
    }

    private static void assertLineHive(List<Column> columns, String line, String regex, boolean caseSensitive, List<Object> expectedValues)
            throws IOException
    {
        List<Object> actualValues = readLineHive(columns, line, regex, caseSensitive);
        for (int i = 0; i < columns.size(); i++) {
            Type type = columns.get(i).type();
            Object actualValue = actualValues.get(i);
            Object expectedValue = expectedValues.get(i);
            assertColumnValueEquals(type, actualValue, expectedValue);
        }
    }

    private static List<Object> readLineHive(List<Column> columns, String line, String regex, boolean caseSensitive)
            throws IOException
    {
        JobConf configuration = new JobConf(newEmptyConfiguration());

        Properties schema = new Properties();
        schema.setProperty(META_TABLE_COLUMNS, columns.stream()
                .sorted(Comparator.comparing(Column::ordinal))
                .map(Column::name)
                .collect(joining(",")));
        schema.setProperty(
                META_TABLE_COLUMN_TYPES,
                columns.stream()
                        .sorted(Comparator.comparing(Column::ordinal))
                        .map(Column::type)
                        .map(FormatTestUtils::getJavaObjectInspector)
                        .map(ObjectInspector::getTypeName)
                        .collect(joining(",")));
        schema.putAll(createRegexProperties(regex, caseSensitive));
        // this is required in the Hive serde for some reason
        schema.put("columns.comments", columns.stream()
                .map(column -> "\0")
                .collect(joining(",")));

        try {
            Deserializer deserializer = new RegexSerDe();
            deserializer.initialize(configuration, schema);
            Object rowData = deserializer.deserialize(new Text(line));

            StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();
            List<Object> values = new ArrayList<>();
            for (Column column : columns) {
                StructField field = rowInspector.getStructFieldRef(column.name());
                Object value = rowInspector.getStructFieldData(rowData, field);
                Object decodedValue = decodeRecordReaderValue(column.type(), value);
                values.add(decodedValue);
            }
            return values;
        }
        catch (SerDeException e) {
            throw new IOException(e);
        }
    }

    private static Map<String, String> createRegexProperties(String regex, boolean caseSensitive)
    {
        ImmutableMap.Builder<String, String> schema = ImmutableMap.builder();
        schema.put(RegexDeserializerFactory.REGEX_KEY, regex);
        schema.put(RegexDeserializerFactory.REGEX_CASE_SENSITIVE_KEY, String.valueOf(caseSensitive));
        return schema.buildOrThrow();
    }
}
