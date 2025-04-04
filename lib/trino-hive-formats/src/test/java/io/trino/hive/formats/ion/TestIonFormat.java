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
package io.trino.hive.formats.ion;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ionpathextraction.exceptions.PathExtractionException;
import com.google.common.collect.ImmutableMap;
import io.trino.hive.formats.line.Column;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static io.trino.hive.formats.FormatTestUtils.assertColumnValuesEquals;
import static io.trino.hive.formats.FormatTestUtils.readTrinoValues;
import static io.trino.hive.formats.FormatTestUtils.toPage;
import static io.trino.hive.formats.FormatTestUtils.toSqlTimestamp;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIonFormat
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private static final List<Column> TEST_COLUMNS = List.of(
            new Column("magic_num", INTEGER, 0),
            new Column("some_text", VARCHAR, 1),
            new Column("is_summer", BOOLEAN, 2),
            new Column("byte_clob", VARBINARY, 3),
            new Column("sequencer", new ArrayType(INTEGER), 4),
            new Column("struction", RowType.rowType(
                    field("foo", INTEGER),
                    field("bar", VARCHAR)), 5),
            new Column("map", new MapType(VARCHAR, INTEGER, TYPE_OPERATORS), 6),
            new Column("double_value", DOUBLE, 7),
            new Column("decimal_value", DecimalType.createDecimalType(10, 2), 8),
            new Column("tiny_int", TINYINT, 9),
            new Column("small_int", SMALLINT, 10),
            new Column("big_int", BIGINT, 11),
            new Column("real_num", REAL, 12),
            new Column("date", DATE, 13));

    @Test
    public void testSuperBasicStruct()
            throws IOException
    {
        assertValues(
                RowType.rowType(
                        field("foo", INTEGER),
                        field("bar", VARCHAR)),
                "{ bar: baz, foo: 31, ignored: true }",
                List.of(31, "baz"));
    }

    @Test
    public void testMap()
            throws IOException
    {
        MapType mapType = new MapType(VarcharType.createVarcharType(3), INTEGER, TYPE_OPERATORS);
        assertValues(
                RowType.rowType(field("foo", mapType)),
                "{ foo: { bar: 1, bar: 2, baz: 5, quxx: 8 } } { foo: { bar: 17, baz: 31, qux: 53 } }",
                List.of(Map.of("bar", 2, "baz", 5, "qux", 8)),
                List.of(Map.of("bar", 17, "baz", 31, "qux", 53)));

        mapType = new MapType(CharType.createCharType(3), INTEGER, TYPE_OPERATORS);
        assertValues(
                RowType.rowType(field("foo", mapType)),
                "{ foo: { bar: 1, bar: 2, baz: 5, quxx: 8 } }",
                List.of(Map.of("bar", 2, "baz", 5, "qux", 8)));
    }

    @Test
    public void testUnsupportedMapKeys()
            throws IOException
    {
        MapType mapType = new MapType(INTEGER, INTEGER, TYPE_OPERATORS);
        Assertions.assertThrows(UnsupportedOperationException.class, () ->
                assertValues(RowType.rowType(field("bad_map", mapType)), "", List.of()));
    }

    @Test
    public void testVariousTlvsStrict()
            throws IOException
    {
        RowType rowType = RowType.rowType(field("foo", INTEGER), field("bar", VARCHAR));
        IonDecoderConfig decoderConfig = IonDecoderConfig.defaultConfig().withStrictTyping();
        List<Object> expected = new ArrayList<>(2);
        expected.add(null);
        expected.add(null);

        assertValues(rowType,
                decoderConfig,
                // empty struct, untyped null, struct null, and explicitly typed null null, phew.
                "{} null null.struct null.null",
                expected, expected, expected, expected);

        Assertions.assertThrows(PathExtractionException.class, () -> {
            assertValues(rowType, decoderConfig, "null.int", expected);
            assertValues(rowType, decoderConfig, "[]", expected);
        });
    }

    @Test
    public void testVariousTlvsLax()
            throws IOException
    {
        RowType rowType = RowType.rowType(field("foo", INTEGER), field("bar", VARCHAR));
        List<Object> expected = new ArrayList<>(2);
        expected.add(null);
        expected.add(null);

        assertValues(rowType,
                "{} 37 null.list null.struct null spam false",
                expected, expected, expected, expected, expected, expected, expected);
    }

    @Test
    public void testColumnMistypings()
    {
        RowType rowType = RowType.rowType(field("foo", INTEGER), field("bar", BOOLEAN));

        List<String> ions = List.of(
                "{ foo: blarg,     bar: false }",
                "{ foo: 12345,     bar: blarg }",
                "{ foo: null.list, bar: false }",
                "{ foo: 12345,     bar: null.int }");

        for (String ion : ions) {
            Assertions.assertThrows(TrinoException.class, () -> {
                assertValues(rowType, ion, List.of());
            });
        }
    }

    @Test
    public void testTextTruncation()
            throws IOException
    {
        String ionText = """
                { my_text: 'abcdefghijk' }
                { my_text: 'abcd    ' }
                { my_text: 'abcd    ijk' }""";

        assertValues(RowType.rowType(field("my_text", VarcharType.createVarcharType(8))),
                ionText,
                List.of("abcdefgh"), List.of("abcd    "), List.of("abcd    "));

        assertValues(RowType.rowType(field("my_text", CharType.createCharType(8))),
                ionText,
                List.of("abcdefgh"), List.of("abcd    "), List.of("abcd    "));
    }

    @Test
    public void testCaseInsensitivityOfKeys()
            throws IOException
    {
        assertValues(
                RowType.rowType(
                        field("Foo", INTEGER),
                        field("BAR", VARCHAR)),
                "{ Bar: baz, foo: 31 }",
                List.of(31, "baz"));
    }

    @Test
    public void testStringCoercions()
            throws IOException
    {
        assertValues(
                RowType.rowType(
                        field("foo", VARCHAR)),
                "{ foo: true }",
                List.of("true"));
        assertValues(
                RowType.rowType(
                        field("foo", VARCHAR)),
                "{ foo: 31 }",
                List.of("31"));
        assertValues(
                RowType.rowType(
                        field("foo", VARCHAR)),
                "{ foo: 31.50 }",
                List.of("31.50"));
        assertValues(
                RowType.rowType(
                        field("foo", VARCHAR)),
                "{ foo: [1, 2, 3] }",
                List.of("[1,2,3]"));
        assertValues(
                RowType.rowType(
                        field("foo", VARCHAR)),
                "{ foo: \"bar\" }",
                List.of("bar"));
        assertValues(
                RowType.rowType(
                        field("foo", VARCHAR)),
                "{ foo: { nested_foo: 12 } }",
                List.of("{nested_foo:12}"));
    }

    @Test
    public void testCaseInsensitivityOfDuplicateKeys()
            throws IOException
    {
        // this test asserts that duplicate key behavior works as expected(i.e. capturing the last value),
        // for duplicate keys with different casing.
        assertValues(
                RowType.rowType(
                        field("Foo", INTEGER),
                        field("BAR", VARCHAR)),
                "{ bar: baz, Foo: 31, foo: 5 }",
                List.of(5, "baz"));
    }

    @Test
    public void testCaseSensitiveExtraction()
            throws IOException
    {
        assertValues(
                RowType.rowType(
                        field("Foo", INTEGER),
                        field("Bar", VARCHAR)),
                IonDecoderConfig.defaultConfig().withCaseSensitive(),
                // assumes duplicate fields overwrite, which is asserted in the test above
                "{ Bar: baz, bar: blegh, Foo: 31, foo: 67 }",
                List.of(31, "baz"));
    }

    @Test
    public void testStructWithNullAndMissingValues()
            throws IOException
    {
        assertValues(
                RowType.rowType(
                        field("foo", INTEGER),
                        field("bar", VARCHAR)),
                "{ bar: null.symbol }",
                Arrays.asList(null, null));
    }

    @Test
    public void testNestedList()
            throws IOException
    {
        assertValues(
                RowType.rowType(
                        field("primes", new ArrayType(INTEGER))),
                "{ primes: [ 17, 31, 51 ] }",
                List.of(List.of(17, 31, 51)));
    }

    @Test
    public void testNestedStruct()
            throws IOException
    {
        assertValues(
                RowType.rowType(
                        field("name", RowType.rowType(
                                field("first", VARCHAR),
                                field("last", VARCHAR)))),
                "{ name: { first: Woody, last: Guthrie, superfluous: ignored } }",
                List.of(List.of("Woody", "Guthrie")));
    }

    /**
     * The Ion Hive SerDe captures the last value for fields with
     * duplicate keys. There is different behavior for nested Rows,
     * which you can see below.
     */
    @Test
    public void testTopLevelStructWithDuplicateKeys()
            throws IOException
    {
        assertValues(
                RowType.rowType(field("foo", INTEGER)),
                "{ foo: 17, foo: 31, foo: 53 } { foo: 67 }",
                List.of(53), List.of(67));
    }

    /**
     * The Ion Hive SerDe captures the first value for duplicate fields in
     * nested Rows, so that is what we default to here.
     */
    @Test
    public void testNestedStructWithDuplicateAndMissingKeys()
            throws IOException
    {
        assertValues(
                RowType.rowType(
                        field("name", RowType.rowType(
                                field("first", VARCHAR),
                                field("last", VARCHAR)))),
                """
                        { name: { last: Guthrie, last: Godfrey } }
                        { name: { first: Joan, last: Baez } }
                        """,
                List.of(Arrays.asList(null, "Guthrie")),
                List.of(List.of("Joan", "Baez")));
    }

    @Test
    public void testStructInList()
            throws IOException
    {
        assertValues(
                RowType.rowType(
                        field("elements", new ArrayType(
                                RowType.rowType(
                                        field("foo", INTEGER))))),
                "{ elements: [ { foo: 13 }, { foo: 17 } ] }",
                // yes, there are three layers of list here:
                // top-level struct (row), list of elements (array), then inner struct (row)
                List.of(
                        List.of(List.of(13), List.of(17))));
    }

    @Test
    public void testIntsOfVariousSizes()
            throws IOException
    {
        List<String> ions = List.of(
                "{ ion_int: 0x7f }", // < one byte
                "{ ion_int: 0x7fff }", // < two bytes
                "{ ion_int: 0x7fffffff }",  // < four bytes
                "{ ion_int: 0x7fffffffffffffff }", // < eight bytes
                "{ ion_int: 0x7fffffffffffffff1 }" // > eight bytes
        );

        List<Type> intTypes = List.of(TINYINT, SMALLINT, INTEGER, BIGINT);
        List<Object> expected = List.of((byte) 0x7f, (short) 0x7fff, 0x7fffffff, 0x7fffffffffffffffL);
        for (int i = 0; i < intTypes.size(); i++) {
            RowType rowType = RowType.rowType(field("ion_int", intTypes.get(i)));
            assertValues(
                    rowType,
                    ions.get(i),
                    List.of(expected.get(i)));

            int nextIon = i + 1;
            Assertions.assertThrows(TrinoException.class, () -> {
                assertValues(rowType,
                        ions.get(nextIon),
                        List.of());
            });
        }
    }

    @Test
    public void testFloat()
            throws IOException
    {
        RowType rowType = RowType.rowType(field("my_double", DoubleType.DOUBLE));
        assertValues(
                rowType,
                "{ my_double: 4444e-4 }",
                List.of(.4444));
    }

    @Test
    public void testBytes()
            throws IOException
    {
        RowType rowType = RowType.rowType(field("blobby", VARBINARY));
        assertValues(
                rowType,
                "{ blobby: {{ YmxvYmJ5IG1jYmxvYmZhY2U= }} }",
                List.of(new SqlVarbinary("blobby mcblobface".getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    public void testDoubleAsFloat()
            throws IOException
    {
        RowType rowType = RowType.rowType(field("my_float", REAL));
        assertValues(
                rowType,
                "{ my_float: 625e-3 }",
                List.of(.625f));

        Assertions.assertThrows(TrinoException.class, () -> {
            assertValues(
                    rowType,
                    "{ my_float: 9e+99 }",
                    List.of());
        });
    }

    @Test
    public void testDateDecoding()
            throws IOException
    {
        RowType rowType = RowType.rowType(field("my_date", DATE));
        SqlDate expected = new SqlDate((int) LocalDate.of(2022, 2, 22).toEpochDay());

        List<String> ions = List.of(
                "{ my_date: 2022-02-22T }",
                "{ my_date: 2022-02-21T12:00-12:00 } ",
                "{ my_date: 2022-02-22T22:22:22Z }",
                "{ my_date: 2022-02-23T00:00+01:00 }",
                "{ my_date: 2022-02-22T00:01Z }",
                "{ my_date: 2022-02-22T00:00:01Z }",
                "{ my_date: 2022-02-22T00:00:00.001Z }",
                "{ my_date: 2022-02-22T23:59:59.999999999Z }");

        for (String ion : ions) {
            assertValues(rowType, ion, List.of(expected));
        }
    }

    @Test
    public void testTimestampDecoding()
            throws IOException
    {
        List<String> ions = List.of(
                "{ my_ts: 2067-08-09T11:22:33Z }",
                "{ my_ts: 2067-08-09T11:22:33.111Z }",
                "{ my_ts: 2067-08-09T11:22:33.111222Z }",
                "{ my_ts: 2067-08-09T11:22:33.111222333Z }",
                "{ my_ts: 2067-08-09T11:22:33.111222333444Z }",
                // fraction beyond picos is truncated
                "{ my_ts: 2067-08-09T11:22:33.111222333444555Z }");

        LocalDateTime dateTimeToSeconds = LocalDateTime.of(2067, 8, 9, 11, 22, 33);
        List<SqlTimestamp> sqlTimestamps = List.of(
                toSqlTimestamp(TimestampType.TIMESTAMP_SECONDS, dateTimeToSeconds),
                toSqlTimestamp(TimestampType.TIMESTAMP_MILLIS, dateTimeToSeconds.plusNanos(111000000)),
                toSqlTimestamp(TimestampType.TIMESTAMP_MICROS, dateTimeToSeconds.plusNanos(111222000)),
                toSqlTimestamp(TimestampType.TIMESTAMP_NANOS, dateTimeToSeconds.plusNanos(111222333)),
                toSqlTimestamp(TimestampType.TIMESTAMP_PICOS, dateTimeToSeconds.plusNanos(111222333), 444));

        for (int i = 0; i < sqlTimestamps.size(); i++) {
            SqlTimestamp sqlTimestamp = sqlTimestamps.get(i);
            RowType rowType = RowType.rowType(
                    field("my_ts", TimestampType.createTimestampType(sqlTimestamp.getPrecision())));

            for (int j = i; j < ions.size(); j++) {
                assertValues(rowType, ions.get(j), List.of(sqlTimestamp));
            }
        }
    }

    @Test
    public void testDecimalPrecisionAndScale()
            throws IOException
    {
        assertValues(
                RowType.rowType(
                        field("amount", DecimalType.createDecimalType(10, 2)),
                        field("big_amount", DecimalType.createDecimalType(25, 5))),
                "{ amount: 1234.00, big_amount: 1234.00000 }"
                        + "{ amount: 1234d0, big_amount: 1234d0 }"
                        + "{ amount: 12d2, big_amount: 12d2 }"
                        + "{ amount: 1234.000, big_amount: 1234.000000 }"
                        + "{ amount: 1.234e3, big_amount: 1.234e3 }" // these are both IonFloats
                        + "{ amount: 1234, big_amount: 1234 }", // these are both IonInts
                List.of(new SqlDecimal(BigInteger.valueOf(123400), 10, 2), new SqlDecimal(BigInteger.valueOf(123400000), 25, 5)),
                List.of(new SqlDecimal(BigInteger.valueOf(123400), 10, 2), new SqlDecimal(BigInteger.valueOf(123400000), 25, 5)),
                List.of(new SqlDecimal(BigInteger.valueOf(120000), 10, 2), new SqlDecimal(BigInteger.valueOf(120000000), 25, 5)),
                List.of(new SqlDecimal(BigInteger.valueOf(123400), 10, 2), new SqlDecimal(BigInteger.valueOf(123400000), 25, 5)),
                List.of(new SqlDecimal(BigInteger.valueOf(123400), 10, 2), new SqlDecimal(BigInteger.valueOf(123400000), 25, 5)),
                List.of(new SqlDecimal(BigInteger.valueOf(123400), 10, 2), new SqlDecimal(BigInteger.valueOf(123400000), 25, 5)));
    }

    @Test
    public void testNumbersTooBigForShortDecimal()
    {
        RowType rowType = RowType.rowType(
                field("amount", DecimalType.createDecimalType(4, 2)));

        List<String> ions = List.of(
                "{ amount: 123.4 }",
                "{ amount: 1.234 }",
                "{ amount: 123 }",
                "{ amount: 1234d-10 }",
                "{ amount: 1234d2 }");

        for (String ionText : ions) {
            Assertions.assertThrows(TrinoException.class, () ->
                    assertValues(rowType, ionText, List.of()));
        }
    }

    @Test
    public void testNumbersTooBigForDecimal128()
    {
        RowType rowType = RowType.rowType(
                field("amount", DecimalType.createDecimalType(20, 2)));

        List<String> ions = List.of(
                "{ amount: 12345678901234567890.4 }",
                "{ amount: 1.234 }",
                "{ amount: 12345678901234567890 }",
                "{ amount: 999999999999999999999999999999999999.999 }", // 39 nines
                "{ amount: 1234d-10 }",
                "{ amount: 1234d22 }");

        for (String ionText : ions) {
            Assertions.assertThrows(TrinoException.class, () ->
                    assertValues(rowType, ionText, List.of()));
        }
    }

    @Test
    public void testPathExtraction()
            throws IOException
    {
        Map<String, String> pathExtractions = Map.of("bar", "(foo bar)", "baz", "(foo baz)");
        assertValues(
                RowType.rowType(field("qux", BOOLEAN), field("bar", INTEGER), field("baz", VARCHAR)),
                IonDecoderConfig.defaultConfig().withPathExtractors(pathExtractions),
                "{ foo: { bar: 31, baz: quux }, qux: true }",
                List.of(true, 31, "quux"));
    }

    @Test
    public void testNonStructTlvPathExtraction()
            throws IOException
    {
        Map<String, String> pathExtractions = Map.of("tlv", "()");
        assertValues(
                RowType.rowType(field("tlv", new ArrayType(INTEGER))),
                IonDecoderConfig.defaultConfig().withPathExtractors(pathExtractions),
                "[13, 17] [19, 23]",
                List.of(List.of(13, 17)),
                List.of(List.of(19, 23)));
    }

    /**
     * Shows how users can configure mapping sequence positions from Ion values to a Trino row.
     */
    @Test
    public void testPositionalPathExtraction()
            throws IOException
    {
        Map<String, String> pathExtractions = Map.of(
                "foo", "(0)",
                "bar", "(1)");
        RowType rowType = RowType.rowType(
                field("foo", INTEGER),
                field("bar", VARCHAR));

        assertValues(
                rowType,
                IonDecoderConfig.defaultConfig().withPathExtractors(pathExtractions),
                "[13, baz] [17, qux]",
                List.of(13, "baz"),
                List.of(17, "qux"));
    }

    @Test
    public void testEncode()
            throws IOException
    {
        Map<String, Integer> map = new HashMap<>();
        map.put("a", 2);
        map.put("b", 5);
        map.put("c", null);
        List<Object> row1 = List.of(17, "something", true, new SqlVarbinary(new byte[] {(byte) 0xff}), List.of(1, 2,
                3), List.of(51, "baz"), map, 5e0, new SqlDecimal(BigInteger.valueOf(123400), 10, 2), (byte) -1, (short) 32767,
                15L, 12.0f, new SqlDate(toIntExact(LocalDate.of(2025, 1, 1).toEpochDay())));

        List<Object> row2 = List.of(31, "somebody", false, new SqlVarbinary(new byte[] {(byte) 0x01, (byte) 0xaa}),
                List.of(7, 8, 9), List.of(67, "qux"), ImmutableMap.builder()
                        .put("foo", 12)
                        .put("bar", 50)
                        .buildOrThrow(), 5e0, new SqlDecimal(BigInteger.valueOf(123400), 10, 2), (byte) 0, (short) -1
                , 0L, 0.0f, new SqlDate(toIntExact(LocalDate.of(2025, 1, 1).toEpochDay())));
        String ionText = """
                { magic_num:17, some_text:"something", is_summer:true, byte_clob:{{/w==}}, sequencer:[1,2,3], struction:{ foo:51, bar:"baz"}, map: {a: 2, b: 5, c: null}, double_value: 5e0, decimal_value: 1234.00, tiny_int: -1, small_int: 32767, big_int: 15, real_num: 12e0, date: 2025-01-01T00:00:00.000Z }
                { magic_num:31, some_text:"somebody", is_summer:false, byte_clob:{{Aao=}}, sequencer:[7,8,9], struction:{ foo:67, bar:"qux"}, map: {foo: 12, bar: 50}, double_value: 5e0, decimal_value: 1234.00, tiny_int: 0, small_int: -1, big_int: 0, real_num: 0e0, date: 2025-01-01T00:00:00.000Z }
                """;

        Page page = toPage(TEST_COLUMNS, row1, row2);
        assertIonEquivalence(TEST_COLUMNS, page, ionText);
    }

    @Test
    public void testEncodeTimestamp()
            throws IOException
    {
        List<Column> timestampColumn = List.of(new Column("my_ts", TimestampType.TIMESTAMP_NANOS, 0));
        Page page = toPage(timestampColumn, List.of(
                toSqlTimestamp(TimestampType.TIMESTAMP_NANOS, LocalDateTime.of(2024, 11, 23, 1, 23, 45, 666777888))));
        assertIonEquivalence(timestampColumn, page, "{ my_ts: 2024-11-23T01:23:45.666777888Z }");
    }

    @Test
    public void testEncodeMixedCaseColumn()
            throws IOException
    {
        List<Column> casedColumn = List.of(
                new Column("TheAnswer", INTEGER, 0));

        Page page = toPage(casedColumn, List.of(42));
        assertIonEquivalence(casedColumn, page, "{ TheAnswer: 42 }");
    }

    @Test
    public void testEncodeWithNullField()
            throws IOException
    {
        List<Object> row1 = Arrays.asList(null, null, null, null, null, null, null, null, null, null, null, null,
                null, null);
        String ionText = """
                {}
                """;

        Page page = toPage(TEST_COLUMNS, row1);
        assertIonEquivalence(TEST_COLUMNS, page, ionText);
    }

    @Test
    public void testEncodeWithNullNestedField()
            throws IOException
    {
        List<Object> row1 = Arrays.asList(17, "something", true, new SqlVarbinary(new byte[] {(byte) 0xff}),
                List.of(1, 2, 3), Arrays.asList(null, "baz"), ImmutableMap.builder()
                        .put("a", 2)
                        .put("b", 5)
                        .buildOrThrow(), 5e0, new SqlDecimal(BigInteger.valueOf(123400), 10, 2), (byte) -1,
                (short) 32767, 15L, 12.0f, new SqlDate(toIntExact(LocalDate.of(2025, 1, 1).toEpochDay())));
        List<Object> row2 = Arrays.asList(31, "somebody", null, new SqlVarbinary(new byte[] {(byte) 0x01,
                (byte) 0xaa}), List.of(7, 8, 9), Arrays.asList(null, "qux"), ImmutableMap.builder()
                .put("foo", 12)
                .put("bar", 50)
                .buildOrThrow(), 5e0, new SqlDecimal(BigInteger.valueOf(123400), 10, 2), (byte) 0, (short) -1, 0L,
                0.0f, new SqlDate(toIntExact(LocalDate.of(2025, 1, 1).toEpochDay())));
        String ionText = """
                { magic_num:17, some_text:"something", is_summer:true, byte_clob:{{/w==}}, sequencer:[1,2,3], struction:{bar:"baz"},  map: {a: 2, b: 5}, double_value: 5e0, decimal_value: 1234.00, tiny_int: -1, small_int: 32767, big_int: 15, real_num: 12e0, date: 2025-01-01T00:00:00.000Z }
                { magic_num:31, some_text:"somebody", byte_clob:{{Aao=}}, sequencer:[7,8,9], struction:{bar:"qux"}, map: {foo: 12, bar: 50}, double_value: 5e0, decimal_value: 1234.00, tiny_int: 0, small_int: -1, big_int: 0, real_num: 0e0, date: 2025-01-01T00:00:00.000Z }
                """;

        Page page = toPage(TEST_COLUMNS, row1, row2);
        assertIonEquivalence(TEST_COLUMNS, page, ionText);
    }

    private void assertValues(RowType rowType, String ionText, List<Object>... expected)
            throws IOException
    {
        assertValues(rowType, IonDecoderConfig.defaultConfig(), ionText, expected);
    }

    private void assertValues(RowType rowType, IonDecoderConfig config, String ionText, List<Object>... expected)
            throws IOException
    {
        List<Column> columns = new LinkedList<>();
        for (RowType.Field field : rowType.getFields()) {
            columns.add(new Column(field.getName().get(), field.getType(), columns.size()));
        }

        PageBuilder pageBuilder = new PageBuilder(expected.length, rowType.getTypeParameters());
        IonDecoder decoder = new IonDecoder(columns, config);

        try (IonReader ionReader = IonReaderBuilder.standard().build(ionText)) {
            for (int i = 0; i < expected.length; i++) {
                assertThat(ionReader.next()).isNotNull();
                decoder.decode(ionReader, pageBuilder);
            }
            assertThat(ionReader.next()).isNull();
        }

        for (int i = 0; i < expected.length; i++) {
            List<Object> actual = readTrinoValues(columns, pageBuilder.build(), i);
            assertColumnValuesEquals(columns, actual, expected[i]);
        }
    }

    /**
     * Encodes the page as Ion and asserts its equivalence to ionText, per the Ion datamodel.
     * <br>
     * This allows us to make assertions about how the data is encoded that may be equivalent
     * in the trino datamodel but distinct per the Ion datamodel. Some examples:
     * - absent fields vs null field values
     * - Symbol vs String for text values
     * - Timestamps with UTC vs unknown offset
     */
    private void assertIonEquivalence(List<Column> columns, Page page, String ionText)
            throws IOException
    {
        IonSystem system = IonSystemBuilder.standard().build();
        IonDatagram datagram = system.newDatagram();
        IonEncoder encoder = new IonEncoder(columns);
        IonWriter ionWriter = system.newWriter(datagram);
        encoder.encode(ionWriter, page);
        ionWriter.close();

        IonDatagram expected = system.getLoader().load(ionText);
        Assertions.assertEquals(datagram.size(), expected.size());
        for (int i = 0; i < expected.size(); i++) {
            // IonValue.equals() is Ion model equivalence.
            Assertions.assertEquals(expected.get(i), datagram.get(i));
        }
    }
}
