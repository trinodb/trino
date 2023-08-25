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
package io.trino.cli;

import com.google.common.collect.ImmutableList;
import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;
import org.testng.annotations.Test;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.trino.client.ClientStandardTypes.ARRAY;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.ClientStandardTypes.MAP;
import static io.trino.client.ClientStandardTypes.VARBINARY;
import static io.trino.client.ClientStandardTypes.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toMap;
import static org.testng.Assert.assertEquals;

public class TestAlignedTablePrinter
{
    @Test
    public void testAlignedPrinting()
            throws Exception
    {
        List<Column> columns = ImmutableList.<Column>builder()
                .add(column("first", VARCHAR))
                .add(column("last", VARCHAR))
                .add(column("quantity", BIGINT))
                .build();
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AlignedTablePrinter(columns, writer);

        printer.printRows(rows(
                row("hello", "world", 123),
                row("a", null, 4.5),
                row("b", null, null),
                row("some long\ntext that\ndoes not\nfit on\none line", "more\ntext", 4567),
                row("bye", "done", -15)),
                true);
        printer.finish();

        String expected = "" +
                "   first   | last  | quantity \n" +
                "-----------+-------+----------\n" +
                " hello     | world |      123 \n" +
                " a         | NULL  |      4.5 \n" +
                " b         | NULL  |     NULL \n" +
                " some long+| more +|     4567 \n" +
                " text that+| text  |          \n" +
                " does not +|       |          \n" +
                " fit on   +|       |          \n" +
                " one line  |       |          \n" +
                " bye       | done  |      -15 \n" +
                "(5 rows)\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testHexPrintingInLists()
            throws Exception
    {
        List<Column> columns = ImmutableList.of(column("list", ARRAY));

        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AlignedTablePrinter(columns, writer);

        byte[] value = "hello".getBytes(UTF_8);

        printer.printRows(rows(row(ImmutableList.of(value))), true);
        printer.finish();

        String expected = "" +
                "       list       \n" +
                "------------------\n" +
                " [68 65 6c 6c 6f] \n" +
                "(1 row)\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testHexPrintingInMaps()
            throws Exception
    {
        List<Column> columns = ImmutableList.of(column("map", MAP));

        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AlignedTablePrinter(columns, writer);

        byte[] value = "hello".getBytes(UTF_8);

        printer.printRows(rows(row(map(item("key", value), item("key2", value)))), true);
        printer.finish();

        String expected = "" +
                "                    map                    \n" +
                "-------------------------------------------\n" +
                " {key2=68 65 6c 6c 6f, key=68 65 6c 6c 6f} \n" +
                "(1 row)\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testHexPrintingInMapKeys()
            throws Exception
    {
        List<Column> columns = ImmutableList.of(column("map", MAP));

        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AlignedTablePrinter(columns, writer);

        byte[] value = "hello".getBytes(UTF_8);

        printer.printRows(rows(row(map(item(value, "world")))), true);
        printer.finish();

        String expected = "" +
                "          map           \n" +
                "------------------------\n" +
                " {68 65 6c 6c 6f=world} \n" +
                "(1 row)\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testHexPrintingInNestedStructures()
            throws Exception
    {
        List<Column> columns = ImmutableList.of(column("map", MAP));

        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AlignedTablePrinter(columns, writer);

        byte[] value = "hello".getBytes(UTF_8);

        printer.printRows(rows(row(map(item("key", asList(value, null)), item("key2", map(item("nested", value)))))), true);
        printer.finish();

        String expected = "" +
                "                            map                             \n" +
                "------------------------------------------------------------\n" +
                " {key2={nested=68 65 6c 6c 6f}, key=[68 65 6c 6c 6f, NULL]} \n" +
                "(1 row)\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testAlignedPrintingOneRow()
            throws Exception
    {
        List<Column> columns = ImmutableList.<Column>builder()
                .add(column("first", VARCHAR))
                .add(column("last", VARCHAR))
                .build();
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AlignedTablePrinter(columns, writer);

        printer.printRows(rows(row("a long line\nwithout wrapping", "text")), true);
        printer.finish();

        String expected = "" +
                "      first       | last \n" +
                "------------------+------\n" +
                " a long line      | text \n" +
                " without wrapping |      \n" +
                "(1 row)\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testAlignedPrintingNoRows()
            throws Exception
    {
        List<Column> columns = ImmutableList.<Column>builder()
                .add(column("first", VARCHAR))
                .add(column("last", VARCHAR))
                .build();
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AlignedTablePrinter(columns, writer);

        printer.finish();

        String expected = "" +
                " first | last \n" +
                "-------+------\n" +
                "(0 rows)\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testAlignedPrintingHex()
            throws Exception
    {
        List<Column> columns = ImmutableList.<Column>builder()
                .add(column("first", VARCHAR))
                .add(column("binary", VARBINARY))
                .add(column("last", VARCHAR))
                .build();
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AlignedTablePrinter(columns, writer);

        printer.printRows(rows(
                row("hello", bytes("hello"), "world"),
                row("a", bytes("some long text that is more than 16 bytes"), "b"),
                row("cat", bytes(""), "dog")),
                true);
        printer.finish();

        String expected = "" +
                " first |                     binary                      | last  \n" +
                "-------+-------------------------------------------------+-------\n" +
                " hello | 68 65 6c 6c 6f                                  | world \n" +
                " a     | 73 6f 6d 65 20 6c 6f 6e 67 20 74 65 78 74 20 74+| b     \n" +
                "       | 68 61 74 20 69 73 20 6d 6f 72 65 20 74 68 61 6e+|       \n" +
                "       | 20 31 36 20 62 79 74 65 73                      |       \n" +
                " cat   |                                                 | dog   \n" +
                "(3 rows)\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testAlignedPrintingWideCharacters()
            throws Exception
    {
        List<Column> columns = ImmutableList.<Column>builder()
                .add(column("go\u7f51", VARCHAR))
                .add(column("last", VARCHAR))
                .add(column("quantity\u7f51", BIGINT))
                .build();
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AlignedTablePrinter(columns, writer);

        printer.printRows(rows(
                row("hello", "wide\u7f51", 123),
                row("some long\ntext \u7f51\ndoes not\u7f51\nfit", "more\ntext", 4567),
                row("bye", "done", -15)),
                true);
        printer.finish();

        String expected = "" +
                "    go\u7f51    |  last  | quantity\u7f51 \n" +
                "------------+--------+------------\n" +
                " hello      | wide\u7f51 |        123 \n" +
                " some long +| more  +|       4567 \n" +
                " text \u7f51   +| text   |            \n" +
                " does not\u7f51+|        |            \n" +
                " fit        |        |            \n" +
                " bye        | done   |        -15 \n" +
                "(3 rows)\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    static Column column(String name, String type)
    {
        return new Column(name, type, new ClientTypeSignature(type));
    }

    static List<?> row(Object... values)
    {
        return asList(values);
    }

    static Map<?, ?> map(KeyValue... values)
    {
        return Arrays.stream(values).collect(toMap(KeyValue::getKey, KeyValue::getValue));
    }

    static KeyValue item(Object key, Object value)
    {
        return new KeyValue(key, value);
    }

    static List<List<?>> rows(List<?>... rows)
    {
        return asList(rows);
    }

    static byte[] bytes(String value)
    {
        return value.getBytes(UTF_8);
    }

    static class KeyValue
    {
        private final Object key;
        private final Object value;

        KeyValue(Object key, Object value)
        {
            this.key = key;
            this.value = value;
        }

        public Object getKey()
        {
            return key;
        }

        public Object getValue()
        {
            return value;
        }
    }
}
