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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import static io.trino.cli.CsvPrinter.CsvOutputFormat.NO_HEADER;
import static io.trino.cli.CsvPrinter.CsvOutputFormat.NO_HEADER_AND_QUOTES;
import static io.trino.cli.CsvPrinter.CsvOutputFormat.NO_QUOTES;
import static io.trino.cli.CsvPrinter.CsvOutputFormat.STANDARD;
import static io.trino.cli.TestAlignedTablePrinter.item;
import static io.trino.cli.TestAlignedTablePrinter.map;
import static io.trino.cli.TestAlignedTablePrinter.row;
import static io.trino.cli.TestAlignedTablePrinter.rows;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCsvPrinter
{
    @Test
    public void testCsvPrinting()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new CsvPrinter(fieldNames, writer, STANDARD);

        printRows(
                printer,
                row("hello", "world", 123),
                row("a", null, 4.5),
                row("some long\ntext that\ndoes not\nfit on\none line", "more\ntext", 4567),
                row("bye", "done", -15));
        printer.finish();

        String expected = "" +
                "\"first\",\"last\",\"quantity\"\n" +
                "\"hello\",\"world\",\"123\"\n" +
                "\"a\",\"\",\"4.5\"\n" +
                "\"some long\n" +
                "text that\n" +
                "does not\n" +
                "fit on\n" +
                "one line\",\"more\n" +
                "text\",\"4567\"\n" +
                "\"bye\",\"done\",\"-15\"\n";

        assertThat(writer.getBuffer().toString()).isEqualTo(expected);
    }

    @Test
    public void testCsvPrintingNoRows()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last");
        OutputPrinter printer = new CsvPrinter(fieldNames, writer, STANDARD);

        printer.finish();

        assertThat(writer.getBuffer().toString()).isEqualTo("\"first\",\"last\"\n");
    }

    @Test
    public void testCsvPrintingNoHeader()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new CsvPrinter(fieldNames, writer, NO_HEADER);

        printRows(
                printer,
                row("hello", "world", 123),
                row("a", null, 4.5));
        printer.finish();

        String expected = "" +
                "\"hello\",\"world\",\"123\"\n" +
                "\"a\",\"\",\"4.5\"\n";

        assertThat(writer.getBuffer().toString()).isEqualTo(expected);
    }

    @Test
    public void testCsvPrintingWithoutQuotes()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new CsvPrinter(fieldNames, writer, NO_QUOTES);

        printRows(
                printer,
                row("hello", "world", 123),
                row("a", null, 4.5),
                row("\"quote\"", null, 2137));
        printer.finish();

        String expected = "" +
                "first,last,quantity\n" +
                "hello,world,123\n" +
                "a,,4.5\n" + "" +
                "\"quote\",,2137\n";

        assertThat(writer.getBuffer().toString()).isEqualTo(expected);
    }

    @Test
    public void testCsvPrintingNoRowsWithoutQuotes()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last");
        OutputPrinter printer = new CsvPrinter(fieldNames, writer, NO_QUOTES);

        printer.finish();

        assertThat(writer.getBuffer().toString()).isEqualTo("first,last\n");
    }

    @Test
    public void testCsvPrintingNoHeaderWithoutQuotes()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new CsvPrinter(fieldNames, writer, NO_HEADER_AND_QUOTES);

        printRows(
                printer,
                row("hello", "world", 123),
                row("a", null, 4.5));
        printer.finish();

        String expected = "" +
                "hello,world,123\n" +
                "a,,4.5\n";

        assertThat(writer.getBuffer().toString()).isEqualTo(expected);
    }

    @Test
    public void testCsvPrintingNoRowsWithNoHeaderAndWithoutQuotes()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new CsvPrinter(fieldNames, writer, NO_HEADER_AND_QUOTES);

        printer.finish();

        String expected = "";

        assertThat(writer.getBuffer().toString()).isEqualTo(expected);
    }

    @Test
    public void testCsvVarbinaryPrinting()
            throws IOException
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new CsvPrinter(fieldNames, writer, NO_HEADER);

        printRows(printer, row("hello".getBytes(UTF_8), null, 123));
        printer.finish();

        String expected = "\"68 65 6c 6c 6f\",\"\",\"123\"\n";

        assertThat(writer.getBuffer().toString()).isEqualTo(expected);
    }

    @Test
    public void testCsvVarbinaryInMaps()
            throws IOException
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("map", "value");
        OutputPrinter printer = new CsvPrinter(fieldNames, writer, STANDARD);
        printRows(printer, row(map(item("key", "value".getBytes(UTF_8))), "value"));
        printer.finish();

        String expected = "\"map\",\"value\"\n" +
                "\"{key=76 61 6c 75 65}\",\"value\"\n";

        assertThat(writer.getBuffer().toString()).isEqualTo(expected);
    }

    @Test
    public void testCsvVarbinaryInList()
            throws IOException
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("list", "value");
        OutputPrinter printer = new CsvPrinter(fieldNames, writer, STANDARD);
        printRows(printer, row(ImmutableList.of("value".getBytes(UTF_8)), "value"));
        printer.finish();

        String expected = "\"list\",\"value\"\n" +
                "\"[76 61 6c 75 65]\",\"value\"\n";

        assertThat(writer.getBuffer().toString()).isEqualTo(expected);
    }

    private static void printRows(OutputPrinter printer, List<?>... rows)
            throws IOException
    {
        printer.printRows(rows(rows), true);
    }
}
