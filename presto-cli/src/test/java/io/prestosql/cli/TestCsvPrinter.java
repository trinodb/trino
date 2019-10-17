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
package io.prestosql.cli;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import static io.prestosql.cli.CsvPrinter.CsvOutputFormat.NO_HEADER;
import static io.prestosql.cli.CsvPrinter.CsvOutputFormat.NO_HEADER_AND_QUOTES;
import static io.prestosql.cli.CsvPrinter.CsvOutputFormat.NO_QUOTES;
import static io.prestosql.cli.CsvPrinter.CsvOutputFormat.STANDARD;
import static io.prestosql.cli.TestAlignedTablePrinter.row;
import static io.prestosql.cli.TestAlignedTablePrinter.rows;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

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

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testCsvPrintingNoRows()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last");
        OutputPrinter printer = new CsvPrinter(fieldNames, writer, STANDARD);

        printer.finish();

        assertEquals(writer.getBuffer().toString(), "\"first\",\"last\"\n");
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

        assertEquals(writer.getBuffer().toString(), expected);
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
                row("a", null, 4.5));
        printer.finish();

        String expected = "" +
                "first,last,quantity\n" +
                "hello,world,123\n" +
                "a,,4.5\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testCsvPrintingNoRowsWithoutQuotes()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last");
        OutputPrinter printer = new CsvPrinter(fieldNames, writer, NO_QUOTES);

        printer.finish();

        assertEquals(writer.getBuffer().toString(), "first,last\n");
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

        assertEquals(writer.getBuffer().toString(), expected);
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

        assertEquals(writer.getBuffer().toString(), expected);
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

        assertEquals(writer.getBuffer().toString(), expected);
    }

    private static void printRows(OutputPrinter printer, List<?>... rows)
            throws IOException
    {
        printer.printRows(rows(rows), true);
    }
}
