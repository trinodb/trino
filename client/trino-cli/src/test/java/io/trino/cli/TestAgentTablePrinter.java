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
import io.trino.client.Column;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.cli.AgentTablePrinter.EARLY_STOP_ROWS;
import static io.trino.cli.TestMarkdownTablePrinter.column;
import static io.trino.cli.TestMarkdownTablePrinter.row;
import static io.trino.cli.TestMarkdownTablePrinter.rows;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.ClientStandardTypes.VARCHAR;
import static java.util.Collections.reverse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAgentTablePrinter
{
    private static final List<Column> COLUMNS = ImmutableList.of(column("name", VARCHAR), column("value", BIGINT));
    private static final AgentLimits UNLIMITED = new AgentLimits(0, 0, 0);

    @Test
    public void testSmallResultHasNoFooter()
            throws Exception
    {
        assertThat(print(AgentLimits.DEFAULT, row("a", 1), row("b", null), row(null, 3)))
                .isEqualTo("" +
                        "|name:varchar|value:bigint|\n" +
                        "|---|---|\n" +
                        "|a|1|\n" +
                        "|b|NULL|\n" +
                        "|NULL|3|\n" +
                        // keeps the table apart from the output of the next statement
                        "\n");
    }

    @Test
    public void testEmptyResultHasFooter()
            throws Exception
    {
        assertThat(print(AgentLimits.DEFAULT))
                .matches("" +
                        "\\|name:varchar\\|value:bigint\\|\n" +
                        "\\|---\\|---\\|\n" +
                        "\n" +
                        "\\(0 rows, hash: [0-9a-f]{16}\\)\n");
    }

    @Test
    public void testFooterFromTenRows()
            throws Exception
    {
        assertThat(print(AgentLimits.DEFAULT, generateRows(9))).doesNotContain("hash");

        String output = print(AgentLimits.DEFAULT, generateRows(10));
        assertThat(output).contains("|row9|9|\n");
        assertThat(output).matches("(?s).*\n\\(10 rows, hash: [0-9a-f]{16}\\)\n");
    }

    @Test
    public void testHashDistinguishesNullFromNullString()
            throws Exception
    {
        assertThat(hash(row("NULL", 1))).isNotEqualTo(hash(row(null, 1)));
    }

    @Test
    public void testHashDistinguishesCellBoundaries()
            throws Exception
    {
        assertThat(hash(row("ab", 1))).isNotEqualTo(hash(row("a", 1)));
        assertThat(hash(row("a|b", 1))).isNotEqualTo(hash(row("a", 1)));
        assertThat(hash(row("", 11))).isNotEqualTo(hash(row("1", 1)));
    }

    @Test
    public void testHashCoversColumnNamesAndTypes()
            throws Exception
    {
        List<List<?>> rows = generateRows(10);

        String hash = hash(COLUMNS, UNLIMITED, rows);
        assertThat(hash(ImmutableList.of(column("renamed", VARCHAR), column("value", BIGINT)), UNLIMITED, rows)).isNotEqualTo(hash);
        assertThat(hash(ImmutableList.of(column("name", VARCHAR), column("value", VARCHAR)), UNLIMITED, rows)).isNotEqualTo(hash);
        assertThat(hash(ImmutableList.of(column("namevalue", VARCHAR)), UNLIMITED, rows)).isNotEqualTo(hash);
        assertThat(hash(COLUMNS, UNLIMITED, rows)).isEqualTo(hash);
    }

    @Test
    public void testHashIsOrderIndependent()
            throws Exception
    {
        List<List<?>> forward = generateRows(50);
        List<List<?>> backward = new ArrayList<>(forward);
        reverse(backward);

        assertThat(hash(backward.toArray(new List<?>[0]))).isEqualTo(hash(forward.toArray(new List<?>[0])));
    }

    @Test
    public void testHashDoesNotCancelDuplicateRows()
            throws Exception
    {
        assertThat(hash(row("a", 1), row("a", 1))).isNotEqualTo(hash());
    }

    @Test
    public void testRowLimit()
            throws Exception
    {
        String output = print(new AgentLimits(100, 0, 0), generateRows(1500));

        List<String> lines = List.of(output.split("\n"));
        assertThat(lines.subList(0, 3)).containsExactly("|name:varchar|value:bigint|", "|---|---|", "|row0|0|");
        assertThat(lines.get(2 + 19)).isEqualTo("|row19|19|");
        assertThat(lines.get(2 + 20)).isEqualTo("…(+1460 rows)");
        assertThat(lines.get(2 + 21)).isEqualTo("|row1480|1480|");
        assertThat(lines.get(2 + 40)).isEqualTo("|row1499|1499|");
        assertThat(output).matches("(?s).*\n\\(1500 rows, showing the first 20 and last 20, --agent-max-rows 0 for all, hash: [0-9a-f]{16}\\)\n");
    }

    @Test
    public void testHashCoversRowsThatAreNotShown()
            throws Exception
    {
        AgentLimits limits = new AgentLimits(100, 0, 0);
        List<List<?>> rows = generateRows(1500);
        String hash = hash(limits, rows.toArray(new List<?>[0]));
        assertThat(print(limits, rows)).doesNotContain("|row700|700|");

        rows.set(700, row("changed", 700));
        assertThat(hash(limits, rows.toArray(new List<?>[0]))).isNotEqualTo(hash);
    }

    @Test
    public void testRowLimitBelowHeadAndTail()
            throws Exception
    {
        String output = print(new AgentLimits(10, 0, 0), generateRows(30));

        assertThat(output).contains("|row4|4|\n…(+20 rows)\n|row25|25|\n");
        assertThat(output).doesNotContain("|row5|5|");
        assertThat(output).doesNotContain("|row24|24|");
        assertThat(output).matches("(?s).*\n\\(30 rows, showing the first 5 and last 5, --agent-max-rows 0 for all, hash: [0-9a-f]{16}\\)\n");
    }

    @Test
    public void testOddRowLimitGivesTheHeadTheExtraRow()
            throws Exception
    {
        String output = print(new AgentLimits(3, 0, 0), generateRows(10));

        assertThat(output).contains("|row1|1|\n…(+7 rows)\n|row9|9|\n");
        assertThat(output).matches("(?s).*\n\\(10 rows, showing the first 2 and last 1, --agent-max-rows 0 for all, hash: [0-9a-f]{16}\\)\n");
    }

    @Test
    public void testRowLimitOfOne()
            throws Exception
    {
        String output = print(new AgentLimits(1, 0, 0), generateRows(30));

        assertThat(output).contains("|row0|0|\n…(+29 rows)\n");
        assertThat(output).matches("(?s).*\n\\(30 rows, showing the first 1 and last 0, --agent-max-rows 0 for all, hash: [0-9a-f]{16}\\)\n");
    }

    @Test
    public void testByteLimit()
            throws Exception
    {
        String wide = "x".repeat(500);
        List<List<?>> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            rows.add(row(wide + i, i));
        }

        String output = print(new AgentLimits(0, 10_000, 0), rows.toArray(new List<?>[0]));

        assertThat(output.length()).isLessThan(11_000);
        assertThat(output).contains("…(+");
        assertThat(output).contains("showing the first");
    }

    @Test
    public void testFirstRowUsesWholeByteLimit()
            throws Exception
    {
        String wide = "x".repeat(600);
        String output = print(new AgentLimits(0, 1000, 0), row(wide, 1), row(wide, 2));

        assertThat(output).contains("|" + wide + "|1|\n…(+1 rows)\n");
        assertThat(output).doesNotContain("|2|\n");
        assertThat(output).matches("(?s).*\n\\(2 rows, showing the first 1 and last 0, --agent-max-bytes 0 for all, hash: [0-9a-f]{16}\\)\n");
    }

    @Test
    public void testTailGetsTheBytesTheHeadLeaves()
            throws Exception
    {
        // the first row is more than half of the limit, so the tail has room for one of the next two rows
        String wide = "x".repeat(600);
        String middle = "y".repeat(195);
        String last = "z".repeat(195);
        String output = print(new AgentLimits(0, 1000, 0), row(wide, 1), row(middle, 2), row(last, 3));

        assertThat(output).contains("|" + wide + "|1|\n…(+1 rows)\n|" + last + "|3|\n");
        assertThat(output).doesNotContain(middle);
        assertThat(output).matches("(?s).*\n\\(3 rows, showing the first 1 and last 1, --agent-max-bytes 0 for all, hash: [0-9a-f]{16}\\)\n");
    }

    @Test
    public void testHeaderCountsAgainstByteLimit()
            throws Exception
    {
        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            columns.add(column("column_with_a_long_name_" + i, VARCHAR));
        }
        // the header alone is over the limit, so only the first row is printed
        List<List<?>> rows = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            rows.add(ImmutableList.of(i));
        }

        String output = print(columns, new AgentLimits(0, 1000, 0), rows);

        assertThat(output).contains("|0|\n…(+11 rows)\n");
        assertThat(output).contains("showing the first 1 and last 0");
    }

    @Test
    public void testHeaderNamesAreTruncatedWithCells()
            throws Exception
    {
        String output = print(ImmutableList.of(column("a_very_long_column_name", VARCHAR)), new AgentLimits(0, 0, 10), ImmutableList.of());

        assertThat(output).startsWith("|a_very_lon…(+13 chars):varchar|\n|---|\n");
    }

    @Test
    public void testUnlimitedResultIsWrittenWhileItIsRead()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AgentTablePrinter(COLUMNS, writer, UNLIMITED);

        printer.printRows(generateRows(3), false);

        assertThat(writer.toString()).isEqualTo("|name:varchar|value:bigint|\n|---|---|\n|row0|0|\n|row1|1|\n|row2|2|\n");

        printer.printRows(generateRows(20), true);
        printer.finish();
        assertThat(writer.toString()).matches("(?s)\\|name:varchar.*\\|row19\\|19\\|\n\n\\(23 rows, hash: [0-9a-f]{16}\\)\n");
    }

    @Test
    public void testLimitedResultIsNotWrittenBeforeItIsKnownToFit()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AgentTablePrinter(COLUMNS, writer, AgentLimits.DEFAULT);

        printer.printRows(generateRows(3), false);

        assertThat(writer.toString()).isEmpty();
    }

    @Test
    public void testUnlimitedPrintsEverything()
            throws Exception
    {
        String output = print(UNLIMITED, generateRows(3000));

        assertThat(output).contains("|row1500|1500|\n");
        assertThat(output).doesNotContain("…");
    }

    @Test
    public void testCellTruncation()
            throws Exception
    {
        String output = print(new AgentLimits(0, 0, 10), row("0123456789ABCDEF", 1), row("0123456789", 2));

        assertThat(output).contains("|0123456789…(+6 chars)|1|\n");
        assertThat(output).contains("|0123456789|2|\n");
    }

    @Test
    public void testHashIsComputedBeforeCellTruncation()
            throws Exception
    {
        AgentLimits narrow = new AgentLimits(0, 0, 3);

        assertThat(print(narrow, row("abcdef", 1))).contains("|abc…(+3 chars)|1|");
        assertThat(hash(narrow, row("abcdef", 1))).isNotEqualTo(hash(narrow, row("abcdeg", 1)));
    }

    @Test
    public void testEscaping()
            throws Exception
    {
        List<Column> columns = ImmutableList.of(column("a|b", VARCHAR));
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AgentTablePrinter(columns, writer, AgentLimits.DEFAULT);
        printer.printRows(rows(row("x|y\nz\r"), row("c:\\temp\\n")), true);
        printer.finish();

        // a backslash is escaped, so that a line break can be told from the text \n
        assertThat(writer.toString()).isEqualTo("" +
                "|a\\|b:varchar|\n" +
                "|---|\n" +
                "|x\\|y\\nz\\r|\n" +
                "|c:\\\\temp\\\\n|\n" +
                "\n");
    }

    @Test
    public void testBatchesAreCombined()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AgentTablePrinter(COLUMNS, writer, AgentLimits.DEFAULT);
        printer.printRows(rows(row("a", 1)), false);
        printer.printRows(rows(row("b", 2)), false);
        printer.printRows(rows(), true);
        printer.finish();

        assertThat(writer.toString()).endsWith("|a|1|\n|b|2|\n\n");
    }

    @Test
    public void testEarlyStop()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AgentTablePrinter(COLUMNS, writer, new AgentLimits(10, 0, 0));

        List<List<?>> rows = generateRows(10 + 1 + EARLY_STOP_ROWS);
        assertThatThrownBy(() -> printer.printRows(rows, false))
                .isInstanceOf(ResultLimitReachedException.class);
        printer.finish();

        String output = writer.toString();
        assertThat(output).contains("|row4|4|\n…(+");
        assertThat(output).doesNotContain("hash");
        assertThat(output).matches("(?s).*\n\\(at least 100011 rows, showing the first 5, query stopped early, --agent-max-rows 0 for all\\)\n");
    }

    @Test
    public void testNoEarlyStopWhenTheQueryMustNotBeCancelled()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AgentTablePrinter(COLUMNS, writer, new AgentLimits(10, 0, 0), false, () -> true);

        int rows = 10 + 1 + EARLY_STOP_ROWS + 50;
        printer.printRows(generateRows(rows), true);
        printer.finish();

        assertThat(writer.toString()).doesNotContain("stopped early");
        assertThat(writer.toString()).matches("(?s).*\n\\(" + rows + " rows, showing the first 5 and last 5, --agent-max-rows 0 for all, hash: [0-9a-f]{16}\\)\n");
        assertThat(writer.toString()).contains("|row" + (rows - 1) + "|" + (rows - 1) + "|\n");
    }

    @Test
    public void testResultOfAQueryThatDidNotFinishIsIncomplete()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AgentTablePrinter(COLUMNS, writer, AgentLimits.DEFAULT, true, () -> false);
        printer.printRows(generateRows(10), true);
        printer.finish();

        assertThat(writer.toString()).doesNotContain("hash");
        assertThat(writer.toString()).endsWith("|row9|9|\n\n(incomplete: 10 rows received before the query ended)\n");
    }

    @Test
    public void testIncompleteResult()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AgentTablePrinter(COLUMNS, writer, AgentLimits.DEFAULT);
        printer.printRows(generateRows(5), false);
        printer.finish();

        assertThat(writer.toString()).endsWith("|row4|4|\n\n(incomplete: 5 rows received before the query ended)\n");
    }

    @Test
    public void testFirstRowIsPrintedOverByteLimit()
            throws Exception
    {
        String wide = "x".repeat(1500);
        String output = print(new AgentLimits(0, 1000, 0), row(wide, 1), row("short", 2), row("short", 3));

        assertThat(output).contains("|" + wide + "|1|\n…(+");
        assertThat(output).matches("(?s).*\n\\(3 rows, showing the first 1 and last \\d, --agent-max-bytes 0 for all, hash: [0-9a-f]{16}\\)\n");
    }

    @Test
    public void testFooterNamesOnlyTheLimitsThatWereExceeded()
            throws Exception
    {
        List<List<?>> rows = generateRows(100);

        assertThat(print(new AgentLimits(10, 1_000_000, 0), rows))
                .contains(", --agent-max-rows 0 for all, hash: ")
                .doesNotContain("--agent-max-bytes");
        assertThat(print(new AgentLimits(1000, 500, 0), rows))
                .contains(", --agent-max-bytes 0 for all, hash: ")
                .doesNotContain("--agent-max-rows");
        assertThat(print(new AgentLimits(10, 500, 0), rows))
                .contains(", --agent-max-rows 0 --agent-max-bytes 0 for all, hash: ");
        // a result that is shown in full needs none
        assertThat(print(new AgentLimits(10, 0, 0), generateRows(10))).doesNotContain("for all");
    }

    @Test
    public void testValueWithLineBreaksIsPrintedAsText()
            throws Exception
    {
        List<Column> columns = ImmutableList.of(column("Query Plan", VARCHAR));
        String plan = "Fragment 0 [SINGLE]\n    Output layout: [a]\n        - TableScan[nation]\n";

        // the text is neither escaped nor cut at the width of a cell
        assertThat(print(columns, new AgentLimits(0, 0, 10), ImmutableList.of(row(plan)))).isEqualTo(plan + "\n");
        assertThat(print(columns, AgentLimits.DEFAULT, ImmutableList.of(row("a\nb")))).isEqualTo("a\nb\n\n");
    }

    @Test
    public void testTextIsCutByTheByteLimit()
            throws Exception
    {
        List<Column> columns = ImmutableList.of(column("Query Plan", VARCHAR));
        String line = "x".repeat(99) + "\n";
        String plan = line.repeat(50);

        String output = print(columns, new AgentLimits(0, 1000, 0), ImmutableList.of(row(plan)));

        assertThat(output).startsWith(plan.substring(0, 1000) + "…(+4000 chars)\n");
        assertThat(output).matches("(?s).*\n\\(1 row, showing the first 1000 of 5000 characters, --agent-max-bytes 0 for all, hash: [0-9a-f]{16}\\)\n");
    }

    @Test
    public void testTextIsCutBetweenCharacters()
            throws Exception
    {
        List<Column> columns = ImmutableList.of(column("Query Plan", VARCHAR));
        // three bytes each, and a pair of surrogates that takes four
        String plan = "\u20ac\u20ac\u20ac\n\ud83d\ude00\ud83d\ude00\n";

        String output = print(columns, new AgentLimits(0, 11, 0), ImmutableList.of(row(plan)));

        // 3 + 3 + 3 bytes fit, the line break makes 10, and the next character takes 4
        assertThat(output).startsWith("\u20ac\u20ac\u20ac\n…(+5 chars)\n");
    }

    @Test
    public void testSeveralRowsAreNotPrintedAsText()
            throws Exception
    {
        List<Column> columns = ImmutableList.of(column("Query Plan", VARCHAR));

        assertThat(print(columns, AgentLimits.DEFAULT, ImmutableList.of(row("a\nb"), row("c"))))
                .isEqualTo("|Query Plan:varchar|\n|---|\n|a\\nb|\n|c|\n\n");
        assertThat(print(columns, UNLIMITED, ImmutableList.of(row("a\nb"), row("c"))))
                .isEqualTo("|Query Plan:varchar|\n|---|\n|a\\nb|\n|c|\n\n");
        // one value without a line break and a result of several columns stay a table
        assertThat(print(columns, AgentLimits.DEFAULT, ImmutableList.of(row("a")))).isEqualTo("|Query Plan:varchar|\n|---|\n|a|\n\n");
        assertThat(print(AgentLimits.DEFAULT, row("a\nb", 1))).contains("|a\\nb|1|");
    }

    @Test
    public void testTextOfAQueryThatDidNotFinishIsIncomplete()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AgentTablePrinter(ImmutableList.of(column("Query Plan", VARCHAR)), writer, AgentLimits.DEFAULT, true, () -> false);
        printer.printRows(ImmutableList.of(row("a\nb")), true);
        printer.finish();

        assertThat(writer.toString()).isEqualTo("a\nb\n\n(incomplete: 1 rows received before the query ended)\n");
    }

    @Test
    public void testSingleColumnIsWrittenAfterTheSecondRowWhenUnlimited()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AgentTablePrinter(ImmutableList.of(column("a", VARCHAR)), writer, UNLIMITED);

        printer.printRows(ImmutableList.of(row("x")), false);
        // the first row can still turn out to be text
        assertThat(writer.toString()).isEmpty();

        printer.printRows(ImmutableList.of(row("y")), false);
        assertThat(writer.toString()).isEqualTo("|a:varchar|\n|---|\n|x|\n|y|\n");
    }

    private static final Pattern FOOTER = Pattern.compile("hash: ([0-9a-f]{16})");

    // the footer is only printed from ten rows, so the rows under test are followed by ten fixed ones
    private static String hash(List<?>... rows)
            throws Exception
    {
        return hash(UNLIMITED, rows);
    }

    private static String hash(AgentLimits limits, List<?>... rows)
            throws Exception
    {
        return hash(COLUMNS, limits, ImmutableList.<List<?>>builder().add(rows).addAll(generateRows(10)).build());
    }

    private static String hash(List<Column> columns, AgentLimits limits, List<List<?>> rows)
            throws Exception
    {
        Matcher matcher = FOOTER.matcher(print(columns, limits, rows));
        assertThat(matcher.find()).isTrue();
        return matcher.group(1);
    }

    private static String print(AgentLimits limits, List<?>... rows)
            throws Exception
    {
        return print(limits, ImmutableList.copyOf(rows));
    }

    private static String print(AgentLimits limits, List<List<?>> rows)
            throws Exception
    {
        return print(COLUMNS, limits, rows);
    }

    private static String print(List<Column> columns, AgentLimits limits, List<List<?>> rows)
            throws Exception
    {
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new AgentTablePrinter(columns, writer, limits);
        printer.printRows(rows, true);
        printer.finish();
        return writer.toString();
    }

    private static List<List<?>> generateRows(int count)
    {
        List<List<?>> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            rows.add(row("row" + i, i));
        }
        return rows;
    }
}
