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

import com.google.common.hash.Hasher;
import io.trino.client.Column;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.BooleanSupplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.hash.Hashing.murmur3_128;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Prints a compact Markdown table for consumption by AI agents. Large results are cut to the
 * first and last rows, followed by a footer with the row count and an order-independent hash
 * of the column names and types and of the complete result. A result without limits is
 * printed while it is read.
 */
public class AgentTablePrinter
        implements OutputPrinter
{
    static final int HEAD_ROWS = 20;
    static final int TAIL_ROWS = 20;
    // rows read past the point where the output was cut before the query is stopped
    static final int EARLY_STOP_ROWS = 100_000;
    // results with fewer rows print without a footer
    static final int FOOTER_MIN_ROWS = 10;

    private final List<String> headerLines;
    private final AgentLimits limits;
    private final Writer writer;
    private final int headRows;
    private final int tailRows;
    private final long rowBudget;
    private final long headBudget;
    private final boolean streaming;
    private final boolean singleColumn;
    private final boolean stopEarly;
    private final BooleanSupplier queryCompleted;

    // every row while the result fits within the limits
    private final List<String> allRows = new ArrayList<>();
    private final List<String> head = new ArrayList<>();
    private final Deque<String> tail = new ArrayDeque<>();
    private final Deque<Long> tailLineBytes = new ArrayDeque<>();

    private long headBytes;
    private boolean headClosed;
    private long tailBytes;
    private long rowCount;
    private long totalBytes;
    private long hash;
    private boolean headerWritten;
    private boolean cut;
    private long rowCountWhenCut;
    private boolean complete;
    private boolean stoppedEarly;
    private boolean finished;
    // the first row of a single column, written when a second row follows, as it can turn out to be text
    private String pendingLine;
    // the value of the only row of a single column if it has line breaks, up to the byte limit, to be printed as text
    private String text;
    private int textLength;

    public AgentTablePrinter(List<Column> columns, Writer writer, AgentLimits limits)
    {
        this(columns, writer, limits, true, () -> true);
    }

    /**
     * @param stopEarly whether to stop the query when far more rows than the limits were read
     * @param queryCompleted whether the query finished without an error, asked when the output is finished
     */
    public AgentTablePrinter(List<Column> columns, Writer writer, AgentLimits limits, boolean stopEarly, BooleanSupplier queryCompleted)
    {
        requireNonNull(columns, "columns is null");
        this.writer = requireNonNull(writer, "writer is null");
        this.limits = requireNonNull(limits, "limits is null");
        this.stopEarly = stopEarly;
        this.queryCompleted = requireNonNull(queryCompleted, "queryCompleted is null");
        List<String> names = columns.stream()
                .map(column -> escape(truncate(column.getName()) + ":" + truncate(column.getType())))
                .collect(toImmutableList());
        this.headerLines = List.of(
                "|" + String.join("|", names) + "|",
                "|" + "---|".repeat(names.size()));

        // the header counts against the byte limit
        long headerBytes = headerLines.stream().mapToLong(line -> line.getBytes(UTF_8).length + 1L).sum();
        this.totalBytes = headerBytes;
        this.rowBudget = limits.maxBytes() > 0 ? max(0, limits.maxBytes() - headerBytes) : Long.MAX_VALUE;
        this.headBudget = rowBudget / 2;
        // a row limit below the rows kept at both ends is split between them, so the output stays within it
        if (limits.maxRows() > 0) {
            this.headRows = min(HEAD_ROWS, limits.maxRows() - limits.maxRows() / 2);
            this.tailRows = min(TAIL_ROWS, limits.maxRows() / 2);
        }
        else {
            this.headRows = HEAD_ROWS;
            this.tailRows = TAIL_ROWS;
        }
        // nothing is cut, so the rows do not have to be kept
        this.streaming = limits.maxRows() <= 0 && limits.maxBytes() <= 0;
        this.singleColumn = columns.size() == 1;

        Hasher schemaHasher = murmur3_128().newHasher();
        for (Column column : columns) {
            putString(schemaHasher, column.getName());
            putString(schemaHasher, column.getType());
        }
        this.hash = schemaHasher.hash().asLong();
    }

    @Override
    public void printRows(List<List<?>> rows, boolean complete)
            throws IOException
    {
        for (List<?> row : rows) {
            addRow(row);
        }
        if (complete) {
            this.complete = true;
        }
    }

    @Override
    public void finish()
            throws IOException
    {
        if (finished) {
            return;
        }
        finished = true;
        // all rows were read, but the query can still have failed or have been cancelled
        complete = complete && queryCompleted.getAsBoolean();

        if (text != null && rowCount == 1) {
            writeText();
            writer.flush();
            return;
        }

        writeHeader();
        if (pendingLine != null) {
            writer.append(pendingLine).append('\n');
        }
        if (!cut) {
            for (String row : allRows) {
                writer.append(row).append('\n');
            }
            writeFooter(rowCount, rowCount, 0);
        }
        else {
            writeCutRows();
        }
        writer.flush();
    }

    private void writeHeader()
            throws IOException
    {
        if (headerWritten) {
            return;
        }
        headerWritten = true;
        for (String line : headerLines) {
            writer.append(line).append('\n');
        }
    }

    private void addRow(List<?> row)
            throws IOException
    {
        rowCount++;
        String line = renderRow(row);
        if (streaming) {
            if (singleColumn && rowCount == 1) {
                pendingLine = line;
                return;
            }
            writeHeader();
            if (pendingLine != null) {
                writer.append(pendingLine).append('\n');
                pendingLine = null;
            }
            writer.append(line).append('\n');
            return;
        }

        long lineBytes = line.getBytes(UTF_8).length + 1L;
        totalBytes += lineBytes;

        // the first row is printed even when it alone is over the byte limit
        if (!headClosed && head.size() < headRows && (head.isEmpty() || headBytes + lineBytes <= headBudget)) {
            head.add(line);
            headBytes += lineBytes;
        }
        else {
            headClosed = true;
        }

        tail.addLast(line);
        tailLineBytes.addLast(lineBytes);
        tailBytes += lineBytes;
        // the tail gets the bytes the head leaves
        while (tail.size() > tailRows || (!tail.isEmpty() && tailBytes > rowBudget - headBytes)) {
            tail.removeFirst();
            tailBytes -= tailLineBytes.removeFirst();
        }

        if (!cut) {
            allRows.add(line);
            if (exceedsLimits()) {
                cut = true;
                rowCountWhenCut = rowCount;
                allRows.clear();
            }
        }
        else if (stopEarly && rowCount - rowCountWhenCut >= EARLY_STOP_ROWS) {
            stoppedEarly = true;
            throw new ResultLimitReachedException(format("Stopped reading after %s rows", rowCount));
        }
    }

    private boolean exceedsLimits()
    {
        return (limits.maxRows() > 0 && rowCount > limits.maxRows())
                || (limits.maxBytes() > 0 && totalBytes > limits.maxBytes());
    }

    private void writeCutRows()
            throws IOException
    {
        for (String row : head) {
            writer.append(row).append('\n');
        }

        long tailShown = 0;
        if (!stoppedEarly) {
            long omitted = rowCount - head.size() - tail.size();
            if (omitted > 0) {
                writer.append(format("…(+%s rows)", omitted)).append('\n');
            }
            for (String row : tail) {
                writer.append(row).append('\n');
                tailShown++;
            }
        }
        else {
            writer.append(format("…(+%s rows)", rowCount - head.size())).append('\n');
        }
        writeFooter(rowCount, head.size(), tailShown);
    }

    private void writeFooter(long rows, long headShown, long tailShown)
            throws IOException
    {
        if (stoppedEarly) {
            writer.append(format("\n(at least %s rows, showing the first %s, query stopped early%s)\n", rows, headShown, unlimitedHint()));
            return;
        }
        if (!complete) {
            writer.append(format("\n(incomplete: %s rows received before the query ended)\n", rows));
            return;
        }
        if (rows > 0 && rows < FOOTER_MIN_ROWS && !cut) {
            // keeps the table apart from the output of the next statement
            writer.append('\n');
            return;
        }

        String count = format("%s %s", rows, rows == 1 ? "row" : "rows");
        if (headShown + tailShown < rows) {
            count += format(", showing the first %s and last %s%s", headShown, tailShown, unlimitedHint());
        }
        writer.append(format("\n(%s, hash: %016x)\n", count, hash));
    }

    private String renderRow(List<?> row)
    {
        Hasher hasher = murmur3_128().newHasher();
        StringBuilder line = new StringBuilder("|");
        for (Object value : row) {
            if (value == null) {
                hasher.putByte((byte) 0);
                line.append("NULL|");
                continue;
            }
            String formatted = FormatUtils.formatValue(value, "", 0);
            hasher.putByte((byte) 1);
            putString(hasher, formatted);
            line.append(escape(truncate(formatted))).append('|');
            if (singleColumn && rowCount == 1 && formatted.indexOf('\n') >= 0) {
                keepText(formatted);
            }
        }
        hash += hasher.hash().asLong();
        return line.toString();
    }

    private void keepText(String value)
    {
        textLength = value.length();
        // a character takes at least a byte, so the prefix is longer than the byte limit if the value is
        int keep = limits.maxBytes() > 0 ? (int) min(textLength, limits.maxBytes() + 1L) : textLength;
        text = value.substring(0, keep);
    }

    private String unlimitedHint()
    {
        List<String> options = new ArrayList<>();
        if (limits.maxRows() > 0 && rowCount > limits.maxRows()) {
            options.add("--agent-max-rows 0");
        }
        if (limits.maxBytes() > 0 && totalBytes > limits.maxBytes()) {
            options.add("--agent-max-bytes 0");
        }
        return options.isEmpty() ? "" : ", " + String.join(" ", options) + " for all";
    }

    // A value with line breaks, such as a query plan, is printed as it is and not as a table row
    private void writeText()
            throws IOException
    {
        int end = text.length();
        long maxBytes = limits.maxBytes();
        if (maxBytes > 0) {
            end = 0;
            long bytes = 0;
            while (end < text.length()) {
                int codePoint = text.codePointAt(end);
                bytes += utf8Length(codePoint);
                if (bytes > maxBytes) {
                    break;
                }
                end += Character.charCount(codePoint);
            }
        }
        boolean textCut = end < textLength;

        writer.append(text, 0, end);
        if (textCut) {
            writer.append(format("…(+%s chars)", textLength - end));
        }
        if (textCut || !text.endsWith("\n")) {
            writer.append('\n');
        }

        if (!complete) {
            writer.append(format("\n(incomplete: %s rows received before the query ended)\n", rowCount));
        }
        else if (textCut) {
            writer.append(format("\n(1 row, showing the first %s of %s characters, --agent-max-bytes 0 for all, hash: %016x)\n", end, textLength, hash));
        }
        else {
            writer.append('\n');
        }
    }

    private static int utf8Length(int codePoint)
    {
        if (codePoint < 0x80) {
            return 1;
        }
        if (codePoint < 0x800) {
            return 2;
        }
        return codePoint < 0x10000 ? 3 : 4;
    }

    private static void putString(Hasher hasher, String text)
    {
        byte[] bytes = text.getBytes(UTF_8);
        hasher.putInt(bytes.length).putBytes(bytes);
    }

    private String truncate(String text)
    {
        int maxWidth = limits.maxCellWidth();
        if (maxWidth <= 0 || text.length() <= maxWidth) {
            return text;
        }
        int end = maxWidth;
        if (Character.isHighSurrogate(text.charAt(end - 1))) {
            end--;
        }
        return text.substring(0, end) + format("…(+%s chars)", text.length() - end);
    }

    private static String escape(String text)
    {
        return text.replace("\\", "\\\\")
                .replace("|", "\\|")
                .replace("\r", "\\r")
                .replace("\n", "\\n");
    }
}
