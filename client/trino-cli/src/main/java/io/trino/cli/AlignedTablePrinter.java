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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.client.Column;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.repeat;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.cli.FormatUtils.formatValue;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.ClientStandardTypes.DECIMAL;
import static io.trino.client.ClientStandardTypes.DOUBLE;
import static io.trino.client.ClientStandardTypes.INTEGER;
import static io.trino.client.ClientStandardTypes.REAL;
import static io.trino.client.ClientStandardTypes.SMALLINT;
import static io.trino.client.ClientStandardTypes.TINYINT;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.jline.utils.AttributedString.stripAnsi;
import static org.jline.utils.WCWidth.wcwidth;

public class AlignedTablePrinter
        implements OutputPrinter
{
    private static final Set<String> NUMERIC_TYPES = ImmutableSet.of(TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, DECIMAL);

    private static final Splitter LINE_SPLITTER = Splitter.on('\n');

    private final List<String> fieldNames;
    private final List<Boolean> numericFields;
    private final Writer writer;

    private boolean headerRendered;
    private long rowCount;

    public AlignedTablePrinter(List<Column> columns, Writer writer)
    {
        requireNonNull(columns, "columns is null");
        this.fieldNames = columns.stream()
                .map(Column::getName)
                .collect(toImmutableList());
        this.numericFields = columns.stream()
                .map(Column::getTypeSignature)
                .map(signature -> NUMERIC_TYPES.contains(signature.getRawType()))
                .collect(toImmutableList());
        this.writer = requireNonNull(writer, "writer is null");
    }

    @Override
    public void finish()
            throws IOException
    {
        printRows(ImmutableList.of(), true);
        writer.append(format("(%s row%s)%n", rowCount, (rowCount != 1) ? "s" : ""));
        writer.flush();
    }

    @Override
    public void printRows(List<List<?>> rows, boolean complete)
            throws IOException
    {
        rowCount += rows.size();
        int columns = fieldNames.size();

        int[] columnWidth = new int[columns];
        for (int i = 0; i < columns; i++) {
            columnWidth[i] = max(1, consoleWidth(fieldNames.get(i)));
        }

        for (List<?> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                String value = formatValue(row.get(i));
                columnWidth[i] = max(columnWidth[i], maxLineLength(value));
            }
        }

        if (!headerRendered) {
            headerRendered = true;

            for (int i = 0; i < columns; i++) {
                if (i > 0) {
                    writer.append('|');
                }
                writer.append(center(fieldNames.get(i), columnWidth[i], 1));
            }
            writer.append('\n');

            for (int i = 0; i < columns; i++) {
                if (i > 0) {
                    writer.append('+');
                }
                writer.append(repeat("-", columnWidth[i] + 2));
            }
            writer.append('\n');
        }

        for (List<?> row : rows) {
            List<List<String>> columnLines = new ArrayList<>(columns);
            int maxLines = 1;
            for (int i = 0; i < columns; i++) {
                String value = formatValue(row.get(i));
                ImmutableList<String> lines = ImmutableList.copyOf(LINE_SPLITTER.split(value));
                columnLines.add(lines);
                maxLines = max(maxLines, lines.size());
            }

            for (int line = 0; line < maxLines; line++) {
                for (int column = 0; column < columns; column++) {
                    if (column > 0) {
                        writer.append('|');
                    }
                    List<String> lines = columnLines.get(column);
                    String value = (line < lines.size()) ? lines.get(line) : "";
                    boolean numeric = numericFields.get(column);
                    String out = align(value, columnWidth[column], 1, numeric);
                    if ((!complete || (rowCount > 1)) && ((line + 1) < lines.size())) {
                        out = out.substring(0, out.length() - 1) + "+";
                    }
                    writer.append(out);
                }
                writer.append('\n');
            }
        }

        writer.flush();
    }

    private static String center(String value, int maxWidth, int padding)
    {
        int width = consoleWidth(value);
        checkState(width <= maxWidth, format("Variable width %d is greater than column width %d", width, maxWidth));
        int left = (maxWidth - width) / 2;
        int right = maxWidth - (left + width);
        return repeat(" ", left + padding) + value + repeat(" ", right + padding);
    }

    private static String align(String value, int maxWidth, int padding, boolean right)
    {
        int width = consoleWidth(value);
        checkState(width <= maxWidth, format("Variable width %d is greater than column width %d", width, maxWidth));
        String large = repeat(" ", maxWidth - width + padding);
        String small = repeat(" ", padding);
        return right ? (large + value + small) : (small + value + large);
    }

    static int maxLineLength(String value)
    {
        int result = 0;
        for (String line : LINE_SPLITTER.split(value)) {
            result = max(result, consoleWidth(line));
        }
        return result;
    }

    static int consoleWidth(String value)
    {
        CharSequence plain = stripAnsi(value);
        int result = 0;
        for (int i = 0; i < plain.length(); i++) {
            result += max(wcwidth(plain.charAt(i)), 0);
        }
        return result;
    }
}
