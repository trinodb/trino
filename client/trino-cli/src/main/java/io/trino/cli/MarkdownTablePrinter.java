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

import com.google.common.collect.ImmutableSet;
import io.trino.client.Column;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.repeat;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.ClientStandardTypes.DECIMAL;
import static io.trino.client.ClientStandardTypes.DOUBLE;
import static io.trino.client.ClientStandardTypes.INTEGER;
import static io.trino.client.ClientStandardTypes.REAL;
import static io.trino.client.ClientStandardTypes.SMALLINT;
import static io.trino.client.ClientStandardTypes.TINYINT;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static org.jline.utils.AttributedString.stripAnsi;
import static org.jline.utils.WCWidth.wcwidth;

public class MarkdownTablePrinter
        implements OutputPrinter
{
    private static final Set<String> NUMERIC_TYPES = ImmutableSet.of(TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, DECIMAL);
    private final List<String> fieldNames;
    private final List<Align> alignments;
    private final Writer writer;

    private boolean headerRendered;

    public MarkdownTablePrinter(List<Column> columns, Writer writer)
    {
        requireNonNull(columns, "columns is null");
        this.fieldNames = columns.stream()
                .map(Column::getName)
                .collect(toImmutableList());
        this.alignments = columns.stream()
                .map(Column::getTypeSignature)
                .map(signature -> NUMERIC_TYPES.contains(signature.getRawType()) ? Align.RIGHT : Align.LEFT)
                .collect(toImmutableList());
        this.writer = requireNonNull(writer, "writer is null");
    }

    private enum Align
    {
        LEFT,
        RIGHT;
    }

    @Override
    public void printRows(List<List<?>> rows, boolean complete)
            throws IOException
    {
        int columns = fieldNames.size();

        int[] columnWidth = new int[columns];
        for (int i = 0; i < columns; i++) {
            columnWidth[i] = max(1, consoleWidth(fieldNames.get(i)));
        }

        for (List<?> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                String s = formatValue(row.get(i));
                columnWidth[i] = max(columnWidth[i], consoleWidth(s));
            }
        }

        if (!headerRendered) {
            headerRendered = true;

            for (int i = 0; i < columns; i++) {
                writer.append('|');
                writer.append(align(fieldNames.get(i), columnWidth[i], alignments.get(i)));
            }
            writer.append("|\n");

            for (int i = 0; i < columns; i++) {
                writer.append("| ");
                writer.append(repeat("-", columnWidth[i]));
                writer.write(alignments.get(i) == Align.RIGHT ? ':' : ' ');
            }
            writer.append("|\n");
        }

        for (List<?> row : rows) {
            for (int column = 0; column < columns; column++) {
                writer.append('|');
                writer.append(align(formatValue(row.get(column)), columnWidth[column], alignments.get(column)));
            }
            writer.append("|\n");
        }
        writer.flush();
    }

    static String formatValue(Object o)
    {
        return FormatUtils.formatValue(o)
                .replaceAll("([\\\\`*_{}\\[\\]<>()#+!|])", "\\\\$1")
                .replace("\n", "<br>");
    }

    @Override
    public void finish()
            throws IOException
    {
        writer.flush();
    }

    private static String align(String value, int maxWidth, Align align)
    {
        int width = consoleWidth(value);
        checkState(width <= maxWidth, "Variable width %s is greater than column width %s", width, maxWidth);
        String padding = repeat(" ", (maxWidth - width) + 1);
        return align == Align.RIGHT ? (padding + value + " ") : (" " + value + padding);
    }

    static int consoleWidth(String value)
    {
        CharSequence plain = stripAnsi(value);
        int n = 0;
        for (int i = 0; i < plain.length(); i++) {
            n += max(wcwidth(plain.charAt(i)), 0);
        }
        return n;
    }
}
