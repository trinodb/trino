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
package io.trino.tests.product.launcher.util;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tests.product.launcher.util.ConsoleTable.Alignment.RIGHT;
import static java.lang.Math.max;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class ConsoleTable
{
    private ImmutableList.Builder<TableElement> builder = ImmutableList.builder();

    public void addSeparator()
    {
        builder.add(new TableSeparator());
    }

    public ConsoleTable addHeader(Object... values)
    {
        builder.add(new TableSeparator(), new TableRow(RIGHT, values), new TableSeparator());
        return this;
    }

    public ConsoleTable addRow(Object... values)
    {
        builder.add(new TableRow(RIGHT, values));
        return this;
    }

    public ConsoleTable addRow(Alignment alignment, Object... values)
    {
        builder.add(new TableRow(alignment, values));
        return this;
    }

    public String render()
    {
        List<TableElement> elements = builder.build();
        int[] columnsWidth = columnsWidth(elements);

        return elements.stream()
                .map(element -> element.render(columnsWidth))
                .collect(Collectors.joining("\n"));
    }

    private int[] columnsWidth(List<TableElement> elements)
    {
        int numberOfColumns = columns(elements);
        int[] widths = new int[numberOfColumns];

        for (TableElement element : elements) {
            int[] columnsLength = element.columnsWidth();
            for (int i = 0; i < element.columnsCount(); i++) {
                widths[i] = Integer.max(widths[i], columnsLength[i]);
            }
        }

        return widths;
    }

    private int columns(List<TableElement> elements)
    {
        return elements.stream()
                .filter(element -> element instanceof TableRow)
                .map(element -> (TableRow) element)
                .map(TableRow::columnsCount)
                .max(Integer::compareTo)
                .orElse(0);
    }

    @Override
    public String toString()
    {
        return render();
    }

    private interface TableElement
    {
        String render(int[] columnLengths);

        default int columnsCount()
        {
            return 1;
        }

        default int[] columnsWidth()
        {
            return new int[] {1};
        }
    }

    private static class TableRow
            implements TableElement
    {
        public static final Splitter LINE_SPLITTER = Splitter.on('\n')
                .omitEmptyStrings();

        private final Object[] values;
        private final int columns;
        private final Alignment alignment;

        public TableRow(Alignment alignment, Object[] values)
        {
            this.values = requireNonNull(values, "values is null");
            this.alignment = alignment;
            this.columns = this.values.length;
        }

        public Object getValue(int column)
        {
            if (column < 0 || column >= columnsCount()) {
                return "";
            }

            return values[column];
        }

        @Override
        public int columnsCount()
        {
            return columns;
        }

        public int columnWidth(int column)
        {
            return lines(column).stream()
                    .map(String::length)
                    .max(Integer::compareTo)
                    .orElse(0);
        }

        public int columnHeight(int column)
        {
            return lines(column).size();
        }

        @Override
        public int[] columnsWidth()
        {
            int[] lengths = new int[columns];

            for (int i = 0; i < columns; i++) {
                lengths[i] = columnWidth(i);
            }
            return lengths;
        }

        public int rowHeight()
        {
            int height = 0;
            for (int i = 0; i < columns; i++) {
                height = max(height, columnHeight(i));
            }
            return height;
        }

        private List<String> lines(int column)
        {
            return LINE_SPLITTER.splitToList(Objects.toString(getValue(column)));
        }

        private List<String> paddedLines(int column, int rowHeight)
        {
            List<String> lines = lines(column);
            if (lines.size() < rowHeight) {
                return ImmutableList.<String>builder()
                        .addAll(lines)
                        .addAll(nCopies(rowHeight - lines.size(), ""))
                        .build();
            }

            return lines(column);
        }

        @Override
        public String render(int[] columnsWidth)
        {
            StringBuilder builder = new StringBuilder();
            int rowHeight = rowHeight();

            List<List<String>> lines = IntStream.range(0, columns)
                    .mapToObj(column -> paddedLines(column, rowHeight))
                    .collect(toImmutableList());

            for (int row = 0; row < rowHeight; row++) {
                for (int col = 0; col < columnsWidth.length; col++) {
                    String value = "";
                    if (col < lines.size()) {
                        value = lines.get(col).get(row);
                    }

                    builder.append("|").append(" ").append(pad(value, columnsWidth[col], alignment)).append(" ");
                    if (col == columnsWidth.length - 1) {
                        builder.append("|");
                    }
                }

                if (row < rowHeight - 1) {
                    builder.append("\n");
                }
            }

            return builder.toString();
        }

        private String pad(Object obj, int length, Alignment alignment)
        {
            String value = Objects.toString(obj);
            if (value.length() < length) {
                int diff = length - value.length();

                switch (alignment) {
                    case LEFT:
                        return Strings.padEnd(value, length, ' ');

                    case RIGHT:
                        return Strings.padStart(value, length, ' ');

                    case CENTER:
                        int leftPadding = (diff / 2);
                        int rightPadding = diff - leftPadding;

                        return " ".repeat(leftPadding) + value + " ".repeat(rightPadding);
                }
            }

            return value;
        }
    }

    private static class TableSeparator
            implements TableElement
    {
        @Override
        public String render(int[] columnsWidth)
        {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < columnsWidth.length; i++) {
                builder.append("+").append("-".repeat(columnsWidth[i] + 2));
                if (i == columnsWidth.length - 1) {
                    builder.append("+");
                }
            }

            return builder.toString();
        }
    }

    public enum Alignment
    {
        LEFT,
        CENTER,
        RIGHT;
    }
}
