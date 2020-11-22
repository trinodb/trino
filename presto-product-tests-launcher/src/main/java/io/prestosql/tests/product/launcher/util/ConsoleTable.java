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
package io.prestosql.tests.product.launcher.util;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.prestosql.tests.product.launcher.util.ConsoleTable.Alignment.RIGHT;
import static java.util.Objects.requireNonNull;

public class ConsoleTable
{
    private ImmutableList.Builder<TableElement> builder = ImmutableList.builder();

    public void addSeparator()
    {
        builder.add(new TableSeparator());
    }

    public void addHeader(Object... values)
    {
        builder.add(new TableSeparator(), new TableRow(RIGHT, values), new TableSeparator());
    }

    public void addRow(Object... values)
    {
        builder.add(new TableRow(RIGHT, values));
    }

    public void addRow(Alignment alignment, Object... values)
    {
        builder.add(new TableRow(alignment, values));
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
            if (element instanceof TableRow) {
                int[] columnsLength = ((TableRow) element).columnsLength();
                for (int i = 0; i < ((TableRow) element).getColumns(); i++) {
                    widths[i] = Integer.max(widths[i], columnsLength[i]);
                }
            }
        }

        return widths;
    }

    private int columns(List<TableElement> elements)
    {
        return elements.stream()
                .filter(element -> element instanceof TableRow)
                .map(element -> (TableRow) element)
                .map(TableRow::getColumns)
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
    }

    private static class TableRow
            implements TableElement
    {
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
            if (column < 0 || column >= getColumns()) {
                return "";
            }

            return values[column];
        }

        public int getColumns()
        {
            return columns;
        }

        public int columnLength(int column)
        {
            return getValue(column).toString().length();
        }

        public int[] columnsLength()
        {
            int[] lengths = new int[columns];

            for (int i = 0; i < columns; i++) {
                lengths[i] = columnLength(i);
            }
            return lengths;
        }

        @Override
        public String render(int[] columnsWidth)
        {
            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < columnsWidth.length; i++) {
                builder.append("|").append(" " + pad(getValue(i), columnsWidth[i], alignment) + " ");
                if (i == columnsWidth.length - 1) {
                    builder.append("|");
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
