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
package io.trino.testing.containers.environment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Represents an expected row for query result comparisons.
 * <p>
 * Use the static factory method {@link #row(Object...)} to construct rows:
 * <pre>{@code
 * Row expected = row(1, "Alice", 100.0);
 * }</pre>
 */
public final class Row
{
    private final List<Object> values;

    private Row(List<Object> values)
    {
        this.values = requireNonNull(values, "values is null");
    }

    /**
     * Creates a new Row with the given values.
     *
     * @param values the column values in order
     * @return a new Row
     */
    public static Row row(Object... values)
    {
        return new Row(Arrays.asList(values));
    }

    /**
     * Creates a Row from a List of values.
     *
     * @param values the column values in order
     * @return a new Row
     */
    public static Row fromList(List<Object> values)
    {
        // Use ArrayList to allow null values (SQL results often contain NULLs)
        return new Row(new ArrayList<>(values));
    }

    /**
     * Returns the values in this row.
     */
    public List<Object> getValues()
    {
        return values;
    }

    /**
     * Returns the number of columns in this row.
     */
    public int size()
    {
        return values.size();
    }

    /**
     * Compares this row to another and returns a description of any type mismatches
     * where values are numerically equal but Java types differ.
     *
     * @param other the row to compare against
     * @return an Optional containing a description of type mismatches, or empty if none found
     */
    public Optional<String> findTypeMismatches(Row other)
    {
        if (other == null || values.size() != other.values.size()) {
            return Optional.empty();
        }

        List<String> mismatches = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            Object thisValue = values.get(i);
            Object otherValue = other.values.get(i);

            Optional<String> mismatch = findTypeMismatch(i, thisValue, otherValue);
            mismatch.ifPresent(mismatches::add);
        }

        if (mismatches.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(String.join("\n", mismatches));
    }

    private static Optional<String> findTypeMismatch(int columnIndex, Object expected, Object actual)
    {
        // Both must be non-null Number instances
        if (!(expected instanceof Number expectedNum) || !(actual instanceof Number actualNum)) {
            return Optional.empty();
        }

        // Types must differ
        if (expected.getClass().equals(actual.getClass())) {
            return Optional.empty();
        }

        // Check if values are numerically equal
        if (!numericValuesEqual(expectedNum, actualNum)) {
            return Optional.empty();
        }

        return Optional.of(String.format(
                "Column %d: expected %s(%s), actual %s(%s)",
                columnIndex,
                expected.getClass().getSimpleName(),
                expected,
                actual.getClass().getSimpleName(),
                actual));
    }

    private static boolean numericValuesEqual(Number a, Number b)
    {
        // For integer types, compare as long to avoid precision loss
        if (isIntegerType(a) && isIntegerType(b)) {
            return a.longValue() == b.longValue();
        }
        // For floating point or mixed types, compare as double with tolerance
        double da = a.doubleValue();
        double db = b.doubleValue();
        if (Double.isNaN(da) && Double.isNaN(db)) {
            return true;
        }
        if (Double.isInfinite(da) || Double.isInfinite(db)) {
            return da == db;
        }
        double tolerance = Math.max(Math.abs(da), Math.abs(db)) * 1e-12;
        return Math.abs(da - db) <= Math.max(tolerance, 1e-12);
    }

    private static boolean isIntegerType(Number n)
    {
        return n instanceof Byte || n instanceof Short || n instanceof Integer || n instanceof Long;
    }

    /**
     * Returns the value at the specified column index (0-indexed).
     */
    public Object getValue(int index)
    {
        return values.get(index);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Row row = (Row) o;
        if (values.size() != row.values.size()) {
            return false;
        }
        for (int i = 0; i < values.size(); i++) {
            if (!valuesEqual(values.get(i), row.values.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean valuesEqual(Object a, Object b)
    {
        if (Objects.equals(a, b)) {
            return true;
        }
        // Handle byte array comparison (SQL VARBINARY values)
        if (a instanceof byte[] first && b instanceof byte[] second) {
            return Arrays.equals(first, second);
        }
        // Handle floating point comparison with tolerance (matches Tempto behavior)
        // This includes cross-type comparisons (Float vs Double) that can occur
        // when MySQL REAL/FLOAT columns are mapped to Trino real or double types
        if ((a instanceof Float || a instanceof Double) && (b instanceof Float || b instanceof Double)) {
            double da = ((Number) a).doubleValue();
            double db = ((Number) b).doubleValue();
            // Use relative tolerance appropriate for single-precision float (~6-7 significant digits)
            // when either value is a Float, otherwise use double precision tolerance
            double relativeTolerance = (a instanceof Float || b instanceof Float) ? 1e-6 : 1e-12;
            double tolerance = Math.max(Math.abs(da), Math.abs(db)) * relativeTolerance;
            return Math.abs(da - db) <= Math.max(tolerance, relativeTolerance);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        int result = 1;
        for (Object value : values) {
            if (value instanceof byte[] array) {
                result = 31 * result + Arrays.hashCode(array);
            }
            else {
                result = 31 * result + Objects.hashCode(value);
            }
        }
        return result;
    }

    @Override
    public String toString()
    {
        return "row(" + values.stream()
                .map(Row::formatValue)
                .reduce((a, b) -> a + ", " + b)
                .orElse("") + ")";
    }

    private static String formatValue(Object value)
    {
        if (value == null) {
            return "null";
        }
        if (value instanceof String) {
            return "\"" + value + "\"";
        }
        return value.toString();
    }
}
