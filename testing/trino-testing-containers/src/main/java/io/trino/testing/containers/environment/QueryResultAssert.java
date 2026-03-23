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

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * AssertJ-style assertions for {@link QueryResult}.
 * <p>
 * Example usage:
 * <pre>{@code
 * import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
 * import static io.trino.testing.containers.environment.Row.row;
 *
 * assertThat(result).containsOnly(
 *     row(1, "Alice"),
 *     row(2, "Bob"));
 * }</pre>
 */
public class QueryResultAssert
        extends AbstractAssert<QueryResultAssert, QueryResult>
{
    private QueryResultAssert(QueryResult actual)
    {
        super(actual, QueryResultAssert.class);
    }

    /**
     * Creates a new assertion for a QueryResult.
     */
    public static QueryResultAssert assertThat(QueryResult actual)
    {
        return new QueryResultAssert(actual);
    }

    /**
     * Verifies that the result contains exactly the given rows in order.
     *
     * @param expectedRows the expected rows in exact order
     * @return this assertion for chaining
     */
    public QueryResultAssert containsExactlyInOrder(Row... expectedRows)
    {
        return containsExactlyInOrder(Arrays.asList(expectedRows));
    }

    /**
     * Verifies that the result contains exactly the given rows in order.
     *
     * @param expectedRows the expected rows in exact order
     * @return this assertion for chaining
     */
    public QueryResultAssert containsExactlyInOrder(List<Row> expectedRows)
    {
        isNotNull();

        List<Row> actualRows = actual.getRows();

        if (actualRows.size() != expectedRows.size()) {
            failWithMessage("Expected %d rows but got %d.\nExpected:\n%s\nActual:\n%s",
                    expectedRows.size(), actualRows.size(),
                    formatRows(expectedRows),
                    formatRows(actualRows));
        }

        for (int i = 0; i < expectedRows.size(); i++) {
            Row expected = expectedRows.get(i);
            Row actualRow = actualRows.get(i);
            if (!rowsMatch(expected, actualRow)) {
                int rowIndex = i;
                String typeMismatchInfo = expected.findTypeMismatches(actualRow)
                        .map(info -> "\n\nType mismatches detected (values match but types differ):\n  Row " + rowIndex + ": " + info.replace("\n", "\n  Row " + rowIndex + ": "))
                        .orElse("");
                failWithMessage("Row %d mismatch.\nExpected: %s\nActual:   %s%s",
                        i, expected, actualRow, typeMismatchInfo);
            }
        }

        return this;
    }

    /**
     * Verifies that the result contains exactly the given rows in any order.
     *
     * @param expectedRows the expected rows (order independent)
     * @return this assertion for chaining
     */
    public QueryResultAssert containsOnly(Row... expectedRows)
    {
        return containsOnly(Arrays.asList(expectedRows));
    }

    /**
     * Verifies that the result contains exactly the given rows in any order.
     *
     * @param expectedRows the expected rows (order independent)
     * @return this assertion for chaining
     */
    public QueryResultAssert containsOnly(List<Row> expectedRows)
    {
        isNotNull();

        List<Row> actualRows = actual.getRows();

        if (actualRows.size() != expectedRows.size()) {
            failWithMessage("Expected %d rows but got %d.\nExpected:\n%s\nActual:\n%s",
                    expectedRows.size(), actualRows.size(),
                    formatRows(expectedRows),
                    formatRows(actualRows));
        }

        List<Row> remainingExpected = new ArrayList<>(expectedRows);
        List<Row> unexpectedRows = new ArrayList<>();

        for (Row actualRow : actualRows) {
            int matchingExpectedIndex = firstMatchingRowIndex(remainingExpected, actualRow);
            if (matchingExpectedIndex < 0) {
                unexpectedRows.add(actualRow);
            }
            else {
                remainingExpected.remove(matchingExpectedIndex);
            }
        }

        if (!unexpectedRows.isEmpty() || !remainingExpected.isEmpty()) {
            String typeMismatchInfo = findTypeMismatchesBetweenLists(remainingExpected, unexpectedRows);
            if (typeMismatchInfo.isEmpty()) {
                failWithMessage("Row content mismatch.\nMissing rows:\n%s\nUnexpected rows:\n%s",
                        formatRows(remainingExpected),
                        formatRows(unexpectedRows));
            }
            else {
                failWithMessage("Row content mismatch.\nMissing rows:\n%s\nUnexpected rows:\n%s\n\nType mismatches detected (values match but types differ):\n%s",
                        formatRows(remainingExpected),
                        formatRows(unexpectedRows),
                        typeMismatchInfo);
            }
        }

        return this;
    }

    /**
     * Verifies that the result contains at least the given rows (may have additional rows).
     *
     * @param expectedRows the rows that must be present
     * @return this assertion for chaining
     */
    public QueryResultAssert contains(Row... expectedRows)
    {
        return contains(Arrays.asList(expectedRows));
    }

    /**
     * Verifies that the result contains at least the given rows (may have additional rows).
     *
     * @param expectedRows the rows that must be present
     * @return this assertion for chaining
     */
    public QueryResultAssert contains(List<Row> expectedRows)
    {
        isNotNull();

        List<Row> actualRows = actual.getRows();
        List<Row> missingRows = new ArrayList<>();

        for (Row expected : expectedRows) {
            if (firstMatchingRowIndex(actualRows, expected) < 0) {
                missingRows.add(expected);
            }
        }

        if (!missingRows.isEmpty()) {
            String typeMismatchInfo = findTypeMismatchesBetweenLists(missingRows, actual.getRows());
            if (typeMismatchInfo.isEmpty()) {
                failWithMessage("Missing expected rows:\n%s\nActual rows:\n%s",
                        formatRows(missingRows),
                        formatRows(actual.getRows()));
            }
            else {
                failWithMessage("Missing expected rows:\n%s\nActual rows:\n%s\n\nType mismatches detected (values match but types differ):\n%s",
                        formatRows(missingRows),
                        formatRows(actual.getRows()),
                        typeMismatchInfo);
            }
        }

        return this;
    }

    /**
     * Verifies that the result has no rows.
     *
     * @return this assertion for chaining
     */
    public QueryResultAssert hasNoRows()
    {
        isNotNull();

        if (actual.getRowsCount() != 0) {
            failWithMessage("Expected no rows but got %d:\n%s",
                    actual.getRowsCount(),
                    formatRows(actual.getRows()));
        }

        return this;
    }

    /**
     * Verifies that the result has at least one row.
     *
     * @return this assertion for chaining
     */
    public QueryResultAssert hasAnyRows()
    {
        isNotNull();

        if (actual.getRowsCount() == 0) {
            failWithMessage("Expected at least one row but got none");
        }

        return this;
    }

    /**
     * Verifies that the result has the expected number of rows.
     *
     * @param expected the expected row count
     * @return this assertion for chaining
     */
    public QueryResultAssert hasRowsCount(int expected)
    {
        isNotNull();

        if (actual.getRowsCount() != expected) {
            failWithMessage("Expected %d rows but got %d", expected, actual.getRowsCount());
        }

        return this;
    }

    /**
     * Verifies that the result has the expected column types (JDBC type codes).
     *
     * @param expectedTypes the expected column types from {@link java.sql.Types}
     * @return this assertion for chaining
     */
    public QueryResultAssert hasColumns(List<Integer> expectedTypes)
    {
        isNotNull();

        List<Integer> actualTypes = actual.getColumnTypes();
        if (!actualTypes.equals(expectedTypes)) {
            failWithMessage("Expected column types %s but got %s", expectedTypes, actualTypes);
        }

        return this;
    }

    /**
     * Alias for hasRowsCount - verifies that the result has the expected number of rows.
     *
     * @param expected the expected row count
     * @return this assertion for chaining
     */
    public QueryResultAssert hasRowCount(int expected)
    {
        return hasRowsCount(expected);
    }

    /**
     * Verifies that this result has the same rows as another result (order independent).
     *
     * @param other the other query result to compare against
     * @return this assertion for chaining
     */
    public QueryResultAssert hasSameRowsAs(QueryResult other)
    {
        isNotNull();

        return containsOnly(other.getRows());
    }

    /**
     * Returns an assertion on a specific column of the result.
     *
     * @param columnIndex 1-based column index
     * @return list of values in the column
     */
    public ListAssert<Object> column(int columnIndex)
    {
        isNotNull();

        List<Object> columnValues = actual.getRows().stream()
                .map(row -> row.getValue(columnIndex - 1))  // Convert from 1-based to 0-based
                .toList();

        return Assertions.assertThat(columnValues);
    }

    private static String formatRows(List<Row> rows)
    {
        if (rows.isEmpty()) {
            return "  (none)";
        }
        StringBuilder sb = new StringBuilder();
        int limit = Math.min(rows.size(), 20);
        for (int i = 0; i < limit; i++) {
            sb.append("  ").append(rows.get(i)).append("\n");
        }
        if (rows.size() > limit) {
            sb.append("  ... (").append(rows.size() - limit).append(" more rows)\n");
        }
        return sb.toString();
    }

    /**
     * Finds type mismatches between missing (expected) rows and unexpected (actual) rows.
     * This helps identify cases where rows appear different due to type mismatches
     * (e.g., Integer vs Long) even though values are numerically equal.
     */
    private static String findTypeMismatchesBetweenLists(List<Row> missingRows, List<Row> unexpectedRows)
    {
        List<String> allMismatches = new ArrayList<>();

        for (int expectedIdx = 0; expectedIdx < missingRows.size(); expectedIdx++) {
            Row expectedRow = missingRows.get(expectedIdx);

            for (int actualIdx = 0; actualIdx < unexpectedRows.size(); actualIdx++) {
                Row actualRow = unexpectedRows.get(actualIdx);

                expectedRow.findTypeMismatches(actualRow).ifPresent(mismatch -> {
                    // Format each column mismatch with row context
                    for (String line : mismatch.split("\n")) {
                        allMismatches.add("  " + line);
                    }
                });
            }
        }

        return String.join("\n", allMismatches);
    }

    private static int firstMatchingRowIndex(List<Row> candidates, Row target)
    {
        for (int i = 0; i < candidates.size(); i++) {
            if (rowsMatch(candidates.get(i), target)) {
                return i;
            }
        }
        return -1;
    }

    private static boolean rowsMatch(Row expected, Row actual)
    {
        if (expected.size() != actual.size()) {
            return false;
        }
        for (int column = 0; column < expected.size(); column++) {
            if (!valuesMatch(expected.getValue(column), actual.getValue(column))) {
                return false;
            }
        }
        return true;
    }

    private static boolean valuesMatch(Object expected, Object actual)
    {
        if (Objects.equals(expected, actual)) {
            return true;
        }
        if (expected instanceof byte[] expectedBytes && actual instanceof byte[] actualBytes) {
            return Arrays.equals(expectedBytes, actualBytes);
        }
        if (expected instanceof Number expectedNumber && actual instanceof Number actualNumber) {
            return numericValuesEqual(expectedNumber, actualNumber);
        }
        return false;
    }

    private static boolean numericValuesEqual(Number expected, Number actual)
    {
        if (isIntegerType(expected) && isIntegerType(actual)) {
            return expected.longValue() == actual.longValue();
        }
        if (expected instanceof BigDecimal || actual instanceof BigDecimal) {
            try {
                return new BigDecimal(expected.toString()).compareTo(new BigDecimal(actual.toString())) == 0;
            }
            catch (NumberFormatException ignored) {
                // Fall through to double-based comparison.
            }
        }
        double expectedDouble = expected.doubleValue();
        double actualDouble = actual.doubleValue();
        if (Double.isNaN(expectedDouble) && Double.isNaN(actualDouble)) {
            return true;
        }
        if (Double.isInfinite(expectedDouble) || Double.isInfinite(actualDouble)) {
            return expectedDouble == actualDouble;
        }
        double relativeTolerance = (expected instanceof Float || actual instanceof Float) ? 1e-6 : 1e-12;
        double tolerance = Math.max(Math.abs(expectedDouble), Math.abs(actualDouble)) * relativeTolerance;
        return Math.abs(expectedDouble - actualDouble) <= Math.max(tolerance, relativeTolerance);
    }

    private static boolean isIntegerType(Number number)
    {
        return number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long;
    }
}
