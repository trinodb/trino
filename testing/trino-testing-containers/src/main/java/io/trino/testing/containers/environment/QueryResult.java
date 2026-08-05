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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Represents the result of a SQL query for validation in product tests.
 * <p>
 * This is a lightweight alternative to Tempto's QueryResult that works with
 * standard JDBC ResultSets. Use the static factory {@link #forResultSet(ResultSet)}
 * to create instances.
 * <p>
 * Example usage:
 * <pre>{@code
 * QueryResult result = env.executeTrino("SELECT * FROM nation");
 * assertThat(result).containsOnly(TpchTableResults.NATION_ROWS);
 * }</pre>
 */
public final class QueryResult
{
    private final List<List<Object>> values;
    private final List<String> columnNames;
    private final List<Integer> columnTypes;

    private QueryResult(List<List<Object>> values, List<String> columnNames, List<Integer> columnTypes)
    {
        this.values = requireNonNull(values, "values is null");
        this.columnNames = requireNonNull(columnNames, "columnNames is null");
        this.columnTypes = requireNonNull(columnTypes, "columnTypes is null");
    }

    /**
     * Creates a QueryResult from a JDBC ResultSet.
     * <p>
     * This method consumes the entire ResultSet, so it should not be called
     * on very large result sets.
     *
     * @param rs the ResultSet to read from (will be fully consumed)
     * @return a new QueryResult containing all rows
     * @throws SQLException if reading the ResultSet fails
     */
    public static QueryResult forResultSet(ResultSet rs)
            throws SQLException
    {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        List<String> columnNames = new ArrayList<>();
        List<Integer> columnTypes = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            columnNames.add(meta.getColumnName(i));
            columnTypes.add(meta.getColumnType(i));
        }

        List<List<Object>> values = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(rs.getObject(i));
            }
            values.add(row);
        }
        return new QueryResult(values, columnNames, columnTypes);
    }

    /**
     * Creates a QueryResult from pre-parsed row values.
     * <p>
     * This is useful for environments where query execution does not use JDBC
     * result sets directly (for example CLI-based execution in containers).
     *
     * @param values row values; each inner list is a single row
     * @return a new QueryResult
     */
    public static QueryResult forRows(List<List<Object>> values)
    {
        requireNonNull(values, "values is null");
        int columnCount = values.isEmpty() ? 0 : values.getFirst().size();

        List<String> columnNames = new ArrayList<>(columnCount);
        List<Integer> columnTypes = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            columnNames.add("col" + i);
            columnTypes.add(Types.JAVA_OBJECT);
        }

        return new QueryResult(values, columnNames, columnTypes);
    }

    /**
     * Returns all result rows.
     */
    public List<List<Object>> rows()
    {
        return values;
    }

    /**
     * Returns a list of Row objects for assertion purposes.
     */
    public List<Row> getRows()
    {
        return values.stream()
                .map(Row::fromList)
                .toList();
    }

    /**
     * Returns values from a single column.
     *
     * @param index the 1-indexed column number (to match JDBC conventions)
     * @return list of values from that column
     */
    public List<Object> column(int index)
    {
        if (index < 1 || index > getColumnCount()) {
            throw new IndexOutOfBoundsException("Column index " + index + " out of range [1, " + getColumnCount() + "]");
        }
        return values.stream()
                .map(row -> row.get(index - 1))
                .toList();
    }

    /**
     * Returns the column names from the result metadata.
     */
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    /**
     * Returns the JDBC column types from the result metadata.
     * The values are constants from {@link java.sql.Types}.
     */
    public List<Integer> getColumnTypes()
    {
        return columnTypes;
    }

    /**
     * Returns the number of columns in the result.
     */
    public int getColumnCount()
    {
        return columnNames.size();
    }

    /**
     * Returns the single value from a 1x1 result.
     *
     * @return the only value
     * @throws IllegalStateException if the result is not exactly 1 row with 1 column
     */
    public Object getOnlyValue()
    {
        if (values.size() != 1) {
            throw new IllegalStateException("Expected exactly 1 row, but got " + values.size());
        }
        List<Object> row = values.get(0);
        if (row.size() != 1) {
            throw new IllegalStateException("Expected exactly 1 column, but got " + row.size());
        }
        return row.get(0);
    }

    /**
     * Returns the number of rows in the result.
     */
    public int getRowsCount()
    {
        return values.size();
    }

    /**
     * Returns a new QueryResult with only the specified columns.
     *
     * @param columns the 1-indexed column numbers to include
     * @return a new QueryResult with projected columns
     */
    public QueryResult project(int... columns)
    {
        List<String> projectedNames = new ArrayList<>();
        List<Integer> projectedTypes = new ArrayList<>();
        for (int col : columns) {
            if (col < 1 || col > getColumnCount()) {
                throw new IndexOutOfBoundsException("Column index " + col + " out of range [1, " + getColumnCount() + "]");
            }
            projectedNames.add(columnNames.get(col - 1));
            projectedTypes.add(columnTypes.get(col - 1));
        }

        List<List<Object>> projectedValues = new ArrayList<>();
        for (List<Object> row : values) {
            List<Object> projectedRow = new ArrayList<>();
            for (int col : columns) {
                projectedRow.add(row.get(col - 1));
            }
            projectedValues.add(projectedRow);
        }
        return new QueryResult(projectedValues, projectedNames, projectedTypes);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("QueryResult[columns=").append(columnNames).append(", rows=").append(values.size()).append("]\n");
        for (int i = 0; i < Math.min(values.size(), 10); i++) {
            sb.append("  ").append(values.get(i)).append("\n");
        }
        if (values.size() > 10) {
            sb.append("  ... (").append(values.size() - 10).append(" more rows)\n");
        }
        return sb.toString();
    }
}
