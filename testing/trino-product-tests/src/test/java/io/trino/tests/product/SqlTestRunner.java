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
package io.trino.tests.product;

import com.google.common.io.Resources;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static com.google.common.io.Resources.getResource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public final class SqlTestRunner
{
    private static final Pattern QUERY_SPLITTER = Pattern.compile(";[ ]*\\R");
    private static final double FLOAT_TOLERANCE = 0.000001;

    private SqlTestRunner() {}

    public static void run(ProductTestEnvironment environment, String testCase)
            throws IOException
    {
        String resourcePrefix = "sql-tests/testcases/" + testCase;
        String query = Resources.toString(getResource(resourcePrefix + ".sql"), UTF_8);
        ResultDescriptor expected = readResultDescriptor(resourcePrefix + ".result");

        environment.executeTrinoInSession(session -> {
            List<String> statements = QUERY_SPLITTER.splitAsStream(removeDescriptor(query))
                    .map(String::trim)
                    .filter(statement -> !statement.isEmpty())
                    .toList();
            assertThat(statements).as("SQL statements for %s", resourcePrefix).isNotEmpty();

            for (String statement : statements.subList(0, statements.size() - 1)) {
                session.executeUpdate(statement);
            }
            assertResult(testCase, expected, session.executeQuery(statements.getLast()));
        });
    }

    private static String removeDescriptor(String query)
    {
        int firstNewline = query.indexOf('\n');
        if (query.startsWith("--") && firstNewline >= 0) {
            return query.substring(firstNewline + 1);
        }
        return query;
    }

    private static ResultDescriptor readResultDescriptor(String resource)
            throws IOException
    {
        List<String> lines = Resources.readLines(getResource(resource), UTF_8);
        assertThat(lines).as("result descriptor %s", resource).isNotEmpty();

        Map<String, String> properties = parseProperties(lines.getFirst());
        List<JDBCType> types = Arrays.stream(properties.getOrDefault("types", "").split("\\|"))
                .filter(type -> !type.isEmpty())
                .map(JDBCType::valueOf)
                .toList();
        List<String> rows = lines.stream()
                .skip(1)
                .filter(line -> !line.isBlank())
                .toList();
        return new ResultDescriptor(
                types,
                rows,
                properties.getOrDefault("delimiter", "|"),
                Boolean.parseBoolean(properties.getOrDefault("ignoreOrder", "false")),
                Boolean.parseBoolean(properties.getOrDefault("ignoreExcessRows", "false")),
                Boolean.parseBoolean(properties.getOrDefault("trimValues", "false")));
    }

    private static Map<String, String> parseProperties(String line)
    {
        assertThat(line).as("result descriptor header").startsWith("--");
        Map<String, String> properties = new LinkedHashMap<>();
        for (String property : line.substring(2).split(";")) {
            if (property.isBlank()) {
                continue;
            }
            String[] parts = property.trim().split(":", 2);
            assertThat(parts).as("result descriptor property %s", property).hasSize(2);
            properties.put(parts[0].trim(), parts[1].trim());
        }
        return properties;
    }

    private static void assertResult(String testCase, ResultDescriptor expected, QueryResult actual)
    {
        List<JDBCType> actualTypes = actual.getColumnTypes().stream()
                .map(JDBCType::valueOf)
                .toList();
        if (!expected.types().isEmpty()) {
            assertThat(actualTypes).as("column types for test case %s", testCase).isEqualTo(expected.types());
        }

        List<List<Object>> expectedRows = expected.rows().stream()
                .map(line -> parseRow(line, actualTypes, expected.delimiter(), expected.trimValues()))
                .toList();
        if (expected.ignoreExcessRows()) {
            assertThat(actual.rows()).as("row count for test case %s", testCase).hasSizeGreaterThanOrEqualTo(expectedRows.size());
        }
        else {
            assertThat(actual.rows()).as("row count for test case %s", testCase).hasSize(expectedRows.size());
        }

        if (expected.ignoreOrder()) {
            assertRowsInAnyOrder(testCase, actualTypes, expectedRows, actual.rows(), expected.ignoreExcessRows());
            return;
        }

        for (int row = 0; row < expectedRows.size(); row++) {
            assertRow(testCase, row, actualTypes, expectedRows.get(row), actual.rows().get(row));
        }
    }

    private static List<Object> parseRow(String line, List<JDBCType> types, String delimiter, boolean trimValues)
    {
        List<String> fields = new ArrayList<>(Arrays.asList(line.split(Pattern.quote(delimiter), -1)));
        if (line.endsWith(delimiter)) {
            fields.removeLast();
        }
        if (trimValues) {
            fields.replaceAll(String::trim);
        }
        assertThat(fields).as("result row %s", line).hasSize(types.size());

        List<Object> values = new ArrayList<>();
        for (int column = 0; column < fields.size(); column++) {
            values.add(parseValue(fields.get(column), types.get(column)));
        }
        return values;
    }

    private static Object parseValue(String value, JDBCType type)
    {
        if (value.equals("null")) {
            return null;
        }
        return switch (type) {
            case TINYINT -> Byte.valueOf(value);
            case SMALLINT -> Short.valueOf(value);
            case INTEGER -> Integer.valueOf(value);
            case BIGINT -> Long.valueOf(value);
            case REAL -> Float.valueOf(value);
            case FLOAT, DOUBLE -> Double.valueOf(value);
            case DECIMAL, NUMERIC -> new BigDecimal(value);
            case BOOLEAN, BIT -> Boolean.valueOf(value);
            case CHAR, VARCHAR, LONGVARCHAR, NCHAR, NVARCHAR, LONGNVARCHAR, DATE, TIME, TIME_WITH_TIMEZONE, TIMESTAMP, TIMESTAMP_WITH_TIMEZONE -> value;
            default -> throw new IllegalArgumentException("Unsupported result type: " + type);
        };
    }

    private static void assertRowsInAnyOrder(
            String testCase,
            List<JDBCType> types,
            List<List<Object>> expectedRows,
            List<List<Object>> actualRows,
            boolean ignoreExcessRows)
    {
        List<List<Object>> remaining = new ArrayList<>(actualRows);
        for (List<Object> expectedRow : expectedRows) {
            int matchingRow = -1;
            for (int index = 0; index < remaining.size(); index++) {
                if (rowsEqual(types, expectedRow, remaining.get(index))) {
                    matchingRow = index;
                    break;
                }
            }
            assertThat(matchingRow)
                    .as("test case %s contains expected row %s; remaining rows: %s", testCase, expectedRow, remaining)
                    .isGreaterThanOrEqualTo(0);
            remaining.remove(matchingRow);
        }
        if (!ignoreExcessRows) {
            assertThat(remaining).as("unexpected rows for test case %s", testCase).isEmpty();
        }
    }

    private static void assertRow(
            String testCase,
            int row,
            List<JDBCType> types,
            List<Object> expected,
            List<Object> actual)
    {
        assertThat(actual).as("column count for test case %s row %s", testCase, row).hasSize(expected.size());
        for (int column = 0; column < expected.size(); column++) {
            assertThat(valuesEqual(types.get(column), expected.get(column), actual.get(column)))
                    .as("test case %s row %s column %s expected <%s> but was <%s>", testCase, row, column, expected.get(column), actual.get(column))
                    .isTrue();
        }
    }

    private static boolean rowsEqual(List<JDBCType> types, List<Object> expected, List<Object> actual)
    {
        if (expected.size() != actual.size()) {
            return false;
        }
        for (int column = 0; column < expected.size(); column++) {
            if (!valuesEqual(types.get(column), expected.get(column), actual.get(column))) {
                return false;
            }
        }
        return true;
    }

    private static boolean valuesEqual(JDBCType type, Object expected, Object actual)
    {
        if (expected == null || actual == null) {
            return expected == actual;
        }
        return switch (type) {
            case REAL, FLOAT, DOUBLE -> {
                double expectedValue = ((Number) expected).doubleValue();
                double actualValue = ((Number) actual).doubleValue();
                yield Math.abs(actualValue - expectedValue) <= Math.abs(expectedValue * FLOAT_TOLERANCE);
            }
            case DECIMAL, NUMERIC -> ((BigDecimal) expected).compareTo((BigDecimal) actual) == 0;
            case TINYINT, SMALLINT, INTEGER, BIGINT -> ((Number) expected).longValue() == ((Number) actual).longValue();
            case DATE, TIME, TIME_WITH_TIMEZONE, TIMESTAMP, TIMESTAMP_WITH_TIMEZONE -> Objects.toString(actual).equals(expected);
            default -> expected.equals(actual);
        };
    }

    private record ResultDescriptor(
            List<JDBCType> types,
            List<String> rows,
            String delimiter,
            boolean ignoreOrder,
            boolean ignoreExcessRows,
            boolean trimValues) {}
}
