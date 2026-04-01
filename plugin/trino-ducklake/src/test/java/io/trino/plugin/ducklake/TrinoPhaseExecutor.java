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
package io.trino.plugin.ducklake;

import io.trino.Session;
import io.trino.plugin.ducklake.TestFileParser.QueryBlock;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * Trino-side executor for SQLLogicTest query blocks. Executes read-side SQL
 * against a Trino QueryRunner and compares results to expected output from
 * the .test file.
 * <p>
 * This executor works in interleaved mode: the test runner alternates between
 * DuckDB writes and Trino reads in the exact order specified by the .test file.
 * This ensures intermediate states (write-check-write-check) are verified correctly.
 */
public final class TrinoPhaseExecutor
{
    private final QueryRunner queryRunner;
    private final Session session;

    public TrinoPhaseExecutor(QueryRunner queryRunner, String catalogName, String schemaName)
    {
        this.queryRunner = queryRunner;
        this.session = testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema(schemaName)
                .build();
    }

    public record TestResult(String sql, Status status, String detail)
    {
        public enum Status
        {
            PASSED,
            FAILED,
            SKIPPED
        }
    }

    /**
     * Executes a single query block against Trino and returns the result.
     *
     * @param queryBlock the query to execute
     * @param envVars environment variables for SQL adaptation
     * @return the test result
     */
    public TestResult executeQuery(QueryBlock queryBlock, Map<String, String> envVars)
    {
        String originalSql = queryBlock.sql();

        if (SqlAdapter.isSkippable(originalSql)) {
            return new TestResult(originalSql, TestResult.Status.SKIPPED, "DuckDB-specific SQL");
        }

        String adaptedSql = SqlAdapter.adapt(originalSql, session.getSchema().orElse("main"), envVars);

        if (SqlAdapter.isSkippable(adaptedSql)) {
            return new TestResult(adaptedSql, TestResult.Status.SKIPPED, "Skippable after adaptation");
        }

        try {
            MaterializedResult result = queryRunner.execute(session, adaptedSql);
            List<String> actualRows = materializeRows(result);
            List<String> expectedRows = queryBlock.expectedRows();

            if (expectedRows.isEmpty() && actualRows.isEmpty()) {
                return new TestResult(adaptedSql, TestResult.Status.PASSED, "Both empty");
            }

            // Sort both sides — Trino may return rows in different order than DuckDB
            boolean match = compareResults(actualRows, expectedRows);
            if (match) {
                return new TestResult(adaptedSql, TestResult.Status.PASSED, null);
            }
            else {
                String detail = formatMismatch(actualRows, expectedRows);
                return new TestResult(adaptedSql, TestResult.Status.FAILED, detail);
            }
        }
        catch (Exception e) {
            return new TestResult(adaptedSql, TestResult.Status.FAILED, "Query error: " + e.getMessage());
        }
    }

    private static List<String> materializeRows(MaterializedResult result)
    {
        List<String> rows = new ArrayList<>();
        for (MaterializedRow row : result.getMaterializedRows()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < row.getFieldCount(); i++) {
                if (i > 0) {
                    sb.append("\t");
                }
                Object value = row.getField(i);
                sb.append(value == null ? "NULL" : formatValue(value));
            }
            rows.add(sb.toString());
        }
        return rows;
    }

    private static String formatValue(Object value)
    {
        if (value instanceof Boolean b) {
            return b ? "true" : "false";
        }
        if (value instanceof Double d) {
            if (d == Math.floor(d) && !Double.isInfinite(d)) {
                long longVal = (long) d.doubleValue();
                return Long.toString(longVal);
            }
            return Double.toString(d);
        }
        if (value instanceof Float f) {
            if (f == Math.floor(f) && !Float.isInfinite(f)) {
                long longVal = (long) f.floatValue();
                return Long.toString(longVal);
            }
            return Float.toString(f);
        }
        return value.toString();
    }

    /**
     * Compares result rows with sorting — Trino may return rows in a different order than DuckDB.
     */
    private static boolean compareResults(List<String> actual, List<String> expected)
    {
        if (actual.size() != expected.size()) {
            return false;
        }

        // Normalize before sorting so format differences don't affect sort order
        List<String> normalizedActual = actual.stream().map(TrinoPhaseExecutor::normalizeRow).sorted().toList();
        List<String> normalizedExpected = expected.stream().map(TrinoPhaseExecutor::normalizeRow).sorted().toList();

        for (int i = 0; i < normalizedActual.size(); i++) {
            String actualRow = normalizedActual.get(i);
            String expectedRow = normalizedExpected.get(i);

            if (expectedRow.startsWith("<REGEX>:")) {
                String regex = expectedRow.substring("<REGEX>:".length());
                if (!Pattern.compile(regex).matcher(actualRow).find()) {
                    return false;
                }
            }
            else if (!actualRow.equals(expectedRow)) {
                return false;
            }
        }
        return true;
    }

    // Matches DuckDB struct literals: {'key': value, 'key2': value2}
    private static final Pattern DUCKDB_STRUCT_PAIR = Pattern.compile(
            "'\\w+':\\s*");

    // Normalizes timestamp formats: "2026-03-10T00:00" → "2026-03-10 00:00:00"
    private static final Pattern TRINO_TIMESTAMP = Pattern.compile(
            "(\\d{4}-\\d{2}-\\d{2})T(\\d{2}:\\d{2})(?::(\\d{2}))?");

    private static String normalizeRow(String row)
    {
        String normalized = row.trim().replace("(empty)", "");

        // Normalize timestamp format FIRST (before whitespace normalization destroys date-time spaces)
        // Trino: 2026-03-10T00:00 → 2026-03-10 00:00:00
        normalized = TRINO_TIMESTAMP.matcher(normalized).replaceAll(mr -> {
            String seconds = mr.group(3);
            return mr.group(1) + " " + mr.group(2) + ":" + (seconds != null ? seconds : "00");
        });

        // Normalize whitespace to tabs (field separators)
        normalized = normalized.replaceAll("\t+", "\t");

        // Normalize NULL casing: DuckDB uses NULL, Trino uses null inside containers
        normalized = normalized.replace("[null", "[NULL").replace(", null", ", NULL");

        // Convert DuckDB struct format {'key': val, ...} → [val, ...]
        normalized = normalizeDuckDbStructs(normalized);

        return normalized;
    }

    /**
     * Converts DuckDB struct literals like {'i': 1, 'j': 2} to [1, 2]
     * to match Trino's ROW type toString format.
     * <p>
     * Approach: strip the 'key': prefixes from within {...}, then replace {} with [].
     */
    private static String normalizeDuckDbStructs(String value)
    {
        if (!value.contains("{'")) {
            return value;
        }
        // Strip 'key': prefixes inside struct literals
        String result = DUCKDB_STRUCT_PAIR.matcher(value).replaceAll("");
        // Replace { } with [ ] for struct delimiters
        result = result.replace('{', '[').replace('}', ']');
        return result;
    }

    private static String formatMismatch(List<String> actual, List<String> expected)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Row count: actual=").append(actual.size()).append(", expected=").append(expected.size());
        int limit = Math.min(5, Math.max(actual.size(), expected.size()));
        for (int i = 0; i < limit; i++) {
            sb.append("\n  row ").append(i).append(": ");
            if (i < expected.size()) {
                sb.append("expected=[").append(expected.get(i)).append("]");
            }
            else {
                sb.append("expected=<missing>");
            }
            sb.append(" ");
            if (i < actual.size()) {
                sb.append("actual=[").append(actual.get(i)).append("]");
            }
            else {
                sb.append("actual=<missing>");
            }
        }
        return sb.toString();
    }
}
