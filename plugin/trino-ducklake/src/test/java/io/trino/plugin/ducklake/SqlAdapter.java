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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal DuckDB→Trino SQL adapter for the two-phase test runner.
 * Rewrites DuckDB-specific SQL syntax to Trino-compatible equivalents,
 * and identifies statements that should be skipped when running against Trino.
 */
public final class SqlAdapter
{
    // DuckDB's "FROM table" shorthand (no SELECT keyword)
    private static final Pattern FROM_WITHOUT_SELECT = Pattern.compile(
            "^FROM\\s+", Pattern.CASE_INSENSITIVE);

    // EXCLUDE clause: SELECT * EXCLUDE (col1, col2) FROM ...
    private static final Pattern EXCLUDE_CLAUSE = Pattern.compile(
            "\\s+EXCLUDE\\s*\\([^)]+\\)", Pattern.CASE_INSENSITIVE);

    // DuckDB table functions
    private static final Pattern TABLE_FUNCTION = Pattern.compile(
            "\\b(test_all_types|range|glob)\\s*\\(", Pattern.CASE_INSENSITIVE);

    // DuckDB-specific functions in SELECT
    private static final Pattern STATS_FUNCTION = Pattern.compile(
            "\\bstats\\s*\\(", Pattern.CASE_INSENSITIVE);

    // DuckDB struct field access in WHERE: col.field = value (Trino uses different syntax)
    private static final Pattern STRUCT_FIELD_IN_WHERE = Pattern.compile(
            "WHERE\\s+\\w+\\.\\w+\\s*[=<>!]", Pattern.CASE_INSENSITIVE);

    // Environment variable references: ${VAR}
    private static final Pattern ENV_VAR = Pattern.compile("\\$\\{(\\w+)}");

    // DuckDB catalog reference: ducklake.tablename or ducklake.schema.tablename
    private static final Pattern DUCKLAKE_CATALOG_REF = Pattern.compile(
            "\\bducklake\\.(\\w+(?:\\.\\w+)?)");

    // Connection label patterns: con1, con2, etc.
    private static final Pattern CONNECTION_LABEL = Pattern.compile(
            "\\bcon[0-9]+$");

    private SqlAdapter() {}

    /**
     * Returns true if this SQL statement should be skipped when executing against Trino.
     * These are DuckDB-specific statements that have no Trino equivalent.
     */
    public static boolean isSkippable(String sql)
    {
        String upper = sql.trim().toUpperCase();
        // DDL and transaction control handled by DuckDB phase only
        if (upper.startsWith("ATTACH") || upper.startsWith("DETACH") ||
                upper.startsWith("USE ") || upper.startsWith("BEGIN") ||
                upper.startsWith("COMMIT") || upper.startsWith("ROLLBACK")) {
            return true;
        }
        // DuckDB-specific commands
        if (upper.startsWith("CALL ") || upper.startsWith("CHECKPOINT") ||
                upper.startsWith("SET ") || upper.startsWith("PRAGMA")) {
            return true;
        }
        // Write operations — Trino is read-only
        if (upper.startsWith("CREATE ") || upper.startsWith("INSERT ") ||
                upper.startsWith("UPDATE ") || upper.startsWith("DELETE ") ||
                upper.startsWith("ALTER ") || upper.startsWith("DROP ")) {
            return true;
        }
        // EXPLAIN ANALYZE output format differs
        if (upper.startsWith("EXPLAIN ANALYZE")) {
            return true;
        }
        // DuckDB table functions
        if (TABLE_FUNCTION.matcher(sql).find()) {
            return true;
        }
        // stats() function is DuckDB-specific
        if (STATS_FUNCTION.matcher(sql).find()) {
            return true;
        }
        // SHOW ALL TABLES is DuckDB-specific
        if (upper.startsWith("SHOW ALL TABLES")) {
            return true;
        }
        // Struct field access in WHERE (e.g., s.i=1) — Trino connector may not resolve this
        if (STRUCT_FIELD_IN_WHERE.matcher(sql).find()) {
            return true;
        }
        // DuckDB time-travel syntax: AT (VERSION => N)
        if (upper.contains("AT (VERSION")) {
            return true;
        }
        return false;
    }

    /**
     * Adapts DuckDB SQL syntax to Trino-compatible SQL.
     *
     * @param sql the DuckDB SQL statement
     * @param schemaName the Trino schema name to use in qualified references
     * @param envVars environment variable map for ${VAR} resolution
     * @return adapted SQL suitable for Trino execution
     */
    public static String adapt(String sql, String schemaName, Map<String, String> envVars)
    {
        String adapted = sql.trim();

        // Strip trailing semicolons (DuckDB test files include them, Trino rejects them)
        while (adapted.endsWith(";")) {
            adapted = adapted.substring(0, adapted.length() - 1).trim();
        }

        // Resolve environment variables
        adapted = resolveEnvVars(adapted, envVars);

        // FROM xyz → SELECT * FROM xyz
        if (FROM_WITHOUT_SELECT.matcher(adapted).find()) {
            adapted = "SELECT * " + adapted;
        }

        // Remove EXCLUDE clause (simplification: just drop it and use SELECT *)
        adapted = EXCLUDE_CLAUSE.matcher(adapted).replaceAll("");

        // Replace ducklake.tablename → schemaName.tablename
        adapted = rewriteCatalogRefs(adapted, schemaName);

        return adapted;
    }

    /**
     * Resolves ${VAR} references and __TEST_DIR__ / {UUID} placeholders.
     */
    public static String resolveEnvVars(String sql, Map<String, String> envVars)
    {
        String result = sql;
        Matcher matcher = ENV_VAR.matcher(result);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String varName = matcher.group(1);
            String replacement = envVars.getOrDefault(varName, matcher.group(0));
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        result = sb.toString();

        // Also resolve bare __TEST_DIR__ (without ${})
        if (envVars.containsKey("__TEST_DIR__")) {
            result = result.replace("__TEST_DIR__", envVars.get("__TEST_DIR__"));
        }
        return result;
    }

    private static String rewriteCatalogRefs(String sql, String schemaName)
    {
        // Replace "ducklake.tablename" with just "tablename" since Trino session is configured
        // with the right catalog and schema. For "ducklake.schema.table" keep "schema.table".
        Matcher matcher = DUCKLAKE_CATALOG_REF.matcher(sql);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String ref = matcher.group(1);
            // If it's "ducklake.schema.table", keep "schema.table"
            // If it's "ducklake.table", just use "table"
            matcher.appendReplacement(sb, Matcher.quoteReplacement(ref));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}
