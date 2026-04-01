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

import io.trino.plugin.ducklake.TestFileParser.QueryBlock;
import io.trino.plugin.ducklake.TestFileParser.SkipBlock;
import io.trino.plugin.ducklake.TestFileParser.StatementBlock;
import io.trino.plugin.ducklake.TestFileParser.TestBlock;
import io.trino.plugin.ducklake.TestFileParser.TestEnvBlock;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DuckDB-side executor for SQLLogicTest blocks. Handles all write operations
 * (CREATE, INSERT, ALTER, DELETE, etc.) and runs query blocks against DuckDB
 * for baseline verification.
 * <p>
 * This executor rewrites ATTACH statements to use SQLite as the catalog backend
 * (required by the Trino connector) and disables data inlining so all data
 * is written to Parquet files immediately.
 */
public final class DuckDbPhaseExecutor
        implements AutoCloseable
{
    // Matches: ATTACH 'ducklake:path' AS name ...
    private static final Pattern ATTACH_PATTERN = Pattern.compile(
            "^ATTACH\\s+'ducklake:([^']+)'\\s+AS\\s+(\\w+)(.*)",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private final Path catalogDbPath;
    private final Path dataPath;
    private final Map<String, String> envVars;
    private Connection connection;
    private String attachedCatalogAlias;
    private int skippedCount;
    private int executedCount;
    private int failedCount;

    public DuckDbPhaseExecutor(Path baseDir)
            throws Exception
    {
        this.catalogDbPath = baseDir.resolve("catalog.db");
        this.dataPath = baseDir.resolve("data");
        this.envVars = new HashMap<>();

        Files.createDirectories(baseDir);
        Files.createDirectories(dataPath);

        // Set default environment variables
        envVars.put("__TEST_DIR__", baseDir.toAbsolutePath().toString());

        // Open DuckDB in-memory connection
        connection = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSTALL ducklake");
            stmt.execute("INSTALL sqlite");
            stmt.execute("LOAD ducklake");
            stmt.execute("LOAD sqlite");
        }
    }

    /**
     * Executes a single block against DuckDB. Called by the interleaved runner
     * which alternates between DuckDB writes and Trino reads.
     *
     * @return true if the block was executed (not skipped)
     */
    public boolean executeBlock(TestBlock block)
    {
        if (block instanceof SkipBlock) {
            skippedCount++;
            return false;
        }

        if (block instanceof TestEnvBlock testEnv) {
            String value = testEnv.value();
            value = value.replace("{UUID}", UUID.randomUUID().toString());
            value = value.replace("__TEST_DIR__", envVars.getOrDefault("__TEST_DIR__", ""));
            envVars.put(testEnv.name(), value);
            return false;
        }

        if (block instanceof StatementBlock stmtBlock) {
            executeStatement(stmtBlock);
            return true;
        }

        if (block instanceof QueryBlock queryBlock) {
            executeQuery(queryBlock);
            return true;
        }

        return false;
    }

    /**
     * Executes all blocks from a parsed .test file against DuckDB.
     * Used for the full write-phase when not doing interleaved execution.
     */
    public void executeAll(List<TestBlock> blocks)
    {
        boolean inLoop = false;

        for (TestBlock block : blocks) {
            if (block instanceof SkipBlock skipBlock) {
                if ("loop".equals(skipBlock.directive())) {
                    inLoop = true;
                }
                else if ("endloop".equals(skipBlock.directive())) {
                    inLoop = false;
                }
                skippedCount++;
                continue;
            }

            if (inLoop) {
                skippedCount++;
                continue;
            }

            executeBlock(block);
        }
    }

    private void executeStatement(StatementBlock block)
    {
        String sql = SqlAdapter.resolveEnvVars(block.sql(), envVars);

        // Rewrite ATTACH to use SQLite catalog backend with inlining disabled
        sql = rewriteAttach(sql);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            if (!block.expectOk()) {
                failedCount++;
            }
            else {
                executedCount++;
            }
        }
        catch (Exception e) {
            if (block.expectOk()) {
                System.err.println("DuckDB phase: statement failed (expected ok): " + sql);
                System.err.println("  Error: " + e.getMessage());
                failedCount++;
            }
            else {
                executedCount++;
            }
        }
    }

    private void executeQuery(QueryBlock block)
    {
        String sql = SqlAdapter.resolveEnvVars(block.sql(), envVars);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                // drain results
            }
            executedCount++;
        }
        catch (Exception e) {
            System.err.println("DuckDB phase: query failed: " + sql);
            System.err.println("  Error: " + e.getMessage());
            failedCount++;
        }
    }

    /**
     * Rewrites DuckDB ATTACH statements to use SQLite as the catalog backend
     * and disables data inlining (DATA_INLINING_ROW_LIMIT 0).
     * <p>
     * DuckDB test files use {@code ATTACH 'ducklake:path.db'} which creates a DuckDB-native catalog.
     * The Trino connector requires a SQLite catalog. Data inlining is disabled because the Trino
     * connector reads Parquet files directly and cannot access inlined data stored in the catalog.
     */
    private String rewriteAttach(String sql)
    {
        Matcher matcher = ATTACH_PATTERN.matcher(sql.trim());
        if (!matcher.matches()) {
            return sql;
        }

        String catalogAlias = matcher.group(2);
        attachedCatalogAlias = catalogAlias;

        return String.format(
                "ATTACH 'ducklake:sqlite:%s' AS %s (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0)",
                catalogDbPath.toAbsolutePath(),
                catalogAlias,
                dataPath.toAbsolutePath());
    }

    public Path getCatalogDbPath()
    {
        return catalogDbPath;
    }

    public Path getDataPath()
    {
        return dataPath;
    }

    public Map<String, String> getEnvVars()
    {
        return Map.copyOf(envVars);
    }

    public int getExecutedCount()
    {
        return executedCount;
    }

    public int getSkippedCount()
    {
        return skippedCount;
    }

    public int getFailedCount()
    {
        return failedCount;
    }

    @Override
    public void close()
            throws Exception
    {
        if (connection != null) {
            try (Statement stmt = connection.createStatement()) {
                if (attachedCatalogAlias != null) {
                    try {
                        stmt.execute("CHECKPOINT " + attachedCatalogAlias);
                    }
                    catch (Exception e) {
                        // ignore
                    }
                    try {
                        stmt.execute("DETACH " + attachedCatalogAlias);
                    }
                    catch (Exception e) {
                        // ignore
                    }
                }
            }
            catch (Exception e) {
                // ignore cleanup errors
            }
            connection.close();
            connection = null;
        }
    }
}
