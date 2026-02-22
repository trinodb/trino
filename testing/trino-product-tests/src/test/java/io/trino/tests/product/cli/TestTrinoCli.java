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
package io.trino.tests.product.cli;

import io.trino.testing.TestingNames;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the Trino CLI.
 * <p>
 * These tests run the Trino CLI inside the container using docker exec.
 * Tests use tpch.tiny.nation as the test table since TPCH is built into Trino.
 */
@ProductTest
@RequiresEnvironment(CliEnvironment.class)
@TestGroup.Cli
class TestTrinoCli
{
    // Expected output for SELECT * FROM tpch.tiny.nation (batch mode, CSV format)
    // All 25 rows from the nation table
    private static final List<String> NATION_TABLE_BATCH_LINES = List.of(
            "\"0\",\"ALGERIA\",\"0\",\" haggle. carefully final deposits detect slyly agai\"",
            "\"1\",\"ARGENTINA\",\"1\",\"al foxes promise slyly according to the regular accounts. bold requests alon\"",
            "\"2\",\"BRAZIL\",\"1\",\"y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special \"",
            "\"3\",\"CANADA\",\"1\",\"eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold\"",
            "\"4\",\"EGYPT\",\"4\",\"y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d\"",
            "\"5\",\"ETHIOPIA\",\"0\",\"ven packages wake quickly. regu\"",
            "\"6\",\"FRANCE\",\"3\",\"refully final requests. regular, ironi\"",
            "\"7\",\"GERMANY\",\"3\",\"l platelets. regular accounts x-ray: unusual, regular acco\"",
            "\"8\",\"INDIA\",\"2\",\"ss excuses cajole slyly across the packages. deposits print aroun\"",
            "\"9\",\"INDONESIA\",\"2\",\" slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull\"",
            "\"10\",\"IRAN\",\"4\",\"efully alongside of the slyly final dependencies. \"",
            "\"11\",\"IRAQ\",\"4\",\"nic deposits boost atop the quickly final requests? quickly regula\"",
            "\"12\",\"JAPAN\",\"2\",\"ously. final, express gifts cajole a\"",
            "\"13\",\"JORDAN\",\"4\",\"ic deposits are blithely about the carefully regular pa\"",
            "\"14\",\"KENYA\",\"0\",\" pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t\"",
            "\"15\",\"MOROCCO\",\"0\",\"rns. blithely bold courts among the closely regular packages use furiously bold platelets?\"",
            "\"16\",\"MOZAMBIQUE\",\"0\",\"s. ironic, unusual asymptotes wake blithely r\"",
            "\"17\",\"PERU\",\"1\",\"platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun\"",
            "\"18\",\"CHINA\",\"2\",\"c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos\"",
            "\"19\",\"ROMANIA\",\"3\",\"ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account\"",
            "\"20\",\"SAUDI ARABIA\",\"4\",\"ts. silent requests haggle. closely express packages sleep across the blithely\"",
            "\"21\",\"VIETNAM\",\"2\",\"hely enticingly express accounts. even, final \"",
            "\"22\",\"RUSSIA\",\"3\",\" requests against the platelets use never according to the quickly regular pint\"",
            "\"23\",\"UNITED KINGDOM\",\"3\",\"eans boost carefully special requests. accounts are. carefull\"",
            "\"24\",\"UNITED STATES\",\"1\",\"y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be\"");

    @Test
    void shouldDisplayVersion(CliEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli("--version");

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout().trim()).isEqualTo("Trino CLI " + readTrinoCliVersion());
    }

    @Test
    void shouldRunBatchQuery(CliEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli("--execute", "SELECT * FROM tpch.tiny.nation");

        assertThat(result.getExitCode()).isZero();
        List<String> outputLines = trimLines(result.getStdout());
        assertThat(outputLines).containsAll(NATION_TABLE_BATCH_LINES);
    }

    @Test
    void shouldRunQuery(CliEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCliWithStdin("SELECT * FROM tpch.tiny.nation;");

        assertThat(result.getExitCode()).isZero();
        List<String> outputLines = trimLines(result.getStdout());
        assertThat(outputLines).containsAll(NATION_TABLE_BATCH_LINES);
    }

    @Test
    void shouldRunBatchQueryWithStdinRedirect(CliEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCliWithStdin("SELECT * FROM tpch.tiny.nation;");

        assertThat(result.getExitCode()).isZero();
        List<String> outputLines = trimLines(result.getStdout());
        assertThat(outputLines).containsAll(NATION_TABLE_BATCH_LINES);
    }

    @Test
    void shouldRunQueryFromFile(CliEnvironment env)
            throws Exception
    {
        // Create a temporary SQL file inside the container
        ExecResult createFile = env.getTrinoContainer().execInContainer(
                "/bin/bash", "-c",
                "echo 'SELECT * FROM tpch.tiny.nation;' > /tmp/test_query.sql");
        assertThat(createFile.getExitCode()).isZero();

        ExecResult result = env.executeCliWithFile("/tmp/test_query.sql");

        assertThat(result.getExitCode()).isZero();
        List<String> outputLines = trimLines(result.getStdout());
        assertThat(outputLines).containsAll(NATION_TABLE_BATCH_LINES);
    }

    @Test
    void shouldExitOnErrorFromFile(CliEnvironment env)
            throws Exception
    {
        // Create a SQL file with an invalid query followed by a valid query
        ExecResult createFile = env.getTrinoContainer().execInContainer(
                "/bin/bash", "-c",
                "printf 'SELECT * FROM tpch.tiny.nations;\\nSELECT * FROM tpch.tiny.nation;\\n' > /tmp/test_error.sql");
        assertThat(createFile.getExitCode()).isZero();

        ExecResult result = env.executeCliWithFile("/tmp/test_error.sql");

        // Exit code is non-zero because first query fails
        assertThat(result.getExitCode()).isNotZero();
        // Second query never runs, so no nation table output
        assertThat(result.getStdout().trim()).isEmpty();
    }

    @Test
    void shouldNotExitOnErrorFromFile(CliEnvironment env)
            throws Exception
    {
        // Create a SQL file with an invalid query followed by a valid query
        ExecResult createFile = env.getTrinoContainer().execInContainer(
                "/bin/bash", "-c",
                "printf 'SELECT * FROM tpch.tiny.nations;\\nSELECT * FROM tpch.tiny.nation;\\n' > /tmp/test_ignore_error.sql");
        assertThat(createFile.getExitCode()).isZero();

        ExecResult result = env.executeCliWithFile("/tmp/test_ignore_error.sql", "--ignore-errors");

        // Exit code is still non-zero because of the error
        assertThat(result.getExitCode()).isNotZero();
        // But second query runs and produces output
        List<String> outputLines = trimLines(result.getStdout());
        assertThat(outputLines).containsAll(NATION_TABLE_BATCH_LINES);
    }

    @Test
    void shouldRunMultipleBatchQueriesWithStdinRedirect(CliEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCliWithStdin("SELECT * FROM tpch.tiny.nation;SELECT 1;SELECT 2");

        assertThat(result.getExitCode()).isZero();
        List<String> outputLines = trimLines(result.getStdout());
        assertThat(outputLines).containsAll(NATION_TABLE_BATCH_LINES);
        assertThat(outputLines).contains("\"1\"");
        assertThat(outputLines).contains("\"2\"");
    }

    @Test
    void shouldExecuteEmptyListOfStatements(CliEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli("--execute", "");

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout().trim()).isEmpty();
    }

    @Test
    void shouldPassSessionUser(CliEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli("--session-user", "other-user", "--execute", "SELECT current_user");

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("other-user");
    }

    @Test
    void shouldUseCatalogAndSchemaOptions(CliEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli("--catalog", "tpch", "--schema", "tiny", "--execute", "SELECT * FROM nation");

        assertThat(result.getExitCode()).isZero();
        List<String> outputLines = trimLines(result.getStdout());
        assertThat(outputLines).containsAll(NATION_TABLE_BATCH_LINES);
    }

    @Test
    void shouldExitOnErrorFromExecute(CliEnvironment env)
            throws Exception
    {
        String sql = "SELECT * FROM tpch.tiny.nations; SELECT * FROM tpch.tiny.nation;";
        ExecResult result = env.executeCli("--execute", sql);

        assertThat(result.getExitCode()).isNotZero();
        // First query fails (nations doesn't exist), second query never runs
        assertThat(result.getStdout().trim()).isEmpty();
    }

    @Test
    void shouldNotExitOnErrorFromExecute(CliEnvironment env)
            throws Exception
    {
        String sql = "SELECT * FROM tpch.tiny.nations; SELECT * FROM tpch.tiny.nation;";
        ExecResult result = env.executeCli("--ignore-errors", "--execute", sql);

        // Exit code is still non-zero because of the error
        assertThat(result.getExitCode()).isNotZero();
        // But second query runs and produces output
        List<String> outputLines = trimLines(result.getStdout());
        assertThat(outputLines).containsAll(NATION_TABLE_BATCH_LINES);
    }

    @Test
    void shouldHandleUseStatement(CliEnvironment env)
            throws Exception
    {
        // Test USE statement via stdin
        ExecResult result = env.executeCliWithStdin("USE tpch.tiny;\nSELECT count(*) FROM nation;");

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("25");
    }

    @Test
    void shouldHandleSession(CliEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCliWithStdin(
                "USE tpch.tiny;\n" +
                "SELECT count(*) FROM nation;\n" +
                "SHOW SESSION LIKE 'join_distribution_type';");

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("25");
        assertThat(result.getStdout()).contains("join_distribution_type");
    }

    @Test
    void shouldHandleSessionSettings(CliEnvironment env)
            throws Exception
    {
        // Test SET SESSION via stdin
        ExecResult result = env.executeCliWithStdin(
                "SET SESSION join_distribution_type = 'BROADCAST';\nSHOW SESSION LIKE 'join_distribution_type';");

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("BROADCAST");
    }

    @Test
    void shouldHandleTransaction(CliEnvironment env)
            throws Exception
    {
        String rolledBackTable = "txn_test_" + TestingNames.randomNameSuffix();
        String committedTable1 = "txn_test1_" + TestingNames.randomNameSuffix();
        String committedTable2 = "txn_test2_" + TestingNames.randomNameSuffix();
        ExecResult result = env.executeCliWithStdin(
                "USE hive.default;\n" +
                "DROP TABLE IF EXISTS " + rolledBackTable + ";\n" +
                "DROP TABLE IF EXISTS " + committedTable1 + ";\n" +
                "DROP TABLE IF EXISTS " + committedTable2 + ";\n" +
                "START TRANSACTION;\n" +
                "CREATE TABLE " + rolledBackTable + " (x bigint);\n" +
                "SELECT foo;\n" +
                "SELECT * FROM tpch.tiny.nation;\n" +
                "ROLLBACK;\n" +
                "SHOW TABLES;\n" +
                "START TRANSACTION;\n" +
                "CREATE TABLE " + committedTable1 + " (x bigint);\n" +
                "CREATE TABLE " + committedTable2 + " (x bigint);\n" +
                "COMMIT;\n" +
                "SHOW TABLES;\n" +
                "DROP TABLE IF EXISTS " + committedTable1 + ";\n" +
                "DROP TABLE IF EXISTS " + committedTable2 + ";\n",
                "--ignore-errors");

        assertThat(result.getExitCode())
                .describedAs("stdout=%s, stderr=%s", result.getStdout(), result.getStderr())
                .isNotZero();
        String combinedOutput = result.getStdout() + "\n" + result.getStderr();
        assertThat(combinedOutput).contains("Column 'foo' cannot be resolved");
        assertThat(combinedOutput).contains("Current transaction is aborted, commands ignored until end of transaction block");
        assertThat(result.getStdout()).doesNotContain(rolledBackTable);
        assertThat(result.getStdout()).contains(committedTable1, committedTable2);
    }

    @Test
    void shouldHandleTransactionRollback(CliEnvironment env)
            throws Exception
    {
        String rolledBackTable = "txn_rollback_" + TestingNames.randomNameSuffix();
        ExecResult result = env.executeCliWithStdin(
                "USE hive.default;\n" +
                "DROP TABLE IF EXISTS " + rolledBackTable + ";\n" +
                "START TRANSACTION;\n" +
                "CREATE TABLE " + rolledBackTable + " (x bigint);\n" +
                "SELECT foo;\n" +
                "ROLLBACK;\n" +
                "SELECT count(*) FROM tpch.tiny.nation;\n" +
                "SHOW TABLES;\n",
                "--ignore-errors");

        assertThat(result.getExitCode())
                .describedAs("stdout=%s, stderr=%s", result.getStdout(), result.getStderr())
                .isNotZero();
        String combinedOutput = result.getStdout() + "\n" + result.getStderr();
        assertThat(combinedOutput).contains("Column 'foo' cannot be resolved");
        assertThat(result.getStdout()).contains("\"25\"");
        assertThat(result.getStdout()).doesNotContain(rolledBackTable);
    }

    @Test
    void shouldPrintExplainAnalyzePlan(CliEnvironment env)
            throws Exception
    {
        // Use PostgreSQL catalog for row-level update/delete support.
        ExecResult dropResult = env.executeCli(
                "--execute",
                "DROP TABLE IF EXISTS postgresql.public.test_print_explain_analyze");
        assertThat(dropResult.getExitCode())
                .describedAs("DROP TABLE failed: stdout=%s, stderr=%s", dropResult.getStdout(), dropResult.getStderr())
                .isZero();

        ExecResult createResult = env.executeCli(
                "--execute",
                "CREATE TABLE postgresql.public.test_print_explain_analyze AS SELECT * FROM tpch.tiny.nation LIMIT 10");
        assertThat(createResult.getExitCode())
                .describedAs("CREATE TABLE failed: stdout=%s, stderr=%s", createResult.getStdout(), createResult.getStderr())
                .isZero();

        try {
            // EXPLAIN ANALYZE CREATE TABLE AS SELECT
            assertExplainAnalyzeResult(
                    env.executeCli(
                            "--execute",
                            "EXPLAIN ANALYZE CREATE TABLE postgresql.public.test_print_explain_analyze_ctas AS SELECT * FROM tpch.tiny.nation LIMIT 5"),
                    "CREATE TABLE");

            // EXPLAIN ANALYZE INSERT
            assertExplainAnalyzeResult(
                    env.executeCli(
                            "--execute",
                            "EXPLAIN ANALYZE INSERT INTO postgresql.public.test_print_explain_analyze VALUES(100, 'URUGUAY', 3, 'test comment')"),
                    "INSERT");

            // EXPLAIN ANALYZE UPDATE
            assertExplainAnalyzeResult(
                    env.executeCli(
                            "--execute",
                            "EXPLAIN ANALYZE UPDATE postgresql.public.test_print_explain_analyze SET comment = 'testValue 5' WHERE nationkey = 100"),
                    "UPDATE");

            // EXPLAIN ANALYZE DELETE
            assertExplainAnalyzeResult(
                    env.executeCli(
                            "--execute",
                            "EXPLAIN ANALYZE DELETE FROM postgresql.public.test_print_explain_analyze WHERE nationkey = 100"),
                    "DELETE");
        }
        finally {
            // Cleanup
            env.executeCli("--execute", "DROP TABLE IF EXISTS postgresql.public.test_print_explain_analyze");
            env.executeCli("--execute", "DROP TABLE IF EXISTS postgresql.public.test_print_explain_analyze_ctas");
        }
    }

    @Test
    void shouldShowCatalogs(CliEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli("--execute", "SHOW CATALOGS");

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("tpch");
        assertThat(result.getStdout()).contains("memory");
        assertThat(result.getStdout()).contains("system");
    }

    @Test
    void shouldShowSchemas(CliEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli("--execute", "SHOW SCHEMAS FROM tpch");

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("tiny");
        assertThat(result.getStdout()).contains("sf1");
    }

    @Test
    void shouldShowTables(CliEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli("--execute", "SHOW TABLES FROM tpch.tiny");

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("nation");
        assertThat(result.getStdout()).contains("region");
        assertThat(result.getStdout()).contains("customer");
    }

    @Test
    void shouldDescribeTable(CliEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli("--execute", "DESCRIBE tpch.tiny.nation");

        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains("nationkey");
        assertThat(result.getStdout()).contains("name");
        assertThat(result.getStdout()).contains("regionkey");
        assertThat(result.getStdout()).contains("comment");
    }

    @Test
    void shouldSelectWithOutputFormat(CliEnvironment env)
            throws Exception
    {
        ExecResult result = env.executeCli("--output-format", "JSON", "--execute", "SELECT nationkey, name FROM tpch.tiny.nation WHERE nationkey = 0");

        assertThat(result.getExitCode()).isZero();
        // JSON format output
        assertThat(result.getStdout()).contains("\"nationkey\"");
        assertThat(result.getStdout()).contains("\"ALGERIA\"");
    }

    @Test
    void shouldHandleConfigEnvVariable(CliEnvironment env)
            throws Exception
    {
        // Create a config file and set TRINO_CONFIG env var
        // Config file format uses property names without dashes: catalog=tpch
        ExecResult createConfig = env.getTrinoContainer().execInContainer(
                "/bin/bash", "-c",
                "printf 'catalog=tpch\\nschema=tiny\\n' > /tmp/trino_config && chmod 644 /tmp/trino_config && cat /tmp/trino_config");
        assertThat(createConfig.getExitCode())
                .describedAs("Create config failed: stdout=%s, stderr=%s", createConfig.getStdout(), createConfig.getStderr())
                .isZero();

        ExecResult result = env.executeCliWithEnv(
                Map.of("TRINO_CONFIG", "/tmp/trino_config"),
                "--execute", "SELECT * FROM nation LIMIT 1");

        assertThat(result.getExitCode())
                .describedAs("stdout=%s, stderr=%s", result.getStdout(), result.getStderr())
                .isZero();
        // Should work without specifying catalog.schema because config file sets defaults
        assertThat(result.getStdout()).contains("ALGERIA");
    }

    @Test
    void shouldUseCatalogAndSchemaOptionsFromConfigFile(CliEnvironment env)
            throws Exception
    {
        shouldHandleConfigEnvVariable(env);
    }

    @Test
    void shouldPreferCommandLineArgumentOverConfigDefault(CliEnvironment env)
            throws Exception
    {
        // Create a config file with memory catalog (which doesn't have the nation table)
        ExecResult createConfig = env.getTrinoContainer().execInContainer(
                "/bin/bash", "-c",
                "printf 'catalog=memory\\nschema=default\\n' > /tmp/trino_config_override_test && chmod 644 /tmp/trino_config_override_test");
        assertThat(createConfig.getExitCode())
                .describedAs("Create config failed: stdout=%s, stderr=%s", createConfig.getStdout(), createConfig.getStderr())
                .isZero();

        // CLI args (--catalog tpch --schema tiny) should override config file (catalog=memory, schema=default)
        ExecResult result = env.executeCliWithEnv(
                Map.of("TRINO_CONFIG", "/tmp/trino_config_override_test"),
                "--catalog", "tpch", "--schema", "tiny",
                "--execute", "SELECT count(*) FROM nation");

        assertThat(result.getExitCode())
                .describedAs("stdout=%s, stderr=%s", result.getStdout(), result.getStderr())
                .isZero();
        // Query should succeed (using tpch.tiny from CLI args, not memory.default from config)
        assertThat(result.getStdout()).contains("25");
    }

    @Test
    void shouldExitWithErrorOnUnknownPropertiesInConfigFile(CliEnvironment env)
            throws Exception
    {
        // Create a config file with an unknown property
        ExecResult createConfig = env.getTrinoContainer().execInContainer(
                "/bin/bash", "-c",
                "printf 'catalog=tpch\\nunknown_property=value\\n' > /tmp/trino_config_unknown && chmod 644 /tmp/trino_config_unknown");
        assertThat(createConfig.getExitCode())
                .describedAs("Create config failed: stdout=%s, stderr=%s", createConfig.getStdout(), createConfig.getStderr())
                .isZero();

        ExecResult result = env.executeCliWithEnv(
                Map.of("TRINO_CONFIG", "/tmp/trino_config_unknown"),
                "--execute", "SELECT 1");

        // CLI should exit with non-zero code due to unknown property
        assertThat(result.getExitCode())
                .describedAs("stdout=%s, stderr=%s", result.getStdout(), result.getStderr())
                .isNotZero();
        // Error message should mention the unknown property
        assertThat(result.getStderr()).containsIgnoringCase("unknown_property");
    }

    private static List<String> trimLines(String output)
    {
        return Arrays.stream(output.split("\n"))
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .collect(toList());
    }

    private static void assertExplainAnalyzeResult(ExecResult result, String operation)
    {
        assertThat(result.getExitCode())
                .describedAs("EXPLAIN ANALYZE %s failed: stdout=[%s], stderr=[%s]", operation, result.getStdout(), result.getStderr())
                .isZero();

        String combinedOutput = result.getStdout() + result.getStderr();
        assertThat(combinedOutput)
                .describedAs("EXPLAIN ANALYZE %s output should contain operation and plan details", operation)
                .contains(operation)
                .containsAnyOf("Query Plan", "Fragment");
    }

    private static String readTrinoCliVersion()
    {
        try (var input = TestTrinoCli.class.getClassLoader().getResourceAsStream("trino-cli-version.txt")) {
            if (input == null) {
                throw new IllegalStateException("Resource not found: trino-cli-version.txt");
            }
            String version = new String(input.readAllBytes(), UTF_8).trim();
            if (version.isEmpty()) {
                throw new IllegalStateException("trino-cli-version.txt is empty");
            }
            return version;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
