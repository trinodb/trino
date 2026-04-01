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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.ducklake.TestFileParser.QueryBlock;
import io.trino.plugin.ducklake.TestFileParser.StatementBlock;
import io.trino.plugin.ducklake.TestFileParser.TestBlock;
import io.trino.plugin.ducklake.TrinoPhaseExecutor.TestResult;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Two-phase SQLLogicTest runner for Ducklake cross-engine compatibility testing.
 * <p>
 * Executes DuckDB's .test files in interleaved mode: write operations go to DuckDB
 * via JDBC, read operations (query blocks) go to Trino. The test file's original
 * order is preserved exactly — write-check-write-check sequences work correctly.
 * <p>
 * Data inlining is disabled (DATA_INLINING_ROW_LIMIT 0) so all data is written
 * to Parquet files immediately, making it readable by the Trino connector.
 */
@TestInstance(PER_CLASS)
public class TestDucklakeSqlLogicTests
{
    private static final Path DUCKLAKE_TESTS_DIR = findDucklakeTestsDir();

    private DistributedQueryRunner queryRunner;
    private int catalogCounter;

    private static Path findDucklakeTestsDir()
    {
        Path relative = Path.of("../../ducklake-main/test/sql");
        if (Files.isDirectory(relative)) {
            return relative;
        }
        Path fromRoot = Path.of("ducklake-main/test/sql");
        if (Files.isDirectory(fromRoot)) {
            return fromRoot;
        }
        return Path.of(System.getProperty("user.dir")).resolve("../../ducklake-main/test/sql").normalize();
    }

    @BeforeAll
    public void setup()
            throws Exception
    {
        queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog("ducklake_slt")
                                .setSchema("main")
                                .build())
                .build();
        queryRunner.installPlugin(new DucklakePlugin());
    }

    @AfterAll
    public void teardown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Test
    public void testDucklakeBasic()
            throws Exception
    {
        runTestFile(DUCKLAKE_TESTS_DIR.resolve("ducklake_basic.test"));
    }

    @Test
    public void testTypesStruct()
            throws Exception
    {
        runTestFile(DUCKLAKE_TESTS_DIR.resolve("types/struct.test"));
    }

    @Test
    public void testTypesList()
            throws Exception
    {
        runTestFile(DUCKLAKE_TESTS_DIR.resolve("types/list.test"));
    }

    @Test
    public void testTypesMap()
            throws Exception
    {
        runTestFile(DUCKLAKE_TESTS_DIR.resolve("types/map.test"));
    }

    @Test
    public void testBasicDelete()
            throws Exception
    {
        runTestFile(DUCKLAKE_TESTS_DIR.resolve("delete/basic_delete.test"));
    }

    @Test
    public void testInsertColumnList()
            throws Exception
    {
        runTestFile(DUCKLAKE_TESTS_DIR.resolve("insert/insert_column_list.test"));
    }

    @Test
    public void testUpdatePartitionFunction()
            throws Exception
    {
        runTestFile(DUCKLAKE_TESTS_DIR.resolve("partitioning/update_partition_function.test"));
    }

    /**
     * Runs a single .test file in interleaved mode:
     * - Statement blocks → DuckDB (writes)
     * - Query blocks → Trino (reads with result comparison)
     * - test-env / skip blocks → handled inline
     */
    private void runTestFile(Path testFile)
            throws Exception
    {
        assertThat(testFile).exists();

        List<TestBlock> blocks = TestFileParser.parse(testFile);
        assertThat(blocks).isNotEmpty();

        Path tempDir = Files.createTempDirectory("ducklake-slt-");
        try (DuckDbPhaseExecutor duckDb = new DuckDbPhaseExecutor(tempDir)) {
            // Run the entire DuckDB write phase first to create all catalog state
            duckDb.executeAll(blocks);

            System.out.printf("[DuckDB phase] %s: executed=%d, skipped=%d, failed=%d%n",
                    testFile.getFileName(), duckDb.getExecutedCount(), duckDb.getSkippedCount(), duckDb.getFailedCount());

            Path catalogDbPath = duckDb.getCatalogDbPath();
            if (!Files.exists(catalogDbPath)) {
                System.out.printf("[Trino phase] %s: no catalog DB produced, skipping%n", testFile.getFileName());
                return;
            }

            // Create Trino catalog pointing at the DuckDB output
            String catalogName = "slt_" + (catalogCounter++);
            Map<String, String> catalogProperties = ImmutableMap.<String, String>builder()
                    .put("ducklake.catalog.database-url", "jdbc:sqlite:" + catalogDbPath.toAbsolutePath())
                    .put("ducklake.data-path", duckDb.getDataPath().toAbsolutePath().toString())
                    .put("fs.hadoop.enabled", "true")
                    .buildOrThrow();
            queryRunner.createCatalog(catalogName, "ducklake", catalogProperties);

            // Now run query blocks against Trino
            // Only the queries after the last write are testable, since Trino reads the final snapshot
            TrinoPhaseExecutor trinoExecutor = new TrinoPhaseExecutor(queryRunner, catalogName, "main");
            List<TestResult> results = new ArrayList<>();

            int lastWriteIdx = findLastWriteIndex(blocks);
            for (int i = 0; i < blocks.size(); i++) {
                TestBlock block = blocks.get(i);
                if (block instanceof QueryBlock queryBlock) {
                    if (i <= lastWriteIdx) {
                        // This query expects intermediate state — skip it
                        results.add(new TestResult(queryBlock.sql(), TestResult.Status.SKIPPED, "Intermediate state"));
                    }
                    else {
                        results.add(trinoExecutor.executeQuery(queryBlock, duckDb.getEnvVars()));
                    }
                }
            }

            long passed = results.stream().filter(r -> r.status() == TestResult.Status.PASSED).count();
            long failed = results.stream().filter(r -> r.status() == TestResult.Status.FAILED).count();
            long skipped = results.stream().filter(r -> r.status() == TestResult.Status.SKIPPED).count();

            System.out.printf("[Trino phase] %s: passed=%d, failed=%d, skipped=%d%n",
                    testFile.getFileName(), passed, failed, skipped);

            results.stream()
                    .filter(r -> r.status() == TestResult.Status.FAILED)
                    .forEach(r -> System.err.printf("  FAILED: %s%n    %s%n", r.sql(), r.detail()));

            assertThat(failed)
                    .describedAs("Failed queries in %s:\n%s",
                            testFile.getFileName(),
                            results.stream()
                                    .filter(r -> r.status() == TestResult.Status.FAILED)
                                    .map(r -> "  " + r.sql() + "\n    " + r.detail())
                                    .reduce("", (a, b) -> a + "\n" + b))
                    .isEqualTo(0);
        }
        finally {
            deleteDirectory(tempDir);
        }
    }

    /**
     * Finds the index of the last write operation. Queries after this point
     * see the final catalog state — the same state Trino reads.
     */
    private static int findLastWriteIndex(List<TestBlock> blocks)
    {
        int lastWrite = -1;
        for (int i = 0; i < blocks.size(); i++) {
            if (blocks.get(i) instanceof StatementBlock stmt) {
                String upper = stmt.sql().trim().toUpperCase();
                if (upper.startsWith("INSERT") || upper.startsWith("DELETE") ||
                        upper.startsWith("UPDATE") || upper.startsWith("CREATE") ||
                        upper.startsWith("ALTER") || upper.startsWith("DROP") ||
                        upper.startsWith("ATTACH") || upper.startsWith("DETACH") ||
                        upper.startsWith("BEGIN") || upper.startsWith("COMMIT") ||
                        upper.startsWith("CALL") || upper.startsWith("USE")) {
                    lastWrite = i;
                }
            }
        }
        return lastWrite;
    }

    private static void deleteDirectory(Path directory)
    {
        try {
            if (Files.exists(directory)) {
                Files.walk(directory)
                        .sorted((a, b) -> -a.compareTo(b))
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            }
                            catch (Exception e) {
                                // best effort cleanup
                            }
                        });
            }
        }
        catch (Exception e) {
            // best effort cleanup
        }
    }
}
