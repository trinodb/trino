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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD) // Filesystem span assertions inspect the last query.
public class TestHiveTableLocalStaging
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner runner = HiveQueryRunner.builder()
                .setWorkerCount(2)
                .addHiveProperty("hive.table-local-staging-directory-enabled", "true")
                .addHiveProperty("hive.non-managed-table-writes-enabled", "true")
                .addHiveProperty("hive.allow-register-partition-procedure", "true")
                .build();
        runner.createCatalog("hive_legacy", "hive", ImmutableMap.of());
        runner.createCatalog("hive_master_disabled", "hive", ImmutableMap.of(
                "hive.table-local-staging-directory-enabled", "true",
                "hive.temporary-staging-directory-enabled", "false"));
        runner.execute("CREATE SCHEMA hive_legacy.tpch");
        runner.execute("CREATE SCHEMA hive_master_disabled.tpch");
        return runner;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testManagedCtasAndInsert(boolean partitioned)
    {
        String table = "test_local_ctas_" + randomNameSuffix();
        String properties = partitioned ? " WITH (partitioned_by = ARRAY['p'])" : "";
        try {
            assertUpdate("CREATE TABLE " + table + properties + " AS SELECT * FROM (VALUES (1, 1), (2, 2)) t(a, p)", 2);
            assertQuery("SELECT * FROM " + table, "VALUES (1, 1), (2, 2)");
            // Exercise both append to an existing partition and creation of a new partition.
            assertUpdate("INSERT INTO " + table + " VALUES (3, 1), (4, 3)", 2);
            assertQuery("SELECT * FROM " + table, "VALUES (1, 1), (2, 2), (3, 1), (4, 3)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + table);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEmptyManagedCtas(boolean partitioned)
    {
        String table = "test_local_empty_ctas_" + randomNameSuffix();
        String properties = partitioned ? " WITH (partitioned_by = ARRAY['p'])" : "";
        try {
            assertUpdate("CREATE TABLE " + table + properties + " AS SELECT 1 a, 1 p WITH NO DATA", 0);
            assertQuery("SELECT count(*) FROM " + table, "SELECT CAST(0 AS BIGINT)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    void testDistributedCtasAndInsert()
    {
        String table = "test_local_distributed_" + randomNameSuffix();
        try {
            assertThat(getDistributedQueryRunner().getNodeCount()).isEqualTo(3);
            assertUpdate("CREATE TABLE " + table + " WITH (partitioned_by = ARRAY['p']) AS SELECT orderkey a, orderkey % 4 p FROM tpch.tiny.orders", 15000);
            assertQuery("SELECT count(*), sum(a) FROM " + table, "SELECT count(*), sum(orderkey) FROM orders");
            assertUpdate("INSERT INTO " + table + " SELECT orderkey, orderkey % 5 FROM tpch.tiny.orders", 15000);
            assertQuery("SELECT count(*) FROM " + table, "SELECT 30000");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + table);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOverwriteRejectedPreservesData(boolean partitioned)
    {
        String table = "test_local_overwrite_" + randomNameSuffix();
        String properties = partitioned ? " WITH (partitioned_by = ARRAY['p'])" : "";
        Session overwrite = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "insert_existing_partitions_behavior", "OVERWRITE")
                .build();
        try {
            assertUpdate("CREATE TABLE " + table + properties + " AS SELECT 1 a, 1 p", 1);
            assertQueryFails(overwrite, "INSERT INTO " + table + " VALUES (2, 1)", ".*Overwrite is not supported with table-local staging.*");
            assertQuery("SELECT * FROM " + table, "VALUES (1, 1)");
            if (partitioned) {
                assertUpdate(overwrite, "INSERT INTO " + table + " VALUES (3, 2)", 1);
                assertQuery("SELECT * FROM " + table, "VALUES (1, 1), (3, 2)");
            }
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    void testCustomPartitionLocation()
            throws IOException
    {
        String table = "test_local_custom_" + randomNameSuffix();
        String custom = "custom_partition_" + randomNameSuffix();
        Path customPath = getDistributedQueryRunner().getCoordinator().getBaseDataDir().resolve("hive_data").resolve(custom);
        Files.createDirectories(customPath.resolve(".hive-staging/other-operation"));
        Path sentinel = customPath.resolve(".hive-staging/other-operation/data");
        Files.writeString(sentinel, "unrelated staging");
        try {
            assertUpdate("CREATE TABLE " + table + " (a integer, p integer) WITH (external_location = 'local:///" + table + "', partitioned_by = ARRAY['p'])");
            assertUpdate("CALL system.register_partition('tpch', '" + table + "', ARRAY['p'], ARRAY['1'], 'local:///" + custom + "')");
            getDistributedQueryRunner().executeWithPlan(getSession(), "INSERT INTO " + table + " VALUES (1, 1)");
            assertThat(renameSources()).isNotEmpty().allMatch(source -> source.startsWith("local:///" + custom + "/.hive-staging/"));
            assertUpdate("INSERT INTO " + table + " VALUES (2, 1), (3, 2)", 2);
            assertQuery("SELECT * FROM " + table, "VALUES (1, 1), (2, 1), (3, 2)");
            assertQuery("SELECT count(*) FROM " + table + " WHERE p = 1 AND \"$path\" LIKE 'local:///" + custom + "/%'", "SELECT 2");
            assertThat(Files.readString(sentinel)).isEqualTo("unrelated staging");
            try (var operations = Files.list(customPath.resolve(".hive-staging"))) {
                assertThat(operations.map(path -> path.getFileName().toString())).containsExactly("other-operation");
            }
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    void testCatalogSwitches()
    {
        for (String catalog : List.of("hive", "hive_legacy", "hive_master_disabled")) {
            String table = catalog + ".tpch.test_switch_" + randomNameSuffix();
            try {
                getDistributedQueryRunner().executeWithPlan(getSession(), "CREATE TABLE " + table + " AS SELECT 1 a");
                List<String> sources = renameSources();
                switch (catalog) {
                    case "hive" -> assertThat(sources).isNotEmpty().allMatch(source -> source.contains("/.hive-staging/"));
                    case "hive_legacy" -> assertThat(sources).isNotEmpty().allMatch(source -> source.startsWith("local:///tmp/presto-"));
                    case "hive_master_disabled" -> assertThat(sources).isEmpty();
                    default -> throw new IllegalArgumentException("Unexpected catalog: " + catalog);
                }
                assertUpdate("INSERT INTO " + table + " VALUES 2", 1);
                assertQuery("SELECT * FROM " + table, "VALUES 1, 2");
            }
            finally {
                assertUpdate("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    @Test
    void testWorkerExpressionFailureRollbackAndRetry()
    {
        String table = "test_local_failed_ctas_" + randomNameSuffix();
        Path target = getDistributedQueryRunner().getCoordinator().getBaseDataDir().resolve("hive_data/hive/tpch").resolve(table);
        try {
            // The cast depends on worker input, so it cannot fail during constant folding.
            assertQueryFails(
                    "CREATE TABLE " + table + " AS SELECT CAST(CAST(orderkey AS varchar) || 'invalid' AS bigint) a FROM tpch.tiny.orders",
                    ".*Cannot cast.*");
            assertThat(target).doesNotExist();
            assertUpdate("CREATE TABLE " + table + " AS SELECT 1 a", 1);
            assertQueryFails(
                    "INSERT INTO " + table + " SELECT CAST(CAST(orderkey AS varchar) || 'invalid' AS integer) FROM tpch.tiny.orders",
                    ".*Cannot cast.*");
            assertQuery("SELECT * FROM " + table, "VALUES 1");
            assertUpdate("INSERT INTO " + table + " VALUES 2", 1);
            assertQuery("SELECT * FROM " + table, "VALUES 1, 2");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    void testTransactionalCtasUnsupportedByFileMetastore()
    {
        // This harness cannot validate successful ACID execution; preserve its existing rejection.
        String table = "test_local_acid_" + randomNameSuffix();
        try {
            assertQueryFails("CREATE TABLE " + table + " WITH (transactional = true) AS SELECT 1 a", "FileHiveMetastore does not support ACID tables");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + table);
        }
    }

    private List<String> renameSources()
    {
        return getQueryRunner().getSpans().stream()
                .filter(span -> span.getName().equals("FileSystem.renameFile") || span.getName().equals("FileSystem.renameDirectory"))
                .map(span -> span.getAttributes().get(FILE_LOCATION))
                .toList();
    }

    @Test
    void testExternalCtas()
    {
        String table = "test_local_external_ctas_" + randomNameSuffix();
        try {
            getDistributedQueryRunner().executeWithPlan(getSession(), "CREATE TABLE " + table + " WITH (external_location = 'local:///" + table + "') AS SELECT 1 a");
            assertThat(renameSources()).isEmpty();
            assertQuery("SELECT * FROM " + table, "VALUES 1");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + table);
        }
    }
}
