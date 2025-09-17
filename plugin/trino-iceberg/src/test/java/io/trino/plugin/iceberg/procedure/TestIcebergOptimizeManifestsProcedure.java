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
package io.trino.plugin.iceberg.procedure;

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.IcebergTestUtils;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergOptimizeManifestsProcedure
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder().build();
        metastore = getHiveMetastore(queryRunner);
        fileSystemFactory = getFileSystemFactory(queryRunner);
        return queryRunner;
    }

    @Test
    void testOptimizeManifestWithNullPartitions()
    {
        try (TestTable table = newTrinoTable("test_optimize_null_partition", "(c1 int, c2 date, c3 double) WITH (partitioning = ARRAY['c1', 'month(c2)'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (null, date '2025-07-10', 0.1)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (null, date '2025-07-10', 0.2)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, null, 0.1)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, date '2025-10-10', 0)", 1);

            // all partitions are null
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (null, null, 0.3)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (null, null, 0.5)", 1);

            Set<String> manifestFiles = manifestFiles(table.getName());
            assertThat(manifestFiles).hasSize(6);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests");

            assertThat(manifestFiles(table.getName()))
                    .hasSize(1)
                    .doesNotContainAnyElementsOf(manifestFiles);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES " +
                            "(null, date '2025-07-10', double '0.1'), " +
                            "(null, date '2025-07-10', double '0.2'), " +
                            "(3, null, double '0.1'), " +
                            "(3, date '2025-10-10', double '0.0'), " +
                            "(null, null, double '0.3'), " +
                            "(null, null, double '0.5')");
        }
    }

    @Test
    void testOptimizeManifests()
    {
        try (TestTable table = newTrinoTable("test_optimize_manifests", "(x int)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);

            Set<String> manifestFiles = manifestFiles(table.getName());
            assertThat(manifestFiles).hasSize(2);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests");
            assertThat(manifestFiles(table.getName()))
                    .hasSize(1)
                    .doesNotContainAnyElementsOf(manifestFiles);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES 1, 2");
        }
    }

    @Test
    void testSplitManifests()
    {
        try (TestTable table = newTrinoTable("test_optimize_manifests", "(x int)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 3", 1);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests");
            assertThat(manifestFiles(table.getName()))
                    .hasSize(1);

            // Set small target size to force split
            BaseTable icebergTable = loadTable(table.getName());
            icebergTable.updateProperties()
                    .set("commit.manifest.target-size-bytes", "1")
                    .commit();
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests");

            assertThat(manifestFiles(table.getName()))
                    .hasSize(3);
        }
    }

    @Test
    void testPartitionTable()
    {
        try (TestTable table = newTrinoTable("test_partition", "(id int, part int) WITH (partitioning = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 10)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2, 10)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 20)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (4, 20)", 1);

            Set<String> manifestFiles = manifestFiles(table.getName());
            assertThat(manifestFiles).hasSize(4);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests");
            assertThat(manifestFiles(table.getName()))
                    .hasSize(1)
                    .doesNotContainAnyElementsOf(manifestFiles);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (1, 10), (2, 10), (3, 20), (4, 20)");
        }
    }

    @Test
    void testMultiplePartitioningColumns()
    {
        try (TestTable table = newTrinoTable("test_partition", "(id int, part int, nested int) WITH (partitioning = ARRAY['part', 'nested'])")) {
            for (int i = 0; i < 30; i++) {
                assertUpdate("INSERT INTO " + table.getName() + " VALUES (%d, %d, %d)".formatted(i, i % 10, i % 3), 1);
            }

            Set<String> manifestFiles = manifestFiles(table.getName());
            assertThat(manifestFiles).hasSize(30);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests");
            Set<String> currentManifestFiles = manifestFiles(table.getName());
            assertThat(currentManifestFiles)
                    .hasSize(1)
                    .doesNotContainAnyElementsOf(manifestFiles);

            assertThat(query("SELECT COUNT(*) FROM " + table.getName()))
                    .matches("VALUES BIGINT '30'");

            // Set small target size to force split
            BaseTable icebergTable = loadTable(table.getName());
            icebergTable.updateProperties()
                    .set("commit.manifest.target-size-bytes", "8000")
                    .commit();
            manifestFiles = currentManifestFiles;
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests");

            currentManifestFiles = manifestFiles(table.getName());
            assertThat(currentManifestFiles)
                    .hasSize(2)
                    .doesNotContainAnyElementsOf(manifestFiles);

            assertThat(query("SELECT COUNT(*) FROM " + table.getName()))
                    .matches("VALUES BIGINT '30'");
        }
    }

    @Test
    void testEmptyManifest()
    {
        try (TestTable table = newTrinoTable("test_no_rewrite", "(x int)")) {
            Set<String> manifestFiles = manifestFiles(table.getName());
            assertThat(manifestFiles).isEmpty();

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests");
            assertThat(manifestFiles(table.getName())).isEmpty();

            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());
        }
    }

    @Test
    void testNotRewriteSingleManifest()
    {
        try (TestTable table = newTrinoTable("test_no_rewrite", "(x int)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);

            Set<String> manifestFiles = manifestFiles(table.getName());
            assertThat(manifestFiles).hasSize(1);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests");
            assertThat(manifestFiles(table.getName()))
                    .hasSize(1)
                    .isEqualTo(manifestFiles);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES 1");
        }
    }

    @Test
    void testUnsupportedWhere()
    {
        try (TestTable table = newTrinoTable("test_unsupported_where", "WITH (partitioning = ARRAY['part']) AS SELECT 1 id, 1 part")) {
            assertQueryFails("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests WHERE id = 1", ".* WHERE not supported for procedure OPTIMIZE_MANIFESTS");
            assertQueryFails("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests WHERE part = 10", ".* WHERE not supported for procedure OPTIMIZE_MANIFESTS");
        }
    }

    private Set<String> manifestFiles(String tableName)
    {
        return computeActual("SELECT path FROM \"" + tableName + "$manifests\"").getOnlyColumnAsSet().stream()
                .map(path -> (String) path)
                .collect(toImmutableSet());
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "hive", "tpch");
    }
}
