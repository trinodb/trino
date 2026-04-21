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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.IcebergTestUtils;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getTrinoCatalog;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergOptimizeManifestsProcedure
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;
    private TrinoCatalog catalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build();
    }

    @BeforeAll
    public void setUp()
    {
        metastore = getHiveMetastore(getQueryRunner());
        fileSystemFactory = getFileSystemFactory(getQueryRunner());
        catalog = getTrinoCatalog(metastore, fileSystemFactory, "iceberg");
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
            assertUpdate("DELETE FROM " + table.getName() + " WHERE x = 1", 1);

            Set<String> manifestFiles = manifestFiles(table.getName());
            assertThat(manifestFiles).hasSize(3);

            // Delete manifest is left as-is, while the data manifests are combined
            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests",
                    "VALUES " +
                            "('rewritten_manifests_count', 2), " +
                            "('added_manifests_count', 1), " +
                            "('kept_manifests_count', 1), " +
                            "('processed_manifest_entries_count', 2)");
            Set<String> optimizedManifestFiles = manifestFiles(table.getName());
            assertThat(optimizedManifestFiles).hasSize(2);
            assertThat(Sets.intersection(optimizedManifestFiles, manifestFiles)).hasSize(1);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES 2");
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
            setManifestTargetSizeBytes(table.getName(), 1);
            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests",
                    "VALUES " +
                            "('rewritten_manifests_count', 1), " +
                            "('added_manifests_count', 3), " +
                            "('kept_manifests_count', 0), " +
                            "('processed_manifest_entries_count', 3)");

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

            assertUpdate(
                    "ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests",
                    "VALUES " +
                            "('rewritten_manifests_count', 4), " +
                            "('added_manifests_count', 1), " +
                            "('kept_manifests_count', 0), " +
                            "('processed_manifest_entries_count', 4)");
            assertThat(manifestFiles(table.getName()))
                    .hasSize(1)
                    .doesNotContainAnyElementsOf(manifestFiles);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (1, 10), (2, 10), (3, 20), (4, 20)");
        }
    }

    @Test
    void testWithBucketTransform()
    {
        // Bucket a string column so that source type is different from the partition type (integer)
        try (TestTable table = newTrinoTable(
                "test_bucket_transform",
                "(id int, category varchar) WITH (partitioning = ARRAY['bucket(category, 4)'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'alpha'), (2, 'delta')", 2);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 'beta'), (4, 'gamma')", 2);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (5, 'zeta'), (6, 'theta')", 2);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (7, 'beta'), (8, 'omega')", 2);

            Set<String> manifestFiles = manifestFiles(table.getName());
            assertThat(manifestFiles).hasSize(4);

            // Set small target size to allow multiple manifest files
            setManifestTargetSizeBytes(table.getName(), 16000);
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests");
            assertThat(manifestFiles(table.getName()))
                    .hasSize(2)
                    .doesNotContainAnyElementsOf(manifestFiles);

            // Verify clustering by bucket numbers
            List<PartitionSummary> summaries = partitionSummaries(table.getName());
            assertThat(summaries).hasSize(2);
            assertThat(summaries.get(0).lowerBound()).isEqualTo(0);
            assertThat(summaries.get(0).upperBound()).isEqualTo(1);
            assertThat(summaries.get(1).lowerBound()).isEqualTo(2);
            assertThat(summaries.get(1).upperBound()).isEqualTo(3);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES (1, 'alpha'), (2, 'delta'), (3, 'beta'), (4, 'gamma'), (5, 'zeta'), (6, 'theta'), (7, 'beta'), (8, 'omega')");
        }
    }

    @Test
    void testPartitionEvolutionIdentityToBucket()
    {
        try (TestTable table = newTrinoTable("test_partition_evolution", "(id int, category varchar)")) {
            // Insert data with identity partitioning on string column
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY['category']");
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'abc')", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2, 'def')", 1);

            // Change partitioning from identity to bucket transform on the same column
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY['bucket(category, 5)']");
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 'ijk')", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (4, 'lmn')", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (5, 'opqr')", 1);

            Set<String> manifestFiles = manifestFiles(table.getName());
            assertThat(manifestFiles).hasSize(4);

            // Set small target size to allow clustering of manifest files
            setManifestTargetSizeBytes(table.getName(), 16000);
            // Optimize manifests should work across different partition transforms
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests");
            assertThat(manifestFiles(table.getName()))
                    .hasSize(3)
                    .doesNotContainAnyElementsOf(manifestFiles);

            // Verify all data is preserved
            assertThat(query("SELECT * FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "(1, 'abc'), (2, 'def'), (3, 'ijk'), (4, 'lmn'), (5, 'opqr')");
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
            setManifestTargetSizeBytes(table.getName(), 8000);
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
    void testClusterByFirstPartitionField()
    {
        try (TestTable table = newTrinoTable("test_partition", "(id int, part int, nested int) WITH (partitioning = ARRAY['part', 'nested'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (0, 5, 100)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 10, 100)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2, 10, 200)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 20, 300)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (4, 20, 400)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (5, 25, 400)", 1);

            Set<String> manifestFiles = manifestFiles(table.getName());
            assertThat(manifestFiles).hasSize(6);

            // Set small target size to allow multiple manifest files
            setManifestTargetSizeBytes(table.getName(), 24000);
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE optimize_manifests");
            assertThat(manifestFiles(table.getName()))
                    .hasSize(2)
                    .doesNotContainAnyElementsOf(manifestFiles);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES (0, 5, 100), (1, 10, 100), (2, 10, 200), (3, 20, 300), (4, 20, 400), (5, 25, 400)");

            // Verify clustering by first partition field
            List<PartitionSummary> summaries = partitionSummaries(table.getName());
            assertThat(summaries).hasSize(2);
            assertThat(summaries.get(0).lowerBound()).isEqualTo(5);
            assertThat(summaries.get(0).upperBound()).isEqualTo(10);
            assertThat(summaries.get(1).lowerBound()).isEqualTo(20);
            assertThat(summaries.get(1).upperBound()).isEqualTo(25);
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
    void testNoSnapshot()
    {
        SchemaTableName tableName = new SchemaTableName("tpch", "test_no_snapshot" + randomNameSuffix());

        catalog.newCreateTableTransaction(
                        SESSION,
                        tableName,
                        new Schema(Types.NestedField.required(1, "x", Types.LongType.get())),
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        Optional.ofNullable(catalog.defaultTableLocation(SESSION, tableName)),
                        ImmutableMap.of())
                .commitTransaction();
        assertThat(catalog.loadTable(SESSION, tableName).currentSnapshot()).isNull();

        assertUpdate("ALTER TABLE " + tableName + " EXECUTE optimize_manifests");
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

    private List<PartitionSummary> partitionSummaries(String tableName)
    {
        return computeActual("SELECT CAST(partition_summaries[1].lower_bound AS INT), CAST(partition_summaries[1].upper_bound AS INT) FROM \"" + tableName + "$manifests\" ORDER BY 1")
                .getMaterializedRows().stream()
                .map(row -> new PartitionSummary((int) row.getField(0), (int) row.getField(1)))
                .collect(toImmutableList());
    }

    private record PartitionSummary(int lowerBound, int upperBound) {}

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "hive", "tpch");
    }

    private void setManifestTargetSizeBytes(String tableName, long targetSizeBytes)
    {
        BaseTable icebergTable = loadTable(tableName);
        icebergTable.updateProperties()
                .set("commit.manifest.target-size-bytes", String.valueOf(targetSizeBytes))
                .commit();
    }
}
